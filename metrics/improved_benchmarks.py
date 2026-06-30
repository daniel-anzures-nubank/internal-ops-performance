"""improved_benchmarks — the Improved Benchmarks metric (Core / Fraud only), PySpark.

Part of the **metrics layer**, but structurally different from the agent-grain
metrics: Improved Benchmarks is a **squad-level, district-level and XForce-level**
roll-up of *month-over-month benchmark improvements*. It answers "what share of
this squad's / district's / XForce's benchmarks improved vs. the previous month?".

    improved_benchmarks = COUNT(improved benchmarks) / COUNT(comparable benchmarks)

**Target >= 60%.** Output is **month grain only** (benchmarks are monthly; legacy
``improved_benchmark_final`` also emits the week grain, but the new pipeline only
materializes the month grain — the XForce roll-up that ``xforce_index`` consumes
is month-only).

Two benchmark families are compared (matches legacy
``[IO] Performance 2026.sql`` + ``[IO] Performance 2026 - S&D.sql``):

* **NTPJ benchmark** — per ``job_id`` (job type): the monthly
  ``exp_duration_job`` (cohort-wide expected seconds, with the NTPJ trailing-
  window rule). **"Improved" = benchmark <= previous month** (faster is better).
* **Occupancy benchmark** — per ``district + shift``: the monthly NO benchmark
  (mean of squad occupancy ratios). **"Improved" = benchmark >= previous month**
  (higher occupancy is better). Only from ``2026-03-01`` onward (the legacy
  occupancy source ``normalized_occupancy_final`` is filtered ``date >=
  '2026-03-01'`` — there is no February occupancy benchmark, so February cannot
  become March's previous-month comparator).

Both benchmarks are ``ROUND(..., 5)`` before the month-over-month LAG/compare
(legacy ``ntpj_benchmark_agg`` / ``occupancy_benchmark`` round to 5 decimals
before comparing to the previous month, so near-ties cannot flip).

Ties ("stayed the same") count as **improved**. A benchmark's first month (no
previous month to compare) is **not counted** (numerator or denominator).

Benchmark units carry the agent's ``(xforce, xplead, squad, district)`` directly
(legacy ``ntpj_benchmark`` / ``occupancy_benchmark`` carry squad/squad_district
per agent), so a single ``job_id`` can split across multiple squad/district rows.
They are then rolled up:

* ``improved_benchmark_squad``    — summed (COUNT DISTINCT job_id) per squad;
* ``improved_benchmark_district`` — summed per district;
* ``improved_benchmark_xforce``   — summed per ``(xforce, xplead)`` (legacy
  ``improved_benchmark``; squad/district NULL). This XForce roll-up is what the
  composite ``xforce_index`` metric consumes (it keys on the metric name
  ``improved_benchmark_xforce``).

XForce gating (legacy ``improved_benchmark_monthly`` / ``_weekly``)
-------------------------------------------------------------------
The XForce roll-up gates to ``date_reference < 2026-05-01`` (flat, for ALL
teams) PLUS ``NOT (xplead == 'david.fernandez' AND date_reference >=
2026-04-01)``. There is **no per-team Core/Fraud removal cutover** — non-david
Core April-2026 xforces survive (4-component for ``xforce_index``).

The squad / district roll-ups (legacy ``improved_benchmark_squad_monthly`` /
``_district_monthly``, S&D deck) have **NO month gate and NO team cutover** —
they emit every benchmark month present (pre-cutover).

ntpj_xforce LEFT-JOIN gating
----------------------------
Legacy ``improved_benchmark_final`` is driven by the ``ntpj_xforce`` output rows
(``FROM ntpj_xforces`` / ``internal_ops_performance_2026 WHERE
metric='ntpj_xforce'`` LEFT JOIN the benchmark units on ``month + xforce``), so a
benchmark unit for an ``(xforce, month)`` that has **no** ``ntpj_xforce`` output
row that month is dropped — this changes squad/district AND xforce denominators.
An ``ntpj_xforce`` row exists for an ``(xforce, month)`` iff at least one of that
xforce's agents produced an NTPJ agent row that month — i.e. a finished,
required-activity, **active-roster** job. We reproduce that gate from
``jobs_raw`` (no separate ``ntpj_xforce`` table is passed in).

Scope (per the SOT + product guidance)
---------------------------------------
* **Core / Fraud only** — Social Media and Content never had Improved Benchmarks.

Inputs
------
* ``io_jobs_raw`` (NTPJ benchmark) — needs a benchmark look-back before the
  output period (the build script reads ~6 extra months).
* ``io_occupancy_time_raw`` (occupancy benchmark) — needs ~2 extra months.

Output — tidy long format (squad / district / xforce rows)
-----------------------------------------------------------
``agent, xforce, xplead, team, squad, district, shift, date_reference,
date_granularity, metric, numerator, denominator, metric_value``. ``metric`` is
one of ``improved_benchmark_squad`` (squad set), ``improved_benchmark_district``
(district set), or ``improved_benchmark_xforce`` (``xforce`` + ``xplead`` set).
``agent`` and ``shift`` are always NULL; ``date_granularity`` is always
``month``. ``metric_value = numerator / denominator * 100`` (NULL if denominator 0).
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from metric_utils import METRIC_COLUMNS, empty_metric_frame
from ntpj import (
    ACTIVE_ROSTER_STATUS,
    FINISHED_STATUS,
    _expected_duration_by_month,
)
from adjustments.manual import (
    drop_cross_support_jobs,
    drop_excluded_jobs,
    drop_slot_windows,
    reclassify_dime_slots,
)

SQUAD_METRIC = "improved_benchmark_squad"
DISTRICT_METRIC = "improved_benchmark_district"
XFORCE_METRIC = "improved_benchmark_xforce"

# Improved Benchmarks only ever applied to Core and Fraud.
IMPROVED_BENCHMARKS_TEAMS: tuple[str, ...] = ("core", "fraud")

# XForce roll-up gating (legacy improved_benchmark_monthly / _weekly):
#  * flat upper bound for ALL teams;
#  * plus the david.fernandez Apr-2026 carve-out.
# NOTE: this gate applies ONLY to the xforce roll-up. The squad / district
# roll-ups have NO month gate and NO team cutover (legacy S&D deck).
XFORCE_REMOVAL_MONTH: date = date(2026, 5, 1)
DAVID_XPLEAD = "david.fernandez"
DAVID_REMOVAL_MONTH: date = date(2026, 4, 1)

# Occupancy benchmark only exists from this month. The legacy occupancy source
# (normalized_occupancy_final, filtered date >= '2026-03-01') has NO February
# data, so the earliest occupancy benchmark month is March 2026 — February must
# not become March's previous-month comparator via the LAG.
OCCUPANCY_BENCHMARK_START_MONTH: date = date(2026, 3, 1)

# Non-productive slots excluded from the occupancy benchmark (same as NO).
NO_EXCLUDED_ACTIVITY_TYPES: tuple[str, ...] = ("lunch_break", "time_off", "shrinkage")

# Decimals the legacy rounds the benchmark to before the LAG/compare.
BENCHMARK_ROUND_DECIMALS = 5

# The columns of a benchmark-unit frame (one row per benchmark key per
# (xforce, xplead, month, squad, district), with improved / counted flags).
_UNIT_COLS: tuple[str, ...] = (
    "key", "xforce", "xplead", "month", "team", "squad", "district",
    "improved", "counted",
)


def _empty_units(spark) -> DataFrame:
    schema = T.StructType(
        [
            T.StructField("key", T.StringType()),
            T.StructField("xforce", T.StringType()),
            T.StructField("xplead", T.StringType()),
            T.StructField("month", T.DateType()),
            T.StructField("team", T.StringType()),
            T.StructField("squad", T.StringType()),
            T.StructField("district", T.StringType()),
            T.StructField("improved", T.LongType()),
            T.StructField("counted", T.LongType()),
        ]
    )
    return spark.createDataFrame([], schema)


def _flag_improved(units: DataFrame, *, direction: str) -> DataFrame:
    """Add ``improved`` / ``counted`` via month-over-month LAG within (key, xforce).

    ``units`` carries ``key, xforce, xplead, month, team, squad, district,
    benchmark``. The benchmark is rounded to :data:`BENCHMARK_ROUND_DECIMALS`
    before the LAG/compare (legacy parity). Ties count as improved; the first
    month (NULL previous) is neither improved nor counted.
    """
    bench = F.round(F.col("benchmark"), BENCHMARK_ROUND_DECIMALS)
    # LAG partition is (key, xforce) ORDER BY month — matches legacy
    # `PARTITION BY job_id, xforce ORDER BY benchmark_month`. The rows carry
    # squad/district but the partition deliberately does not (legacy parity).
    w = Window.partitionBy("key", "xforce").orderBy("month")
    prev = F.lag(bench).over(w)

    if direction == "lower":
        improved = bench <= prev
    else:
        improved = bench >= prev

    counted = prev.isNotNull()
    return units.select(
        "key",
        "xforce",
        "xplead",
        "month",
        "team",
        "squad",
        "district",
        (improved & counted).cast("long").alias("improved"),
        counted.cast("long").alias("counted"),
    )


def _ntpj_benchmark_units(jobs: DataFrame) -> DataFrame:
    """Per ``(job_id, xforce, xplead, month, squad, district)`` NTPJ benchmark + flag."""
    spark = jobs.sparkSession
    finished = jobs.filter(
        F.lower(F.col("status")) == F.lit(FINISHED_STATUS)
    ).withColumn("month", F.trunc(F.to_date(F.col("date")), "month"))

    if len(finished.take(1)) == 0:
        return _empty_units(spark)

    # Benchmark from ALL finished jobs of that job_id, windowed by month (legacy
    # NTPJ trailing-window rule), reusing ntpj._expected_duration_by_month.
    monthly_totals = finished.groupBy("job_id", "month").agg(
        F.sum("duration_seconds").alias("tot_duration"),
        F.count(F.lit(1)).alias("tot_count"),
    )
    expected = _expected_duration_by_month(monthly_totals)  # job_id, month, exp_duration_job

    # The benchmark units (squad/district splitting): one row per
    # (job_id, xforce, xplead, month, squad, district), carrying the agent's
    # roster attribution. Legacy ntpj_benchmark joins normalized_time_per_job to
    # agent_information per-agent, then GROUP BY ALL averages exp_duration_job;
    # here every job already carries its agent's squad/district, so we take the
    # distinct attribution keys per (job_id, month) and attach the shared
    # per-job_id benchmark. The agent's contribution rows are required-activity
    # only (legacy required_hours IS NOT NULL).
    contrib = finished.filter(F.col("required_activity_on_day_flag") == F.lit(1))
    if len(contrib.take(1)) == 0:
        return _empty_units(spark)

    keys = contrib.select(
        F.col("job_id"),
        "xforce",
        "xplead",
        "month",
        F.lower(F.col("team")).alias("team"),
        "squad",
        "district",
    ).distinct()

    units = keys.join(
        expected.select("job_id", "month", "exp_duration_job"),
        on=["job_id", "month"],
        how="inner",
    ).select(
        F.col("job_id").alias("key"),
        "xforce",
        "xplead",
        "month",
        "team",
        "squad",
        "district",
        F.col("exp_duration_job").alias("benchmark"),
    )
    return _flag_improved(units, direction="lower")


def _occupancy_benchmark_units(occ: DataFrame) -> DataFrame:
    """Per ``(district-shift, xforce, xplead, month, squad, district)`` occupancy benchmark + flag."""
    spark = occ.sparkSession
    act = F.lower(F.col("activity_type_required"))
    productive = (
        occ.filter(~act.isin(list(NO_EXCLUDED_ACTIVITY_TYPES)))
        .withColumn("month", F.trunc(F.to_date(F.col("date")), "month"))
        .filter(F.col("month") >= F.lit(OCCUPANCY_BENCHMARK_START_MONTH))
    )
    if len(productive.take(1)) == 0:
        return _empty_units(spark)

    # NO benchmark: mean of squad occupancy ratios per (month, district, shift).
    squad = productive.groupBy("month", "district", "shift", "squad").agg(
        F.sum("occupancy_minutes").alias("occ"),
        F.sum("required_minutes").alias("req"),
    )
    squad = squad.withColumn(
        "ratio",
        F.when(F.col("req") > 0, F.col("occ") / F.col("req")).otherwise(
            F.lit(None).cast("double")
        ),
    )
    bench = squad.groupBy("month", "district", "shift").agg(
        F.avg("ratio").alias("benchmark")
    )

    # Benchmark units carry the agent's roster attribution (legacy occupancy_benchmark
    # carries squad/squad_district/shift per agent). The unit key is the slot's
    # district-shift; the benchmark value comes from the (month, district, shift) mean.
    keys = productive.select(
        F.concat_ws(" - ", F.col("district"), F.col("shift")).alias("key"),
        "xforce",
        "xplead",
        "month",
        F.lower(F.col("team")).alias("team"),
        "squad",
        "district",
        "shift",
    ).distinct()

    units = keys.join(
        bench, on=["month", "district", "shift"], how="inner"
    ).select(
        "key",
        "xforce",
        "xplead",
        "month",
        "team",
        "squad",
        "district",
        "benchmark",
    )
    return _flag_improved(units, direction="higher")


def _ntpj_xforce_months(jobs: DataFrame) -> DataFrame:
    """The ``(xforce, month)`` pairs that have an ``ntpj_xforce`` output row.

    An ``ntpj_xforce`` row exists for an ``(xforce, month)`` iff at least one of
    that xforce's agents produced an NTPJ agent row that month — i.e. a finished,
    required-activity, **active-roster** job. Legacy ``improved_benchmark_final``
    LEFT-JOINs the benchmark units onto these rows, dropping any unit for an
    ``(xforce, month)`` with no ``ntpj_xforce`` row (Fix #5).

    Returns a frame of distinct ``(xforce, month)``.
    """
    return (
        jobs.filter(
            (F.lower(F.col("status")) == F.lit(FINISHED_STATUS))
            & (F.col("required_activity_on_day_flag") == F.lit(1))
            & (F.lower(F.col("roster_status")) == F.lit(ACTIVE_ROSTER_STATUS))
        )
        .withColumn("month", F.trunc(F.to_date(F.col("date")), "month"))
        .select("xforce", "month")
        .distinct()
    )


def _rollup(units: DataFrame, *, level: str, metric_name: str) -> DataFrame:
    """Sum improved / counted per ``(<level>, month)`` into metric rows.

    ``level`` is ``squad`` / ``district`` (key sets that column; ``xforce`` /
    ``xplead`` / ``team`` NULL — legacy squad/district views carry no team) or
    ``xforce`` (sets ``xforce`` + ``xplead``, squad/district NULL — legacy
    ``improved_benchmark``).

    Legacy counts ``COUNT(DISTINCT job_id)``; the unit rows are already unique
    per ``(key, xforce, month, squad, district)``, but a job_id can appear under
    two xforces, so we count distinct ``(key, xforce)`` of the improved / counted
    units to stay exactly faithful.
    """
    # Count DISTINCT (key, xforce) of the improved / counted units (legacy
    # COUNT(DISTINCT job_id), but a job_id can recur across xforces so we pair it
    # with xforce). A NULL key (non-improved / non-counted) is ignored by
    # countDistinct, so the masked keys behave like the legacy CASE-WHEN.
    improved_key = F.when(F.col("improved") == F.lit(1), F.col("key"))
    counted_key = F.when(F.col("counted") == F.lit(1), F.col("key"))

    is_xforce = level == "xforce"
    if is_xforce:
        grp = units.groupBy("xforce", "xplead", "month").agg(
            F.countDistinct(improved_key, F.col("xforce")).alias("numerator"),
            F.countDistinct(counted_key, F.col("xforce")).alias("denominator"),
        )
    else:
        grp = (
            units.groupBy(level, "month")
            .agg(
                F.countDistinct(improved_key, F.col("xforce")).alias("numerator"),
                F.countDistinct(counted_key, F.col("xforce")).alias("denominator"),
            )
            .filter(F.col(level).isNotNull())
        )

    null_str = F.lit(None).cast("string")
    out = grp.select(
        F.lit(None).cast("string").alias("agent"),
        (F.col("xforce") if is_xforce else null_str).alias("xforce"),
        (F.col("xplead") if is_xforce else null_str).alias("xplead"),
        null_str.alias("team"),
        (F.col(level) if level == "squad" else null_str).alias("squad"),
        (F.col(level) if level == "district" else null_str).alias("district"),
        null_str.alias("shift"),
        F.col("month").alias("date_reference"),
        F.lit("month").alias("date_granularity"),
        F.lit(metric_name).alias("metric"),
        F.col("numerator").cast("double").alias("numerator"),
        F.col("denominator").cast("double").alias("denominator"),
    )
    out = out.withColumn(
        "metric_value",
        F.when(
            F.col("denominator") > 0,
            F.col("numerator") / F.col("denominator") * F.lit(100.0),
        ).otherwise(F.lit(None).cast("double")),
    )
    return out.select(*METRIC_COLUMNS)


def compute_improved_benchmarks(
    jobs_raw: DataFrame,
    occupancy_time: DataFrame,
    period_start: date,
    period_end: date,
    *,
    general_exclusions: DataFrame | None = None,
    dime_inconsistencies: DataFrame | None = None,
    cross_support: DataFrame | None = None,
    job_exclusions: DataFrame | None = None,
) -> DataFrame:
    """Compute Improved Benchmarks (squad + district + xforce, month grain, Core/Fraud).

    Args:
        jobs_raw: ``io_jobs_raw`` incl. a benchmark look-back before period_start.
        occupancy_time: ``io_occupancy_time_raw`` incl. a short look-back.
        period_start / period_end: inclusive output window (by month). Look-back
            rows are used only for benchmarks / the previous-month comparison.

    Returns:
        Tidy long-format metric rows (squad + district + xforce), month grain.
    """
    spark = jobs_raw.sparkSession

    jobs_empty = len(jobs_raw.take(1)) == 0
    occ_empty = len(occupancy_time.take(1)) == 0
    if jobs_empty and occ_empty:
        return empty_metric_frame(spark)

    # --- manual adjustments (mirror the pandas / NTPJ path) -----------------
    jobs_source = jobs_raw
    if not jobs_empty:
        jobs_source = jobs_raw.withColumn(
            "slot_time", F.date_format(F.col("start_time"), "HH:mm:ss")
        )
        jobs_source = drop_slot_windows(jobs_source, general_exclusions)
        jobs_source = drop_cross_support_jobs(jobs_source, cross_support)
        jobs_source = drop_excluded_jobs(jobs_source, job_exclusions)
        jobs_source = jobs_source.drop("slot_time")

    occ_source = occupancy_time
    if not occ_empty:
        occ_source = reclassify_dime_slots(occupancy_time, dime_inconsistencies)
        occ_source = drop_slot_windows(occ_source, general_exclusions)

    # --- Core / Fraud only ---------------------------------------------------
    teams = list(IMPROVED_BENCHMARKS_TEAMS)
    jobs = (
        jobs_source.filter(F.lower(F.col("team")).isin(teams))
        if not jobs_empty
        else jobs_source
    )
    occ = (
        occ_source.filter(F.lower(F.col("team")).isin(teams))
        if not occ_empty
        else occ_source
    )

    parts: list[DataFrame] = []
    if not jobs_empty:
        parts.append(_ntpj_benchmark_units(jobs))
    if not occ_empty:
        parts.append(_occupancy_benchmark_units(occ))
    parts = [p for p in parts if len(p.take(1)) > 0]
    if not parts:
        return empty_metric_frame(spark)

    units = parts[0]
    for extra in parts[1:]:
        units = units.unionByName(extra)

    # --- Fix #5: ntpj_xforce LEFT-JOIN gating --------------------------------
    # Drop benchmark units for an (xforce, month) with no ntpj_xforce output row
    # that month. The ntpj_xforce rows come from the NTPJ agent contribution
    # (finished + required-activity + active roster) — see _ntpj_xforce_months.
    if not jobs_empty:
        xforce_months = _ntpj_xforce_months(jobs)
        units = units.join(xforce_months, on=["xforce", "month"], how="left_semi")
    else:
        # No jobs => no ntpj_xforce rows => no benchmark units survive the gate.
        return empty_metric_frame(spark)

    # --- restrict to the OUTPUT period (look-back months drop out) ----------
    start_m = date(period_start.year, period_start.month, 1)
    end_m = date(period_end.year, period_end.month, 1)
    units = units.filter(
        (F.col("month") >= F.lit(start_m)) & (F.col("month") <= F.lit(end_m))
    )
    if len(units.take(1)) == 0:
        return empty_metric_frame(spark)

    units = units.persist()
    try:
        # Squad / district roll-ups: NO month gate, NO team cutover (Fix #1).
        squad_rows = _rollup(units, level="squad", metric_name=SQUAD_METRIC)
        district_rows = _rollup(units, level="district", metric_name=DISTRICT_METRIC)

        # XForce roll-up: flat < 2026-05 for all teams + david carve-out (Fix #2).
        xforce_units = units.filter(
            (F.col("month") < F.lit(XFORCE_REMOVAL_MONTH))
            & ~(
                (F.col("xplead") == F.lit(DAVID_XPLEAD))
                & (F.col("month") >= F.lit(DAVID_REMOVAL_MONTH))
            )
        )
        xforce_rows = _rollup(xforce_units, level="xforce", metric_name=XFORCE_METRIC)

        result = squad_rows.unionByName(district_rows).unionByName(xforce_rows)
        result = result.select(*METRIC_COLUMNS).persist()
        result.count()  # materialize before unpersisting `units`
    finally:
        units.unpersist()
    return result


IO_IMPROVED_BENCHMARKS_METRIC_SCHEMA: tuple[tuple[str, str], ...] = (
    ("agent", "STRING"),
    ("xforce", "STRING"),
    ("xplead", "STRING"),
    ("team", "STRING"),
    ("squad", "STRING"),
    ("district", "STRING"),
    ("shift", "STRING"),
    ("date_reference", "DATE"),
    ("date_granularity", "STRING"),
    ("metric", "STRING"),
    ("numerator", "DOUBLE"),
    ("denominator", "DOUBLE"),
    ("metric_value", "DOUBLE"),
)
