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
We gate on the ``ntpj_xforce`` metric (``io_ntpj_xforce_metric``, the month rows)
passed in as ``ntpj_xforce``. When it is not supplied we derive the **identical**
presence from ``normalized_time_per_job`` — an ``ntpj_xforce`` row exists for an
``(xforce, month)`` iff at least one of that xforce's agents has an NTPJ
contribution (a ``normalized_time_per_job`` row) that month. The gate is a
**no-op for NTPJ units** (their xforce is self-present); it only drops occupancy
units for xforces with no NTPJ row that month.

Scope (per the SOT + product guidance)
---------------------------------------
* **Core / Fraud only** — Social Media and Content never had Improved Benchmarks.

Inputs
------
* ``io_normalized_time_per_job`` (NTPJ benchmark + attribution) — legacy's
  ``normalized_time_per_job``, materialized from the NTPJ build. Needs **one
  previous month** before the output period for the month-over-month LAG (its
  own build carries the NTPJ trailing-window look-back for the benchmark values).
* ``io_occupancy_time_raw`` (occupancy benchmark) — needs ~2 extra months.
* ``io_ntpj_xforce_metric`` (the gate driver) — the month rows supply the
  ``(xforce, month)`` presence that survives the LEFT-JOIN gate.

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
from adjustments.manual import (
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


def _flag_improved(benchmarks: DataFrame, *, direction: str) -> DataFrame:
    """Per ``(key, month)`` ``improved`` / ``counted`` via month-over-month LAG.

    ``benchmarks`` carries ``key, month, benchmark`` — **one row per
    ``(key, month)``**. The benchmark is a property of the key (a job_id's
    expected duration / a district-shift's occupancy), NOT of the
    xforce/squad/district attribution, so the LAG partitions by ``key`` ONLY
    (legacy ``PARTITION BY job_id``/``district-shift``). The benchmark is rounded
    to :data:`BENCHMARK_ROUND_DECIMALS` before the compare. Ties count as
    improved; the first month (NULL previous) is neither improved nor counted.

    Returns ``key, month, improved, counted`` — the per-key flag that is then
    joined onto the xforce/squad/district attribution rows.
    """
    bench = F.round(F.col("benchmark"), BENCHMARK_ROUND_DECIMALS)
    w = Window.partitionBy("key").orderBy("month")
    prev = F.lag(bench).over(w)

    improved = (bench <= prev) if direction == "lower" else (bench >= prev)
    counted = prev.isNotNull()
    return benchmarks.select(
        "key",
        "month",
        (improved & counted).cast("long").alias("improved"),
        counted.cast("long").alias("counted"),
    )


def _ntpj_benchmark_units(ntpj: DataFrame) -> DataFrame:
    """NTPJ benchmark units from the ``normalized_time_per_job`` substrate.

    ``ntpj`` is legacy ``normalized_time_per_job``: one row per ``(agent, job_id,
    benchmark_month, xforce, xplead, team, squad, district)`` with the cohort-wide
    ``exp_duration_job``. This matches legacy exactly — the benchmark and its
    (xforce, xplead, squad, district) attribution come from the NTPJ dataset, not
    re-derived from raw jobs:

    * benchmark per ``(job_id, month)`` = ``AVG(exp_duration_job)`` (a per-job_id
      constant, so ``AVG`` = the value) — legacy ``ntpj_benchmark_agg``;
    * the improvement flag is computed once per job_id (``_flag_improved`` LAGs by
      the key), then joined onto the distinct ``(job_id, xforce, xplead, month,
      squad, district)`` attribution rows — so a job_id splits across every
      (squad, district) its agents worked (legacy ``ntpj_benchmark`` GROUP BY).

    Returns the :data:`_UNIT_COLS`.
    """
    spark = ntpj.sparkSession
    rows = ntpj.withColumn("month", F.col("benchmark_month"))
    if len(rows.take(1)) == 0:
        return _empty_units(spark)

    # Benchmark per (job_id, month): cohort-wide exp_duration_job, constant across
    # the attribution rows (legacy ntpj_benchmark_agg = ROUND(AVG(...),5); the
    # ROUND happens inside _flag_improved before the LAG/compare).
    benchmarks = rows.groupBy(
        F.col("job_id").alias("key"), "month"
    ).agg(F.avg("exp_duration_job").alias("benchmark"))
    flags = _flag_improved(benchmarks, direction="lower")  # key, month, improved, counted

    attribution = rows.select(
        F.col("job_id").alias("key"),
        "xforce",
        "xplead",
        "month",
        F.lower(F.col("team")).alias("team"),
        "squad",
        "district",
    ).distinct()

    return attribution.join(flags, on=["key", "month"], how="inner").select(*_UNIT_COLS)


def _occupancy_benchmark_units(occ: DataFrame) -> DataFrame:
    """Occupancy benchmark units: attribution rows + the per-district-shift flag.

    The benchmark is per ``(district, shift, month)`` (mean of the squad
    occupancy ratios). The improvement flag is computed at that grain (LAG by the
    ``district - shift`` key), then joined onto the distinct ``(key, xforce,
    xplead, month, squad, district)`` attribution rows. Returns :data:`_UNIT_COLS`.
    """
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
    benchmarks = (
        squad.groupBy("month", "district", "shift")
        .agg(F.avg("ratio").alias("benchmark"))
        .select(
            F.concat_ws(" - ", F.col("district"), F.col("shift")).alias("key"),
            "month",
            "benchmark",
        )
    )
    flags = _flag_improved(benchmarks, direction="higher")  # key, month, improved, counted

    # Attribution: distinct (district-shift key, xforce, xplead, month, squad,
    # district) over the productive slots (legacy occupancy_benchmark carries
    # squad/squad_district per agent).
    attribution = productive.select(
        F.concat_ws(" - ", F.col("district"), F.col("shift")).alias("key"),
        "xforce",
        "xplead",
        "month",
        F.lower(F.col("team")).alias("team"),
        "squad",
        "district",
    ).distinct()

    return attribution.join(flags, on=["key", "month"], how="inner").select(*_UNIT_COLS)


def _ntpj_xforce_months_from_metric(ntpj_xforce: DataFrame) -> DataFrame:
    """The ``(xforce, month)`` pairs present in the real ``ntpj_xforce`` metric.

    This is the **exact** gate driver (Fix #5): legacy ``improved_benchmark_final``
    is ``FROM ntpj_xforces`` (the month rows), so a benchmark unit survives only
    if its ``(xforce, month)`` has an ``ntpj_xforce`` output row. We take the
    month-grain rows of ``io_ntpj_xforce_metric`` — whose presence already
    reflects every NTPJ exclusion — and reduce to distinct ``(xforce, month)``
    (``date_reference`` is already the month start for month rows).

    Returns a frame of distinct ``(xforce, month)``.
    """
    return (
        ntpj_xforce.filter(F.col("date_granularity") == F.lit("month"))
        .select(
            F.col("xforce"),
            F.trunc(F.to_date(F.col("date_reference")), "month").alias("month"),
        )
        .distinct()
    )


def _ntpj_xforce_months_from_substrate(ntpj: DataFrame) -> DataFrame:
    """The ``(xforce, month)`` gate derived from ``normalized_time_per_job``.

    Equivalent to :func:`_ntpj_xforce_months_from_metric`: an ``ntpj_xforce`` row
    exists for an ``(xforce, month)`` iff at least one of that xforce's agents has
    an NTPJ contribution row that month — i.e. a ``normalized_time_per_job`` row.
    So the distinct ``(xforce, benchmark_month)`` of the substrate is exactly the
    monthly ``ntpj_xforce`` presence, and (unlike the separately-built metric) is
    guaranteed to cover the emitted window. Used when no explicit ``ntpj_xforce``
    is supplied. Note the gate is a **no-op for NTPJ units** (their xforce is
    self-present) — it only drops occupancy units for NTPJ-absent xforces.

    Returns a frame of distinct ``(xforce, month)``.
    """
    return ntpj.select(
        F.col("xforce"),
        F.col("benchmark_month").alias("month"),
    ).distinct()


# The unit columns NOT in a roll-up's grouping key — the tuple whose DISTINCT
# count gives that roll-up's numerator/denominator. Legacy counts
# COUNT(DISTINCT job_id) per (xforce, squad, squad_district) and sums into each
# roll-up, i.e. for any roll-up the count is the number of distinct benchmark
# units = distinct (key, xforce, squad, district) tuples within the group.
_ROLLUP_ID_COLS: dict[str, tuple[str, ...]] = {
    "xforce": ("key", "squad", "district"),    # within (xforce, xplead)
    "squad": ("key", "xforce", "district"),     # within squad
    "district": ("key", "xforce", "squad"),     # within district
}


def _masked_unit_id(cols: tuple[str, ...], when: Column) -> Column:
    """A null-safe string id of ``cols`` (NULL outside ``when``) for countDistinct."""
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("∅")) for c in cols]
    return F.when(when, F.concat_ws("", *parts))


def _rollup(units: DataFrame, *, level: str, metric_name: str) -> DataFrame:
    """Count distinct benchmark units per ``(<level>, month)`` into metric rows.

    ``level`` is ``squad`` / ``district`` (key sets that column; ``xforce`` /
    ``xplead`` / ``team`` NULL — legacy squad/district views carry no team) or
    ``xforce`` (sets ``xforce`` + ``xplead``, squad/district NULL — legacy
    ``improved_benchmark``).

    Legacy improved_benchmark counts ``COUNT(DISTINCT job_id)`` per ``(xforce,
    squad, squad_district)`` and sums into the roll-up, so the numerator /
    denominator is the number of distinct benchmark units — distinct
    ``(key, xforce, squad, district)`` tuples — within the group (verified
    against legacy: xforce den 66 ≈ distinct (job_id, squad, district); squad /
    district den sums 1035 ≈ distinct (job_id, xforce, district) / (job_id,
    xforce, squad)).
    """
    id_cols = _ROLLUP_ID_COLS[level]
    numerator = F.countDistinct(
        _masked_unit_id(id_cols, F.col("improved") == F.lit(1))
    ).alias("numerator")
    denominator = F.countDistinct(
        _masked_unit_id(id_cols, F.col("counted") == F.lit(1))
    ).alias("denominator")

    is_xforce = level == "xforce"
    if is_xforce:
        grp = units.groupBy("xforce", "xplead", "month").agg(numerator, denominator)
    else:
        grp = (
            units.groupBy(level, "month")
            .agg(numerator, denominator)
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
    normalized_time_per_job: DataFrame,
    occupancy_time: DataFrame,
    period_start: date,
    period_end: date,
    *,
    ntpj_xforce: DataFrame | None = None,
    general_exclusions: DataFrame | None = None,
    dime_inconsistencies: DataFrame | None = None,
) -> DataFrame:
    """Compute Improved Benchmarks (squad + district + xforce, month grain, Core/Fraud).

    Args:
        normalized_time_per_job: the NTPJ benchmark substrate (legacy
            ``normalized_time_per_job`` / ``io_normalized_time_per_job``), incl.
            **one previous month** before ``period_start`` for the month-over-month
            LAG. Its manual adjustments were applied upstream (in the NTPJ build),
            so none are re-applied here.
        occupancy_time: ``io_occupancy_time_raw`` incl. a short look-back.
        period_start / period_end: inclusive output window (by month). Look-back
            rows are used only for benchmarks / the previous-month comparison.
        ntpj_xforce: ``io_ntpj_xforce_metric`` rows — the gate driver (Fix #5),
            legacy ``FROM ntpj_xforces``. Its month rows supply the
            ``(xforce, month)`` presence a benchmark unit must have to survive.
            If ``None``, the gate derives the identical presence from
            ``normalized_time_per_job`` (which always covers the emitted window).
        general_exclusions: ``adj_exclusiones_generales`` slot/date windows —
            applied to the **occupancy** source only (the NTPJ side is already
            adjusted upstream).
        dime_inconsistencies: ``adj_inconsistencias_dime`` — occupancy DIME
            reclassification.

    Returns:
        Tidy long-format metric rows (squad + district + xforce), month grain.
    """
    spark = normalized_time_per_job.sparkSession

    ntpj_empty = len(normalized_time_per_job.take(1)) == 0
    occ_empty = len(occupancy_time.take(1)) == 0
    if ntpj_empty and occ_empty:
        return empty_metric_frame(spark)

    # --- occupancy manual adjustments (the NTPJ side is adjusted upstream) ---
    occ_source = occupancy_time
    if not occ_empty:
        occ_source = reclassify_dime_slots(occupancy_time, dime_inconsistencies)
        occ_source = drop_slot_windows(occ_source, general_exclusions)

    # --- Core / Fraud only ---------------------------------------------------
    # Filtering the substrate to Core/Fraud keeps the cohort-wide exp_duration_job
    # (baked in upstream over ALL teams) while restricting the attribution units.
    teams = list(IMPROVED_BENCHMARKS_TEAMS)
    ntpj = (
        normalized_time_per_job.filter(F.lower(F.col("team")).isin(teams))
        if not ntpj_empty
        else normalized_time_per_job
    )
    occ = (
        occ_source.filter(F.lower(F.col("team")).isin(teams))
        if not occ_empty
        else occ_source
    )

    parts: list[DataFrame] = []
    if not ntpj_empty:
        parts.append(_ntpj_benchmark_units(ntpj))
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
    # that month. Prefer the explicit ntpj_xforce metric (legacy FROM ntpj_xforces);
    # otherwise derive the identical presence from the substrate. The gate is a
    # no-op for NTPJ units (self-present) — it only drops NTPJ-absent occupancy
    # units.
    if ntpj_xforce is not None:
        xforce_months = _ntpj_xforce_months_from_metric(ntpj_xforce)
    elif not ntpj_empty:
        xforce_months = _ntpj_xforce_months_from_substrate(ntpj)
    else:
        # No NTPJ substrate and no ntpj_xforce => no gate rows => nothing survives.
        return empty_metric_frame(spark)
    units = units.join(xforce_months, on=["xforce", "month"], how="left_semi")

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
