"""normalized_occupancy — the Normalized Occupancy (NO) metric (all teams, PySpark).

NO compares an agent's occupancy against the average occupancy of their
**district + shift** cohort that month:

    occupancy = SUM(occupancy_minutes) / SUM(required_minutes)   (per agent)
    NO        = occupancy / occupancy_benchmark

**Target ≥ 100%.** All four teams have NO (Core / Fraud / Social Media /
Content); Social Media occupancy comes from Sprinklr, already folded into the
raw table via ``sm_jobs`` (and only from the 2026-07-01 cutover on — see
``metrics_data/occupancy_time.py``).

Input
-----
``io_occupancy_time_raw`` (one row per agent per DIME slot), via
``metrics_data/occupancy_time.py``. Required columns: ``agent, xforce, xplead,
team, squad, district, shift, date, activity_type_required, required_minutes,
occupancy_minutes``.

The benchmark (matches legacy `[IO] Normalized Occupancy Dataset.sql`)
----------------------------------------------------------------------
Two-step, per **month**:

1. Per ``(month, district, shift, squad)``: ``SUM(occupancy_minutes) /
   SUM(required_minutes)`` — that squad-cohort's occupancy ratio.
2. Per ``(month, district, shift)``: the **average of the squad ratios** from
   step 1 (equal-weight across squads — the legacy ``AVG(occupancy_monthly)``
   over the squads sharing a district + shift).

Each slot carries its ``(month, district, shift)`` benchmark; a bucket's
benchmark is ``MAX`` over its slots (legacy ``MAX(occupancy_exp)`` at every
grain — a single-month bucket keeps that month's benchmark; a week straddling
a month boundary takes the HIGHER of the two month benchmarks).

**Deck exception (Content):** legacy's Content deck keys this benchmark on
``squad_district`` only — no shift. Content's roster carries a NULL shift, so we
force it onto one shift-agnostic key (``district``-only) rather than losing the
benchmark to the ``NULL != NULL`` equi-join. Every other deck stays on
``district + shift`` (a NULL-shift main-deck row keeps a NULL benchmark, exactly
as legacy leaves it). See :data:`CONTENT_BENCH_SHIFT`.

Output convention
-----------------
To keep the shared ``metric_value = numerator / denominator * 100`` contract,
``numerator`` is the agent's **occupancy %** and ``denominator`` is the
**benchmark %**, so ``metric_value`` is NO %.

Filters applied here (deferred by the raw layer)
------------------------------------------------
* Drop non-productive slots: ``activity_type_required`` in
  ``{lunch_break, time_off, shrinkage}`` (case-insensitive). The remaining slots
  feed both the agent occupancy and the benchmark.
* Manual approved adjustment from ``Ajustes Index``: suppress ``nitza.zarza``
  from the NO metric output for Apr-May 2026. Her slots still feed the
  district/shift benchmark, matching the legacy placement of this filter after
  benchmark construction.

NOT applied here (future Adjustments layer)
-------------------------------------------
* Per-agent vacation / outage-date exclusions (e.g. 2026-03-27, 2026-04-09) —
  wired via ``drop_slot_windows`` once the adjustment tables are populated.

Note on the fixed DIME filters
------------------------------
Legacy's ``dimensioned_activity`` meeting/leave carve-out and the DIME-squad
exclusion (wfm / credit_evolution / dote) are applied **upstream** as fixed DIME
filters in the raw layer (``metrics_data/occupancy_time.py`` → ``filter_dime``),
so — unlike before — they DO already constrain both the agent occupancy and the
peer benchmark here. ``social`` DIME slots are KEPT on all dates: Social-Media
occupancy is Sprinklr-sourced (``sm_jobs``) and intentionally ON for the whole
history, a documented divergence from legacy (see the raw layer's docstring).

Output — tidy long format, one row per (agent, date_reference, granularity)
---------------------------------------------------------------------------
``agent, xforce, xplead, team, squad, district, shift, date_reference,
date_granularity, metric, numerator, denominator, metric_value``.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from metric_utils import (
    DIM_COLS,
    GRANULARITIES,
    METRIC_COLUMNS,
    bucket_date,
    empty_metric_frame,
    latest_dims,
)
from adjustments.manual import drop_slot_windows, reclassify_dime_slots

METRIC_NAME = "normalized_occupancy"

NITZA_NO_SUPPRESSION_AGENT = "nitza.zarza"
NITZA_NO_SUPPRESSION_START: date = date(2026, 4, 1)
NITZA_NO_SUPPRESSION_END: date = date(2026, 5, 31)

# Legacy deck split for the NOcc benchmark grain: the main deck
# (`[IO] Normalized Occupancy Dataset.sql`) keys on district + shift, but the
# Content deck (`[IO] Performance 2026 - Content Temp Fix.sql`) keys on
# squad_district ONLY (no shift). Content's roster carries a NULL shift, so under
# the plain `shift = shift` equi-join its benchmark row never rejoins
# (NULL != NULL in Spark) — the exact bug that NULLed every Content NO. We force
# Content onto one shift-agnostic key so its benchmark is district-only (matching
# legacy Content), while every other deck stays on district + shift — including
# the NULL-shift rows the main deck ALSO leaves unmatched under `a.shift = b.shift`,
# preserving Core/Fraud parity.
CONTENT_TEAM = "content"
CONTENT_BENCH_SHIFT = "__content_all__"

# Same non-productive activity types excluded from adherence.
EXCLUDED_ACTIVITY_TYPES: tuple[str, ...] = (
    "lunch_break",
    "time_off",
    "shrinkage",
)


def _occupancy_benchmark(slots: DataFrame) -> DataFrame:
    """Per ``(month, district, _bench_shift)`` benchmark = mean of squad ratios.

    ``_bench_shift`` is ``shift`` for every deck except Content, which is forced
    onto a single shift-agnostic key (:data:`CONTENT_BENCH_SHIFT`) so its
    benchmark is district-only, matching legacy Content (see the constant).

    Step 1: per-``(month, district, _bench_shift, squad)`` ratio = SUM(occ)/SUM(req).
    Step 2: mean of those squad ratios per ``(month, district, _bench_shift)``.
    Both group steps keep NULL keys (a NULL-shift main-deck row keeps a NULL
    ``_bench_shift`` and — like legacy — never rejoins).
    """
    squad = slots.groupBy("month", "district", "_bench_shift", "squad").agg(
        F.sum("occupancy_minutes").alias("occ"),
        F.sum("required_minutes").alias("req"),
    )
    squad = squad.withColumn(
        "ratio",
        F.when(F.col("req") > 0, F.col("occ") / F.col("req")).otherwise(
            F.lit(None).cast("double")
        ),
    )
    bench = squad.groupBy("month", "district", "_bench_shift").agg(
        F.avg("ratio").alias("benchmark")
    )
    return bench


def _aggregate(slots: DataFrame, granularity: str) -> DataFrame:
    """One NO row per (agent, bucket) for a single granularity."""
    work = slots.withColumn(
        "date_reference", bucket_date(F.col("date"), granularity)
    )

    sums = work.groupBy("agent", "date_reference").agg(
        F.sum("occupancy_minutes").alias("_occ"),
        F.sum("required_minutes").alias("_req"),
        F.max("benchmark").alias("_bench"),
    )

    # Agent occupancy % and the bucket benchmark %. Legacy takes
    # MAX(occupancy_exp) over the bucket at EVERY grain ([IO] Performance
    # 2026.sql L695/713/739; the SM deck likewise) — identical to the single
    # month benchmark for day/month buckets, but for a week straddling a month
    # boundary it picks the HIGHER of the two month benchmarks. An earlier
    # required-minutes-weighted average here was an unflagged mis-port that
    # diverged on exactly those cross-month weeks (e.g. 2026-03-30, 2026-04-27).
    numerator = F.when(
        F.col("_req") > 0, F.col("_occ") / F.col("_req") * F.lit(100.0)
    ).otherwise(F.lit(None).cast("double"))
    denominator = F.when(
        F.col("_req") > 0, F.col("_bench") * F.lit(100.0)
    ).otherwise(F.lit(None).cast("double"))

    sums = sums.withColumn("numerator", numerator).withColumn(
        "denominator", denominator
    )
    sums = sums.withColumn(
        "metric_value",
        F.when(
            F.col("denominator") > 0,
            F.col("numerator") / F.col("denominator") * F.lit(100.0),
        ).otherwise(F.lit(None).cast("double")),
    )

    latest = latest_dims(work, order_col="date", dim_cols=DIM_COLS)
    out = (
        sums.join(latest, on=["agent", "date_reference"], how="left")
        .withColumn("date_granularity", F.lit(granularity))
        .withColumn("metric", F.lit(METRIC_NAME))
        .select(*METRIC_COLUMNS)
    )
    return out


def compute_normalized_occupancy(
    occupancy_time: DataFrame,
    *,
    general_exclusions: DataFrame | None = None,
    dime_inconsistencies: DataFrame | None = None,
) -> DataFrame:
    """Compute the Normalized Occupancy metric at all granularities.

    Args:
        occupancy_time: the ``io_occupancy_time_raw`` table (one row per slot).
        general_exclusions: ``adj_exclusiones_generales`` slot windows to drop
            (``None`` to skip).
        dime_inconsistencies: ``adj_inconsistencias_dime`` slot relabels
            (``None`` to skip).

    Returns:
        Tidy long-format metric rows (see module docstring). Empty input
        naturally yields an empty frame with the metric schema.
    """
    spark = occupancy_time.sparkSession

    work = reclassify_dime_slots(occupancy_time, dime_inconsistencies)
    work = drop_slot_windows(work, general_exclusions)

    productive = work.filter(
        ~F.lower(F.col("activity_type_required")).isin(
            list(EXCLUDED_ACTIVITY_TYPES)
        )
    ).withColumn("month", F.trunc(F.to_date(F.col("date")), "month"))

    # Benchmark key: Content is district-only (shift-agnostic); all other decks
    # keep district + shift. See CONTENT_BENCH_SHIFT.
    productive = productive.withColumn(
        "_bench_shift",
        F.when(
            F.col("team") == F.lit(CONTENT_TEAM), F.lit(CONTENT_BENCH_SHIFT)
        ).otherwise(F.col("shift")),
    )
    bench = _occupancy_benchmark(productive)
    productive = productive.join(
        bench, on=["month", "district", "_bench_shift"], how="left"
    ).drop("_bench_shift")

    # Approved manual adjustment, captured in the `Ajustes Index` tab:
    # nitza.zarza's NO is excluded from her metric output in Apr-May 2026, but
    # NOT from the peer benchmark that other agents are compared against — hence
    # this drop happens AFTER the benchmark join (her slots already fed it).
    cal = F.to_date(F.col("date"))
    suppress_nitza = (
        (F.col("agent") == F.lit(NITZA_NO_SUPPRESSION_AGENT))
        & (cal >= F.lit(NITZA_NO_SUPPRESSION_START))
        & (cal <= F.lit(NITZA_NO_SUPPRESSION_END))
    )
    productive = productive.filter(~suppress_nitza)

    parts = [_aggregate(productive, g) for g in GRANULARITIES]
    if not parts:
        return empty_metric_frame(spark)

    result = parts[0]
    for extra in parts[1:]:
        result = result.unionByName(extra)
    return result


IO_NORMALIZED_OCCUPANCY_METRIC_SCHEMA: tuple[tuple[str, str], ...] = (
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
