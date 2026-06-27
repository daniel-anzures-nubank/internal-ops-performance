"""ntpj — Normalized Time Per Job (Core / Fraud / Content), PySpark.

NTPJ compares the time an agent spends on jobs against a monthly expected-time
benchmark:

    ntpj = SUM(actual job duration) / SUM(exp_duration_job * job_count)

per agent per period. **Target ≤ 100%** (lower = faster than benchmark).

Social Media has **no NTPJ** — social jobs aren't in the shuffle/OOS sources, so
social agents simply have no rows in the input table.

Input
-----
``io_jobs_raw`` (one row per job), via ``metrics_data/jobs_raw.py``. Required
columns: ``agent, xforce, xplead, team, squad, district, shift, roster_status,
date, job_id, activity_type, status, duration_seconds,
required_activity_on_day_flag``.

> The input must include a **benchmark look-back** before the output period (the
> build script reads ~4 extra months), because a month's benchmark can use a
> trailing window — see below.

How the benchmark works (matches legacy `[IO] NTPJ Dataset.sql`)
---------------------------------------------------------------
* ``exp_duration_job(job_id, month)`` = ``SUM(duration) / SUM(count)`` across
  **all finished jobs** of that ``job_id`` (every agent — including non-active
  roster agents), over a month window:
    - target month **≤ 2026-03** → trailing window ``[M-4 … M]`` (5 calendar
      months inclusive — the legacy "4-month window");
    - target month **≥ 2026-04** → the current month only.
* The benchmark is computed from **all finished jobs** (no required-day filter,
  no ``status='active'`` roster filter — legacy's ``expected_duration_per_job_ntpj``
  self-joins the un-roster-filtered ``jobs_base_ntpj``). The agent's own
  contribution rows are restricted to required activities AND active roster.

Filters applied here
--------------------
* Finished jobs only (``status == 'finished'``; OOS jobs are synthesized as
  ``finished`` in the raw table). Applies to BOTH the benchmark and the
  contribution.
* Agent contribution rows additionally require:
    - ``required_activity_on_day_flag == 1`` (the agent was scheduled for that
      job's ``activity_type`` that day — legacy "required_hours IS NOT NULL").
    - ``roster_status == 'active'`` (legacy applies the active filter only to
      the contribution, not the benchmark — see ``ntpj_all_info_2025/2026``).
* Outage-date drop: ``date NOT IN (2026-03-27, 2026-04-09)`` — **contribution
  only**. Legacy's ``expected_duration_per_job_ntpj`` self-join filters the
  outage dates on the **target (`a`) side only**; the benchmark value averages
  over the **`b` side, which is never outage-filtered**, so outage-day jobs
  still feed the per-job_id benchmark pool. ``ntpj_calculations`` then drops the
  outage dates from the emitted contribution. We reproduce this asymmetry —
  keep outage days in the benchmark, drop them from the contribution.
* Hardcoded per-agent date exclusions (vacation / leave / holiday / license /
  days off) — un-ported legacy hardcodes reproduced from ``dime_ntpj`` /
  ``manual_adjustments_ntpj`` / ``ntpj_all_info_2026``. Contribution-only (the
  agent's jobs on those dates leave the metric). See
  :data:`HARDCODED_AGENT_DATE_EXCLUSIONS`.
* Manual adjustments (when the ``adj_*`` tables are present): outage / cross-
  support / job exclusions via ``drop_slot_windows`` / ``drop_cross_support_jobs``
  / ``drop_excluded_jobs``, applied to the adjusted frame BEFORE the benchmark
  groupby so they leave both the benchmark and the contribution (matching legacy,
  whose ``manual_adjustments_ntpj`` / ``manual_queue_exclusions_ntpj`` are applied
  before the benchmark self-join).

Legacy ``ntpj_base`` semantics (no-benchmark rows are kept)
----------------------------------------------------------
Legacy LEFT-joins ``exp_duration_job`` and keeps a contribution row even when
its ``job_id`` has no benchmark in the window (NULL ``exp_duration_job`` →
NULL ``total_exp_duration``); only ``required_hours IS NOT NULL`` filters rows.
We reproduce that: the benchmark is LEFT-joined, a row with no window keeps a
NULL ``expected_seconds``, and the per-(agent, date) SUM skips that NULL in the
denominator (Spark ``SUM`` ignores NULLs) while still counting the actual
duration in the numerator.

Output — tidy long format, one row per (agent, date_reference, granularity)
---------------------------------------------------------------------------
``agent, xforce, xplead, team, squad, district, shift, date_reference,
date_granularity, metric, numerator, denominator, metric_value`` where
``numerator`` = actual job seconds, ``denominator`` = expected job seconds,
``metric_value`` = ``numerator / denominator * 100`` (NULL if denominator 0).
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from metric_utils import aggregate_long, empty_metric_frame
from adjustments.manual import (
    drop_cross_support_jobs,
    drop_excluded_jobs,
    drop_reassignment_jobs,
    drop_slot_windows,
)

METRIC_NAME = "ntpj"

FINISHED_STATUS = "finished"
ACTIVE_ROSTER_STATUS = "active"

# Month at/after which the benchmark uses the current month only; earlier months
# use the trailing window.
BENCHMARK_CUTOVER_MONTH: date = date(2026, 4, 1)

# Trailing-window length for pre-cutover months: months b with M-4 <= b <= M
# (5 calendar months inclusive — the legacy "4-month window").
TRAILING_WINDOW_MONTHS = 4

# Legacy whole-pipeline outage-date drops (general access problems). Dropped for
# ALL agents from the CONTRIBUTION only — they remain in the benchmark pool
# (legacy's expected_duration_per_job_ntpj filters only the self-join target
# side). See compute_ntpj.
OUTAGE_DATES: tuple[date, ...] = (date(2026, 3, 27), date(2026, 4, 9))


# ---------------------------------------------------------------------------
# Hardcoded per-agent NTPJ exclusions (UN-PORTED LEGACY HARDCODES)
# ---------------------------------------------------------------------------
#
# Source / legacy parity: ``legacy/[IO] NTPJ Dataset.sql`` hardcodes per-agent
# vacation / leave / holiday / license / day-off date exclusions that were never
# moved to the ``adj_*`` adjustment sheets. They appear in three places that all
# converge on the SAME net effect — the agent's jobs on those dates leave the
# metric:
#   * ``dime_ntpj``                (lines 523-538) — drops the DIME required-set,
#                                   so ``required_hours IS NOT NULL`` removes the
#                                   contribution row;
#   * ``manual_adjustments_ntpj``  (lines 201-223) — ``exclude = TRUE`` CASE;
#   * ``ntpj_all_info_2026``       (lines 612-627) — final ``NOT (...)`` filters.
#
# We reproduce the net effect with a single contribution-side filter (the
# contribution is already scoped to active roster + required-activity flag),
# mirroring the existing NTPJ hardcode precedent (``nitza.zarza`` /
# ``luis.contreras`` in the occupancy modules). These are documented legacy facts
# applied UNCONDITIONALLY in legacy (no 2026-07-01 cutover gate), so we apply them
# as-is — they are pre-cutover historical days off.
#
# TODO: these are un-ported legacy hardcodes; they should eventually move to the
# ``adj_exclusiones_generales`` adjustment sheet so this list can be deleted.
#
# Each entry is ``(agent, start_date_inclusive, end_date_inclusive)``.
HARDCODED_AGENT_DATE_EXCLUSIONS: tuple[tuple[str, date, date], ...] = (
    # -- maternity leave --
    ("maria.reyes", date(2026, 2, 1), date(2026, 2, 28)),
    # -- vacation --
    ("tania.enciso", date(2026, 5, 8), date(2026, 5, 9)),
    ("yerck.tellez", date(2026, 3, 3), date(2026, 3, 3)),
    ("yerck.tellez", date(2026, 4, 28), date(2026, 4, 28)),
    ("gabriela.vega", date(2026, 5, 12), date(2026, 5, 12)),
    ("dulce.rivera", date(2026, 3, 29), date(2026, 3, 29)),
    ("nadia.tovias", date(2026, 4, 23), date(2026, 4, 23)),
    ("rodrigo.padilla", date(2026, 3, 10), date(2026, 3, 10)),
    ("israel.cadena", date(2026, 3, 19), date(2026, 3, 19)),
    ("uriel.alfaro", date(2026, 4, 6), date(2026, 4, 7)),
    ("uriel.alfaro", date(2026, 5, 13), date(2026, 5, 15)),
    ("yuridia.agama", date(2026, 5, 11), date(2026, 5, 14)),
    ("alexis.torres", date(2026, 5, 21), date(2026, 5, 22)),
    ("lucia.espinosa", date(2026, 4, 11), date(2026, 4, 11)),
    ("adriana.lopez", date(2026, 5, 14), date(2026, 5, 14)),
    # -- day controls (2026-03-24 .. 2026-03-28) --
    ("jose.velez", date(2026, 3, 24), date(2026, 3, 28)),
    ("carlos.gonzalez", date(2026, 3, 24), date(2026, 3, 28)),
    ("jorge.ortega", date(2026, 3, 24), date(2026, 3, 28)),
    ("luisa.castaneda", date(2026, 3, 24), date(2026, 3, 28)),
    ("janet.castro", date(2026, 3, 24), date(2026, 3, 28)),
    ("karen.ortega", date(2026, 3, 24), date(2026, 3, 28)),
    # -- one-off manual exclusion --
    ("jonathan.pineda", date(2026, 2, 26), date(2026, 2, 26)),
    # NOTE: the ``manual_adjustments_ntpj`` whole-day exclusions (jefferson.nunes /
    # patricia.gomez / carmina.venegas / cecilia.ortiz / federico.gaona /
    # ignacio.herbert / marcos.caudillo / maria.castillo / evelyn.macedo /
    # jorge.delgado / claudia.brigada / omar.morales / luis.delgadillo) used to live
    # here too, but have been MOVED to the ``Reasignaciones DIME`` adjustment sheet
    # (blank ``actividad_dimensionada`` = whole-day) so they now feed BOTH the
    # benchmark and the contribution (matching legacy ``manual_adjustments_ntpj``).
    # The entries that REMAIN above come from ``dime_ntpj`` (lines 523-538) — a
    # separate legacy mechanism that drops the DIME required-set, i.e. the
    # contribution only — so they stay hardcoded here.
)


def _hardcoded_exclusion_mask(df: DataFrame) -> "F.Column":
    """OR-mask matching any ``(agent, date)`` in :data:`HARDCODED_AGENT_DATE_EXCLUSIONS`."""
    cal = F.to_date(F.col("date"))
    agent = F.lower(F.col("agent"))
    mask = F.lit(False)
    for name, start, end in HARDCODED_AGENT_DATE_EXCLUSIONS:
        mask = mask | (
            (agent == F.lit(name))
            & (cal >= F.lit(start))
            & (cal <= F.lit(end))
        )
    return mask


def _expected_duration_by_month(monthly_totals: DataFrame) -> DataFrame:
    """Per ``(job_id, target_month)`` expected duration from windowed monthly totals.

    ``monthly_totals`` has columns ``job_id, month (DATE month-start),
    tot_duration, tot_count``. For each target month we sum the per-month
    totals over the benchmark window for that target, then divide. The window
    is expressed as a self-join on ``month``:

      * target ``>= BENCHMARK_CUTOVER_MONTH`` → only ``b.month == target``;
      * target ``<  BENCHMARK_CUTOVER_MONTH`` → ``target-4mo <= b.month <= target``.

    Returns ``job_id, month (target), exp_duration_job``.
    """
    a = monthly_totals.select(F.col("month").alias("target_month")).distinct()
    b = monthly_totals.select(
        F.col("job_id"),
        F.col("month").alias("src_month"),
        F.col("tot_duration"),
        F.col("tot_count"),
    )

    cutover = F.lit(BENCHMARK_CUTOVER_MONTH)
    window_ok = F.when(
        F.col("target_month") >= cutover,
        F.col("src_month") == F.col("target_month"),
    ).otherwise(
        (F.col("src_month") <= F.col("target_month"))
        & (
            F.col("src_month")
            >= F.add_months(F.col("target_month"), -TRAILING_WINDOW_MONTHS)
        )
    )

    joined = a.crossJoin(b).filter(window_ok)
    agg = joined.groupBy("job_id", "target_month").agg(
        F.sum("tot_duration").alias("tot_duration"),
        F.sum("tot_count").alias("tot_count"),
    )
    return agg.select(
        F.col("job_id"),
        F.col("target_month").alias("month"),
        F.when(
            F.col("tot_count") > 0,
            F.col("tot_duration") / F.col("tot_count"),
        )
        .otherwise(F.lit(None).cast("double"))
        .alias("exp_duration_job"),
    )


def compute_ntpj(
    jobs_raw: DataFrame,
    period_start: date,
    period_end: date,
    *,
    general_exclusions: DataFrame | None = None,
    cross_support: DataFrame | None = None,
    job_exclusions: DataFrame | None = None,
    reassignments: DataFrame | None = None,
) -> DataFrame:
    """Compute the NTPJ metric at all granularities.

    Args:
        jobs_raw: the ``io_jobs_raw`` table, including the benchmark look-back
            months before ``period_start``.
        period_start / period_end: inclusive output window (rows outside are
            used only for benchmarks, not emitted).
        general_exclusions: ``adj_exclusiones_generales`` slot/date windows to
            drop (``None`` to skip).
        cross_support: ``adj_cross_support`` queue exclusions (``None`` to skip).
        job_exclusions: ``adj_exclusiones_jobs`` job exclusions (``None`` to skip).
        reassignments: ``adj_reasignaciones_dime`` DIME-activity reassignment
            exclusions — agents pulled onto a BKO task force whose jobs during the
            reassigned ``dimensioned_activity`` (blank = whole-day) leave both the
            benchmark and the contribution. Reproduces legacy
            ``manual_adjustments_ntpj`` (``None`` to skip).

    Returns:
        Tidy long-format metric rows (see module docstring).
    """
    spark = jobs_raw.sparkSession

    # Manual adjustments first (before the benchmark groupby), so an excluded
    # job leaves BOTH the benchmark and the contribution (matches legacy, which
    # applies manual_adjustments_ntpj / manual_queue_exclusions_ntpj before the
    # expected-duration self-join). The slot-window matcher needs a slot_time.
    adjusted = jobs_raw.withColumn(
        "slot_time", F.date_format(F.col("start_time"), "HH:mm:ss")
    )
    adjusted = drop_slot_windows(adjusted, general_exclusions)
    adjusted = drop_cross_support_jobs(adjusted, cross_support)
    adjusted = drop_excluded_jobs(adjusted, job_exclusions)
    # DIME-activity reassignments (matches legacy manual_adjustments_ntpj). Uses
    # the per-job ``dimensioned_activity`` attached in jobs_raw; applied here so
    # excluded jobs leave both the benchmark and the contribution.
    adjusted = drop_reassignment_jobs(adjusted, reassignments)
    adjusted = adjusted.drop("slot_time")

    # Finished only — applies to both the benchmark and the contribution.
    # NOTE: the outage-date drop is deliberately NOT applied here (it would
    # remove 2026-04-09 from the benchmark pool too). Legacy keeps outage dates
    # in the benchmark self-join's `b` side and drops them only from the
    # contribution — see the contribution filter below.
    finished = adjusted.filter(
        F.lower(F.col("status")) == F.lit(FINISHED_STATUS)
    ).withColumn("month", F.trunc(F.to_date(F.col("date")), "month"))

    # --- benchmark: from ALL finished jobs, windowed by month ---------------
    monthly_totals = finished.groupBy("job_id", "month").agg(
        F.sum("duration_seconds").alias("tot_duration"),
        F.count(F.lit(1)).alias("tot_count"),
    )
    expected = _expected_duration_by_month(monthly_totals)

    # --- agent contribution: required activities + active roster only -------
    # The outage-date drop (2026-03-27 / 2026-04-09) is CONTRIBUTION-ONLY:
    # legacy filters outage dates from the target/contribution side
    # (`ntpj_calculations`) but NOT from the benchmark self-join's `b` side
    # (`expected_duration_per_job_ntpj` filters only `a`), so outage-day jobs
    # still feed the per-job_id benchmark pool. Reproducing that asymmetry is
    # what makes April's benchmark match legacy (April is the only month whose
    # current-month benchmark contains an outage date).
    contrib = finished.filter(
        (F.col("required_activity_on_day_flag") == F.lit(1))
        & (F.lower(F.col("roster_status")) == F.lit(ACTIVE_ROSTER_STATUS))
        & (~F.to_date(F.col("date")).isin(list(OUTAGE_DATES)))
    )

    # Un-ported legacy hardcodes: drop specific agent-days (vacation / leave /
    # holiday / license / days off). Contribution-only — legacy applies these via
    # dime_ntpj / manual_adjustments_ntpj / ntpj_all_info_2026, whose net effect is
    # to remove the agent's jobs on those dates. See
    # HARDCODED_AGENT_DATE_EXCLUSIONS.
    contrib = contrib.filter(~_hardcoded_exclusion_mask(contrib))

    base = contrib.groupBy(
        "agent",
        "xforce",
        "xplead",
        "team",
        "squad",
        "district",
        "shift",
        "date",
        "month",
        "job_id",
    ).agg(
        F.count(F.lit(1)).alias("count"),
        F.sum("duration_seconds").alias("actual_seconds"),
    )

    # LEFT-join the benchmark: legacy ntpj_base keeps a row even when its job_id
    # has no benchmark window (NULL exp_duration_job). The SUM over the
    # (agent, date) bucket skips that NULL in the denominator while still
    # counting actual_seconds in the numerator.
    base = base.join(expected, on=["job_id", "month"], how="left")
    base = base.withColumn(
        "expected_seconds", F.col("exp_duration_job") * F.col("count")
    )

    # --- restrict OUTPUT to the requested period (look-back rows drop out) ---
    cal_base = F.to_date(F.col("date"))
    base = base.filter(
        (cal_base >= F.lit(period_start)) & (cal_base <= F.lit(period_end))
    )

    if len(base.take(1)) == 0:
        return empty_metric_frame(spark)

    return aggregate_long(
        base,
        numerator_col="actual_seconds",
        denominator_col="expected_seconds",
        metric_name=METRIC_NAME,
    )


IO_NTPJ_METRIC_SCHEMA: tuple[tuple[str, str], ...] = (
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
