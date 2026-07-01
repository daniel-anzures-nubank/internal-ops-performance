"""content_sla_ntpj — Content NTPJ as SLA-weighted compliance (PySpark).

Content's NTPJ is **not** the duration ``actual/expected`` ratio that Core/Fraud
use — it is a **jobs-within-SLA compliance** metric (higher-is-better, bounded
≤100). Legacy calls it ``ntpj_sla_old`` but ships it as ``metric='ntpj_agent'``
for standardization; we emit it as ``metric='ntpj'`` so ``io_ntpj_metric`` stays a
single standardized table (``build_ntpj.py`` unions these Content rows in place of
the duration rows).

    ntpj (Content) = SUM(sla_seconds of on-time jobs) / SUM(sla_seconds) * 100

Input
-----
``io_jobs_within_sla_raw`` (one row per Content OOS job; ``metrics_data/jobs_within_sla.py``),
already scoped to Content agents and to ``date >= 2025-12-01`` minus the outage
dates, and carrying ``sla_met_seconds`` / ``sla_seconds`` + ``roster_status``.

Filters applied here
--------------------
* ``roster_status == 'active'`` (carried by the raw table, applied here — matching
  the ``jobs_raw`` → ``ntpj`` split). The date scoping was applied upstream in the
  raw layer (before the ``content_id`` grouping, for ``2026-03-10``-boundary parity).
* Restrict the output to ``[period_start, period_end]``.

Output — tidy long, one row per (agent, date_reference, granularity), ``metric='ntpj'``.
Bounded ≤100 (higher-is-better) — the composite ``xpeer_index`` adds Content NTPJ
**raw** (not folded around 100), matching legacy's Content deck.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from metric_utils import aggregate_long, empty_metric_frame

METRIC_NAME = "ntpj"
ACTIVE_ROSTER_STATUS = "active"


def compute_content_sla_ntpj(
    jobs_within_sla: DataFrame,
    period_start: date,
    period_end: date,
) -> DataFrame:
    """Compute Content NTPJ (SLA-weighted compliance) at all granularities.

    Args:
        jobs_within_sla: the ``io_jobs_within_sla_raw`` table (one row per job).
        period_start / period_end: inclusive output window.

    Returns:
        Tidy long-format rows with ``metric='ntpj'`` (see module docstring).
    """
    spark = jobs_within_sla.sparkSession

    base = jobs_within_sla.filter(
        F.col("roster_status") == F.lit(ACTIVE_ROSTER_STATUS)
    )
    cal = F.to_date(F.col("date"))
    base = base.filter((cal >= F.lit(period_start)) & (cal <= F.lit(period_end)))

    if len(base.take(1)) == 0:
        return empty_metric_frame(spark)

    return aggregate_long(
        base,
        numerator_col="sla_met_seconds",
        denominator_col="sla_seconds",
        metric_name=METRIC_NAME,
        date_col="date",
        scale=100.0,
    )
