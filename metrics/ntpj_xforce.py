"""ntpj_xforce — the XForce-grain roll-up of NTPJ, PySpark.

``ntpj_xforce`` answers "what share of this XForce's agents hit the NTPJ target
(``ntpj <= 100``) this period?":

    ntpj_xforce = COUNT(DISTINCT agents with ntpj <= 100) / COUNT(DISTINCT agents)

per ``(xforce, xplead)`` per period. It is a **roll-up of the agent-grain NTPJ
metric** (``io_ntpj_metric``), not a fresh computation — exactly like legacy
``ntpj_xforces_monthly`` / ``ntpj_xforces_weekly`` which ``GROUP BY`` the
``ntpj_agents_monthly`` / ``ntpj_agents_weekly`` views
(``legacy/[IO] Performance 2026.sql`` lines 560-598).

Two reasons this metric exists in the new pipeline:

1. **Output-table parity.** Legacy emits standalone ``metric = 'ntpj_xforce'``
   rows (main deck ``ntpj_xforces``; S&D deck ``internal_ops_performance_2026``
   where ``metric = 'ntpj_xforce'``). The new pipeline emitted it nowhere.
2. **It gates ``improved_benchmarks``.** Legacy ``improved_benchmark_final`` is
   *driven by* ``ntpj_xforces`` (``FROM ntpj_xforces a LEFT JOIN
   improved_benchmark_base b ON month + xforce``), so a benchmark unit for an
   ``(xforce, month)`` with **no** ``ntpj_xforce`` output row that month is
   dropped. See ``metrics/improved_benchmarks.py``.

Legacy shape (matched here)
---------------------------
* Grouped by ``(xforce, xplead, date_reference, date_granularity)``
  (``GROUP BY ALL`` over the SELECT list); ``agent``, ``squad``, ``district``,
  ``shift`` are ``NULL``.
* ``numerator``   = ``COUNT(DISTINCT CASE WHEN metric_value <= 100 THEN agent END)``
* ``denominator`` = ``COUNT(DISTINCT agent)``
* ``metric_value`` = ``TRY_DIVIDE(numerator, denominator) * 100`` (NULL when the
  denominator is 0 — cannot happen for a group that exists, but kept for safety).
* Only the **week and month** grains — legacy only rolls up ``ntpj_agents_monthly``
  and ``ntpj_agents_weekly`` (there is no day/quarter/semester/year ``ntpj_xforce``).

An agent contributes to the denominator iff it has an ``io_ntpj_metric`` row for
that ``(xforce, period)`` — i.e. a finished, required-activity, active-roster job
(the agent-grain NTPJ already applied every outage / hardcode / manual-adjustment
exclusion). A NULL ``metric_value`` (denominator 0 in the agent metric) fails the
``<= 100`` test, so such an agent counts in the denominator but not the numerator
— matching legacy's ``metric_value <= 100`` CASE (``NULL <= 100`` is not true).

Scope
-----
Every team present in ``io_ntpj_metric`` (Core / Fraud / Content). Social Media
has no NTPJ rows, so no SM ``ntpj_xforce`` is produced. ``team`` is carried
through the group key (an xforce maps to a single team, so this never splits a
row) so downstream consumers can scope by team.

Input
-----
``io_ntpj_metric`` (tidy long agent-grain NTPJ), via ``metrics/ntpj.py``.

Output — tidy long format, one row per (xforce, xplead, date_reference, week|month)
-----------------------------------------------------------------------------------
The shared :data:`metric_utils.METRIC_COLUMNS`. ``agent``, ``squad``,
``district``, ``shift`` are always NULL; ``metric`` is ``ntpj_xforce``.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from metric_utils import METRIC_COLUMNS, empty_metric_frame

METRIC_NAME = "ntpj_xforce"

# Source agent metric name and the grains legacy rolls up (ntpj_agents_monthly /
# _weekly only — no day/quarter/semester/year ntpj_xforce).
SOURCE_METRIC = "ntpj"
ROLLUP_GRANULARITIES: tuple[str, ...] = ("week", "month")

# The NTPJ on-target threshold (lower is better): an agent is "on target" when
# ntpj <= 100. Matches legacy `metric_value <= 100`.
NTPJ_TARGET = 100.0


def compute_ntpj_xforce(ntpj_metric: DataFrame) -> DataFrame:
    """Roll the agent-grain NTPJ metric up to the XForce grain.

    Args:
        ntpj_metric: ``io_ntpj_metric`` rows (tidy long, agent grain). Only
            ``metric == 'ntpj'`` week/month rows are consumed; any other metric
            or granularity is ignored.

    Returns:
        Tidy long-format ``ntpj_xforce`` rows (see module docstring). Empty input
        (or no ntpj week/month rows) returns an empty metric frame.
    """
    spark = ntpj_metric.sparkSession

    agents = ntpj_metric.filter(
        (F.col("metric") == F.lit(SOURCE_METRIC))
        & (F.col("date_granularity").isin(list(ROLLUP_GRANULARITIES)))
    )
    if len(agents.take(1)) == 0:
        return empty_metric_frame(spark)

    on_target_agent = F.when(
        F.col("metric_value") <= F.lit(NTPJ_TARGET), F.col("agent")
    )

    grouped = agents.groupBy(
        "team", "xforce", "xplead", "date_reference", "date_granularity"
    ).agg(
        F.countDistinct(on_target_agent).cast("double").alias("numerator"),
        F.countDistinct(F.col("agent")).cast("double").alias("denominator"),
    )

    null_str = F.lit(None).cast("string")
    out = grouped.select(
        null_str.alias("agent"),
        F.col("xforce"),
        F.col("xplead"),
        F.col("team"),
        null_str.alias("squad"),
        null_str.alias("district"),
        null_str.alias("shift"),
        F.col("date_reference"),
        F.col("date_granularity"),
        F.lit(METRIC_NAME).alias("metric"),
        F.col("numerator"),
        F.col("denominator"),
        F.when(
            F.col("denominator") > 0,
            F.col("numerator") / F.col("denominator") * F.lit(100.0),
        )
        .otherwise(F.lit(None).cast("double"))
        .alias("metric_value"),
    )
    return out.select(*METRIC_COLUMNS)


IO_NTPJ_XFORCE_METRIC_SCHEMA: tuple[tuple[str, str], ...] = (
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
