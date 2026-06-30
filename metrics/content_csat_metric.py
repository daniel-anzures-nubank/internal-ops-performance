"""content_csat — the Content Quality (CSAT) performance metric (Content only), PySpark.

(Module file is ``content_csat_metric.py`` — not ``content_csat.py`` — because
the raw-layer module is already ``metrics_data/content_csat.py``; the two share a
name and would collide on the import path. The metric itself is ``content_csat``.)

Part of the **metrics layer**: consumes the raw ``io_content_csat_raw`` table
(one row per CSAT survey response × content agent) and produces a finished,
agent-level metric at day / week / month / quarter / semester / year grain.

CSAT is the share of survey questions answered favourably ("promoter" = answer
``>= 4`` on the 1-5 scale), across the 8 questions of each monthly Content survey:

    content_csat = SUM(promoters) / SUM(number_of_questions)

per agent per period. **Target >= 95%.** This is **Content's** quality component
(Core / Fraud / Social Media use the Playvox ``quality`` metric instead — see
``docs/metrics_definitions.md``).

A survey response is credited to *every* active content agent who supports the
rated ``target_squad`` that month — that fan-out already happened in the raw
layer, so this module just sums the per-row promoter / question counts per agent.

Input
-----
The ``io_content_csat_raw`` table (``metrics_data/content_csat.py``), one row per
survey response × content agent. Required columns: ``agent, xforce, xplead, team,
squad, district, shift, date, promoters, number_of_questions``.

Filters / rules applied here (deferred by the raw layer)
--------------------------------------------------------
* **Team scope** — keep ``team = 'content'`` (defensive; the raw table is
  content-only).
* **Aggregation** — ``SUM(promoters) / SUM(number_of_questions)`` per
  ``(agent, period)``.

NOT applied here (future Adjustments layer)
-------------------------------------------
* Any per-agent manual adjustments / outage-date carve-outs.

Output — one row per (agent, date_reference, granularity)
---------------------------------------------------------
Tidy "long" metric shape shared across the metrics layer:
``agent, xforce, xplead, team, squad, district, shift, date_reference,
date_granularity, metric, numerator, denominator, metric_value``. ``numerator`` =
promoter answers, ``denominator`` = total questions, ``metric_value`` =
``numerator / denominator * 100`` (percentage; NULL when the denominator is 0).
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from metric_utils import aggregate_long, empty_metric_frame

METRIC_NAME = "content_csat"

# CSAT only applies to Content.
CSAT_TEAM = "content"


def compute_content_csat(content_csat: DataFrame) -> DataFrame:
    """Compute the Content CSAT metric at all granularities.

    Args:
        content_csat: the ``io_content_csat_raw`` table (one row per response ×
            content agent).

    Returns:
        Tidy long-format metric rows (see module docstring / schema).
    """
    spark = content_csat.sparkSession

    work = content_csat.filter(F.lower(F.col("team")) == F.lit(CSAT_TEAM))

    if len(work.take(1)) == 0:
        return empty_metric_frame(spark)

    # Coerce the numerator/denominator to numeric; NULL -> 0 (matches the legacy
    # pandas `fillna(0)`).
    work = work.withColumn(
        "promoters", F.coalesce(F.col("promoters").cast("double"), F.lit(0.0))
    ).withColumn(
        "number_of_questions",
        F.coalesce(F.col("number_of_questions").cast("double"), F.lit(0.0)),
    )

    return aggregate_long(
        work,
        numerator_col="promoters",
        denominator_col="number_of_questions",
        metric_name=METRIC_NAME,
    )


IO_CONTENT_CSAT_METRIC_SCHEMA: tuple[tuple[str, str], ...] = (
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
