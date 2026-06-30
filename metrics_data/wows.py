"""wows — one row per Social-Media WoW experience (PySpark).

This is a RAW dataset, not a finished metric. It exposes every WoW experience
logged for an active social agent, one row per WoW, with its case id. A
downstream ``metrics`` layer (``metrics/wows_metric.py``) counts these
(``COUNT(DISTINCT case_id)`` per agent / period) into the **WoWs** metric
(monthly target >= 5).

WoWs only apply to **Social Media**. The source sheet only contains social
agents' WoWs.

Public API
----------
``compute_wows(agent_info, wows)`` takes Spark DataFrames (the extractor
outputs) and returns one Spark DataFrame with one row per WoW experience.

Source tables (via extractors)
------------------------------
* ``agent_information`` → ``etl.mx__series_contract.cx_mx_bdx_snapshots`` (+ ``ops_actors``).
* ``wows``             → ``gsheets.sheets.mx_wows_daniel_temp`` (WoWs Google Sheet).

Filters applied here (deliberately minimal — this is a raw table)
-----------------------------------------------------------------
* Drop rows whose ``agent`` did not resolve (null / empty string).
* Roster: ``status = 'active'`` and non-null ``squad`` (inner join attaches the
  dimensions / scopes output to active agents on the WoW's snapshot_month).

Filters deferred to the metrics layer (``metrics/wows_metric.py``, NOT applied here)
------------------------------------------------------------------------------------
* ``COUNT(DISTINCT case_id)`` aggregation and the monthly target (>= 5).
* The outage-date exclusion (``2026-03-27``).

Output schema (one row per WoW experience)
------------------------------------------
    agent       STRING
    xforce      STRING
    xplead      STRING
    team        STRING   performance team (from roster; see team_squad_mapping)
    squad       STRING   roster squad
    district    STRING   roster district (was ``squad_district``)
    shift       STRING   roster shift
    date        DATE     day the WoW was logged (MX local)
    case_id     STRING   the WoW's case identifier
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Roster-level squad exclusions. Currently empty — scope is source-driven
# (only social agents appear in the WoWs sheet).
WOWS_OUT_OF_SCOPE_SQUADS: tuple[str, ...] = ()


# ---------------------------------------------------------------------------
# Orchestrator — roster join (no aggregation)
# ---------------------------------------------------------------------------


def compute_wows(
    agent_info: DataFrame,
    wows: DataFrame,
) -> DataFrame:
    """End-to-end wows pipeline (one row per WoW experience)."""
    rows = wows.filter(F.col("agent").isNotNull() & (F.col("agent") != F.lit("")))

    # --- roster join --------------------------------------------------------
    roster = agent_info.filter(
        (F.col("status") == F.lit("active")) & F.col("squad").isNotNull()
    )
    if WOWS_OUT_OF_SCOPE_SQUADS:
        roster = roster.filter(~F.col("squad").isin(list(WOWS_OUT_OF_SCOPE_SQUADS)))
    roster = roster.select(
        "agent",
        "xforce",
        "xplead",
        "team",
        "squad",
        F.col("squad_district").alias("district"),
        "shift",
        F.to_date(F.col("snapshot_date")).alias("_snapshot_date"),
        F.trunc(F.to_date(F.col("snapshot_month")), "month").alias("snapshot_month"),
    )

    # Deduplicate the roster to exactly ONE row per (agent, snapshot_month) BEFORE
    # the join (mirrors tnps_responses / quality_evaluations): the content branch
    # of `agent_information` can yield >1 identical row per (agent, snapshot_month),
    # which would fan out the inner join and double-count WoWs. Keep the latest
    # snapshot deterministically.
    roster_dedup_window = Window.partitionBy("agent", "snapshot_month").orderBy(
        F.col("_snapshot_date").desc_nulls_last(),
        F.col("squad").asc_nulls_last(),
        F.col("district").asc_nulls_last(),
        F.col("shift").asc_nulls_last(),
    )
    roster = (
        roster.withColumn("_roster_rn", F.row_number().over(roster_dedup_window))
        .filter(F.col("_roster_rn") == 1)
        .drop("_roster_rn", "_snapshot_date")
    )

    enriched = rows.withColumn(
        "snapshot_month", F.trunc(F.to_date(F.col("date")), "month")
    ).join(roster, on=["agent", "snapshot_month"], how="inner")

    out = enriched.select(
        "agent",
        "xforce",
        "xplead",
        "team",
        "squad",
        "district",
        "shift",
        F.to_date(F.col("date")).alias("date"),
        F.col("case_id").cast("string").alias("case_id"),
    )

    return out.orderBy("date", "agent", "case_id")


# ---------------------------------------------------------------------------
# Output schema declaration — used by scripts/metrics_data_scripts/build_wows.py
# ---------------------------------------------------------------------------

IO_WOWS_SCHEMA: tuple[tuple[str, str], ...] = (
    ("agent", "STRING"),
    ("xforce", "STRING"),
    ("xplead", "STRING"),
    ("team", "STRING"),
    ("squad", "STRING"),
    ("district", "STRING"),
    ("shift", "STRING"),
    ("date", "DATE"),
    ("case_id", "STRING"),
)
