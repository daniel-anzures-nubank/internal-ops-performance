"""tnps_responses — one row per Social-Media tNPS survey response (PySpark).

This is a RAW dataset, not a finished metric. It exposes every transactional-NPS
(tNPS) survey response attributable to an active social agent, one row per
response, with its raw 0-10 score. A downstream ``metrics`` layer
(``metrics/tnps.py``) applies the validity window, classifies each response
(promoter ``>= 9`` / detractor ``<= 6`` / neutral 7-8), and computes the
**Human tNPS** metric: ``(promoters - detractors) / valid_responses`` via
classify-then-COUNT(DISTINCT case_number).

tNPS only applies to **Social Media**. The source
(``sprinklr_tnps_data``) only contains surveys for cases handled by a human social
agent, so every row here is a human social agent's survey.

Agent attribution — direct from ``agent_email_id`` (NOT ``sprinklr_sm_users``)
------------------------------------------------------------------------------
The extractor normalizes the agent key with
``LOWER(REGEXP_EXTRACT(agent_email_id, '^[a-zA-Z]+\\.[a-zA-Z]+', 0))`` directly
off the survey's ``agent_email_id`` — matching legacy ``tnps_initial_base``
(``REGEXP_EXTRACT(agent_email_id, ...)``). It does **not** join the
``usr.mx__enablement.sprinklr_sm_users`` mapping (which has known swapped
name<->email rows), so tNPS is not exposed to that attribution defect.

Public API
----------
``compute_tnps_responses(agent_info, tnps)`` takes Spark DataFrames (the
extractor outputs) and returns one Spark DataFrame with one row per survey
response.

Source tables (via extractors)
------------------------------
* ``agent_information`` → ``etl.mx__series_contract.cx_mx_bdx_snapshots`` (+ ``ops_actors``).
* ``tnps_responses``    → ``usr.sprinklr_api_data_integration.sprinklr_tnps_data``.

Filters applied here (deliberately minimal — this is a raw table)
-----------------------------------------------------------------
* Drop rows whose ``agent`` did not resolve (null / empty string) — i.e.
  unattributable / non-human surveys.
* Roster: ``status = 'active'`` and non-null ``squad`` (inner join attaches the
  dimensions / scopes output to active agents on the response's snapshot_month).

Filters deferred to the metrics layer (``metrics/tnps.py``, NOT applied here)
-----------------------------------------------------------------------------
* Promoter / detractor / neutral classification and the NPS ratio.
* The validity window ``survey_response_date <= date + 1 day``.
* The outage-date exclusion (``2026-03-27``).
* The classify-then-COUNT(DISTINCT case_number) aggregation.

Output schema (one row per survey response)
--------------------------------------------
    agent                 STRING
    xforce                STRING
    xplead                STRING
    team                  STRING   performance team (from roster; see team_squad_mapping)
    squad                 STRING   roster squad
    district              STRING   roster district (was ``squad_district``)
    shift                 STRING   roster shift
    date                  DATE     case closure day (MX local) the response is attributed to
    case_number           STRING   the case / survey identifier
    survey_response_date  DATE     when the customer answered the survey
    survey_score          INT      raw 0-10 NPS score (nullable)
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Roster-level squad exclusions. Currently empty — scope is source-driven
# (only social agents appear in the tNPS source).
TNPS_OUT_OF_SCOPE_SQUADS: tuple[str, ...] = ()


# ---------------------------------------------------------------------------
# Orchestrator — roster join (no aggregation)
# ---------------------------------------------------------------------------


def compute_tnps_responses(
    agent_info: DataFrame,
    tnps: DataFrame,
) -> DataFrame:
    """End-to-end tnps_responses pipeline (one row per survey response)."""
    spark = tnps.sparkSession

    responses = tnps.filter(
        F.col("agent").isNotNull() & (F.col("agent") != F.lit(""))
    )

    # --- roster join --------------------------------------------------------
    roster = agent_info.filter(
        (F.col("status") == F.lit("active")) & F.col("squad").isNotNull()
    )
    if TNPS_OUT_OF_SCOPE_SQUADS:
        roster = roster.filter(~F.col("squad").isin(list(TNPS_OUT_OF_SCOPE_SQUADS)))
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
    # the join (mirrors quality_evaluations): the content branch of
    # `agent_information` can yield >1 identical row per (agent, snapshot_month),
    # which would fan out the inner join and double-count responses. Keep the
    # latest snapshot deterministically.
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

    enriched = responses.withColumn(
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
        F.col("case_number").cast("string").alias("case_number"),
        F.to_date(F.col("survey_response_date")).alias("survey_response_date"),
        F.col("survey_score").cast("int").alias("survey_score"),
    )

    return out.orderBy("date", "agent", "case_number")


# ---------------------------------------------------------------------------
# Output schema declaration — used by scripts/metrics_data_scripts/build_tnps_responses.py
# ---------------------------------------------------------------------------

IO_TNPS_RESPONSES_SCHEMA: tuple[tuple[str, str], ...] = (
    ("agent", "STRING"),
    ("xforce", "STRING"),
    ("xplead", "STRING"),
    ("team", "STRING"),
    ("squad", "STRING"),
    ("district", "STRING"),
    ("shift", "STRING"),
    ("date", "DATE"),
    ("case_number", "STRING"),
    ("survey_response_date", "DATE"),
    ("survey_score", "INT"),
)
