"""content_csat — one row per Content CSAT survey response × content agent (PySpark).

This is a RAW dataset feeding the Content **Quality (CSAT)** metric. Each Content
CSAT survey response rates how well the Content (enablement) team supported a
given squad that month. The survey sheet has 8 question columns, but legacy
scores only the **5** CSAT questions (a "promoter" is any answer >= 4 on the 1-5
scale). The per-response score is ``promoters / number_of_questions``.

CSAT is attributed by **target_squad**, not by individual agent: a single survey
response is credited to *every* active content agent who supports that squad
(``target_squad``). This module reproduces the legacy fan-out — joining each
response to the content roster on ``target_squad`` — so the grain here is **one
row per (survey response × content agent)**, carrying the standard shared
dimensions. The metrics layer then aggregates per agent as
``SUM(promoters) / SUM(number_of_questions)``.

CSAT only applies to **Content**.

Public API
----------
``compute_content_csat(agent_info, csat)`` takes Spark DataFrames (the extractor
outputs) and returns one Spark DataFrame with one row per (response × content
agent).

Source tables (via extractors)
------------------------------
* ``agent_information`` → BDX snapshots + ``mx_content_bdx`` (the content roster,
  which carries each content agent's ``target_squad``).
* ``content_csat``     → ``gsheets.sheets.mx_content_csat_daniel_anz_temp``.

Filters applied here (minimal — raw table)
------------------------------------------
* Roster: ``status = 'active'`` and non-null / non-empty ``target_squad`` (inner
  join on ``(target_squad, snapshot_month)`` — this scopes output to content and
  fans the response out to that month's content agents serving the squad).

Deferred to the metrics layer (NOT applied here)
------------------------------------------------
* Per-agent / per-period aggregation (``SUM(promoters) / SUM(number_of_questions)``).
* Any per-agent manual adjustments / outage-date carve-outs.

Output schema (one row per survey response × content agent)
-----------------------------------------------------------
    agent              STRING
    xforce             STRING
    xplead             STRING
    team               STRING   always 'content'
    squad              STRING   roster squad (content agents: 'enablement')
    district           STRING   roster district (content agents: 'content')
    shift              STRING   roster shift (NULL for content)
    date               DATE     month rated (DATE of date_reference)
    target_squad       STRING   the supported squad the survey is about (join key)
    requested_by       STRING   respondent email prefix
    survey_timestamp   TIMESTAMP when the survey was filled
    promoters          INT      # of the 5 scored questions answered >= 4
    number_of_questions INT     always 5 (legacy scores 5 of the survey's 8)
    csat_score         DOUBLE   promoters / number_of_questions (0-1, per response)
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# The 5 CSAT questions legacy scores (each 1-5; promoter if >= 4). The survey
# sheet carries 8 question columns, but legacy `[IO] Performance 2026 - Content`
# (qa_base) scores only these first 5 — the trailing 3 (`manejo_de_cambios`,
# `expectativas`, `aportacion_estrategica`) are excluded. Verified against the
# legacy `qa_score_agent` output: first-5 reproduces legacy num/den exactly
# (denominator is 5/response, not 8). Owner decision: keep legacy's 5-question
# CSAT for all dates (no cutover correction to 8).
_QUESTION_COLS: tuple[str, ...] = (
    "facilidad",
    "comprension",
    "comunicacion",
    "calidad",
    "tiempo",
)

NUMBER_OF_QUESTIONS: int = len(_QUESTION_COLS)  # 5
PROMOTER_THRESHOLD: int = 4

# [Manual Fix] Content Temp Fix (2026-06-30 legacy re-export): exclude the
# 'tiempo de entrega' question (`tiempo`, the 5th scored question) for these
# agents in May 2026 — promoters drop that question's flag and
# number_of_questions goes 5 -> 4. Applied after the roster join because legacy
# keys it on the joined agent. Tracked in the adjustments sheet under
# 'exclusiones generales'.
TIEMPO_QUESTION_COL: str = "tiempo"
TIEMPO_EXCLUSION_AGENTS: tuple[str, ...] = ("jesus.morales", "luis.contreras")
TIEMPO_EXCLUSION_MONTH: date = date(2026, 5, 1)

# Display-form squad labels that map to the 'emi_general' target_squad (legacy).
_EMI_GENERAL_LABELS: tuple[str, ...] = (
    "E.M.I.",
    "GENERAL (CHANNEL SOLUTIONS, PLANNING, SERVICE EXCELLENCE, QA, OPS DEFENSE)",
)


# ---------------------------------------------------------------------------
# Orchestrator — promoter count + fan-out roster join (no aggregation)
# ---------------------------------------------------------------------------


def compute_content_csat(
    agent_info: DataFrame,
    csat: DataFrame,
) -> DataFrame:
    """End-to-end content CSAT pipeline (one row per response × content agent)."""
    # --- per-response promoter count -------------------------------------
    # For each question, promoter = (score >= 4). A NULL score is NOT a promoter
    # (null >= 4 -> null -> otherwise(0)). promoters = sum of the 8 flags.
    flags = [
        F.when(F.col(c).cast("double") >= F.lit(PROMOTER_THRESHOLD), F.lit(1))
        .otherwise(F.lit(0))
        for c in _QUESTION_COLS
    ]
    promoters = flags[0]
    for flag in flags[1:]:
        promoters = promoters + flag

    rows = (
        csat.withColumn("promoters", promoters.cast("int"))
        .withColumn("number_of_questions", F.lit(NUMBER_OF_QUESTIONS).cast("int"))
        .withColumn(
            "csat_score",
            (F.col("promoters").cast("double") / F.lit(float(NUMBER_OF_QUESTIONS))),
        )
    )

    # --- join key normalization ------------------------------------------
    # The survey's display-form `squad` maps to the roster `target_squad`:
    # 'E.M.I.' and the long 'GENERAL (...)' label both become 'emi_general';
    # everything else is lowercased.
    target_squad = (
        F.when(
            F.col("squad").isin(list(_EMI_GENERAL_LABELS)), F.lit("emi_general")
        ).otherwise(F.lower(F.col("squad")))
    )
    rows = (
        rows.withColumn("target_squad", target_squad)
        .withColumn("date", F.to_date(F.col("date_reference")))
        .withColumn(
            "snapshot_month", F.trunc(F.to_date(F.col("date_reference")), "month")
        )
        # Drop the raw display `squad` so it doesn't collide with the roster's
        # `squad` (the agent's roster squad, e.g. 'enablement') on the join.
        .drop("squad")
    )

    # --- content roster -----------------------------------------------------
    roster = agent_info.filter(
        (F.col("status") == F.lit("active"))
        & F.col("target_squad").isNotNull()
        & (F.col("target_squad") != F.lit(""))
    ).select(
        "agent",
        "xforce",
        "xplead",
        "team",
        "squad",
        F.col("squad_district").alias("district"),
        "shift",
        "target_squad",
        F.to_date(F.col("snapshot_date")).alias("_snapshot_date"),
        F.trunc(F.to_date(F.col("snapshot_month")), "month").alias("snapshot_month"),
    )

    # Deduplicate the roster to exactly ONE row per
    # (agent, target_squad, snapshot_month) BEFORE the join (mirrors
    # tnps_responses): the content branch of `agent_information` can yield >1
    # identical row per (agent, target_squad, snapshot_month), which would fan
    # out the inner join and double-count the numerator/denominator. Keep the
    # latest snapshot deterministically.
    roster_dedup_window = Window.partitionBy(
        "agent", "target_squad", "snapshot_month"
    ).orderBy(
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

    # --- fan-out join to the content roster on (target_squad, snapshot_month).
    # This is the intentional fan-out: one response -> many agents serving the
    # squad that month.
    enriched = rows.join(roster, on=["target_squad", "snapshot_month"], how="inner")

    # --- [Manual Fix] 'tiempo de entrega' exclusion --------------------------
    # For jesus.morales + luis.contreras in May 2026, drop the `tiempo` question:
    # promoters lose that question's promoter flag and number_of_questions goes
    # 5 -> 4 (legacy Content Temp Fix, keyed on the joined agent). Recompute the
    # per-response csat_score from the adjusted counts.
    tiempo_excluded = F.col("agent").isin(*TIEMPO_EXCLUSION_AGENTS) & (
        F.col("snapshot_month") == F.lit(TIEMPO_EXCLUSION_MONTH)
    )
    tiempo_promoter = F.when(
        F.col(TIEMPO_QUESTION_COL).cast("double") >= F.lit(PROMOTER_THRESHOLD), F.lit(1)
    ).otherwise(F.lit(0))
    enriched = (
        enriched.withColumn(
            "promoters",
            F.when(tiempo_excluded, F.col("promoters") - tiempo_promoter)
            .otherwise(F.col("promoters"))
            .cast("int"),
        )
        .withColumn(
            "number_of_questions",
            F.when(tiempo_excluded, F.col("number_of_questions") - F.lit(1))
            .otherwise(F.col("number_of_questions"))
            .cast("int"),
        )
        .withColumn(
            "csat_score",
            F.when(
                F.col("number_of_questions") > F.lit(0),
                F.col("promoters").cast("double") / F.col("number_of_questions"),
            ).otherwise(F.lit(None).cast("double")),
        )
    )

    out = enriched.select(
        "agent",
        "xforce",
        "xplead",
        "team",
        "squad",
        "district",
        "shift",
        "date",
        "target_squad",
        "requested_by",
        "survey_timestamp",
        "promoters",
        "number_of_questions",
        "csat_score",
    )

    return out.orderBy("date", "target_squad", "agent", "survey_timestamp")


# ---------------------------------------------------------------------------
# Output schema declaration — used by
# scripts/metrics_data_scripts/build_content_csat.py
# ---------------------------------------------------------------------------

IO_CONTENT_CSAT_SCHEMA: tuple[tuple[str, str], ...] = (
    ("agent", "STRING"),
    ("xforce", "STRING"),
    ("xplead", "STRING"),
    ("team", "STRING"),
    ("squad", "STRING"),
    ("district", "STRING"),
    ("shift", "STRING"),
    ("date", "DATE"),
    ("target_squad", "STRING"),
    ("requested_by", "STRING"),
    ("survey_timestamp", "TIMESTAMP"),
    ("promoters", "INT"),
    ("number_of_questions", "INT"),
    ("csat_score", "DOUBLE"),
)
