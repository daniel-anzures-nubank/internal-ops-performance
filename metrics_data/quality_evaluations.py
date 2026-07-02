"""quality_evaluations — one row per individual QA evaluation (PySpark).

This is a RAW dataset, not a finished metric. It exposes every QA evaluation
attributed to an active roster agent, one row per evaluation, with its score. A
downstream ``metrics`` layer (``metrics/quality.py``) dedups latest-per-
``evaluation_id`` (within each source), drops Content, applies the legacy
blacklists, and averages the raw ``qa_score`` values into the Quality score at
whatever grain it wants.

Sources (Playvox + Sprinklr SM — a UNION, matching legacy)
----------------------------------------------------------
* **Playvox** (``qmo_playvox_consolidated``) — Quality of record for Core and
  Fraud, and Social Media's original QA feed. **No upper date bound**: legacy's
  Playvox branch carries no SM/May cutoff (SM Playvox evaluations keep flowing
  until they naturally end after 2026-05-15), and neither does ours.
* **Sprinklr SM** (``social_media_case_summary_information``) — Social-Media
  case QA, **floored at 2026-05-01** (``SPRINKLR_SM_CUTOVER``), matching
  legacy's Sprinklr branch (``sm.report_date >= "2026-05-01"``, ``[IO]
  Performance 2026 - Social Media Temp Fix.sql`` ``qa_base`` line 3025). Both
  feeds report ``qa_score`` on the same 0-100 scale. A ``source`` column
  ('playvox' / 'sprinklr_sm') tags each row's provenance.

Note: the two sources UNION (legacy parity) — there is NO source switch
-----------------------------------------------------------------------
Legacy SM quality (``[IO] Performance 2026 - Social Media Temp Fix.sql``
``qa_base``, lines 2988-3028) ``UNION ALL``s the two branches: Playvox is
unbounded and Sprinklr starts at 2026-05-01, so in early May an SM agent can
contribute BOTH a Playvox and a Sprinklr evaluation to the same period's mean.
We reproduce that union exactly — Playvox Social-Media rows dated on/after
2026-05-01 are **kept**. (An earlier revision implemented a "clean switch" that
dropped them; reverted for legacy parity.) Double-counting is not a concern:
the metric layer dedups latest-per-``evaluation_id`` within each source, and
the Playvox ``evaluation__id`` / Sprinklr ``case_number`` id spaces are
disjoint, so cross-source dedup is a no-op.

Public API
----------
``compute_quality_evaluations(agent_info, playvox, sprinklr_sm=None)`` takes
Spark DataFrames (the extractor outputs) and returns one Spark DataFrame with
one row per evaluation. ``sprinklr_sm`` is optional; when omitted the table is
Playvox-only.

Source tables (via extractors)
------------------------------
* ``agent_information``       → ``etl.mx__series_contract.cx_mx_bdx_snapshots`` (+ ``ops_actors``).
* ``playvox_evaluations``     → Playvox QA evaluations (one row per evaluation).
* ``sprinklr_sm_evaluations`` → Sprinklr SM case QA (one row per evaluation, >= cutover).

Filters applied here (deliberately minimal — this is a raw table)
-----------------------------------------------------------------
* Playvox: ``team_name NOT IN ('REGULATORY SOLUTIONS', 'AML')`` and the
  Nubank-MX agent-email regex ``^[a-z]+\\.[a-z]+[0-9]*@nu\\.com\\.mx$``
  (these mirror legacy's source-level ``qa_base`` gate). Sprinklr SM rows are
  NOT run through this Playvox-specific gate; their source-level filtering
  (agent mapping, monitor exclusion, cutover) lives in the extractor.
* Sprinklr SM: a defensive ``date >= SPRINKLR_SM_CUTOVER`` floor (the extractor
  already enforces it; re-applied here so the module is self-contained).
* Roster: ``status='active'`` and non-null ``squad`` (inner join attaches the
  dimensions / scopes output).

Filters deferred to the metrics layer (``metrics/quality.py``, NOT applied here)
-------------------------------------------------------------------------------
* The team-scoped ``scorecard_id`` / ``evaluation_id`` blacklists. ``scorecard_id``
  is carried through this raw table so the metric layer can apply them.
* Latest-per-``evaluation_id`` dedup (per source), Content exclusion, and the
  ``COUNT(DISTINCT evaluation_id)`` denominator.

No dates are dropped for quality — anywhere. The 2026-06-30 legacy re-export
re-included the 2026-03-27 / 2026-04-09 outage rows (the published
``usr.mx__cx.quality_io`` and ``usr.danielanzures.sm_temp_quality`` both carry
those dates), so neither this raw layer nor the metric layer applies any
quality date drop.

Output schema (one row per evaluation)
--------------------------------------
    agent            STRING
    xforce           STRING
    xplead           STRING
    team             STRING     performance team (from roster; see team_squad_mapping)
    squad            STRING     roster squad
    district         STRING     roster district (was ``squad_district``)
    shift            STRING     roster shift
    date             DATE       calendar day the evaluation was logged (MX local)
    created_at       TIMESTAMP  raw evaluation timestamp (legacy dedup order key)
    evaluation_id    STRING
    team_name        STRING     source team / scorecard team
    scorecard_id     STRING     source scorecard id (for the metric-layer blacklist)
    source           STRING     'playvox' | 'sprinklr_sm'
    qa_score         DOUBLE     the evaluation's score
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PLAYVOX_TEAM_NAME_EXCLUSIONS: tuple[str, ...] = (
    "REGULATORY SOLUTIONS",
    "AML",
)

# Roster-level squad exclusions. Currently empty — all squads in scope.
QUALITY_OUT_OF_SCOPE_SQUADS: tuple[str, ...] = ()

# Source tags written to the ``source`` provenance column.
SOURCE_PLAYVOX = "playvox"
SOURCE_SPRINKLR_SM = "sprinklr_sm"

# Sprinklr SM scorecard literal (legacy SM ``qa_base`` Sprinklr branch assigns
# this constant). Carried so the metric-layer blacklist sees a stable value.
SPRINKLR_SCORECARD_ID = "SprinklrScorecardV1"

# Floor of the Sprinklr SM feed (SM QA started being logged in Sprinklr in May
# 2026). Matches legacy's Sprinklr branch ``sm.report_date >= "2026-05-01"``
# ([IO] Performance 2026 - Social Media Temp Fix.sql qa_base line 3025); the
# extractor hard-floors it too, and it is re-applied here defensively. This is
# a FLOOR on the Sprinklr feed only — Playvox is NOT cut off at this date: the
# two sources UNION, exactly like legacy, and SM Playvox evaluations keep
# flowing until they naturally end after 2026-05-15.
SPRINKLR_SM_CUTOVER: date = date(2026, 5, 1)

# Legacy affiliation regex (Playvox path): lowercase "first.last" with an
# optional trailing integer suffix on the @nu.com.mx domain.
_NUBANK_EMAIL_REGEX = r"^[a-zA-Z]+[.][a-zA-Z]+[0-9]*@nu[.]com[.]mx$"


# ---------------------------------------------------------------------------
# Step 1: Playvox-only source gate (team_name + Nubank-email regex)
# ---------------------------------------------------------------------------


def filter_playvox(playvox: DataFrame) -> DataFrame:
    """Apply the Playvox source gate (team_name exclusions + Nubank-email regex).

    Mirrors legacy ``qa_base``'s Playvox WHERE clause:
    ``evaluation__team_name NOT IN (...)`` and the affiliation RLIKE that keeps
    only ``first.last[N]@nu.com.mx`` emails.
    """
    return playvox.filter(
        ~F.col("team_name").isin(list(PLAYVOX_TEAM_NAME_EXCLUSIONS))
        & F.col("agent_email").rlike(_NUBANK_EMAIL_REGEX)
    )


# ---------------------------------------------------------------------------
# Step 2: shape each source into the common per-evaluation frame
# ---------------------------------------------------------------------------


def _shape_evaluations(evals: DataFrame, *, scorecard_default: str | None) -> DataFrame:
    """Shape a source frame into the common per-evaluation columns.

    Result has one row per evaluation with:
        evaluation_id, agent, qa_score, team_name, scorecard_id, created_at, date
    where ``date`` = the calendar day of ``created_at`` (MX local). Rows whose
    ``agent`` is null/empty (unmappable email) are dropped — legacy's regex
    extraction yields an empty string for those, which never join the roster.
    """
    has_scorecard = "scorecard_id" in evals.columns
    scorecard_col = (
        F.col("scorecard_id")
        if has_scorecard
        else F.lit(scorecard_default).cast("string")
    )
    shaped = evals.select(
        F.col("evaluation_id").cast("string").alias("evaluation_id"),
        F.col("agent").cast("string").alias("agent"),
        F.col("qa_score").cast("double").alias("qa_score"),
        F.col("team_name").cast("string").alias("team_name"),
        scorecard_col.alias("scorecard_id"),
        F.to_timestamp(F.col("created_at")).alias("created_at"),
    ).withColumn("date", F.to_date(F.col("created_at")))
    return shaped.filter(
        F.col("agent").isNotNull() & (F.col("agent") != F.lit(""))
    )


# ---------------------------------------------------------------------------
# Step 3: orchestrator — union the sources, attach the roster (no aggregation)
# ---------------------------------------------------------------------------


def compute_quality_evaluations(
    agent_info: DataFrame,
    playvox: DataFrame,
    sprinklr_sm: DataFrame | None = None,
) -> DataFrame:
    """End-to-end quality_evaluations pipeline (one row per evaluation).

    ``sprinklr_sm`` is the optional Sprinklr SM case-QA feed; when provided, its
    rows on/after :data:`SPRINKLR_SM_CUTOVER` (2026-05-01, the feed's floor) are
    unioned on top of Playvox (tagged ``source='sprinklr_sm'``).

    The two sources UNION, matching legacy: the Playvox branch has no upper date
    bound (SM Playvox evaluations dated on/after 2026-05-01 are KEPT), and the
    Sprinklr branch is floored at 2026-05-01. No dates are dropped.
    """
    spark = playvox.sparkSession

    playvox_evals = _shape_evaluations(
        filter_playvox(playvox), scorecard_default=None
    ).withColumn("source", F.lit(SOURCE_PLAYVOX))

    parts = [playvox_evals]
    if sprinklr_sm is not None:
        sm_evals = (
            _shape_evaluations(sprinklr_sm, scorecard_default=SPRINKLR_SCORECARD_ID)
            .filter(F.col("date") >= F.lit(SPRINKLR_SM_CUTOVER))
            .withColumn("source", F.lit(SOURCE_SPRINKLR_SM))
        )
        parts.append(sm_evals)

    evals = parts[0]
    for extra in parts[1:]:
        evals = evals.unionByName(extra)

    # --- roster join --------------------------------------------------------
    roster = agent_info.filter(
        (F.col("status") == F.lit("active")) & F.col("squad").isNotNull()
    )
    if QUALITY_OUT_OF_SCOPE_SQUADS:
        roster = roster.filter(~F.col("squad").isin(list(QUALITY_OUT_OF_SCOPE_SQUADS)))
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
    # the join. The content branch of `agent_information` cross-joins each
    # Google-Sheet content row against every month, so a content agent on >1 sheet
    # row yields >=2 rows per (agent, snapshot_month) identical on every selected
    # column. Without this the inner join fans out and every evaluation
    # double-counts. Keep the latest snapshot deterministically.
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

    enriched = evals.withColumn(
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
        F.col("created_at"),
        "evaluation_id",
        "team_name",
        "scorecard_id",
        "source",
        F.col("qa_score").cast("double").alias("qa_score"),
    )

    return out.orderBy("date", "agent", "evaluation_id")


# ---------------------------------------------------------------------------
# Output schema declaration — used by scripts/metrics_data_scripts/build_quality_evaluations.py
# ---------------------------------------------------------------------------

IO_QUALITY_EVALUATIONS_SCHEMA: tuple[tuple[str, str], ...] = (
    ("agent", "STRING"),
    ("xforce", "STRING"),
    ("xplead", "STRING"),
    ("team", "STRING"),
    ("squad", "STRING"),
    ("district", "STRING"),
    ("shift", "STRING"),
    ("date", "DATE"),
    ("created_at", "TIMESTAMP"),
    ("evaluation_id", "STRING"),
    ("team_name", "STRING"),
    ("scorecard_id", "STRING"),
    ("source", "STRING"),
    ("qa_score", "DOUBLE"),
)
