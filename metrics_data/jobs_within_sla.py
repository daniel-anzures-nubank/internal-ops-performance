"""jobs_within_sla — one row per Content OOS "job" with its SLA threshold + on-time flag (PySpark).

This is the RAW substrate for **Content NTPJ**, which — unlike Core/Fraud NTPJ
(duration ``actual/expected``, lower-is-better) — is a **jobs-within-SLA
compliance** metric (higher-is-better, bounded ≤100). Legacy calls it
``ntpj_sla_old`` (``[IO] Performance 2026 - Content Temp Fix.sql`` L2006-2200) but
ships it under ``metric='ntpj_agent'`` for standardization. This module reproduces
the per-job base of that calculation; the compliance % is aggregated in
``metrics/content_sla_ntpj.py``.

Public API
----------
``compute_jobs_within_sla(oos_jobs, agent_info, sla_map)`` → one Spark DataFrame,
one row per Content OOS job (a distinct ``content_id`` for most types; one source
row for ``macros``/``faq``/``ar``), carrying its ``sla_seconds`` threshold, the
``actual_seconds`` worked, and whether it was delivered ``within_sla``.

``parse_sla_map(adj_df)`` normalizes the synced ``adj_content_slas`` config table
(the "Content - SLAs" sheet tab) into a ``(job_type, sla_seconds)`` map. The map
is **mandatory** — there is no hardcoded fallback.

Source tables
-------------
* ``oos_jobs`` → ``etl.mx__dataset.taskmaster_consolidated_registry`` (via the
  ``oos_jobs`` extractor, which now also exposes ``ticket__id``).
* ``agent_information`` → the roster (Content comes from the Google-Sheet roster),
  used to scope to Content agents and attach the 7 shared dims + ``roster_status``.
* ``sla_map`` → ``adj_content_slas`` (the OLD-SLA seconds per job type).

Job grain (legacy L2057-2083)
-----------------------------
* ``macros`` / ``faq`` / ``ar`` → one "job" = one source row.
* every other job type → one "job" = one distinct ``content_id`` (MOS ticket),
  with ``actual_seconds = SUM(net_time_spent_seconds)`` over that ``content_id``.
An **INNER JOIN** to ``sla_map`` drops job types with no Content SLA (legacy's
``mastery_cx``, ``sop``, generic ``projects``, stray Core/Fraud OOS types).

Filters applied here (intrinsic to the legacy source view)
----------------------------------------------------------
* Scoped to **Content agents** (roster ``team = 'content'``), matching legacy's
  ``content_agents`` filter.
* **Date scoping** ``date >= 2025-12-01`` and dropping ``2026-03-10 / 2026-03-27 /
  2026-04-09`` is applied **here, before the ``content_id`` grouping**, so a
  ``content_id`` that straddles the ``2026-03-10`` boundary is truncated exactly as
  legacy's source-level drop (``Content Temp Fix.sql`` L986), not kept whole. The
  metric layer only restricts to the output period + active roster.

Deferred to the metric layer (``content_sla_ntpj.py``)
------------------------------------------------------
* ``roster_status == 'active'`` scoping (carried here, applied there — matching
  ``jobs_raw``).
* Aggregation to the SLA-weighted compliance % and the tidy long roll-ups.

Output schema (one row per job)
-------------------------------
    agent, xforce, xplead, team, squad, district, shift, roster_status,
    date, job_type, content_id,
    actual_seconds  BIGINT   seconds worked on the job
    sla_seconds     BIGINT   the job type's OLD-SLA threshold (from the sheet map)
    within_sla      INT      1 if actual_seconds <= sla_seconds, else 0
    sla_met_seconds BIGINT   sla_seconds if within_sla else 0 (all-or-nothing credit)
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

CONTENT_TEAM = "content"

# Legacy sla_old source date scoping (Content Temp Fix.sql L986 + agent base
# L2110-2111). Applied BEFORE content_id grouping — see the module docstring.
SLA_START_DATE: date = date(2025, 12, 1)
SLA_EXCLUDED_DATES: tuple[date, ...] = (
    date(2026, 3, 10),
    date(2026, 3, 27),
    date(2026, 4, 9),
)

# Job classifications counted per source row (not grouped by content_id).
ROW_GRAIN_JOB_TYPES: tuple[str, ...] = ("macros", "faq", "ar")


def _normalize_job_type(col: Column) -> Column:
    """Legacy ``oos_jobs_ntpj`` normalization (unconditional for Content).

    ``LOWER(REPLACE(TRIM(REPLACE(job_classification, '(OOS_CONT)', '')), ' ', '_'))``
    (``Content Temp Fix.sql`` L958). Unlike ``jobs_raw._clean_oos_job_classification``
    (which gates on ``squad LIKE '%content%'``), the SLA source applies it to every
    row — this table is Content-only.
    """
    return F.lower(
        F.regexp_replace(
            F.trim(F.regexp_replace(col, r"\(OOS_CONT\)", "")),
            " ",
            "_",
        )
    )


def _content_id(comment: Column, ticket: Column) -> Column:
    """Legacy MOS ``content_id`` parse (``Content Temp Fix.sql`` L965-984).

    ``COALESCE(<MOS over comment>, <MOS over ticket__id>)``. Each source: an
    ``MOS…digits`` match, a bare ``digits`` match (case-preserving, matching
    legacy), or a ``TICKET … digits`` match → ``MOS-<digits>``; else NULL.
    """

    def mos(col: Column) -> Column:
        upper = F.upper(col)
        return (
            F.when(
                upper.rlike(r"MOS[\s_-]*\d{3,}"),
                F.concat(F.lit("MOS-"), F.regexp_extract(upper, r"MOS[\s_-]*(\d{3,})", 1)),
            )
            .when(
                col.rlike(r"^\s*\d{3,}\s*$"),
                F.concat(F.lit("MOS-"), F.trim(col)),
            )
            .when(
                upper.rlike(r"TICKET\s+(MOS-?\s*)?\d{3,}"),
                F.concat(
                    F.lit("MOS-"),
                    F.regexp_extract(upper, r"TICKET\s+(?:MOS-?\s*)?(\d{3,})", 1),
                ),
            )
            .otherwise(F.lit(None).cast("string"))
        )

    return F.coalesce(mos(comment), mos(ticket))


def parse_sla_map(adj_df: DataFrame | None) -> DataFrame:
    """Normalize the synced ``adj_content_slas`` config into a ``(job_type, sla_seconds)`` map.

    The map is mandatory — a missing/empty table raises (no hardcoded fallback),
    so a mis-synced sheet fails the build loudly instead of silently
    under-crediting via the INNER JOIN.
    """
    if adj_df is None:
        raise ValueError(
            "adj_content_slas not found — the Content SLA map is required. "
            "Run: sync_adjustments.py --tabs 'Content - SLAs'."
        )
    out = (
        adj_df.select(
            F.lower(F.trim(F.col("job_type"))).alias("job_type"),
            F.col("sla_seconds").cast("long").alias("sla_seconds"),
        )
        .filter(F.col("job_type").isNotNull() & (F.col("sla_seconds") > 0))
        .dropDuplicates(["job_type"])
    )
    if len(out.take(1)) == 0:
        raise ValueError(
            "adj_content_slas parsed to zero rows — check the 'Content - SLAs' tab "
            "(expected columns 'Job Type' and 'SLA Seconds')."
        )
    return out


def compute_jobs_within_sla(
    oos_jobs: DataFrame,
    agent_info: DataFrame,
    sla_map: DataFrame,
) -> DataFrame:
    """End-to-end jobs_within_sla pipeline (one row per Content OOS job)."""
    jobs = oos_jobs.select(
        F.col("agent"),
        F.to_date(F.col("date")).alias("date"),
        F.col("local_start_date"),
        _normalize_job_type(F.col("job_classification")).alias("job_type"),
        F.col("net_time_spent_seconds").cast("long").alias("net_seconds"),
        _content_id(F.col("comment"), F.col("ticket__id")).alias("content_id"),
    )

    # Source-level date scoping (before content_id grouping) — see docstring.
    cal = F.col("date")
    jobs = jobs.filter(
        (cal >= F.lit(SLA_START_DATE)) & (~cal.isin(list(SLA_EXCLUDED_DATES)))
    )

    # --- grain split -------------------------------------------------------
    is_row = F.col("job_type").isin(list(ROW_GRAIN_JOB_TYPES))
    row_jobs = jobs.filter(is_row).select(
        "agent",
        "job_type",
        "date",
        F.col("net_seconds").alias("actual_seconds"),
        F.lit(None).cast("string").alias("content_id"),
    )
    content_jobs = (
        jobs.filter(~is_row & F.col("content_id").isNotNull())
        .groupBy("agent", "job_type", "content_id")
        .agg(
            F.sum("net_seconds").alias("actual_seconds"),
            F.min("local_start_date").alias("start_ts"),
        )
        .select(
            "agent",
            "job_type",
            F.to_date(F.col("start_ts")).alias("date"),
            "actual_seconds",
            "content_id",
        )
    )
    all_jobs = row_jobs.unionByName(content_jobs)

    # --- SLA map INNER JOIN (drops no-SLA job types) + on-time flags -------
    all_jobs = all_jobs.join(F.broadcast(sla_map), on="job_type", how="inner")
    within = F.col("actual_seconds") <= F.col("sla_seconds")
    all_jobs = all_jobs.withColumn("within_sla", within.cast("int")).withColumn(
        "sla_met_seconds",
        F.when(within, F.col("sla_seconds")).otherwise(F.lit(0)).cast("long"),
    )

    # --- roster join (Content only) + dedup --------------------------------
    roster = agent_info.filter(F.lower(F.col("team")) == F.lit(CONTENT_TEAM)).select(
        "agent",
        "xforce",
        "xplead",
        "team",
        "squad",
        F.col("squad_district").alias("district"),
        "shift",
        F.col("status").alias("roster_status"),
        F.trunc(F.to_date(F.col("snapshot_month")), "month").alias("snapshot_month"),
    )
    # One row per (agent, snapshot_month): the Google-Sheet Content roster
    # cross-joins each agent against every month and can carry >1 row per
    # (agent, month) that differ only on unused columns (target_squad). Dedup as
    # jobs_raw does, to avoid fanning out a content agent's jobs.
    dedup = Window.partitionBy("agent", "snapshot_month").orderBy(
        F.col("roster_status").asc_nulls_last(),
        F.col("squad").asc_nulls_last(),
        F.col("district").asc_nulls_last(),
        F.col("shift").asc_nulls_last(),
    )
    roster = (
        roster.withColumn("_rn", F.row_number().over(dedup))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    all_jobs = all_jobs.withColumn("snapshot_month", F.trunc(F.col("date"), "month"))
    enriched = all_jobs.join(
        roster, on=["agent", "snapshot_month"], how="inner"
    ).drop("snapshot_month")

    out = enriched.select(
        "agent",
        "xforce",
        "xplead",
        "team",
        "squad",
        "district",
        "shift",
        "roster_status",
        F.col("date"),
        "job_type",
        "content_id",
        F.col("actual_seconds").cast("long").alias("actual_seconds"),
        F.col("sla_seconds").cast("long").alias("sla_seconds"),
        F.col("within_sla").cast("int").alias("within_sla"),
        F.col("sla_met_seconds").cast("long").alias("sla_met_seconds"),
    )
    return out.orderBy("date", "agent", "job_type")


IO_JOBS_WITHIN_SLA_SCHEMA: tuple[tuple[str, str], ...] = (
    ("agent", "STRING"),
    ("xforce", "STRING"),
    ("xplead", "STRING"),
    ("team", "STRING"),
    ("squad", "STRING"),
    ("district", "STRING"),
    ("shift", "STRING"),
    ("roster_status", "STRING"),
    ("date", "DATE"),
    ("job_type", "STRING"),
    ("content_id", "STRING"),
    ("actual_seconds", "BIGINT"),
    ("sla_seconds", "BIGINT"),
    ("within_sla", "INT"),
    ("sla_met_seconds", "BIGINT"),
)
