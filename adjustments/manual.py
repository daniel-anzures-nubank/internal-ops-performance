"""PySpark helpers for the manual-adjustment layer.

The adjustment tabs (from the Google Sheet) are synced to small Delta tables —
one per tab — by ``scripts/adjustments_scripts/sync_adjustments.py``. These
helpers read those tables and apply each adjustment to the (large) metric/raw
Spark DataFrames.

Because the adjustment tables are tiny config (a handful of rows), we collect
them to the driver and build Spark ``Column`` predicates row-by-row — this mirrors
the original pandas row-iteration semantics exactly (OR across rows) without an
expensive join.

Column-name convention
----------------------
Delta/Parquet dislikes spaces and parentheses in column names, so the sync step
normalizes the Spanish sheet headers to snake_case (``Fecha Inicio`` ->
``fecha_inicio``, ``Job (Clasificación)`` -> ``job_clasificacion``). These
helpers read those normalized names.
"""

from __future__ import annotations

from datetime import date
from typing import Any

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

# Default location of the synced adjustment Delta tables: one table per tab,
# named ``{ADJUSTMENT_SCHEMA}.adj_{name}``.
ADJUSTMENT_SCHEMA = "usr.danielanzures"
ADJUSTMENT_TABLE_PREFIX = "adj_"

# Normalized (snake_case) column names the sync step writes.
COL_AGENT = "agente"
COL_TEAM = "equipo"
COL_DATE_START = "fecha_inicio"
COL_DATE_END = "fecha_fin"
COL_TIME_START = "hora_inicio"
COL_TIME_END = "hora_fin"
COL_LABEL = "etiqueta_correcta"
COL_QUEUES = "queues_a_excluir"
COL_JOB = "job_clasificacion"
COL_DIMENSIONED_ACTIVITY = "actividad_dimensionada"


def adjustment_table_name(name: str, schema: str = ADJUSTMENT_SCHEMA) -> str:
    return f"{schema}.{ADJUSTMENT_TABLE_PREFIX}{name}"


def read_adjustment_table(
    spark: SparkSession, name: str, schema: str = ADJUSTMENT_SCHEMA
) -> DataFrame | None:
    """Read one synced adjustment Delta table, or ``None`` if it doesn't exist."""
    table = adjustment_table_name(name, schema)
    if not spark.catalog.tableExists(table):
        return None
    return spark.table(table)


# ---------------------------------------------------------------------------
# Small parsing helpers (driver-side, on collected config rows)
# ---------------------------------------------------------------------------


def _rows(adjustments: DataFrame | None) -> list[dict[str, Any]]:
    if adjustments is None:
        return []
    return [r.asDict() for r in adjustments.collect()]


def _to_date(value: object) -> date:
    return date.fromisoformat(str(value).strip()[:10])


def _time_to_minutes(value: object) -> int:
    h, m = str(value).strip()[:5].split(":", maxsplit=1)
    return int(h) * 60 + int(m)


def _slot_minutes_col(df: DataFrame) -> Column:
    parts = F.split(F.col("slot_time"), ":")
    return parts.getItem(0).cast("int") * 60 + parts.getItem(1).cast("int")


def _row_window_mask(df: DataFrame, row: dict[str, Any]) -> Column:
    date_col = F.to_date(F.col("date"))
    date_ok = (date_col >= F.lit(_to_date(row[COL_DATE_START]))) & (
        date_col <= F.lit(_to_date(row[COL_DATE_END]))
    )
    if "slot_time" not in df.columns:
        return date_ok
    slot_minutes = _slot_minutes_col(df)
    start_min = _time_to_minutes(row[COL_TIME_START])
    end_min = _time_to_minutes(row[COL_TIME_END])
    # 23:59 is the full-day sentinel: treat as end-of-day so the 23:30 slot is
    # included while normal windows remain half-open.
    end_exclusive = 24 * 60 if end_min == 23 * 60 + 59 else end_min
    return date_ok & (slot_minutes >= start_min) & (slot_minutes < end_exclusive)


def _str(value: object) -> str:
    return "" if value is None else str(value).strip()


def _scope_mask(df: DataFrame, row: dict[str, Any]) -> Column:
    mask = F.lit(True)
    team = _str(row.get(COL_TEAM)).lower()
    if team and team != "todos" and "team" in df.columns:
        mask = mask & (F.lower(F.col("team")) == team)

    agent = _str(row.get(COL_AGENT))
    agent_l = agent.lower()
    if not agent_l or agent_l == "todos":
        return mask
    if agent_l.startswith("todos (xplead:") and "xplead" in df.columns:
        xplead = agent.split(":", maxsplit=1)[1].rstrip(")").strip().lower()
        return mask & (F.lower(F.col("xplead")) == xplead)
    if agent_l.startswith("todos (squad") and "squad" in df.columns:
        squad = agent.split("squad", maxsplit=1)[1].rstrip(")").strip().lower()
        return mask & F.lower(F.col("squad")).contains(squad)
    if "agent" in df.columns:
        return mask & (F.lower(F.col("agent")) == agent_l)
    return mask


def _combined_window_mask(df: DataFrame, rows: list[dict[str, Any]]) -> Column:
    mask = F.lit(False)
    for row in rows:
        mask = mask | (_scope_mask(df, row) & _row_window_mask(df, row))
    return mask


# ---------------------------------------------------------------------------
# Slot-window adjustments (Exclusiones Generales / Training / Shadowing,
# Inconsistencias DIME, No Shrinkage)
# ---------------------------------------------------------------------------


def reclassify_dime_slots(
    slots: DataFrame, inconsistencies: DataFrame | None
) -> DataFrame:
    rows = _rows(inconsistencies)
    if not rows:
        return slots
    out = slots
    has_shrinkage = "shrinkage_flag" in out.columns
    extra_flags = [
        c
        for c in ("controllable_shrinkage_flag", "uncontrollable_shrinkage_flag")
        if c in out.columns
    ]
    for row in rows:
        label = _str(row.get(COL_LABEL))
        if not label:
            continue
        mask = _scope_mask(out, row) & _row_window_mask(out, row)
        out = out.withColumn(
            "activity_type_required",
            F.when(mask, F.lit(label)).otherwise(F.col("activity_type_required")),
        )
        if has_shrinkage:
            out = out.withColumn(
                "shrinkage_flag",
                F.when(mask, F.lit(int(label == "shrinkage"))).otherwise(
                    F.col("shrinkage_flag")
                ),
            )
            if label != "shrinkage":
                for col in extra_flags:
                    out = out.withColumn(
                        col, F.when(mask, F.lit(0)).otherwise(F.col(col))
                    )
    return out


def drop_slot_windows(df: DataFrame, *adjustments: DataFrame | None) -> DataFrame:
    mask = F.lit(False)
    any_rows = False
    for adj in adjustments:
        rows = _rows(adj)
        if rows:
            any_rows = True
            mask = mask | _combined_window_mask(df, rows)
    if not any_rows:
        return df
    return df.filter(~mask)


def apply_no_shrinkage(df: DataFrame, no_shrinkage: DataFrame | None) -> DataFrame:
    rows = _rows(no_shrinkage)
    if not rows:
        return df
    mask = _combined_window_mask(df, rows)
    out = df.withColumn(
        "shrinkage_flag", F.when(mask, F.lit(0)).otherwise(F.col("shrinkage_flag"))
    )
    for col in ("controllable_shrinkage_flag", "uncontrollable_shrinkage_flag"):
        if col in out.columns:
            out = out.withColumn(col, F.when(mask, F.lit(0)).otherwise(F.col(col)))
    return out


# ---------------------------------------------------------------------------
# Job-window adjustments (Cross Support / Exclusiones Jobs)
# ---------------------------------------------------------------------------


def _job_date_mask(df: DataFrame, row: dict[str, Any]) -> Column:
    date_col = F.to_date(F.col("date"))
    return (date_col >= F.lit(_to_date(row[COL_DATE_START]))) & (
        date_col <= F.lit(_to_date(row[COL_DATE_END]))
    )


def _contains_any(text: Column, values: list[str]) -> Column:
    mask = F.lit(False)
    lowered = F.lower(text)
    for value in values:
        value = value.strip().lower()
        if value:
            mask = mask | lowered.contains(value)
    return mask


def _normalized_queue_col(job_type: Column) -> Column:
    """Normalize a shuffle ``job_type`` (``received_source_q``) to the sheet's
    hyphenated queue token, mirroring legacy ``[IO] NTPJ Dataset.sql`` line 406:

        LOWER(REPLACE(REPLACE(received_source_q, 'incredible_machine__', ''), '_', '-'))

    The raw shuffle queue is prefixed + underscored
    (``incredible_machine__backoffice_payment_srf``) while the sheet's
    ``queues_a_excluir`` is stripped + hyphenated (``backoffice-payment-srf``).
    Without this normalization the values never match and the exclusion silently
    drops nothing — leaving cross-support jobs in both the agent contribution AND
    the shared monthly ``exp_duration_job`` benchmark pool.
    """
    return F.regexp_replace(
        F.regexp_replace(F.lower(job_type), "incredible_machine__", ""),
        "_",
        "-",
    )


def _normalized_queue_matches_any(job_type: Column, values: list[str]) -> Column:
    """True when the normalized ``job_type`` token EQUALS any of ``values``.

    Legacy joins on equality of the normalized source-queue token to the
    excluded queue (``... = excl.queue``), not a loose substring match, so we
    reproduce exact-token equality here.
    """
    normalized = _normalized_queue_col(job_type)
    mask = F.lit(False)
    for value in values:
        value = value.strip().lower()
        if value:
            mask = mask | (normalized == F.lit(value))
    return mask


def drop_cross_support_jobs(
    jobs: DataFrame, cross_support: DataFrame | None
) -> DataFrame:
    rows = _rows(cross_support)
    if not rows:
        return jobs
    # Match the sheet's hyphenated queue against the normalized ``job_type``
    # token (legacy compares the normalized ``received_source_q`` to
    # ``excl.queue`` by equality — see ``_normalized_queue_matches_any``).
    drop = F.lit(False)
    for row in rows:
        queues = _str(row.get(COL_QUEUES)).splitlines()
        drop = drop | (
            _scope_mask(jobs, row)
            & _job_date_mask(jobs, row)
            & _normalized_queue_matches_any(F.col("job_type"), queues)
        )
    return jobs.filter(~drop)


def drop_excluded_jobs(jobs: DataFrame, exclusions: DataFrame | None) -> DataFrame:
    rows = _rows(exclusions)
    if not rows:
        return jobs
    # NOTE: ``exclusiones_jobs`` targets OOS content classifications (free text
    # with spaces / parentheses, e.g. ``99 Minute - Carrier reports (OOS_LCYC)``),
    # NOT the ``incredible_machine__`` shuffle queue. Legacy matches these against
    # the raw ``job_classification`` in ``oos_jobs_ntpj`` (lines 454-459), so the
    # cross-support queue normalization is deliberately NOT applied here — it would
    # mangle the parenthesized OOS token and stop these exclusions from matching.
    # We keep the original loose substring match against the combined job text.
    text = F.concat_ws(" ", F.col("job_id"), F.col("job_type"))
    drop = F.lit(False)
    for row in rows:
        job = _str(row.get(COL_JOB))
        drop = drop | (
            _scope_mask(jobs, row) & _job_date_mask(jobs, row) & _contains_any(text, [job])
        )
    return jobs.filter(~drop)


def drop_reassignment_jobs(
    jobs: DataFrame, reassignments: DataFrame | None
) -> DataFrame:
    """Drop jobs done during a DIME activity the agent was reassigned away from.

    Reproduces legacy ``manual_adjustments_ntpj`` (``[IO] NTPJ Dataset.sql:148-240``):
    an agent temporarily pulled onto a BKO task force (Lifecycle / Cuenta) has the
    jobs they run during the reassigned ``dimensioned_activity`` excluded from
    NTPJ — from BOTH the benchmark pool and the contribution (legacy applies
    ``c.exclude IS NOT TRUE`` in both ``expected_duration_per_job_ntpj`` and
    ``ntpj_initial_base``). Source: the ``Reasignaciones DIME`` sheet tab.

    Per row: drop jobs matching the scope (``equipo``/``agente``) and the date
    window where, if ``actividad_dimensionada`` is set, the job's
    ``dimensioned_activity`` (the DIME slot the job started in — attached in
    ``jobs_raw``) equals it; a BLANK ``actividad_dimensionada`` matches every
    activity (a whole-day exclusion). Applied to the per-job frame BEFORE the
    benchmark / contribution split so excluded jobs leave both.
    """
    rows = _rows(reassignments)
    if not rows:
        return jobs
    drop = F.lit(False)
    for row in rows:
        activity = _str(row.get(COL_DIMENSIONED_ACTIVITY)).lower()
        activity_match = (
            F.lower(F.col("dimensioned_activity")) == F.lit(activity)
            if activity
            else F.lit(True)
        )
        drop = drop | (
            _scope_mask(jobs, row) & _job_date_mask(jobs, row) & activity_match
        )
    return jobs.filter(~drop)


# ---------------------------------------------------------------------------
# Missing DIME slots (committed CSV, appended to the dime extractor output)
# ---------------------------------------------------------------------------


def append_missing_dime_slots(
    dime: DataFrame, missing: DataFrame | None
) -> DataFrame:
    """Append synced missing-DIME-slot rows to the ``dime`` extractor frame.

    ``missing`` is the synced ``io_slots_faltantes_dime`` Delta table (already
    shaped to the dime columns), or ``None``. Columns the dime frame has but the
    missing frame lacks are filled with NULL before the union.
    """
    if missing is None:
        return dime
    aligned = missing
    for col in dime.columns:
        if col not in aligned.columns:
            aligned = aligned.withColumn(col, F.lit(None))
    aligned = aligned.select(*dime.columns)
    return dime.unionByName(aligned)
