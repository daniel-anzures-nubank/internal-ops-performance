"""Data-quality check primitives + per-extractor specs.

This module is intentionally transport-free: every function takes a
``pandas.DataFrame`` and returns a :class:`CheckResult`. Tests can construct
DataFrames in-memory and exercise the primitives directly without any
Databricks connection.

To add a check for a new extractor:
    1. Append a new :class:`ExtractorSpec` to :data:`EXTRACTOR_SPECS`.
    2. That's it — :func:`run_checks_for_extractor` iterates the spec generically.

To add a new *kind* of check (e.g. cardinality, regex match, foreign-key):
    1. Add a field to :class:`ExtractorSpec`.
    2. Add a small ``check_*`` function below.
    3. Wire it into :func:`run_checks_for_extractor`.
"""

from __future__ import annotations

import dataclasses

import pandas as pd


# ---------------------------------------------------------------------------
# Result + spec types
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class CheckResult:
    extractor: str
    check: str
    severity: str  # "ERROR" | "WARN"
    passed: bool
    detail: str


@dataclasses.dataclass(frozen=True)
class ExtractorSpec:
    """Declares what we expect to be true about an extractor's output."""

    name: str
    """Filename without ``.sql``, e.g. ``dime_slots``."""

    unique_keys: tuple[tuple[str, ...], ...] = ()
    """Each tuple is a column combination that MUST form a unique key."""

    not_null: tuple[str, ...] = ()
    """Columns that MUST have zero NULLs. Failure is ERROR."""

    not_null_warn: tuple[str, ...] = ()
    """Columns we *track* for NULLs but tolerate. Failure is WARN.

    Use this for source-side data-quality issues we've explicitly decided to
    let through (e.g. `productivity.activity_end_unix` — the legacy pipeline
    silently neutralizes NULL endpoints, so blocking on them is over-strict).
    """

    value_in_range: tuple[tuple[str, float, float], ...] = ()
    """(column, inclusive_min, inclusive_max) — values outside the range are WARN."""

    temporal_order: tuple[tuple[str, str], ...] = ()
    """(start_col, end_col) where end_col >= start_col must always hold."""

    min_rows: int = 1
    """Below this row count is an ERROR (catches an empty-period or auth issue)."""

    notes: str = ""
    """Free-form context for whoever reads a failure."""


# ---------------------------------------------------------------------------
# Per-extractor specs — one entry per `.sql` file under extractors/
# (coverage enforced by tests/test_checks.py::test_specs_cover_every_extractor_sql)
# ---------------------------------------------------------------------------


EXTRACTOR_SPECS: tuple[ExtractorSpec, ...] = (
    ExtractorSpec(
        name="agent_information",
        unique_keys=(("agent", "snapshot_month"),),
        not_null=("agent", "snapshot_month", "snapshot_date"),
        notes=(
            "MAX(actor__id) dedup means (agent, snapshot_month) MUST be unique. "
            "A failure here means ops_actors started returning multiple rows we "
            "didn't anticipate, or BDX is yielding duplicate snapshots per month."
        ),
    ),
    ExtractorSpec(
        name="dime_slots",
        unique_keys=(("agent", "local_timestamp_dime_slot_starts_at"),),
        not_null=(
            "agent",
            "date",
            "local_timestamp_dime_slot_starts_at",
            "slot_start_local_unix",
            "slot_end_local_unix",
        ),
        temporal_order=(("slot_start_local_unix", "slot_end_local_unix"),),
        notes=(
            "Slot uniqueness directly affects metric correctness: if DIME ever "
            "produces overlapping slots for one agent, the time-range JOIN in "
            "the metric layer double-counts adherent seconds."
        ),
    ),
    ExtractorSpec(
        name="productivity",
        unique_keys=(("actor_id", "timestamp"),),
        not_null=(
            "agent",
            "actor_id",
            "timestamp",
            "activity_start_unix",
        ),
        not_null_warn=("activity_end_unix",),
        temporal_order=(("activity_start_unix", "activity_end_unix"),),
        notes=(
            "One row per activity start per actor. Duplicates here would "
            "fan-out the slot/activity overlap join in metrics/adherence. "
            "`activity_end_unix` is WARN, not ERROR: the source carries a "
            "small fraction (~0.001-0.003%) of rows with NULL `next_event_time`, "
            "and the legacy adherence pipeline lets them through and "
            "silently contributes 0 adherent time via "
            "`COALESCE(LEAST(activity_end, slot_end) - GREATEST(...), 0)`."
        ),
    ),
    ExtractorSpec(
        name="shuffle_jobs",
        # No uniqueness assumed: an agent can have back-to-back jobs starting
        # at the exact same recorded second when timestamps round.
        not_null=(
            "agent",
            "date",
            "local_start_time",
            "status",
            "net_time_spent_seconds",
        ),
        # 86400 = 24h. A single shuffle job spanning > 24h is almost certainly
        # a clock/timezone artifact, not a real job.
        value_in_range=(("net_time_spent_seconds", 0, 86400),),
        temporal_order=(("activity_start_unix", "activity_end_unix"),),
    ),
    ExtractorSpec(
        name="oos_jobs",
        not_null=(
            "agent",
            "date",
            "local_start_date",
            "net_time_spent_seconds",
        ),
        value_in_range=(("net_time_spent_seconds", 0, 86400),),
        temporal_order=(("activity_start_unix", "activity_end_unix"),),
    ),
    ExtractorSpec(
        name="playvox_evaluations",
        unique_keys=(("evaluation_id",),),
        not_null=("evaluation_id", "agent", "created_at"),
        value_in_range=(("qa_score", 0, 100),),
    ),
    ExtractorSpec(
        name="content_csat",
        # No uniqueness assumed: the sheet has no response id, and two squad
        # reps can submit in the same second.
        not_null=("survey_timestamp", "date_reference"),
        not_null_warn=("email_address", "requested_by", "squad"),
        value_in_range=(
            ("facilidad", 1, 5),
            ("comprension", 1, 5),
            ("comunicacion", 1, 5),
            ("calidad", 1, 5),
            ("tiempo", 1, 5),
            ("manejo_de_cambios", 1, 5),
            ("expectativas", 1, 5),
            ("aportacion_estrategica", 1, 5),
            ("nps", 0, 10),
        ),
        notes=(
            "Feeds the Content Quality (CSAT) metric via metrics_data/content_csat. "
            "Google Sheet source, so respondent attribution (email_address, the "
            "requested_by regex prefix, squad) is WARN — a hand-typed sheet can "
            "legitimately leave cells blank or unparseable. The 8 question scores "
            "are 1-5 and `nps` is a separate 0-10 score per the extractor header. "
            "Severities are provisional pending a live-data run against June 2026."
        ),
    ),
    ExtractorSpec(
        name="sm_jobs",
        # No uniqueness assumed: an agent can be assigned two cases at the
        # same recorded second (same reasoning as shuffle_jobs).
        not_null=(
            "date",
            "case_assignment_time",
            "case_unassignment_time",
            "activity_start_unix",
            "activity_end_unix",
            "net_time_spent_seconds",
        ),
        not_null_warn=("agent",),
        # Mirrors the oos_jobs precedent: a single case assignment spanning
        # > 24h is almost certainly a clock/timezone artifact, not real work.
        value_in_range=(("net_time_spent_seconds", 0, 86400),),
        temporal_order=(("activity_start_unix", "activity_end_unix"),),
        notes=(
            "Feeds Social-Media occupancy in metrics_data/occupancy_time (the SM "
            "equivalent of oos_jobs). The SQL already drops NULL assignment/"
            "unassignment times, so the timestamp/unix columns are ERROR. `agent` "
            "is WARN: it is regex-extracted from `agent_email_id`, which the "
            "source can leave NULL/unparseable for unattributed assignments. "
            "Severities are provisional pending a live-data run against June 2026."
        ),
    ),
    ExtractorSpec(
        name="sprinklr_sm_evaluations",
        # The extractor itself dedups to rn = 1 per case_number, so
        # evaluation_id MUST be unique (analogous to playvox_evaluations).
        unique_keys=(("evaluation_id",),),
        not_null=("evaluation_id", "created_at"),
        not_null_warn=("agent",),
        # Same 0-100 scale as the playvox_evaluations spec (verified on live data
        # per the extractor header).
        value_in_range=(("qa_score", 0, 100),),
        notes=(
            "Feeds the Quality metric for Social Media (UNION with Playvox in "
            "metrics_data/quality_evaluations). `agent` is WARN, not ERROR: it is "
            "mapped agent_name -> email via the sprinklr_sm_users LEFT JOIN, so "
            "unmapped agents legitimately yield NULL/empty and are dropped "
            "downstream in build_evaluations. Severities are provisional pending "
            "a live-data run against June 2026."
        ),
    ),
    ExtractorSpec(
        name="tnps_responses",
        # No uniqueness assumed: case_number can repeat (multiple survey rows
        # per case) — the metrics layer dedups via COUNT(DISTINCT case_number).
        not_null=("case_number", "date", "case_closure_time"),
        not_null_warn=("agent", "agent_email_id", "survey_response_date"),
        value_in_range=(("survey_score", 0, 10),),
        notes=(
            "Feeds the SM Human tNPS metric via metrics_data/tnps_responses. "
            "`case_closure_time` is the period filter so it (and the derived "
            "`date`) must be present. `agent`/`agent_email_id` are WARN: the "
            "extractor header documents unattributed responses (empty prefix). "
            "`survey_response_date` is WARN — unsure whether the source can carry "
            "unanswered/late rows; `survey_score` is documented nullable, and "
            "range-check ignores NULLs (0-10 NPS scale). Severities are "
            "provisional pending a live-data run against June 2026."
        ),
    ),
    ExtractorSpec(
        name="wows",
        # No uniqueness assumed: case_id can repeat across rows — the metric is
        # COUNT(DISTINCT case_id) downstream, so duplicates are expected.
        not_null=("date",),
        not_null_warn=("agent", "agent_email", "case_id"),
        notes=(
            "Feeds the SM WoWs count metric via metrics_data/wows. Google Sheet "
            "source, so everything but the period-filtering `date` is WARN — the "
            "sheet's agent/case_id cells can legitimately be blank or carry an "
            "unparseable email (empty regex prefix). A NULL case_id would silently "
            "shrink COUNT(DISTINCT case_id), hence tracking it. Severities are "
            "provisional pending a live-data run against June 2026."
        ),
    ),
)


# ---------------------------------------------------------------------------
# Check primitives — each returns a single CheckResult
# ---------------------------------------------------------------------------


def check_min_rows(df: pd.DataFrame, spec: ExtractorSpec) -> CheckResult:
    n = len(df)
    return CheckResult(
        extractor=spec.name,
        check="min_rows",
        severity="ERROR",
        passed=n >= spec.min_rows,
        detail=f"{n:,} rows (expected >= {spec.min_rows:,})",
    )


def check_unique(
    df: pd.DataFrame, spec: ExtractorSpec, columns: tuple[str, ...]
) -> CheckResult:
    label = f"unique({', '.join(columns)})"
    missing = [c for c in columns if c not in df.columns]
    if missing:
        return CheckResult(spec.name, label, "ERROR", False, f"missing columns: {missing}")

    n_total = len(df)
    n_unique = df.drop_duplicates(subset=list(columns)).shape[0]
    n_dups = n_total - n_unique
    return CheckResult(
        extractor=spec.name,
        check=label,
        severity="ERROR",
        passed=n_dups == 0,
        detail=(
            f"{n_dups:,} duplicate rows on key"
            if n_dups
            else f"0 dupes across {n_total:,} rows"
        ),
    )


def check_not_null(
    df: pd.DataFrame, spec: ExtractorSpec, column: str, severity: str = "ERROR"
) -> CheckResult:
    label = f"not_null({column})"
    if column not in df.columns:
        return CheckResult(spec.name, label, severity, False, "column missing from output")
    n_null = int(df[column].isna().sum())
    return CheckResult(
        extractor=spec.name,
        check=label,
        severity=severity,
        passed=n_null == 0,
        detail=(
            f"{n_null:,} NULL out of {len(df):,}"
            if n_null
            else f"0 NULLs across {len(df):,} rows"
        ),
    )


def check_value_in_range(
    df: pd.DataFrame, spec: ExtractorSpec, column: str, lo: float, hi: float
) -> CheckResult:
    label = f"value_in_range({column}, [{lo}, {hi}])"
    if column not in df.columns:
        return CheckResult(spec.name, label, "WARN", False, "column missing from output")
    series = pd.to_numeric(df[column], errors="coerce").dropna()
    if series.empty:
        return CheckResult(spec.name, label, "WARN", True, "no non-null numeric values")
    n_out = int(((series < lo) | (series > hi)).sum())
    return CheckResult(
        extractor=spec.name,
        check=label,
        severity="WARN",
        passed=n_out == 0,
        detail=(
            f"{n_out:,} out-of-range; observed min={series.min()}, max={series.max()}"
            if n_out
            else f"observed min={series.min()}, max={series.max()}"
        ),
    )


def check_temporal_order(
    df: pd.DataFrame, spec: ExtractorSpec, start_col: str, end_col: str
) -> CheckResult:
    label = f"temporal_order({end_col} >= {start_col})"
    if start_col not in df.columns or end_col not in df.columns:
        return CheckResult(spec.name, label, "ERROR", False, "column missing from output")
    mask = df[end_col] < df[start_col]
    n_bad = int(mask.sum())
    return CheckResult(
        extractor=spec.name,
        check=label,
        severity="ERROR",
        passed=n_bad == 0,
        detail=f"{n_bad:,} rows where {end_col} < {start_col}" if n_bad else "OK",
    )


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


def run_checks_for_extractor(df: pd.DataFrame, spec: ExtractorSpec) -> list[CheckResult]:
    """Run every check in a spec against a DataFrame; return all results."""
    results: list[CheckResult] = [check_min_rows(df, spec)]
    for cols in spec.unique_keys:
        results.append(check_unique(df, spec, cols))
    for col in spec.not_null:
        results.append(check_not_null(df, spec, col, severity="ERROR"))
    for col in spec.not_null_warn:
        results.append(check_not_null(df, spec, col, severity="WARN"))
    for col, lo, hi in spec.value_in_range:
        results.append(check_value_in_range(df, spec, col, lo, hi))
    for start_col, end_col in spec.temporal_order:
        results.append(check_temporal_order(df, spec, start_col, end_col))
    return results


# ---------------------------------------------------------------------------
# Suggestions for future checks (not implemented — discuss before adding)
# ---------------------------------------------------------------------------
#
# Cross-extractor / referential
# -----------------------------
# * Every `agent` in dime_slots / productivity / shuffle_jobs / oos_jobs /
#   *_evaluations also appears in agent_information for the relevant snapshot
#   month. (WARN: legacy tolerates "ghost agents" — would mostly surface
#   roster lag and SQL-typo regressions.)
#
# Volume / drift
# --------------
# * Row count vs. trailing 4-week median per extractor, with a tolerance band
#   (e.g. >25% drop is WARN, >50% drop is ERROR).
# * Distinct-agent count per day vs. trailing median.
#
# Coverage
# --------
# * Every date in [period_start, period_end] has at least one row in
#   dime_slots and productivity.
# * Every active agent in agent_information has at least one DIME slot.
#
# Cardinality / categorical
# -------------------------
# * shuffle_jobs.status, productivity.inferred_status only take values from
#   a known finite set; alert on unseen values.
#
# Source-specific
# ---------------
# * productivity.inferred_status NULL rate < some threshold.
# * dime_slots: (slot_end_local_unix - slot_start_local_unix) == 1800 for
#   every row (DIME slots are exactly 30 minutes by definition).
