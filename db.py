"""Databricks SQL warehouse transport — shared by every script in this repo.

This module is the *only* place that imports `databricks.sql`. Both the DQ
checker and the metric-build scripts use it. Keeping the connection logic in
one place means:

* OAuth/PAT handling is a single source of truth.
* When the project later migrates to Databricks-native execution
  (`spark.sql(...)`), only this file's body changes — every caller keeps
  working unchanged.
* `.env` loading happens once, at import time, so scripts can read
  `os.environ` directly.

Auth modes (matches the `agent-deep-dives` notebooks):
    * `DATABRICKS_TOKEN` set     -> PAT auth.
    * `DATABRICKS_TOKEN` unset   -> OAuth U2M (browser). First run pops a tab;
                                    the SDK caches the token at
                                    ~/.config/databricks-sdk-py/.
"""

from __future__ import annotations

import logging
import os
import secrets
import subprocess
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent
EXTRACTORS_DIR = REPO_ROOT / "extractors"

# Load workspace settings from the repo-root .env (gitignored). Loaded at
# import time so anything that imports this module can rely on os.environ.
load_dotenv(REPO_ROOT / ".env")

LOGGER = logging.getLogger("cx_metrics.db")

# Suffix appended to a base table to hold its append-only run history, and the
# central registry of every run. Both live alongside the data in the
# `usr.danielanzures` schema. See `publish()` and the "Run snapshots" section of
# AGENTS.md.
SNAPSHOT_SUFFIX = "_snapshots"
DEFAULT_REGISTRY_TABLE = "usr.danielanzures.pipeline_runs"
# Env var an orchestrator can export once so every `build_*.py` in the same
# pipeline invocation shares a single run_id (instead of each minting its own).
RUN_ID_ENV_VAR = "PIPELINE_RUN_ID"


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------


def open_connection():
    """Open a Databricks SQL connection from env vars.

    Lazy-imports the connector so `--help` and unit tests don't pull it in.
    """
    try:
        from databricks import sql
    except ImportError as exc:
        raise SystemExit(
            "databricks-sql-connector is not installed. "
            "Run `uv sync` to install project dependencies."
        ) from exc

    server = os.environ.get("DATABRICKS_SERVER_HOSTNAME")
    http_path = os.environ.get("DATABRICKS_HTTP_PATH")
    if not server or not http_path:
        raise SystemExit(
            "Missing DATABRICKS_SERVER_HOSTNAME and/or DATABRICKS_HTTP_PATH. "
            "Set them in a .env file at the repo root (see .env.example)."
        )

    token = os.environ.get("DATABRICKS_TOKEN")
    if token:
        LOGGER.info("Connecting to %s via PAT auth", server)
        return sql.connect(
            server_hostname=server,
            http_path=http_path,
            access_token=token,
        )

    LOGGER.info("Connecting to %s via OAuth U2M (browser on first run)", server)
    return sql.connect(
        server_hostname=server,
        http_path=http_path,
        auth_type="databricks-oauth",
    )


# ---------------------------------------------------------------------------
# Read — run an extractor SQL file, return pandas
# ---------------------------------------------------------------------------


def run_extractor(
    conn, name: str, period_start: date, period_end: date
) -> pd.DataFrame:
    """Execute one ``extractors/{name}.sql`` and return the result as pandas.

    Parameters are bound as `:period_start` / `:period_end` named placeholders.
    """
    sql_path = EXTRACTORS_DIR / f"{name}.sql"
    if not sql_path.is_file():
        raise FileNotFoundError(f"No such extractor: {sql_path}")
    sql_text = sql_path.read_text()
    with conn.cursor() as cur:
        cur.execute(
            sql_text,
            parameters={"period_start": period_start, "period_end": period_end},
        )
        return cur.fetchall_arrow().to_pandas()


# ---------------------------------------------------------------------------
# Read — load a built table (the metrics layer's input)
# ---------------------------------------------------------------------------


def read_table(
    conn,
    table: str,
    period_start: date | None = None,
    period_end: date | None = None,
    date_col: str = "date",
) -> pd.DataFrame:
    """Read a Delta table into pandas, optionally scoped to a date window.

    The raw layer reads ``extractors/*.sql`` via :func:`run_extractor`; the
    metrics layer instead reads the already-built ``io_*_raw`` tables — that's
    what this helper is for.

    Args:
        conn: open connection from :func:`open_connection`.
        table: fully-qualified source table, e.g.
            ``usr.danielanzures.io_adherent_time_raw``.
        period_start / period_end: inclusive bounds on ``date_col``. If either
            is ``None`` the whole table is read.
        date_col: the DATE column to filter on (default ``date``).

    Returns:
        The table contents as a pandas DataFrame.
    """
    if period_start is not None and period_end is not None:
        sql_text = (
            f"SELECT * FROM {table} "
            f"WHERE `{date_col}` BETWEEN :period_start AND :period_end"
        )
        params = {"period_start": period_start, "period_end": period_end}
    else:
        sql_text = f"SELECT * FROM {table}"
        params = None

    with conn.cursor() as cur:
        cur.execute(sql_text, parameters=params)
        return cur.fetchall_arrow().to_pandas()


# ---------------------------------------------------------------------------
# Write — persist a pandas DataFrame back to a Delta table
# ---------------------------------------------------------------------------


def write_dataframe(
    conn,
    df: pd.DataFrame,
    table: str,
    schema: Iterable[tuple[str, str]],
    batch_size: int = 1_000,
) -> int:
    """Replace `table` with the contents of `df`.

    Drops the existing table (if any), creates a Delta table with the explicit
    schema, then inserts rows in batches. Returns the number of rows written.

    Why explicit schema rather than inferring from the DataFrame?
        * pandas dtypes don't map cleanly to Spark types (e.g. `object` could
          be STRING or BINARY; `datetime64[ns]` could be TIMESTAMP or DATE).
        * Callers know their target schema and should declare it. This keeps
          column types stable across runs even if a DataFrame's dtype shifts.

    Args:
        conn: open connection from `open_connection()`.
        df: source DataFrame. Column order MUST match `schema`.
        table: fully-qualified target name, e.g. `usr.danielanzures.io_adherence`.
        schema: ordered iterable of (column_name, spark_type) tuples,
            e.g. `[("agent", "STRING"), ("delivered_hours", "BIGINT")]`.
        batch_size: rows per `executemany` call. 1k is a reasonable default
            for the SQL warehouse; bump up for very small rows.

    Returns:
        Number of rows inserted (== len(df)).
    """
    schema = list(schema)
    expected_cols = [c for c, _ in schema]
    if list(df.columns) != expected_cols:
        raise ValueError(
            f"DataFrame columns {list(df.columns)} do not match schema column "
            f"order {expected_cols}. Reorder df before calling."
        )

    cols_ddl = ", ".join(f"`{c}` {t}" for c, t in schema)
    col_list = "`, `".join(c for c, _ in schema)

    with conn.cursor() as cur:
        LOGGER.info("Replacing %s (%d rows incoming)", table, len(df))
        # CREATE OR REPLACE (instead of DROP + CREATE) keeps the Delta
        # transaction log / version history intact across runs.
        cur.execute(f"CREATE OR REPLACE TABLE {table} ({cols_ddl}) USING DELTA")

        if df.empty:
            return 0

        records = [tuple(r) for r in df.itertuples(index=False, name=None)]
        _insert_records_sql(
            cur,
            table=table,
            col_list=col_list,
            schema=schema,
            records=records,
            batch_size=batch_size,
            log_label="inserted",
        )

    return len(df)


def _sql_literal(value: Any, spark_type: str) -> str:
    """Render one Python/pandas scalar as a Spark SQL literal."""
    if value is None or pd.isna(value):
        return "NULL"

    typ = spark_type.upper()
    if typ == "DATE":
        return f"DATE '{pd.Timestamp(value).date().isoformat()}'"
    if typ == "TIMESTAMP":
        ts = pd.Timestamp(value)
        if ts.tzinfo is not None:
            ts = ts.tz_convert(None)
        return f"TIMESTAMP '{ts.strftime('%Y-%m-%d %H:%M:%S')}'"
    if typ in {"STRING", "VARCHAR", "CHAR"}:
        escaped = str(value).replace("'", "''")
        return f"'{escaped}'"
    if typ == "BOOLEAN":
        return "TRUE" if bool(value) else "FALSE"
    return str(value)


def _insert_records_sql(
    cur,
    *,
    table: str,
    col_list: str,
    schema: list[tuple[str, str]],
    records: list[tuple[Any, ...]],
    batch_size: int,
    log_label: str,
) -> None:
    """Insert records using multi-row SQL VALUES chunks.

    The Databricks connector's ``executemany`` sends many tiny requests for our
    workloads. A single VALUES statement per chunk is much faster for the
    pipeline-sized pandas frames this project writes locally.
    """
    if not records:
        return
    types = [t for _, t in schema]
    for i in range(0, len(records), batch_size):
        chunk = records[i : i + batch_size]
        values_sql = ", ".join(
            "("
            + ", ".join(_sql_literal(value, typ) for value, typ in zip(row, types))
            + ")"
            for row in chunk
        )
        cur.execute(f"INSERT INTO {table} (`{col_list}`) VALUES {values_sql}")
        LOGGER.info(
            "  %s %d / %d rows",
            log_label,
            min(i + batch_size, len(records)),
            len(records),
        )


# ---------------------------------------------------------------------------
# Run snapshots & registry
# ---------------------------------------------------------------------------
#
# Every build script writes two things per run: the *current* table (replaced
# in place by `write_dataframe`) and, when snapshotting is on, an append-only
# `{table}_snapshots` history table tagged with `run_id` + `run_ts`. A central
# `pipeline_runs` registry records one row per (run_id, table) write so a run
# can be looked up, audited, and diffed against another. `publish()` ties the
# three together; build scripts call it instead of `write_dataframe` directly.


@dataclass(frozen=True)
class PublishResult:
    """Outcome of a :func:`publish` call."""

    run_id: str
    run_ts: datetime
    row_count: int
    table: str
    snapshot_table: str | None


def utc_now() -> datetime:
    """Current UTC time as a naive datetime (UTC wall clock).

    Naive (tz-stripped) so it binds cleanly to a Spark ``TIMESTAMP`` parameter;
    the value is always UTC by construction.
    """
    return datetime.now(timezone.utc).replace(tzinfo=None)


def new_run_id(run_ts: datetime | None = None) -> str:
    """Generate a lexicographically-sortable run id, e.g. ``20260616T201500Z-a1b2``.

    The timestamp prefix makes ids sort chronologically; the short random suffix
    avoids collisions when two runs start in the same second.
    """
    run_ts = run_ts or utc_now()
    return f"{run_ts.strftime('%Y%m%dT%H%M%SZ')}-{secrets.token_hex(2)}"


def resolve_run_id(run_id: str | None, run_ts: datetime) -> str:
    """Pick the run id: explicit arg > ``PIPELINE_RUN_ID`` env var > generated.

    Lets an orchestrator export one id for a whole pipeline invocation while
    standalone script runs still get a unique id automatically.
    """
    if run_id:
        return run_id
    env_run_id = os.environ.get(RUN_ID_ENV_VAR)
    if env_run_id:
        return env_run_id
    return new_run_id(run_ts)


def current_git_sha() -> str | None:
    """Best-effort short git SHA of the repo, or ``None`` if unavailable."""
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=5,
            check=True,
        )
        return out.stdout.strip() or None
    except (subprocess.SubprocessError, OSError):
        return None


def _append_snapshot(
    conn,
    df: pd.DataFrame,
    snapshot_table: str,
    schema: list[tuple[str, str]],
    run_id: str,
    run_ts: datetime,
    batch_size: int,
) -> None:
    """Append `df` to `{table}_snapshots`, tagged with run_id + run_ts.

    Creates the history table on first use (base schema + `run_id` / `run_ts`,
    partitioned by `run_id`). Idempotent per run: re-running the same `run_id`
    first deletes that run's partition, so a retried build never double-counts.
    """
    snap_cols = list(schema) + [("run_id", "STRING"), ("run_ts", "TIMESTAMP")]
    cols_ddl = ", ".join(f"`{c}` {t}" for c, t in snap_cols)
    col_list = "`, `".join(c for c, _ in snap_cols)

    with conn.cursor() as cur:
        cur.execute(
            f"CREATE TABLE IF NOT EXISTS {snapshot_table} ({cols_ddl}) "
            f"USING DELTA PARTITIONED BY (run_id)"
        )
        # Idempotent rerun: clear any prior rows for this run_id.
        cur.execute(
            f"DELETE FROM {snapshot_table} WHERE run_id = ?", [run_id]
        )
        if df.empty:
            LOGGER.info("  snapshot %s: 0 rows (empty result)", snapshot_table)
            return

        records: list[tuple[Any, ...]] = [
            (*r, run_id, run_ts)
            for r in df.itertuples(index=False, name=None)
        ]
        _insert_records_sql(
            cur,
            table=snapshot_table,
            col_list=col_list,
            schema=snap_cols,
            records=records,
            batch_size=batch_size,
            log_label="snapshot inserted",
        )
        LOGGER.info(
            "  snapshot %s: appended %d rows (run_id=%s)",
            snapshot_table,
            len(records),
            run_id,
        )


def _record_run(
    conn,
    *,
    registry_table: str,
    run_id: str,
    run_ts: datetime,
    layer: str | None,
    table: str,
    snapshot_table: str | None,
    period_start: date | None,
    period_end: date | None,
    row_count: int,
    status: str,
    git_sha: str | None,
    notes: str | None,
) -> None:
    """Append one row to the central `pipeline_runs` registry."""
    cols = [
        ("run_id", "STRING"),
        ("run_ts", "TIMESTAMP"),
        ("layer", "STRING"),
        ("table_name", "STRING"),
        ("snapshot_table", "STRING"),
        ("period_start", "DATE"),
        ("period_end", "DATE"),
        ("row_count", "BIGINT"),
        ("status", "STRING"),
        ("git_sha", "STRING"),
        ("notes", "STRING"),
    ]
    cols_ddl = ", ".join(f"`{c}` {t}" for c, t in cols)
    col_list = "`, `".join(c for c, _ in cols)
    placeholders = ", ".join(["?"] * len(cols))
    with conn.cursor() as cur:
        cur.execute(
            f"CREATE TABLE IF NOT EXISTS {registry_table} ({cols_ddl}) USING DELTA"
        )
        cur.execute(
            f"INSERT INTO {registry_table} (`{col_list}`) VALUES ({placeholders})",
            [
                run_id,
                run_ts,
                layer,
                table,
                snapshot_table,
                period_start,
                period_end,
                int(row_count),
                status,
                git_sha,
                notes,
            ],
        )


def publish(
    conn,
    df: pd.DataFrame,
    table: str,
    schema: Iterable[tuple[str, str]],
    *,
    layer: str | None = None,
    period_start: date | None = None,
    period_end: date | None = None,
    run_id: str | None = None,
    run_ts: datetime | None = None,
    snapshot: bool = True,
    snapshot_suffix: str = SNAPSHOT_SUFFIX,
    registry_table: str = DEFAULT_REGISTRY_TABLE,
    git_sha: str | None = None,
    notes: str | None = None,
    batch_size: int = 1_000,
) -> PublishResult:
    """Write `df` to the current table, snapshot it, and record the run.

    This is the high-level writer build scripts call (instead of
    :func:`write_dataframe` directly). It performs, in order:

      1. Replace the current ``table`` with ``df`` (:func:`write_dataframe`).
      2. If ``snapshot``: append ``df`` to ``{table}{snapshot_suffix}`` tagged
         with ``run_id`` + ``run_ts`` (idempotent per run_id).
      3. Append a ``success`` row to the ``pipeline_runs`` registry.

    The run id is resolved via :func:`resolve_run_id` (explicit arg >
    ``PIPELINE_RUN_ID`` env var > generated), so all tables written in one
    orchestrated invocation can share a single id. ``run_ts`` is one UTC instant
    for the whole call.

    A crash before step 3 leaves no registry row for the run, so an incomplete
    run is detectable (and re-runnable with the same ``run_id``).

    Returns a :class:`PublishResult` with the resolved ``run_id`` / ``run_ts``,
    row count, and the snapshot table name (``None`` when snapshotting is off).
    """
    schema = list(schema)
    run_ts = run_ts or utc_now()
    run_id = resolve_run_id(run_id, run_ts)
    if git_sha is None:
        git_sha = current_git_sha()

    row_count = write_dataframe(conn, df, table, schema, batch_size=batch_size)

    snapshot_table = f"{table}{snapshot_suffix}" if snapshot else None
    if snapshot_table is not None:
        _append_snapshot(
            conn, df, snapshot_table, schema, run_id, run_ts, batch_size
        )

    _record_run(
        conn,
        registry_table=registry_table,
        run_id=run_id,
        run_ts=run_ts,
        layer=layer,
        table=table,
        snapshot_table=snapshot_table,
        period_start=period_start,
        period_end=period_end,
        row_count=row_count,
        status="success",
        git_sha=git_sha,
        notes=notes,
    )

    return PublishResult(
        run_id=run_id,
        run_ts=run_ts,
        row_count=row_count,
        table=table,
        snapshot_table=snapshot_table,
    )
