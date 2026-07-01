"""Spark transport — shared by every script in this repo.

This module is the *only* place that talks to Spark / Delta for table IO. Both
the DQ checker and the metric-build scripts use it. Keeping the IO in one place
means the pure ``metrics_data/`` and ``metrics/`` modules stay free of any
session/catalog concerns — they just take a Spark ``DataFrame`` and return one.

Execution model
---------------
The pipeline runs **on Databricks**, where an ambient ``SparkSession`` and Delta
are always available. :func:`get_spark` returns that session (``getOrCreate``),
so the same code runs unchanged on a cluster, in a Databricks job, or against a
local Spark for tests.

There is no SQL-warehouse / PAT / OAuth handling anymore: on a cluster the
session is already authenticated. ``.env`` is still loaded at import so the
off-cluster Google-Sheets sync and any optional config can read ``os.environ``.
"""

from __future__ import annotations

import logging
import os
import re
import secrets
import subprocess
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Iterable

# python-dotenv is only needed for off-cluster local runs (to load the gitignored
# .env). On a Databricks cluster it isn't installed and there's no .env — the
# SparkSession is already authenticated — so the import is best-effort.
try:
    from dotenv import load_dotenv
except ModuleNotFoundError:  # pragma: no cover - cluster has no python-dotenv
    load_dotenv = None

if TYPE_CHECKING:  # avoid importing pyspark at module import for `--help`/lint
    from pyspark.sql import DataFrame, SparkSession

REPO_ROOT = Path(__file__).resolve().parent
EXTRACTORS_DIR = REPO_ROOT / "extractors"

# Load workspace settings from the repo-root .env (gitignored). Loaded at import
# time so anything that imports this module can rely on os.environ. No-op on a
# cluster (dotenv absent / no .env file).
if load_dotenv is not None:
    load_dotenv(REPO_ROOT / ".env")

LOGGER = logging.getLogger("cx_metrics.db")

# Suffix appended to a base table to hold its append-only run history, and the
# central registry of every run. Both live alongside the data in the
# `usr.danielanzures` schema. See `publish()` and the "Run snapshots" section of
# CLAUDE.md.
SNAPSHOT_SUFFIX = "_snapshots"
DEFAULT_REGISTRY_TABLE = "usr.danielanzures.pipeline_runs"
# Env var an orchestrator can export once so every `build_*.py` in the same
# pipeline invocation shares a single run_id (instead of each minting its own).
RUN_ID_ENV_VAR = "PIPELINE_RUN_ID"


# ---------------------------------------------------------------------------
# Session
# ---------------------------------------------------------------------------


def get_spark() -> "SparkSession":
    """Return the active SparkSession (the ambient one on Databricks).

    Lazy-imports pyspark so `--help` and non-Spark code paths don't require it.
    """
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:  # pragma: no cover - environment dependent
        raise SystemExit(
            "pyspark is not available. Run this on a Databricks cluster, or "
            "install the dev group (`uv sync --group dev`) on a Python 3.11/3.12 "
            "interpreter to run off-cluster."
        ) from exc

    spark = SparkSession.builder.getOrCreate()
    # The slot-time / night-shift math assumes a UTC session tz (that's how
    # `timestamp_seconds` renders the local wall clock). Set it here so the
    # result is identical on any compute — including serverless, where the
    # cluster-level `spark_conf` can't be set and this is the only knob.
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    return spark


# Backwards-compatible alias: scripts historically called `open_connection()`.
# The "connection" is now just the SparkSession; there is nothing to close.
def open_connection() -> "SparkSession":
    return get_spark()


# ---------------------------------------------------------------------------
# Read — run an extractor SQL file, return a Spark DataFrame
# ---------------------------------------------------------------------------


def run_extractor(
    spark: "SparkSession", name: str, period_start: date, period_end: date
) -> "DataFrame":
    """Execute one ``extractors/{name}.sql`` and return a Spark DataFrame.

    The extractors are parameterized with ``:period_start`` / ``:period_end``.
    We substitute them with explicit ``DATE 'YYYY-MM-DD'`` literals (safe: both
    are ``datetime.date``), so the same SQL runs on any Spark/DBR version
    regardless of named-parameter support.
    """
    sql_path = EXTRACTORS_DIR / f"{name}.sql"
    if not sql_path.is_file():
        raise FileNotFoundError(f"No such extractor: {sql_path}")
    sql_text = _bind_period(sql_path.read_text(), period_start, period_end)
    return spark.sql(sql_text)


def _bind_period(sql_text: str, period_start: date, period_end: date) -> str:
    """Replace ``:period_start`` / ``:period_end`` with DATE literals."""
    return sql_text.replace(
        ":period_start", f"DATE '{period_start.isoformat()}'"
    ).replace(":period_end", f"DATE '{period_end.isoformat()}'")


# ---------------------------------------------------------------------------
# Read — load a built table (the metrics layer's input)
# ---------------------------------------------------------------------------


def read_table(
    spark: "SparkSession",
    table: str,
    period_start: date | None = None,
    period_end: date | None = None,
    date_col: str = "date",
) -> "DataFrame":
    """Read a Delta table, optionally scoped to a date window.

    The raw layer reads ``extractors/*.sql`` via :func:`run_extractor`; the
    metrics layer instead reads the already-built ``io_*_raw`` tables — that's
    what this helper is for.
    """
    from pyspark.sql import functions as F

    df = spark.table(table)
    if period_start is not None and period_end is not None:
        df = df.filter(
            (F.col(date_col) >= F.lit(period_start))
            & (F.col(date_col) <= F.lit(period_end))
        )
    return df


# ---------------------------------------------------------------------------
# Write — persist a Spark DataFrame to a Delta table
# ---------------------------------------------------------------------------


def _apply_schema(df: "DataFrame", schema: list[tuple[str, str]]) -> "DataFrame":
    """Select + cast ``df`` to the declared (name, spark_type) schema, in order.

    This enforces column presence, order, and type so the written table is
    stable across runs even if an upstream DataFrame's column types drift.
    """
    from pyspark.sql import functions as F

    expected = [c for c, _ in schema]
    missing = [c for c in expected if c not in df.columns]
    if missing:
        raise ValueError(
            f"DataFrame is missing columns required by the schema: {missing}. "
            f"Has: {df.columns}"
        )
    return df.select(*[F.col(c).cast(t).alias(c) for c, t in schema])


def write_dataframe(
    spark: "SparkSession",
    df: "DataFrame",
    table: str,
    schema: Iterable[tuple[str, str]],
) -> int:
    """Replace ``table`` with the contents of ``df`` (Delta overwrite).

    Uses ``overwrite`` + ``overwriteSchema`` (instead of DROP + CREATE) so the
    Delta transaction log / version history is preserved across runs. Returns
    the number of rows written.
    """
    schema = list(schema)
    out = _apply_schema(df, schema).persist()
    try:
        row_count = out.count()
        LOGGER.info("Replacing %s (%d rows incoming)", table, row_count)
        (
            out.write.mode("overwrite")
            .option("overwriteSchema", "true")
            .format("delta")
            .saveAsTable(table)
        )
    finally:
        out.unpersist()
    return row_count


# ---------------------------------------------------------------------------
# Run snapshots & registry
# ---------------------------------------------------------------------------
#
# Every build script writes two things per run: the *current* table (replaced in
# place by `write_dataframe`) and, when snapshotting is on, an append-only
# `{table}_snapshots` history table tagged with `run_id` + `run_ts`. A central
# `pipeline_runs` registry records one row per (run_id, table) write so a run can
# be looked up, audited, and diffed against another. `publish()` ties the three
# together; build scripts call it instead of `write_dataframe` directly.


@dataclass(frozen=True)
class PublishResult:
    """Outcome of a :func:`publish` call."""

    run_id: str
    run_ts: datetime
    row_count: int
    table: str
    snapshot_table: str | None


def utc_now() -> datetime:
    """Current UTC time as a naive datetime (UTC wall clock)."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


# run_id is interpolated into SQL and used as a partition value, so restrict its alphabet.
_RUN_ID_RE = re.compile(r"^[A-Za-z0-9._:\-]+$")


def new_run_id(run_ts: datetime | None = None) -> str:
    """Generate a lexicographically-sortable run id, e.g. ``20260616T201500Z-a1b2``."""
    run_ts = run_ts or utc_now()
    return f"{run_ts.strftime('%Y%m%dT%H%M%SZ')}-{secrets.token_hex(2)}"


def resolve_run_id(run_id: str | None, run_ts: datetime) -> str:
    """Pick the run id: explicit arg > ``PIPELINE_RUN_ID`` env var > generated.

    Operator-supplied ids (the explicit ``--run-id`` argument and the
    ``PIPELINE_RUN_ID`` env var) must match ``[A-Za-z0-9._:-]+`` — the id is
    interpolated into the snapshot DELETE statement and used as a Delta
    partition value, so anything else raises ``ValueError``. Generated ids are
    safe by construction and pass through unvalidated.
    """
    if run_id:
        if not _RUN_ID_RE.match(run_id):
            raise ValueError(
                f"invalid run_id from --run-id: {run_id!r}, "
                "must match [A-Za-z0-9._:-]+"
            )
        return run_id
    env_run_id = os.environ.get(RUN_ID_ENV_VAR)
    if env_run_id:
        if not _RUN_ID_RE.match(env_run_id):
            raise ValueError(
                f"invalid run_id from {RUN_ID_ENV_VAR} env var: {env_run_id!r}, "
                "must match [A-Za-z0-9._:-]+"
            )
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
    spark: "SparkSession",
    df: "DataFrame",
    snapshot_table: str,
    schema: list[tuple[str, str]],
    run_id: str,
    run_ts: datetime,
) -> None:
    """Append ``df`` to ``{table}_snapshots``, tagged with run_id + run_ts.

    Creates the history table on first use (base schema + ``run_id`` / ``run_ts``,
    partitioned by ``run_id``). Idempotent per run: re-running the same ``run_id``
    first deletes that run's partition, so a retried build never double-counts.
    """
    from pyspark.sql import functions as F

    snap = (
        _apply_schema(df, schema)
        .withColumn("run_id", F.lit(run_id).cast("string"))
        .withColumn("run_ts", F.lit(run_ts).cast("timestamp"))
    )

    # Idempotent rerun: clear any prior rows for this run_id (no-op if new).
    # run_id is safe to interpolate: operator-supplied values are validated
    # against _RUN_ID_RE in resolve_run_id.
    if spark.catalog.tableExists(snapshot_table):
        spark.sql(
            f"DELETE FROM {snapshot_table} WHERE run_id = '{run_id}'"
        )

    # ``mergeSchema`` lets the append tolerate additive schema evolution (a new
    # column appearing in a metric, e.g. ``roster_status`` in ``io_jobs_raw``)
    # without a destructive drop+recreate of the history table — old rows simply
    # get NULL for the new column.
    (
        snap.write.mode("append")
        .option("mergeSchema", "true")
        .partitionBy("run_id")
        .format("delta")
        .saveAsTable(snapshot_table)
    )
    LOGGER.info("  snapshot %s appended (run_id=%s)", snapshot_table, run_id)


def _record_run(
    spark: "SparkSession",
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
    """Append one row to the central ``pipeline_runs`` registry."""
    from pyspark.sql import types as T

    # Build with an EXPLICIT schema: the single row has NULLs (git_sha, notes,
    # ...), so Spark (esp. Spark Connect) can't infer column types from the data
    # and raises CANNOT_DETERMINE_TYPE. An explicit schema also avoids relying on
    # Row(**kwargs) field ordering. Positional values must match the order below.
    type_map = {
        "STRING": T.StringType(),
        "TIMESTAMP": T.TimestampType(),
        "DATE": T.DateType(),
        "BIGINT": T.LongType(),
    }
    registry_schema = [
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
    struct = T.StructType(
        [T.StructField(name, type_map[sql_type]) for name, sql_type in registry_schema]
    )
    values = [
        (
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
        )
    ]
    df = spark.createDataFrame(values, struct)
    df.write.mode("append").format("delta").saveAsTable(registry_table)


def publish(
    spark: "SparkSession",
    df: "DataFrame",
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
) -> PublishResult:
    """Write ``df`` to the current table, snapshot it, and record the run.

    Performs, in order:
      1. Replace the current ``table`` with ``df`` (:func:`write_dataframe`).
      2. If ``snapshot``: append ``df`` to ``{table}{snapshot_suffix}`` tagged
         with ``run_id`` + ``run_ts`` (idempotent per run_id).
      3. Append a ``success`` row to the ``pipeline_runs`` registry.

    A crash before step 3 leaves no registry row for the run, so an incomplete
    run is detectable (and re-runnable with the same ``run_id``).
    """
    schema = list(schema)
    run_ts = run_ts or utc_now()
    run_id = resolve_run_id(run_id, run_ts)
    if git_sha is None:
        git_sha = current_git_sha()

    row_count = write_dataframe(spark, df, table, schema)

    snapshot_table = f"{table}{snapshot_suffix}" if snapshot else None
    if snapshot_table is not None:
        _append_snapshot(spark, df, snapshot_table, schema, run_id, run_ts)

    _record_run(
        spark,
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
