"""Unit tests for run_id resolution and validation in ``db``.

Pure Python — no SparkSession, no IO. ``resolve_run_id`` interpolates the id
into the snapshot DELETE statement, so operator-supplied values (the explicit
``--run-id`` argument and the ``PIPELINE_RUN_ID`` env var) must be restricted
to a safe alphabet; generated ids are safe by construction.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

import db

RUN_TS = datetime(2026, 7, 1, 12, 0, 0)


@pytest.fixture(autouse=True)
def _clear_run_id_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep each test independent of any PIPELINE_RUN_ID in the environment."""
    monkeypatch.delenv(db.RUN_ID_ENV_VAR, raising=False)


# ---------------------------------------------------------------------------
# Generated ids
# ---------------------------------------------------------------------------


def test_new_run_id_matches_safe_alphabet() -> None:
    run_id = db.new_run_id(RUN_TS)
    assert db._RUN_ID_RE.match(run_id)


def test_new_run_id_passes_resolve_run_id() -> None:
    run_id = db.new_run_id(RUN_TS)
    assert db.resolve_run_id(run_id, RUN_TS) == run_id


def test_resolve_run_id_generates_when_nothing_supplied() -> None:
    run_id = db.resolve_run_id(None, RUN_TS)
    assert run_id.startswith("20260701T120000Z-")
    assert db._RUN_ID_RE.match(run_id)


# ---------------------------------------------------------------------------
# Explicit argument path
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "run_id",
    [
        "20260701T120000Z-ab12",
        "release.2026-07-01",
        "run:2026",
        "123456789",  # Databricks numeric job run id
    ],
)
def test_valid_explicit_run_id_passes_through(run_id: str) -> None:
    assert db.resolve_run_id(run_id, RUN_TS) == run_id


@pytest.mark.parametrize(
    "run_id",
    [
        "o'brien",
        "run id with spaces",
        "x'; DROP TABLE usr.danielanzures.pipeline_runs; --",
        "line\nbreak",
    ],
)
def test_invalid_explicit_run_id_raises(run_id: str) -> None:
    with pytest.raises(ValueError, match=r"--run-id"):
        db.resolve_run_id(run_id, RUN_TS)


# ---------------------------------------------------------------------------
# Env var path
# ---------------------------------------------------------------------------


def test_valid_env_run_id_is_used(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(db.RUN_ID_ENV_VAR, "20260701T120000Z-env1")
    assert db.resolve_run_id(None, RUN_TS) == "20260701T120000Z-env1"


def test_invalid_env_run_id_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(db.RUN_ID_ENV_VAR, "bad'id")
    with pytest.raises(ValueError, match=r"PIPELINE_RUN_ID"):
        db.resolve_run_id(None, RUN_TS)


def test_explicit_run_id_beats_env_var(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(db.RUN_ID_ENV_VAR, "from-env")
    assert db.resolve_run_id("from-arg", RUN_TS) == "from-arg"
