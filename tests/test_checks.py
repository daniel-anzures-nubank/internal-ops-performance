"""Unit tests for the data-quality check primitives.

These tests build small DataFrames in memory and exercise each primitive in
both a passing and a failing configuration. No Databricks, no Spark, no IO —
just pandas. Run them with::

    uv run pytest

The goal is a fast, deterministic feedback loop while we iterate on the
check semantics. End-to-end runs against real extractor output happen via
``scripts/check_extractor_data_quality.py`` once env vars are configured.
"""

from __future__ import annotations

import pandas as pd
import pytest

from checks import (
    EXTRACTOR_SPECS,
    ExtractorSpec,
    check_min_rows,
    check_not_null,
    check_temporal_order,
    check_unique,
    check_value_in_range,
    run_checks_for_extractor,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def spec() -> ExtractorSpec:
    """A spec used by tests that don't care about the specifics of the spec."""
    return ExtractorSpec(name="test_extractor")


# ---------------------------------------------------------------------------
# check_min_rows
# ---------------------------------------------------------------------------


def test_min_rows_passes_when_at_least_one_row(spec: ExtractorSpec) -> None:
    df = pd.DataFrame({"a": [1]})
    result = check_min_rows(df, spec)
    assert result.passed
    assert result.severity == "ERROR"


def test_min_rows_fails_on_empty_dataframe(spec: ExtractorSpec) -> None:
    df = pd.DataFrame({"a": []})
    result = check_min_rows(df, spec)
    assert not result.passed
    assert "0 rows" in result.detail


def test_min_rows_respects_higher_threshold() -> None:
    spec = ExtractorSpec(name="x", min_rows=10)
    df = pd.DataFrame({"a": [1, 2, 3]})
    result = check_min_rows(df, spec)
    assert not result.passed


# ---------------------------------------------------------------------------
# check_unique
# ---------------------------------------------------------------------------


def test_unique_passes_when_no_duplicates(spec: ExtractorSpec) -> None:
    df = pd.DataFrame({"agent": ["a", "b", "c"], "month": [1, 1, 1]})
    result = check_unique(df, spec, ("agent", "month"))
    assert result.passed


def test_unique_fails_when_duplicates_present(spec: ExtractorSpec) -> None:
    df = pd.DataFrame({"agent": ["a", "a", "b"], "month": [1, 1, 1]})
    result = check_unique(df, spec, ("agent", "month"))
    assert not result.passed
    assert "1 duplicate" in result.detail


def test_unique_reports_missing_columns(spec: ExtractorSpec) -> None:
    df = pd.DataFrame({"agent": ["a"]})
    result = check_unique(df, spec, ("agent", "month"))
    assert not result.passed
    assert "missing columns" in result.detail


# ---------------------------------------------------------------------------
# check_not_null
# ---------------------------------------------------------------------------


def test_not_null_passes_with_full_column(spec: ExtractorSpec) -> None:
    df = pd.DataFrame({"agent": ["a", "b"]})
    result = check_not_null(df, spec, "agent")
    assert result.passed


def test_not_null_fails_when_nulls_present(spec: ExtractorSpec) -> None:
    df = pd.DataFrame({"agent": ["a", None, "c"]})
    result = check_not_null(df, spec, "agent")
    assert not result.passed
    assert "1 NULL" in result.detail


def test_not_null_reports_missing_column(spec: ExtractorSpec) -> None:
    df = pd.DataFrame({"x": [1]})
    result = check_not_null(df, spec, "agent")
    assert not result.passed
    assert "missing" in result.detail


def test_not_null_respects_warn_severity_override(spec: ExtractorSpec) -> None:
    df = pd.DataFrame({"end": [1, None, 3]})
    result = check_not_null(df, spec, "end", severity="WARN")
    assert not result.passed
    assert result.severity == "WARN"


def test_run_checks_emits_warn_for_not_null_warn_field() -> None:
    """`not_null_warn` columns produce WARN-severity results, not ERROR."""
    spec = ExtractorSpec(
        name="x",
        not_null=("a",),
        not_null_warn=("b",),
    )
    df = pd.DataFrame({"a": [1, 2, 3], "b": [1, None, 3]})
    results = run_checks_for_extractor(df, spec)
    b_result = next(r for r in results if r.check == "not_null(b)")
    assert not b_result.passed
    assert b_result.severity == "WARN"
    a_result = next(r for r in results if r.check == "not_null(a)")
    assert a_result.passed
    assert a_result.severity == "ERROR"


# ---------------------------------------------------------------------------
# check_value_in_range
# ---------------------------------------------------------------------------


def test_value_in_range_passes_when_all_values_in_bounds(spec: ExtractorSpec) -> None:
    df = pd.DataFrame({"qa_score": [0, 50, 100]})
    result = check_value_in_range(df, spec, "qa_score", 0, 100)
    assert result.passed


def test_value_in_range_fails_when_value_above_max(spec: ExtractorSpec) -> None:
    df = pd.DataFrame({"qa_score": [50, 101, 99]})
    result = check_value_in_range(df, spec, "qa_score", 0, 100)
    assert not result.passed
    assert result.severity == "WARN"
    assert "1 out-of-range" in result.detail


def test_value_in_range_fails_when_value_below_min(spec: ExtractorSpec) -> None:
    df = pd.DataFrame({"secs": [-1, 100]})
    result = check_value_in_range(df, spec, "secs", 0, 86400)
    assert not result.passed


def test_value_in_range_ignores_nulls(spec: ExtractorSpec) -> None:
    df = pd.DataFrame({"qa_score": [50, None, 75]})
    result = check_value_in_range(df, spec, "qa_score", 0, 100)
    assert result.passed


# ---------------------------------------------------------------------------
# check_temporal_order
# ---------------------------------------------------------------------------


def test_temporal_order_passes_when_end_geq_start(spec: ExtractorSpec) -> None:
    df = pd.DataFrame({"start": [1, 2, 3], "end": [1, 5, 10]})
    result = check_temporal_order(df, spec, "start", "end")
    assert result.passed


def test_temporal_order_fails_when_end_before_start(spec: ExtractorSpec) -> None:
    df = pd.DataFrame({"start": [1, 5, 10], "end": [2, 3, 11]})
    result = check_temporal_order(df, spec, "start", "end")
    assert not result.passed
    assert "1 rows where" in result.detail


# ---------------------------------------------------------------------------
# run_checks_for_extractor — integration of primitives + spec
# ---------------------------------------------------------------------------


def test_run_checks_executes_all_declared_checks() -> None:
    """A spec that exercises every category of check should produce results for each."""
    spec = ExtractorSpec(
        name="x",
        unique_keys=(("a",),),
        not_null=("a",),
        value_in_range=(("b", 0, 100),),
        temporal_order=(("start", "end"),),
    )
    df = pd.DataFrame(
        {"a": [1, 2, 3], "b": [10, 50, 90], "start": [1, 2, 3], "end": [2, 3, 4]}
    )
    results = run_checks_for_extractor(df, spec)
    check_names = {r.check for r in results}
    assert "min_rows" in check_names
    assert any("unique" in n for n in check_names)
    assert any("not_null" in n for n in check_names)
    assert any("value_in_range" in n for n in check_names)
    assert any("temporal_order" in n for n in check_names)
    assert all(r.passed for r in results)


def test_run_checks_surfaces_first_failure_when_unique_key_violated() -> None:
    spec = ExtractorSpec(name="x", unique_keys=(("a",),))
    df = pd.DataFrame({"a": [1, 1, 2]})
    results = run_checks_for_extractor(df, spec)
    unique_results = [r for r in results if "unique" in r.check]
    assert len(unique_results) == 1
    assert not unique_results[0].passed


# ---------------------------------------------------------------------------
# Spec registry — guards against typos in column names
# ---------------------------------------------------------------------------


def test_extractor_specs_have_unique_names() -> None:
    names = [s.name for s in EXTRACTOR_SPECS]
    assert len(names) == len(set(names)), f"Duplicate spec names: {names}"


def test_every_spec_has_at_least_one_check() -> None:
    """A spec with zero checks is almost certainly a mistake."""
    for spec in EXTRACTOR_SPECS:
        has_any = bool(
            spec.unique_keys
            or spec.not_null
            or spec.not_null_warn
            or spec.value_in_range
            or spec.temporal_order
            or spec.min_rows > 0
        )
        assert has_any, f"{spec.name} declares no checks"
