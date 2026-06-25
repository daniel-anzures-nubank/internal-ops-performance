"""Unit tests for ``metrics/average_xforce_index.py``.

Small synthetic ``io_xforce_index_metric`` frames. We verify the per-XPLead
mean, the ``xforce_index`` metric filter, NULL handling, multi-team coverage,
granularity passthrough, and the output contract.
"""

from __future__ import annotations

import datetime as dt

import pandas as pd

from metric_utils import METRIC_COLUMNS
from average_xforce_index import (
    IO_AVERAGE_XFORCE_INDEX_METRIC_SCHEMA,
    METRIC_NAME,
    compute_average_xforce_index,
)

D = dt.date(2026, 5, 1)


def idx(xforce, mv, *, xplead="xp", team="core", dref=D, gran="month",
        metric="xforce_index"):
    return {
        "agent": None, "xforce": xforce, "xplead": xplead, "team": team,
        "squad": None, "district": None, "shift": None,
        "date_reference": dref, "date_granularity": gran, "metric": metric,
        "numerator": 0.0, "denominator": 0.0, "metric_value": mv,
    }


def frame(rows):
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=list(METRIC_COLUMNS))


class TestAverage:
    def test_simple_mean_per_xplead(self):
        out = compute_average_xforce_index(
            frame([idx("A", 80.0), idx("B", 100.0), idx("C", 90.0)])
        )
        assert len(out) == 1
        r = out.iloc[0]
        assert r["metric"] == METRIC_NAME
        assert abs(r["metric_value"] - 90.0) < 1e-9
        assert abs(r["numerator"] - 270.0) < 1e-9
        assert abs(r["denominator"] - 3.0) < 1e-9
        assert r["xplead"] == "xp"
        assert r["agent"] is None and r["xforce"] is None and r["squad"] is None

    def test_separate_xpleads(self):
        out = compute_average_xforce_index(
            frame([idx("A", 80.0, xplead="p1"), idx("B", 60.0, xplead="p2")])
        )
        by = out.set_index("xplead")["metric_value"]
        assert abs(by["p1"] - 80.0) < 1e-9
        assert abs(by["p2"] - 60.0) < 1e-9

    def test_null_index_ignored(self):
        out = compute_average_xforce_index(
            frame([idx("A", 80.0), {**idx("B", 0.0), "metric_value": None}])
        )
        r = out.iloc[0]
        assert abs(r["metric_value"] - 80.0) < 1e-9
        assert abs(r["denominator"] - 1.0) < 1e-9

    def test_other_metrics_filtered_out(self):
        out = compute_average_xforce_index(
            frame([idx("A", 80.0), idx("B", 0.0, metric="something_else")])
        )
        r = out.iloc[0]
        assert abs(r["metric_value"] - 80.0) < 1e-9
        assert abs(r["denominator"] - 1.0) < 1e-9


class TestCoverage:
    def test_all_teams_included(self):
        rows = [
            idx("c", 80.0, team="core", xplead="pc"),
            idx("f", 80.0, team="fraud", xplead="pf"),
            idx("s", 80.0, team="social media", xplead="ps"),
            idx("ct", 80.0, team="content", xplead="pct"),
        ]
        out = compute_average_xforce_index(frame(rows))
        assert set(out["team"]) == {"core", "fraud", "social media", "content"}

    def test_granularities_passthrough(self):
        rows = [idx("A", 80.0, gran=g) for g in
                ("day", "week", "month", "quarter", "semester", "year")]
        out = compute_average_xforce_index(frame(rows))
        assert set(out["date_granularity"]) == {
            "day", "week", "month", "quarter", "semester", "year"
        }


class TestContract:
    def test_output_contract(self):
        out = compute_average_xforce_index(frame([idx("A", 80.0)]))
        assert list(out.columns) == list(METRIC_COLUMNS)
        assert [c for c, _ in IO_AVERAGE_XFORCE_INDEX_METRIC_SCHEMA] == list(
            METRIC_COLUMNS
        )

    def test_empty_returns_empty(self):
        out = compute_average_xforce_index(frame([]))
        assert out.empty
        assert list(out.columns) == list(METRIC_COLUMNS)
