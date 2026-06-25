"""Unit tests for ``metrics/average_xpeer_index.py``.

Small synthetic agent-level Xpeer Index frames. We verify the per-XForce mean,
NULL handling, multi-team coverage, granularity passthrough, the
numerator/denominator convention, and the output contract.
"""

from __future__ import annotations

import datetime as dt

import pandas as pd

from metric_utils import METRIC_COLUMNS
from average_xpeer_index import (
    IO_AVERAGE_XPEER_INDEX_METRIC_SCHEMA,
    METRIC_NAME,
    compute_average_xpeer_index,
)

D = dt.date(2026, 5, 1)


def idx(agent, mv, *, xforce="xf", xplead="xp", team="core", dref=D, gran="month"):
    return {
        "agent": agent,
        "xforce": xforce,
        "xplead": xplead,
        "team": team,
        "squad": "sq",
        "district": "di",
        "shift": "morning",
        "date_reference": dref,
        "date_granularity": gran,
        "metric": "xpeer_index",
        "numerator": 0.0,
        "denominator": 0.0,
        "metric_value": mv,
    }


def frame(rows):
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=list(METRIC_COLUMNS))


class TestAverage:
    def test_simple_mean(self):
        out = compute_average_xpeer_index(
            frame([idx("a", 80.0), idx("b", 100.0), idx("c", 90.0)])
        )
        assert len(out) == 1
        r = out.iloc[0]
        assert r["metric"] == METRIC_NAME
        assert abs(r["metric_value"] - 90.0) < 1e-9
        assert abs(r["numerator"] - 270.0) < 1e-9
        assert abs(r["denominator"] - 3.0) < 1e-9
        assert r["agent"] is None and r["squad"] is None and r["shift"] is None
        assert r["xforce"] == "xf" and r["xplead"] == "xp"

    def test_null_index_ignored(self):
        # NULL metric_value agents are excluded from both mean and count.
        out = compute_average_xpeer_index(
            frame([idx("a", 80.0), {**idx("b", 0.0), "metric_value": None}])
        )
        r = out.iloc[0]
        assert abs(r["metric_value"] - 80.0) < 1e-9
        assert abs(r["denominator"] - 1.0) < 1e-9

    def test_separate_xforces(self):
        out = compute_average_xpeer_index(
            frame([idx("a", 80.0, xforce="A"), idx("b", 60.0, xforce="B")])
        )
        assert len(out) == 2
        by_xf = out.set_index("xforce")["metric_value"]
        assert abs(by_xf["A"] - 80.0) < 1e-9
        assert abs(by_xf["B"] - 60.0) < 1e-9


class TestCoverage:
    def test_all_teams_included(self):
        rows = [
            idx("a", 80.0, team="core", xforce="c"),
            idx("b", 80.0, team="fraud", xforce="f"),
            idx("c", 80.0, team="social media", xforce="s"),
            idx("d", 80.0, team="content", xforce="ct"),
        ]
        out = compute_average_xpeer_index(frame(rows))
        assert set(out["team"]) == {"core", "fraud", "social media", "content"}

    def test_granularities_passthrough(self):
        rows = [idx("a", 80.0, gran=g) for g in
                ("day", "week", "month", "quarter", "semester", "year")]
        out = compute_average_xpeer_index(frame(rows))
        assert set(out["date_granularity"]) == {
            "day", "week", "month", "quarter", "semester", "year"
        }


class TestContract:
    def test_output_contract(self):
        out = compute_average_xpeer_index(frame([idx("a", 80.0)]))
        assert list(out.columns) == list(METRIC_COLUMNS)
        assert [c for c, _ in IO_AVERAGE_XPEER_INDEX_METRIC_SCHEMA] == list(
            METRIC_COLUMNS
        )

    def test_empty_returns_empty(self):
        out = compute_average_xpeer_index(frame([]))
        assert out.empty
        assert list(out.columns) == list(METRIC_COLUMNS)

    def test_all_null_returns_empty(self):
        out = compute_average_xpeer_index(
            frame([{**idx("a", 0.0), "metric_value": None}])
        )
        assert out.empty
