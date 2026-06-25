"""Unit tests for ``metrics/wows_metric.py``.

Small synthetic frames mimicking ``io_wows_raw``, no warehouse. WoWs is a COUNT
metric: ``metric_value`` is the number of distinct ``case_id`` per agent per
period (denominator carries the monthly target 5). We verify the distinct count,
the day/week/month bucketing, team scope, dimensions, and the output contract.
"""

from __future__ import annotations

import datetime as dt

import pandas as pd

from wows_metric import (
    IO_WOWS_METRIC_SCHEMA,
    METRIC_NAME,
    MONTHLY_TARGET,
    compute_wows,
)

LOGGED = dt.date(2026, 5, 4)  # a Monday


def make_wow(**o) -> dict:
    base = {
        "agent": "nuberto.lopez",
        "xforce": "nuliana.cruz",
        "xplead": "nuricio.diaz",
        "team": "social media",
        "squad": "social_es",
        "district": "social",
        "shift": "morning",
        "date": LOGGED,
        "case_id": "W1",
    }
    base.update(o)
    return base


def make_raw(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame([make_wow(**r) for r in rows])


def _day(out: pd.DataFrame) -> pd.DataFrame:
    return out[out["date_granularity"] == "day"]


class TestComputeWows:
    def test_counts_distinct_case_ids(self):
        out = _day(compute_wows(make_raw([
            {"case_id": "W1"}, {"case_id": "W2"}, {"case_id": "W3"},
        ])))
        row = out.iloc[0]
        assert row["numerator"] == 3.0
        assert row["metric_value"] == 3.0
        assert row["denominator"] == float(MONTHLY_TARGET)
        assert row["metric"] == METRIC_NAME

    def test_duplicate_case_id_counted_once(self):
        out = _day(compute_wows(make_raw([
            {"case_id": "W1"}, {"case_id": "W1"}, {"case_id": "W2"},
        ])))
        assert out.iloc[0]["metric_value"] == 2.0

    def test_metric_value_equals_numerator_not_ratio(self):
        # WoWs is a count: metric_value must be the count, not numerator/den*100.
        out = _day(compute_wows(make_raw([{"case_id": f"W{i}"} for i in range(7)])))
        row = out.iloc[0]
        assert row["metric_value"] == 7.0
        assert row["numerator"] == 7.0

    def test_weekly_aggregates_across_days(self):
        out = compute_wows(make_raw([
            {"date": dt.date(2026, 5, 4), "case_id": "W1"},
            {"date": dt.date(2026, 5, 5), "case_id": "W2"},
            {"date": dt.date(2026, 5, 6), "case_id": "W3"},
        ]))
        week = out[out["date_granularity"] == "week"].iloc[0]
        assert week["date_reference"] == dt.date(2026, 5, 4)
        assert week["metric_value"] == 3.0

    def test_non_social_team_excluded(self):
        out = compute_wows(make_raw([{"team": "core", "case_id": "W1"}]))
        assert out.empty

    def test_all_granularities_emitted(self):
        out = compute_wows(make_raw([{}]))
        assert set(out["date_granularity"]) == {
            "day", "week", "month", "quarter", "semester", "year"
        }

    def test_per_agent_separation(self):
        out = _day(compute_wows(make_raw([
            {"agent": "a.one", "case_id": "W1"},
            {"agent": "a.one", "case_id": "W2"},
            {"agent": "b.two", "case_id": "W3"},
        ])))
        by_agent = out.set_index("agent")["metric_value"]
        assert by_agent.loc["a.one"] == 2.0
        assert by_agent.loc["b.two"] == 1.0

    def test_dimensions_take_latest_value_in_bucket(self):
        out = compute_wows(make_raw([
            {"date": dt.date(2026, 5, 4), "case_id": "W1", "squad": "social_es"},
            {"date": dt.date(2026, 5, 20), "case_id": "W2", "squad": "social_pt"},
        ]))
        month = out[out["date_granularity"] == "month"].iloc[0]
        assert month["squad"] == "social_pt"

    def test_output_schema_and_column_order(self):
        out = compute_wows(make_raw([{}]))
        assert list(out.columns) == [c for c, _ in IO_WOWS_METRIC_SCHEMA]

    def test_empty_input_yields_empty_frame_with_schema(self):
        out = compute_wows(make_raw([])[0:0])
        assert out.empty
        assert list(out.columns) == [c for c, _ in IO_WOWS_METRIC_SCHEMA]
