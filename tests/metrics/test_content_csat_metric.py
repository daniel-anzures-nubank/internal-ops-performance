"""Unit tests for ``metrics/content_csat_metric.py``.

Small synthetic frames mimicking ``io_content_csat_raw``, no warehouse. We verify
the CSAT ratio (SUM(promoters) / SUM(number_of_questions) * 100), the day/week/
month aggregation across responses, team scope, dimensions, and the output
contract.
"""

from __future__ import annotations

import datetime as dt

import pandas as pd

from content_csat_metric import (
    IO_CONTENT_CSAT_METRIC_SCHEMA,
    METRIC_NAME,
    compute_content_csat,
)

RATED = dt.date(2026, 5, 1)  # a Friday; month rated


def make_row(**o) -> dict:
    base = {
        "agent": "nuberto.lopez",
        "xforce": "nuliana.cruz",
        "xplead": "nuricio.diaz",
        "team": "content",
        "squad": "enablement",
        "district": "content",
        "shift": None,
        "date": RATED,
        "target_squad": "txn",
        "requested_by": "lead.one",
        "survey_timestamp": pd.Timestamp("2026-05-02 10:00:00"),
        "promoters": 8,
        "number_of_questions": 8,
        "csat_score": 1.0,
    }
    base.update(o)
    return base


def make_raw(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame([make_row(**r) for r in rows])


def _day(out: pd.DataFrame) -> pd.DataFrame:
    return out[out["date_granularity"] == "day"]


class TestComputeContentCsat:
    def test_single_response_ratio(self):
        out = _day(compute_content_csat(make_raw([{"promoters": 6}])))
        row = out.iloc[0]
        assert row["numerator"] == 6.0
        assert row["denominator"] == 8.0
        assert row["metric_value"] == 75.0
        assert row["metric"] == METRIC_NAME

    def test_aggregates_across_responses(self):
        # Legacy example: promoters [7,4,8,7,6] over 8 questions each -> 80%.
        rows = [{"promoters": p} for p in (7, 4, 8, 7, 6)]
        out = _day(compute_content_csat(make_raw(rows)))
        row = out.iloc[0]
        assert row["numerator"] == 32.0
        assert row["denominator"] == 40.0
        assert row["metric_value"] == 80.0

    def test_perfect_and_zero(self):
        out = _day(compute_content_csat(make_raw([
            {"agent": "a.one", "promoters": 8},
            {"agent": "b.two", "promoters": 0},
        ])))
        by_agent = out.set_index("agent")["metric_value"]
        assert by_agent.loc["a.one"] == 100.0
        assert by_agent.loc["b.two"] == 0.0

    def test_non_content_team_excluded(self):
        out = compute_content_csat(make_raw([{"team": "core", "promoters": 8}]))
        assert out.empty

    def test_all_granularities_emitted(self):
        out = compute_content_csat(make_raw([{}]))
        assert set(out["date_granularity"]) == {
            "day", "week", "month", "quarter", "semester", "year"
        }

    def test_monthly_aggregates_multiple_dates(self):
        rows = [
            {"date": dt.date(2026, 5, 1), "promoters": 8},
            {"date": dt.date(2026, 5, 20), "promoters": 4},
        ]
        out = compute_content_csat(make_raw(rows))
        month = out[out["date_granularity"] == "month"].iloc[0]
        assert month["numerator"] == 12.0
        assert month["denominator"] == 16.0
        assert month["metric_value"] == 75.0

    def test_dimensions_take_latest_value_in_bucket(self):
        rows = [
            {"date": dt.date(2026, 5, 1), "xplead": "early.lead"},
            {"date": dt.date(2026, 5, 20), "xplead": "late.lead"},
        ]
        month = compute_content_csat(make_raw(rows))
        month = month[month["date_granularity"] == "month"].iloc[0]
        assert month["xplead"] == "late.lead"

    def test_output_schema_and_column_order(self):
        out = compute_content_csat(make_raw([{}]))
        assert list(out.columns) == [c for c, _ in IO_CONTENT_CSAT_METRIC_SCHEMA]

    def test_empty_input_yields_empty_frame_with_schema(self):
        out = compute_content_csat(make_raw([])[0:0])
        assert out.empty
        assert list(out.columns) == [c for c, _ in IO_CONTENT_CSAT_METRIC_SCHEMA]
