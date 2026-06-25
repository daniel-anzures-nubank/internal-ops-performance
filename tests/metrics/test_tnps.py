"""Unit tests for ``metrics/tnps.py``.

Small synthetic frames mimicking ``io_tnps_responses_raw``, no warehouse. We
verify the NPS math (promoters − detractors / valid), the validity window, the
one-response-per-case dedup, the neutral/null handling, negative scores, the
day/week/month aggregation, team scope, and the output contract.
"""

from __future__ import annotations

import datetime as dt

import pandas as pd

from tnps import IO_TNPS_METRIC_SCHEMA, METRIC_NAME, compute_tnps

CLOSE = dt.date(2026, 5, 4)  # a Monday


def make_resp(**o) -> dict:
    base = {
        "agent": "nuberto.lopez",
        "xforce": "nuliana.cruz",
        "xplead": "nuricio.diaz",
        "team": "social media",
        "squad": "social_es",
        "district": "social",
        "shift": "morning",
        "date": CLOSE,
        "case_number": "C1",
        "survey_response_date": CLOSE,
        "survey_score": 10,
    }
    base.update(o)
    return base


def make_raw(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame([make_resp(**r) for r in rows])


def _day(out: pd.DataFrame) -> pd.DataFrame:
    return out[out["date_granularity"] == "day"]


class TestComputeTnps:
    def test_basic_nps(self):
        # [10, 9, 5, 7] -> promoters=2, detractors=1, valid=4 -> (2-1)/4 = 25%.
        rows = [
            {"case_number": "C1", "survey_score": 10},
            {"case_number": "C2", "survey_score": 9},
            {"case_number": "C3", "survey_score": 5},
            {"case_number": "C4", "survey_score": 7},
        ]
        row = _day(compute_tnps(make_raw(rows))).iloc[0]
        assert row["numerator"] == 1.0
        assert row["denominator"] == 4.0
        assert row["metric_value"] == 25.0
        assert row["metric"] == METRIC_NAME

    def test_negative_nps(self):
        # 1 promoter, 3 detractors, valid 4 -> (1-3)/4 = -50%.
        rows = [
            {"case_number": "C1", "survey_score": 10},
            {"case_number": "C2", "survey_score": 2},
            {"case_number": "C3", "survey_score": 4},
            {"case_number": "C4", "survey_score": 6},
        ]
        row = _day(compute_tnps(make_raw(rows))).iloc[0]
        assert row["numerator"] == -2.0
        assert row["denominator"] == 4.0
        assert row["metric_value"] == -50.0

    def test_neutral_counts_only_in_denominator(self):
        rows = [
            {"case_number": "C1", "survey_score": 9},   # promoter
            {"case_number": "C2", "survey_score": 7},   # neutral
            {"case_number": "C3", "survey_score": 8},   # neutral
        ]
        row = _day(compute_tnps(make_raw(rows))).iloc[0]
        assert row["numerator"] == 1.0
        assert row["denominator"] == 3.0

    def test_boundaries(self):
        # 9 = promoter, 6 = detractor (inclusive bounds).
        rows = [
            {"case_number": "C1", "survey_score": 9},
            {"case_number": "C2", "survey_score": 6},
        ]
        row = _day(compute_tnps(make_raw(rows))).iloc[0]
        assert row["numerator"] == 0.0
        assert row["denominator"] == 2.0
        assert row["metric_value"] == 0.0

    def test_null_score_excluded_from_denominator(self):
        rows = [
            {"case_number": "C1", "survey_score": 10},
            {"case_number": "C2", "survey_score": None},
        ]
        row = _day(compute_tnps(make_raw(rows))).iloc[0]
        assert row["numerator"] == 1.0
        assert row["denominator"] == 1.0  # null score not valid

    def test_validity_window_excludes_late_response(self):
        # response 2 days after close -> outside the +1 day window.
        rows = [
            {"case_number": "C1", "survey_score": 10,
             "survey_response_date": CLOSE},
            {"case_number": "C2", "survey_score": 2,
             "survey_response_date": CLOSE + dt.timedelta(days=2)},
        ]
        row = _day(compute_tnps(make_raw(rows))).iloc[0]
        # only C1 survives -> 1 promoter / 1 valid.
        assert row["numerator"] == 1.0
        assert row["denominator"] == 1.0

    def test_validity_window_allows_next_day(self):
        rows = [
            {"case_number": "C1", "survey_score": 2,
             "survey_response_date": CLOSE + dt.timedelta(days=1)},
        ]
        row = _day(compute_tnps(make_raw(rows))).iloc[0]
        assert row["denominator"] == 1.0
        assert row["numerator"] == -1.0

    def test_dedup_one_response_per_case(self):
        # Same case twice -> counted once (latest scored response kept).
        rows = [
            {"case_number": "C1", "survey_score": 10,
             "survey_response_date": CLOSE},
            {"case_number": "C1", "survey_score": 3,
             "survey_response_date": CLOSE + dt.timedelta(days=1)},
        ]
        row = _day(compute_tnps(make_raw(rows))).iloc[0]
        assert row["denominator"] == 1.0
        # latest response (score 3) kept -> detractor.
        assert row["numerator"] == -1.0

    def test_dedup_prefers_scored_row(self):
        rows = [
            {"case_number": "C1", "survey_score": None,
             "survey_response_date": CLOSE + dt.timedelta(days=1)},
            {"case_number": "C1", "survey_score": 10,
             "survey_response_date": CLOSE},
        ]
        row = _day(compute_tnps(make_raw(rows))).iloc[0]
        assert row["denominator"] == 1.0
        assert row["numerator"] == 1.0

    def test_non_social_team_excluded(self):
        out = compute_tnps(make_raw([{"team": "core", "survey_score": 10}]))
        assert out.empty

    def test_all_granularities_emitted(self):
        out = compute_tnps(make_raw([{}]))
        assert set(out["date_granularity"]) == {
            "day", "week", "month", "quarter", "semester", "year"
        }

    def test_week_bucket_is_monday(self):
        out = compute_tnps(make_raw([{"date": dt.date(2026, 5, 6),
                                      "survey_response_date": dt.date(2026, 5, 6)}]))
        week = out[out["date_granularity"] == "week"].iloc[0]
        assert week["date_reference"] == dt.date(2026, 5, 4)

    def test_per_agent_separation(self):
        rows = [
            {"agent": "a.one", "case_number": "C1", "survey_score": 10},
            {"agent": "b.two", "case_number": "C2", "survey_score": 2},
        ]
        day = _day(compute_tnps(make_raw(rows)))
        assert set(day["agent"]) == {"a.one", "b.two"}
        assert day.set_index("agent").loc["a.one", "metric_value"] == 100.0
        assert day.set_index("agent").loc["b.two", "metric_value"] == -100.0

    def test_output_schema_and_column_order(self):
        out = compute_tnps(make_raw([{}]))
        assert list(out.columns) == [c for c, _ in IO_TNPS_METRIC_SCHEMA]

    def test_empty_input_yields_empty_frame_with_schema(self):
        out = compute_tnps(make_raw([])[0:0])
        assert out.empty
        assert list(out.columns) == [c for c, _ in IO_TNPS_METRIC_SCHEMA]
