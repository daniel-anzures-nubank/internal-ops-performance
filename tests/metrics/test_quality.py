"""Unit tests for ``metrics/quality.py``.

Small synthetic frames mimicking ``io_quality_evaluations_raw``, no warehouse.
We verify the mean-score math, the latest-per-evaluation_id dedup, the Content
exclusion, null-score handling, and the output contract.
"""

from __future__ import annotations

import datetime as dt

import pandas as pd

from quality import (
    IO_QUALITY_METRIC_SCHEMA,
    METRIC_NAME,
    compute_quality,
)


def make_eval(**overrides) -> dict:
    base = {
        "agent": "nuberto.lopez",
        "xforce": "nuliana.cruz",
        "xplead": "nuricio.diaz",
        "team": "core",
        "squad": "txn",
        "district": "csi",
        "shift": "morning",
        "date": dt.date(2026, 5, 4),  # a Monday
        "evaluation_id": "ev-1",
        "team_name": "TXN",
        "qa_score": 90.0,
    }
    base.update(overrides)
    return base


def make_raw(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame([make_eval(**r) for r in rows])


class TestComputeQuality:
    def test_mean_of_scores(self):
        out = compute_quality(
            make_raw([
                {"evaluation_id": "a", "qa_score": 95.0},
                {"evaluation_id": "b", "qa_score": 93.0},
                {"evaluation_id": "c", "qa_score": 81.0},
            ])
        )
        day = out[out["date_granularity"] == "day"].iloc[0]
        assert day["numerator"] == 95 + 93 + 81
        assert day["denominator"] == 3
        assert abs(day["metric_value"] - (95 + 93 + 81) / 3) < 1e-9
        assert abs(day["metric_value"] - 89.6667) < 1e-3
        assert day["metric"] == METRIC_NAME

    def test_latest_per_evaluation_id_dedup(self):
        # Same evaluation_id re-scored on a later day → keep the latest score.
        out = compute_quality(
            make_raw([
                {"evaluation_id": "ev-1", "date": dt.date(2026, 5, 4),
                 "qa_score": 70.0},
                {"evaluation_id": "ev-1", "date": dt.date(2026, 5, 6),
                 "qa_score": 100.0},
            ])
        )
        # Monthly grain should average a single (latest) evaluation = 100.
        month = out[out["date_granularity"] == "month"].iloc[0]
        assert month["denominator"] == 1
        assert month["metric_value"] == 100.0

    def test_content_excluded(self):
        out = compute_quality(
            make_raw([
                {"agent": "c.one", "team": "content", "evaluation_id": "x"},
            ])
        )
        assert out.empty

    def test_content_excluded_but_others_kept(self):
        out = compute_quality(
            make_raw([
                {"agent": "core.one", "team": "core", "evaluation_id": "x",
                 "qa_score": 90.0},
                {"agent": "cont.one", "team": "content", "evaluation_id": "y",
                 "qa_score": 50.0},
            ])
        )
        day = out[out["date_granularity"] == "day"]
        assert set(day["agent"]) == {"core.one"}

    def test_social_media_scored(self):
        # Social Media is scored like Core/Fraud (Playvox mean).
        out = compute_quality(
            make_raw([
                {"agent": "s.one", "team": "social media", "squad": "social",
                 "evaluation_id": "p1", "qa_score": 80.0},
                {"agent": "s.one", "team": "social media", "squad": "social",
                 "evaluation_id": "p2", "qa_score": 100.0},
            ])
        )
        day = out[out["date_granularity"] == "day"].iloc[0]
        assert day["denominator"] == 2
        assert day["metric_value"] == 90.0

    def test_null_score_dropped(self):
        out = compute_quality(
            make_raw([
                {"evaluation_id": "a", "qa_score": 90.0},
                {"evaluation_id": "b", "qa_score": None},
            ])
        )
        day = out[out["date_granularity"] == "day"].iloc[0]
        assert day["denominator"] == 1
        assert day["metric_value"] == 90.0

    def test_per_agent_separation(self):
        out = compute_quality(
            make_raw([
                {"agent": "a.one", "evaluation_id": "1", "qa_score": 100.0},
                {"agent": "b.two", "evaluation_id": "2", "qa_score": 50.0},
            ])
        )
        day = out[out["date_granularity"] == "day"].set_index("agent")
        assert day.loc["a.one", "metric_value"] == 100.0
        assert day.loc["b.two", "metric_value"] == 50.0

    def test_monthly_averages_across_days(self):
        # Two evals same agent, different days, same month → monthly mean.
        out = compute_quality(
            make_raw([
                {"evaluation_id": "1", "date": dt.date(2026, 5, 4), "qa_score": 80.0},
                {"evaluation_id": "2", "date": dt.date(2026, 5, 20), "qa_score": 100.0},
            ])
        )
        month = out[out["date_granularity"] == "month"].iloc[0]
        assert month["denominator"] == 2
        assert month["metric_value"] == 90.0

    def test_all_granularities_emitted(self):
        out = compute_quality(make_raw([{}]))
        assert set(out["date_granularity"]) == {
            "day", "week", "month", "quarter", "semester", "year"
        }

    def test_output_schema_and_column_order(self):
        out = compute_quality(make_raw([{}]))
        assert list(out.columns) == [c for c, _ in IO_QUALITY_METRIC_SCHEMA]

    def test_empty_input_yields_empty_frame_with_schema(self):
        out = compute_quality(make_raw([])[0:0])
        assert out.empty
        assert list(out.columns) == [c for c, _ in IO_QUALITY_METRIC_SCHEMA]
