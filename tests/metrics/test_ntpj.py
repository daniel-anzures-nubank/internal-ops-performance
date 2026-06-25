"""Unit tests for ``metrics/ntpj.py``.

Small synthetic frames mimicking ``io_jobs_raw``, no warehouse. We verify the
finished-only filter, the required-day contribution filter (vs the benchmark
using all finished jobs), the monthly benchmark window (current-month cutover
and the pre-cutover trailing window), the actual/expected ratio, the look-back
vs output-period split, and the output contract.
"""

from __future__ import annotations

import datetime as dt

import pandas as pd

from ntpj import (
    IO_NTPJ_METRIC_SCHEMA,
    METRIC_NAME,
    compute_ntpj,
)


def make_job(**overrides) -> dict:
    base = {
        "agent": "nuberto.lopez",
        "xforce": "nuliana.cruz",
        "xplead": "nuricio.diaz",
        "team": "core",
        "squad": "txn",
        "district": "csi",
        "shift": "morning",
        "date": dt.date(2026, 5, 4),  # a Monday, >= cutover (current-month bench)
        "start_time": dt.datetime(2026, 5, 4, 9, 0, 0),
        "end_time": dt.datetime(2026, 5, 4, 9, 5, 0),
        "job_type": "queue-a",
        "activity_type": "backoffice",
        "status": "finished",
        "job_id": "bko - queue-a - finished",
        "duration_seconds": 300,
        "required_activity_on_day_flag": 1,
    }
    base.update(overrides)
    return base


def make_jobs(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame([make_job(**r) for r in rows])


MAY_START = dt.date(2026, 5, 1)
MAY_END = dt.date(2026, 5, 31)


class TestComputeNtpj:
    def test_basic_ratio_current_month(self):
        # Two agents, same job_id, May (current-month benchmark).
        # durations 200 & 100 -> benchmark = 300/2 = 150.
        out = compute_ntpj(
            make_jobs([
                {"agent": "a.one", "duration_seconds": 200},
                {"agent": "b.two", "duration_seconds": 100},
            ]),
            MAY_START,
            MAY_END,
        )
        day = out[out["date_granularity"] == "day"].set_index("agent")
        # a.one: actual 200, expected 150 -> 133.3%
        assert abs(day.loc["a.one", "numerator"] - 200) < 1e-9
        assert abs(day.loc["a.one", "denominator"] - 150) < 1e-9
        assert abs(day.loc["a.one", "metric_value"] - 200 / 150 * 100) < 1e-6
        # b.two: actual 100, expected 150 -> 66.7%
        assert abs(day.loc["b.two", "metric_value"] - 100 / 150 * 100) < 1e-6
        assert day.loc["a.one", "metric"] == METRIC_NAME

    def test_unfinished_jobs_excluded_everywhere(self):
        # A transferred job must not feed the benchmark nor the contribution.
        out = compute_ntpj(
            make_jobs([
                {"agent": "a.one", "duration_seconds": 200, "status": "finished"},
                {"agent": "a.one", "duration_seconds": 999, "status": "transferred"},
            ]),
            MAY_START,
            MAY_END,
        )
        day = out[out["date_granularity"] == "day"].iloc[0]
        # benchmark from the single finished job (200/1=200); actual 200 -> 100%
        assert day["numerator"] == 200
        assert day["denominator"] == 200
        assert day["metric_value"] == 100.0

    def test_required_flag_filters_contribution_not_benchmark(self):
        # a.one has a required job (flag 1); b.two's job (flag 0) only feeds the
        # benchmark. benchmark = (100 + 300) / 2 = 200.
        out = compute_ntpj(
            make_jobs([
                {"agent": "a.one", "duration_seconds": 100,
                 "required_activity_on_day_flag": 1},
                {"agent": "b.two", "duration_seconds": 300,
                 "required_activity_on_day_flag": 0},
            ]),
            MAY_START,
            MAY_END,
        )
        day = out[out["date_granularity"] == "day"]
        # b.two is not in the output (no required contribution rows)
        assert set(day["agent"]) == {"a.one"}
        row = day.iloc[0]
        assert row["denominator"] == 200  # 200 benchmark * count 1
        assert row["metric_value"] == 50.0  # 100 / 200

    def test_multiple_job_types_sum(self):
        # One agent, two job_ids, each its own benchmark, summed in the ratio.
        out = compute_ntpj(
            make_jobs([
                {"job_id": "A", "duration_seconds": 100},
                {"job_id": "B", "duration_seconds": 400},
            ]),
            MAY_START,
            MAY_END,
        )
        # each job alone in its job_id -> benchmark == its own duration.
        # actual = 100+400 = 500, expected = 100+400 = 500 -> 100%.
        day = out[out["date_granularity"] == "day"].iloc[0]
        assert day["numerator"] == 500
        assert day["denominator"] == 500
        assert day["metric_value"] == 100.0

    def test_trailing_window_and_lookback(self):
        # Target Feb 2026 (<= 2026-03) uses a trailing window incl. Jan.
        # Jan dur 100 + Feb dur 300 -> benchmark(Feb) = 400/2 = 200.
        # Output period = Feb only; Jan row is look-back (not emitted).
        out = compute_ntpj(
            make_jobs([
                {"agent": "a.one", "date": dt.date(2026, 1, 15),
                 "duration_seconds": 100},
                {"agent": "a.one", "date": dt.date(2026, 2, 16),
                 "duration_seconds": 300},
            ]),
            dt.date(2026, 2, 1),
            dt.date(2026, 2, 28),
        )
        day = out[out["date_granularity"] == "day"]
        assert len(day) == 1  # only the Feb row emitted
        row = day.iloc[0]
        assert row["date_reference"] == dt.date(2026, 2, 16)
        assert row["denominator"] == 200  # trailing-window benchmark * 1
        assert abs(row["metric_value"] - 300 / 200 * 100) < 1e-6

    def test_current_month_ignores_prior_months(self):
        # Target May (>= cutover) must ignore April data in the benchmark.
        out = compute_ntpj(
            make_jobs([
                {"agent": "a.one", "date": dt.date(2026, 4, 15),
                 "duration_seconds": 1000},
                {"agent": "a.one", "date": dt.date(2026, 5, 4),
                 "duration_seconds": 200},
            ]),
            MAY_START,
            MAY_END,
        )
        day = out[out["date_granularity"] == "day"]
        assert len(day) == 1
        row = day.iloc[0]
        # benchmark(May) uses only the May job: 200/1 = 200 -> 100%
        assert row["denominator"] == 200
        assert row["metric_value"] == 100.0

    def test_all_granularities_emitted(self):
        out = compute_ntpj(make_jobs([{}]), MAY_START, MAY_END)
        assert set(out["date_granularity"]) == {
            "day", "week", "month", "quarter", "semester", "year"
        }

    def test_weekly_aggregates_across_days(self):
        # Two days in the same week, same job_id (benchmark 300 each day's job).
        out = compute_ntpj(
            make_jobs([
                {"date": dt.date(2026, 5, 4), "duration_seconds": 300},
                {"date": dt.date(2026, 5, 5), "duration_seconds": 300},
            ]),
            MAY_START,
            MAY_END,
        )
        week = out[out["date_granularity"] == "week"].iloc[0]
        assert week["date_reference"] == dt.date(2026, 5, 4)  # Monday
        assert week["numerator"] == 600

    def test_dimensions_take_latest_value_in_bucket(self):
        out = compute_ntpj(
            make_jobs([
                {"date": dt.date(2026, 5, 4), "squad": "txn"},
                {"date": dt.date(2026, 5, 20), "squad": "cuenta"},
            ]),
            MAY_START,
            MAY_END,
        )
        month = out[out["date_granularity"] == "month"].iloc[0]
        assert month["squad"] == "cuenta"

    def test_empty_input_yields_empty_frame_with_schema(self):
        out = compute_ntpj(make_jobs([])[0:0], MAY_START, MAY_END)
        assert out.empty
        assert list(out.columns) == [c for c, _ in IO_NTPJ_METRIC_SCHEMA]

    def test_no_finished_jobs_yields_empty(self):
        out = compute_ntpj(
            make_jobs([{"status": "transferred"}]), MAY_START, MAY_END
        )
        assert out.empty

    def test_output_schema_and_column_order(self):
        out = compute_ntpj(make_jobs([{}]), MAY_START, MAY_END)
        assert list(out.columns) == [c for c, _ in IO_NTPJ_METRIC_SCHEMA]
