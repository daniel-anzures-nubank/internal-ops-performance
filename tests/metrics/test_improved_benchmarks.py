"""Unit tests for ``metrics/improved_benchmarks.py``.

Small synthetic frames mimicking ``io_jobs_raw`` / ``io_occupancy_time_raw``,
no warehouse. We verify the NTPJ (lower=better) and occupancy (higher=better)
month-over-month comparisons, the tie rule, first-month exclusion, the squad +
district roll-ups, team scope (Core/Fraud only), the removal cutovers, and the
output contract.

Test months avoid the NTPJ trailing-window complication by using 2026-03 (the
look-back / previous month) and 2026-04 (the compared/emitted month). Fraud is
emittable through April; Core is suppressed from April.
"""

from __future__ import annotations

import datetime as dt

import pandas as pd

from improved_benchmarks import (
    DISTRICT_METRIC,
    IO_IMPROVED_BENCHMARKS_METRIC_SCHEMA,
    SQUAD_METRIC,
    XFORCE_METRIC,
    compute_improved_benchmarks,
)

PREV = dt.date(2026, 3, 15)   # previous month
CUR = dt.date(2026, 4, 15)    # compared month (emitted for fraud)
EMPTY = pd.DataFrame()


def make_job(**o) -> dict:
    base = {
        "agent": "a.one", "xforce": "x.one", "xplead": "p.one",
        "team": "fraud", "squad": "txn", "district": "csi", "shift": "morning",
        "date": CUR, "job_id": "jobA", "activity_type": "bko",
        "status": "finished", "duration_seconds": 100.0,
        "required_activity_on_day_flag": 1,
    }
    base.update(o)
    return base


def make_jobs(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame([make_job(**r) for r in rows])


def make_occ(**o) -> dict:
    base = {
        "agent": "a.one", "xforce": "x.one", "xplead": "p.one",
        "team": "fraud", "squad": "txn", "district": "csi", "shift": "morning",
        "date": CUR, "activity_type_required": "oos",
        "required_minutes": 100.0, "occupancy_minutes": 20.0,
    }
    base.update(o)
    return base


def make_occs(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame([make_occ(**r) for r in rows])


def _run(jobs=EMPTY, occ=EMPTY, start=dt.date(2026, 4, 1), end=dt.date(2026, 4, 30)):
    return compute_improved_benchmarks(jobs, occ, start, end)


class TestNtpjBenchmark:
    def test_lower_benchmark_is_improved(self):
        # jobA: 200s in Mar, 100s in Apr -> benchmark dropped -> improved.
        out = _run(jobs=make_jobs([
            {"date": PREV, "duration_seconds": 200.0},
            {"date": CUR, "duration_seconds": 100.0},
        ]))
        squad = out[out["metric"] == SQUAD_METRIC].iloc[0]
        assert squad["numerator"] == 1.0
        assert squad["denominator"] == 1.0
        assert squad["metric_value"] == 100.0
        assert squad["squad"] == "txn"
        assert squad["district"] is None
        assert squad["date_reference"] == dt.date(2026, 4, 1)
        assert squad["date_granularity"] == "month"

    def test_higher_benchmark_is_not_improved(self):
        # 100s -> 200s: benchmark rose -> not improved (counted but 0).
        out = _run(jobs=make_jobs([
            {"date": PREV, "duration_seconds": 100.0},
            {"date": CUR, "duration_seconds": 200.0},
        ]))
        squad = out[out["metric"] == SQUAD_METRIC].iloc[0]
        assert squad["numerator"] == 0.0
        assert squad["denominator"] == 1.0
        assert squad["metric_value"] == 0.0

    def test_tie_counts_as_improved(self):
        out = _run(jobs=make_jobs([
            {"date": PREV, "duration_seconds": 150.0},
            {"date": CUR, "duration_seconds": 150.0},
        ]))
        squad = out[out["metric"] == SQUAD_METRIC].iloc[0]
        assert squad["numerator"] == 1.0
        assert squad["metric_value"] == 100.0

    def test_first_month_not_counted(self):
        # Only the compared month present -> no previous -> denominator 0.
        out = _run(jobs=make_jobs([{"date": CUR, "duration_seconds": 100.0}]))
        squad = out[out["metric"] == SQUAD_METRIC].iloc[0]
        assert squad["denominator"] == 0.0
        assert pd.isna(squad["metric_value"])

    def test_emits_squad_district_and_xforce_rows(self):
        out = _run(jobs=make_jobs([
            {"date": PREV, "duration_seconds": 200.0},
            {"date": CUR, "duration_seconds": 100.0},
        ]))
        assert set(out["metric"]) == {SQUAD_METRIC, DISTRICT_METRIC, XFORCE_METRIC}
        district = out[out["metric"] == DISTRICT_METRIC].iloc[0]
        assert district["district"] == "csi"
        assert district["squad"] is None
        assert district["metric_value"] == 100.0

    def test_xforce_rollup_sets_xforce_and_xplead(self):
        out = _run(jobs=make_jobs([
            {"date": PREV, "duration_seconds": 200.0},
            {"date": CUR, "duration_seconds": 100.0},
        ]))
        xf = out[out["metric"] == XFORCE_METRIC].iloc[0]
        assert xf["xforce"] == "x.one"
        assert xf["xplead"] == "p.one"
        assert xf["squad"] is None and xf["district"] is None
        assert xf["numerator"] == 1.0 and xf["metric_value"] == 100.0


class TestOccupancyBenchmark:
    def test_higher_occupancy_is_improved(self):
        # occupancy benchmark 0.1 (Mar) -> 0.2 (Apr): rose -> improved.
        out = _run(occ=make_occs([
            {"date": PREV, "occupancy_minutes": 10.0},
            {"date": CUR, "occupancy_minutes": 20.0},
        ]))
        squad = out[out["metric"] == SQUAD_METRIC].iloc[0]
        assert squad["numerator"] == 1.0
        assert squad["denominator"] == 1.0
        assert squad["metric_value"] == 100.0

    def test_lower_occupancy_is_not_improved(self):
        out = _run(occ=make_occs([
            {"date": PREV, "occupancy_minutes": 30.0},
            {"date": CUR, "occupancy_minutes": 20.0},
        ]))
        squad = out[out["metric"] == SQUAD_METRIC].iloc[0]
        assert squad["numerator"] == 0.0
        assert squad["denominator"] == 1.0


class TestScopeAndRemoval:
    def test_social_media_excluded(self):
        out = _run(jobs=make_jobs([
            {"team": "social media", "date": PREV, "duration_seconds": 200.0},
            {"team": "social media", "date": CUR, "duration_seconds": 100.0},
        ]))
        assert out.empty

    def test_content_excluded(self):
        out = _run(jobs=make_jobs([
            {"team": "content", "date": PREV, "duration_seconds": 200.0},
            {"team": "content", "date": CUR, "duration_seconds": 100.0},
        ]))
        assert out.empty

    def test_core_suppressed_from_april(self):
        # Core 2026-04 is removed -> no rows for the April window.
        out = _run(jobs=make_jobs([
            {"team": "core", "squad": "cuenta", "date": PREV,
             "duration_seconds": 200.0},
            {"team": "core", "squad": "cuenta", "date": CUR,
             "duration_seconds": 100.0},
        ]))
        assert out.empty

    def test_core_emitted_in_march(self):
        # Core 2026-03 still applies (cutover is April). Compare Feb->Mar.
        out = compute_improved_benchmarks(
            make_jobs([
                {"team": "core", "squad": "cuenta", "date": dt.date(2026, 2, 15),
                 "duration_seconds": 200.0},
                {"team": "core", "squad": "cuenta", "date": dt.date(2026, 3, 15),
                 "duration_seconds": 100.0},
            ]),
            EMPTY,
            dt.date(2026, 3, 1), dt.date(2026, 3, 31),
        )
        squad = out[out["metric"] == SQUAD_METRIC].iloc[0]
        assert squad["team"] == "core"
        assert squad["metric_value"] == 100.0

    def test_fraud_suppressed_from_may(self):
        out = compute_improved_benchmarks(
            make_jobs([
                {"team": "fraud", "date": dt.date(2026, 4, 15),
                 "duration_seconds": 200.0},
                {"team": "fraud", "date": dt.date(2026, 5, 15),
                 "duration_seconds": 100.0},
            ]),
            EMPTY,
            dt.date(2026, 5, 1), dt.date(2026, 5, 31),
        )
        assert out.empty


class TestOutputContract:
    def test_output_schema_and_column_order(self):
        out = _run(jobs=make_jobs([
            {"date": PREV, "duration_seconds": 200.0},
            {"date": CUR, "duration_seconds": 100.0},
        ]))
        assert list(out.columns) == [c for c, _ in IO_IMPROVED_BENCHMARKS_METRIC_SCHEMA]

    def test_agent_always_null_xforce_only_on_xforce_rows(self):
        out = _run(jobs=make_jobs([
            {"date": PREV, "duration_seconds": 200.0},
            {"date": CUR, "duration_seconds": 100.0},
        ]))
        assert out["agent"].isna().all()
        # xforce populated only on the xforce roll-up rows.
        sd = out[out["metric"].isin([SQUAD_METRIC, DISTRICT_METRIC])]
        assert sd["xforce"].isna().all()
        assert out[out["metric"] == XFORCE_METRIC]["xforce"].notna().all()

    def test_only_month_granularity(self):
        out = _run(jobs=make_jobs([
            {"date": PREV, "duration_seconds": 200.0},
            {"date": CUR, "duration_seconds": 100.0},
        ]))
        assert set(out["date_granularity"]) == {"month"}

    def test_empty_inputs_yield_empty_frame_with_schema(self):
        out = compute_improved_benchmarks(EMPTY, EMPTY, dt.date(2026, 4, 1),
                                          dt.date(2026, 4, 30))
        assert out.empty
        assert list(out.columns) == [c for c, _ in IO_IMPROVED_BENCHMARKS_METRIC_SCHEMA]
