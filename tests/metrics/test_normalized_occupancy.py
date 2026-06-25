"""Unit tests for ``metrics/normalized_occupancy.py``.

Small synthetic frames mimicking ``io_occupancy_time_raw``, no warehouse. We
verify the productive-slot filter, the agent occupancy ratio, the two-step
district+shift benchmark (mean of squad ratios), the NO = occupancy / benchmark
ratio, NULL-shift (content) handling, and the output contract.
"""

from __future__ import annotations

import datetime as dt

import pandas as pd

from normalized_occupancy import (
    EXCLUDED_ACTIVITY_TYPES,
    IO_NORMALIZED_OCCUPANCY_METRIC_SCHEMA,
    METRIC_NAME,
    compute_normalized_occupancy,
)


def make_slot(**overrides) -> dict:
    base = {
        "agent": "nuberto.lopez",
        "xforce": "nuliana.cruz",
        "xplead": "nuricio.diaz",
        "team": "core",
        "squad": "txn",
        "district": "csi",
        "shift": "morning",
        "date": dt.date(2026, 5, 4),  # a Monday
        "slot_time": "09:00:00",
        "activity_type_required": "available",
        "required_minutes": 30.0,
        "occupancy_minutes": 30.0,
    }
    base.update(overrides)
    return base


def make_raw(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame([make_slot(**r) for r in rows])


class TestComputeNormalizedOccupancy:
    def test_single_cohort_no_is_100(self):
        # One squad, one agent -> agent occupancy == benchmark -> NO 100%.
        out = compute_normalized_occupancy(
            make_raw([
                {"slot_time": "09:00:00", "occupancy_minutes": 30.0},
                {"slot_time": "09:30:00", "occupancy_minutes": 15.0},
            ])
        )
        day = out[out["date_granularity"] == "day"].iloc[0]
        # occupancy = 45/60 = 75%
        assert abs(day["numerator"] - 75.0) < 1e-9
        assert abs(day["denominator"] - 75.0) < 1e-9
        assert abs(day["metric_value"] - 100.0) < 1e-9
        assert day["metric"] == METRIC_NAME

    def test_benchmark_is_mean_of_squad_ratios(self):
        # district csi / morning / May has two squads:
        #   txn:    a.one occ (30 + 0)/(30+30) = 0.5
        #   cuenta: b.two occ 30/30          = 1.0
        # benchmark = mean(0.5, 1.0) = 0.75 (75%).
        out = compute_normalized_occupancy(
            make_raw([
                {"agent": "a.one", "squad": "txn", "slot_time": "09:00:00",
                 "occupancy_minutes": 30.0},
                {"agent": "a.one", "squad": "txn", "slot_time": "09:30:00",
                 "occupancy_minutes": 0.0},
                {"agent": "b.two", "squad": "cuenta", "slot_time": "09:00:00",
                 "occupancy_minutes": 30.0},
            ])
        )
        day = out[out["date_granularity"] == "day"].set_index("agent")
        # both agents share benchmark 75%
        assert abs(day.loc["a.one", "denominator"] - 75.0) < 1e-9
        assert abs(day.loc["b.two", "denominator"] - 75.0) < 1e-9
        # a.one occupancy 50% -> NO 66.7%
        assert abs(day.loc["a.one", "numerator"] - 50.0) < 1e-9
        assert abs(day.loc["a.one", "metric_value"] - 50 / 75 * 100) < 1e-6
        # b.two occupancy 100% -> NO 133.3%
        assert abs(day.loc["b.two", "metric_value"] - 100 / 75 * 100) < 1e-6

    def test_excluded_activity_types_dropped(self):
        # lunch_break / time_off / shrinkage must not count in occupancy or bench.
        rows = [{"slot_time": "09:00:00", "occupancy_minutes": 30.0}]
        rows += [
            {"slot_time": "10:00:00", "activity_type_required": t,
             "occupancy_minutes": 0.0}
            for t in EXCLUDED_ACTIVITY_TYPES
        ]
        out = compute_normalized_occupancy(make_raw(rows))
        day = out[out["date_granularity"] == "day"].iloc[0]
        # only the productive slot counts: occupancy 100%, single cohort -> NO 100
        assert abs(day["numerator"] - 100.0) < 1e-9
        assert abs(day["metric_value"] - 100.0) < 1e-9

    def test_exclusion_case_insensitive(self):
        out = compute_normalized_occupancy(
            make_raw([{"activity_type_required": "Shrinkage"}])
        )
        assert out.empty

    def test_null_shift_content_handled(self):
        # content agents have NULL shift; groupby must keep them (dropna=False).
        out = compute_normalized_occupancy(
            make_raw([
                {"agent": "c.one", "team": "content", "squad": "enablement",
                 "district": "content", "shift": None, "occupancy_minutes": 30.0},
                {"agent": "c.two", "team": "content", "squad": "enablement",
                 "district": "content", "shift": None, "occupancy_minutes": 30.0},
            ])
        )
        day = out[out["date_granularity"] == "day"]
        assert set(day["agent"]) == {"c.one", "c.two"}
        # single squad cohort -> NO 100%
        assert (abs(day["metric_value"] - 100.0) < 1e-9).all()

    def test_nitza_no_metric_suppressed_but_still_feeds_benchmark(self):
        out = compute_normalized_occupancy(
            make_raw([
                {"agent": "nitza.zarza", "occupancy_minutes": 30.0},
                {"agent": "peer.agent", "occupancy_minutes": 15.0},
            ])
        )

        day = out[out["date_granularity"] == "day"].set_index("agent")
        assert "nitza.zarza" not in day.index
        # Benchmark still includes nitza: (30 + 15) / (30 + 30) = 75%.
        assert abs(day.loc["peer.agent", "denominator"] - 75.0) < 1e-9

    def test_all_granularities_emitted(self):
        out = compute_normalized_occupancy(make_raw([{}]))
        assert set(out["date_granularity"]) == {
            "day", "week", "month", "quarter", "semester", "year"
        }

    def test_week_bucket_is_monday(self):
        out = compute_normalized_occupancy(make_raw([{"date": dt.date(2026, 5, 6)}]))
        week = out[out["date_granularity"] == "week"].iloc[0]
        assert week["date_reference"] == dt.date(2026, 5, 4)

    def test_dimensions_take_latest_value_in_bucket(self):
        out = compute_normalized_occupancy(
            make_raw([
                {"date": dt.date(2026, 5, 4), "squad": "txn"},
                {"date": dt.date(2026, 5, 20), "squad": "cuenta"},
            ])
        )
        month = out[out["date_granularity"] == "month"].iloc[0]
        assert month["squad"] == "cuenta"

    def test_output_schema_and_column_order(self):
        out = compute_normalized_occupancy(make_raw([{}]))
        assert list(out.columns) == [
            c for c, _ in IO_NORMALIZED_OCCUPANCY_METRIC_SCHEMA
        ]

    def test_empty_input_yields_empty_frame_with_schema(self):
        out = compute_normalized_occupancy(make_raw([])[0:0])
        assert out.empty
        assert list(out.columns) == [
            c for c, _ in IO_NORMALIZED_OCCUPANCY_METRIC_SCHEMA
        ]
