"""Unit tests for ``metrics/shrinkage.py``.

Small synthetic frames mimicking the ``io_shrinkage_slots_raw`` table, no
warehouse. We verify the ratio math, the lunch_break exclusion, the pre/post-
2026-03-01 denominator rule, the day/week/month aggregation + bucketing, the
most-recent-dimension rule, and the output contract.
"""

from __future__ import annotations

import datetime as dt

import pandas as pd

from metric_utils import METRIC_COLUMNS
from shrinkage import (
    IO_SHRINKAGE_METRIC_SCHEMA,
    METRIC_NAME,
    XFORCE_METRIC_NAME,
    XPLEAD_METRIC_NAME,
    compute_shrinkage,
    compute_shrinkage_rollups,
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
        "date": dt.date(2026, 5, 4),  # a Monday, post-cutover
        "slot_time": "09:00:00",
        "activity_type_required": "available",
        "dimensioned_activity": "Available",
        "shrinkage_flag": 0,
        "controllable_shrinkage_flag": 0,
        "uncontrollable_shrinkage_flag": 0,
    }
    base.update(overrides)
    return base


def make_raw(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame([make_slot(**r) for r in rows])


class TestComputeShrinkage:
    def test_basic_ratio_daily(self):
        # 1 shrinkage slot out of 4 required slots -> 25%.
        rows = [
            {"slot_time": "09:00:00", "activity_type_required": "shrinkage",
             "shrinkage_flag": 1},
            {"slot_time": "09:30:00", "activity_type_required": "available"},
            {"slot_time": "10:00:00", "activity_type_required": "oos"},
            {"slot_time": "10:30:00", "activity_type_required": "bko"},
        ]
        out = compute_shrinkage(make_raw(rows))
        day = out[out["date_granularity"] == "day"]
        assert len(day) == 1
        row = day.iloc[0]
        assert row["numerator"] == 1.0
        assert row["denominator"] == 4.0
        assert row["metric_value"] == 25.0
        assert row["metric"] == METRIC_NAME
        assert row["date_reference"] == dt.date(2026, 5, 4)

    def test_lunch_break_dropped_from_both_sides(self):
        # lunch_break must not count in numerator or denominator.
        rows = [
            {"slot_time": "09:00:00", "activity_type_required": "shrinkage",
             "shrinkage_flag": 1},
            {"slot_time": "09:30:00", "activity_type_required": "available"},
            {"slot_time": "13:00:00", "activity_type_required": "lunch_break"},
        ]
        out = compute_shrinkage(make_raw(rows))
        day = out[out["date_granularity"] == "day"].iloc[0]
        assert day["denominator"] == 2.0  # lunch_break excluded
        assert day["numerator"] == 1.0
        assert day["metric_value"] == 50.0

    def test_post_cutover_drops_time_off_from_denominator(self):
        # 2026-05 (post-cutover): time_off is NOT a required slot.
        rows = [
            {"activity_type_required": "shrinkage", "shrinkage_flag": 1},
            {"activity_type_required": "available"},
            {"activity_type_required": "time_off"},
        ]
        out = compute_shrinkage(make_raw(rows))
        day = out[out["date_granularity"] == "day"].iloc[0]
        # required slots = shrinkage + available = 2; time_off dropped.
        assert day["denominator"] == 2.0
        assert day["metric_value"] == 50.0

    def test_post_cutover_keeps_dime_invalid_notation_in_denominator(self):
        # Post-cutover, dime_invalid_notation IS a required slot (only time_off
        # is dropped). A meeting/leave invalid_notation slot is also shrinkage.
        rows = [
            {"activity_type_required": "dime_invalid_notation",
             "dimensioned_activity": "Huddle", "shrinkage_flag": 1},
            {"activity_type_required": "available"},
        ]
        out = compute_shrinkage(make_raw(rows))
        day = out[out["date_granularity"] == "day"].iloc[0]
        assert day["denominator"] == 2.0
        assert day["numerator"] == 1.0
        assert day["metric_value"] == 50.0

    def test_pre_cutover_drops_dime_invalid_notation_from_denominator(self):
        # 2026-02 (pre-cutover): dime_invalid_notation is NOT required.
        d = dt.date(2026, 2, 10)
        rows = [
            {"date": d, "activity_type_required": "shrinkage", "shrinkage_flag": 1},
            {"date": d, "activity_type_required": "available"},
            {"date": d, "activity_type_required": "dime_invalid_notation",
             "dimensioned_activity": "Huddle"},
        ]
        out = compute_shrinkage(make_raw(rows))
        day = out[out["date_granularity"] == "day"].iloc[0]
        # required = shrinkage + available = 2; dime_invalid_notation dropped.
        assert day["denominator"] == 2.0
        assert day["numerator"] == 1.0
        assert day["metric_value"] == 50.0

    def test_pre_cutover_keeps_time_off_in_denominator(self):
        d = dt.date(2026, 2, 10)
        rows = [
            {"date": d, "activity_type_required": "shrinkage", "shrinkage_flag": 1},
            {"date": d, "activity_type_required": "time_off"},
        ]
        out = compute_shrinkage(make_raw(rows))
        day = out[out["date_granularity"] == "day"].iloc[0]
        # pre-cutover: time_off counts in denominator.
        assert day["denominator"] == 2.0
        assert day["metric_value"] == 50.0

    def test_zero_shrinkage_is_zero_percent(self):
        rows = [
            {"activity_type_required": "available"},
            {"activity_type_required": "oos"},
        ]
        out = compute_shrinkage(make_raw(rows))
        day = out[out["date_granularity"] == "day"].iloc[0]
        assert day["numerator"] == 0.0
        assert day["denominator"] == 2.0
        assert day["metric_value"] == 0.0

    def test_all_granularities_emitted(self):
        out = compute_shrinkage(make_raw([{}]))
        assert set(out["date_granularity"]) == {
            "day", "week", "month", "quarter", "semester", "year"
        }

    def test_week_bucket_is_monday(self):
        out = compute_shrinkage(make_raw([{"date": dt.date(2026, 5, 6)}]))
        week = out[out["date_granularity"] == "week"].iloc[0]
        assert week["date_reference"] == dt.date(2026, 5, 4)

    def test_weekly_aggregates_across_days(self):
        # Mon: 1 shrinkage / 2 required; Tue: 0 / 2 -> weekly 1/4 = 25%.
        rows = [
            {"date": dt.date(2026, 5, 4), "activity_type_required": "shrinkage",
             "shrinkage_flag": 1},
            {"date": dt.date(2026, 5, 4), "activity_type_required": "available"},
            {"date": dt.date(2026, 5, 5), "activity_type_required": "available"},
            {"date": dt.date(2026, 5, 5), "activity_type_required": "oos"},
        ]
        out = compute_shrinkage(make_raw(rows))
        week = out[out["date_granularity"] == "week"].iloc[0]
        assert week["numerator"] == 1.0
        assert week["denominator"] == 4.0
        assert week["metric_value"] == 25.0

    def test_dimensions_take_latest_value_in_bucket(self):
        out = compute_shrinkage(
            make_raw([
                {"date": dt.date(2026, 5, 4), "squad": "txn"},
                {"date": dt.date(2026, 5, 20), "squad": "cuenta"},
            ])
        )
        month = out[out["date_granularity"] == "month"].iloc[0]
        assert month["squad"] == "cuenta"

    def test_all_lunch_break_yields_empty(self):
        out = compute_shrinkage(
            make_raw([{"activity_type_required": "lunch_break"}])
        )
        assert out.empty

    def test_per_agent_separation(self):
        out = compute_shrinkage(
            make_raw([
                {"agent": "a.one", "activity_type_required": "shrinkage",
                 "shrinkage_flag": 1},
                {"agent": "b.two", "activity_type_required": "available"},
            ])
        )
        day = out[out["date_granularity"] == "day"]
        assert set(day["agent"]) == {"a.one", "b.two"}
        assert day.set_index("agent").loc["a.one", "metric_value"] == 100.0
        assert day.set_index("agent").loc["b.two", "metric_value"] == 0.0

    def test_output_schema_and_column_order(self):
        out = compute_shrinkage(make_raw([{}]))
        assert list(out.columns) == [c for c, _ in IO_SHRINKAGE_METRIC_SCHEMA]

    def test_empty_input_yields_empty_frame_with_schema(self):
        out = compute_shrinkage(make_raw([])[0:0])
        assert out.empty
        assert list(out.columns) == [c for c, _ in IO_SHRINKAGE_METRIC_SCHEMA]


class TestRollups:
    def _agent_metric(self):
        # Two agents in xforce xf1 / one in xf2 — all under xplead xp.
        # xf1: a1 has 1/4 shrinkage, a2 has 3/4 -> slot-weighted 4/8 = 50%.
        # xf2: b1 has 2/4 -> 50%.
        rows = [
            {"agent": "a1", "xforce": "xf1", "xplead": "xp",
             "activity_type_required": "shrinkage", "shrinkage_flag": 1,
             "slot_time": "09:00:00"},
            {"agent": "a1", "xforce": "xf1", "xplead": "xp",
             "activity_type_required": "available", "slot_time": "09:30:00"},
            {"agent": "a1", "xforce": "xf1", "xplead": "xp",
             "activity_type_required": "oos", "slot_time": "10:00:00"},
            {"agent": "a1", "xforce": "xf1", "xplead": "xp",
             "activity_type_required": "bko", "slot_time": "10:30:00"},
            {"agent": "a2", "xforce": "xf1", "xplead": "xp",
             "activity_type_required": "shrinkage", "shrinkage_flag": 1,
             "slot_time": "09:00:00"},
            {"agent": "a2", "xforce": "xf1", "xplead": "xp",
             "activity_type_required": "shrinkage", "shrinkage_flag": 1,
             "slot_time": "09:30:00"},
            {"agent": "a2", "xforce": "xf1", "xplead": "xp",
             "activity_type_required": "shrinkage", "shrinkage_flag": 1,
             "slot_time": "10:00:00"},
            {"agent": "a2", "xforce": "xf1", "xplead": "xp",
             "activity_type_required": "available", "slot_time": "10:30:00"},
            {"agent": "b1", "xforce": "xf2", "xplead": "xp",
             "activity_type_required": "shrinkage", "shrinkage_flag": 1,
             "slot_time": "09:00:00"},
            {"agent": "b1", "xforce": "xf2", "xplead": "xp",
             "activity_type_required": "shrinkage", "shrinkage_flag": 1,
             "slot_time": "09:30:00"},
            {"agent": "b1", "xforce": "xf2", "xplead": "xp",
             "activity_type_required": "available", "slot_time": "10:00:00"},
            {"agent": "b1", "xforce": "xf2", "xplead": "xp",
             "activity_type_required": "oos", "slot_time": "10:30:00"},
        ]
        return compute_shrinkage(make_raw(rows))

    def test_xforce_rollup_is_slot_weighted(self):
        roll = compute_shrinkage_rollups(self._agent_metric())
        day = roll[(roll["metric"] == XFORCE_METRIC_NAME)
                   & (roll["date_granularity"] == "day")]
        by_xf = day.set_index("xforce")
        assert by_xf.loc["xf1", "numerator"] == 4.0   # 1 + 3
        assert by_xf.loc["xf1", "denominator"] == 8.0  # 4 + 4
        assert by_xf.loc["xf1", "metric_value"] == 50.0
        assert by_xf.loc["xf1", "agent"] is None
        assert by_xf.loc["xf1", "xplead"] == "xp"

    def test_xplead_rollup_aggregates_across_xforces(self):
        roll = compute_shrinkage_rollups(self._agent_metric())
        day = roll[(roll["metric"] == XPLEAD_METRIC_NAME)
                   & (roll["date_granularity"] == "day")]
        assert len(day) == 1
        r = day.iloc[0]
        assert r["xforce"] is None
        assert r["xplead"] == "xp"
        assert r["numerator"] == 6.0    # 1 + 3 + 2
        assert r["denominator"] == 12.0  # 4 + 4 + 4
        assert r["metric_value"] == 50.0

    def test_output_contract(self):
        roll = compute_shrinkage_rollups(self._agent_metric())
        assert list(roll.columns) == list(METRIC_COLUMNS)
        assert set(roll["metric"]) == {XFORCE_METRIC_NAME, XPLEAD_METRIC_NAME}
        assert roll["agent"].isna().all()

    def test_empty_input_yields_empty(self):
        empty = compute_shrinkage(make_raw([])[0:0])
        roll = compute_shrinkage_rollups(empty)
        assert roll.empty
        assert list(roll.columns) == list(METRIC_COLUMNS)
