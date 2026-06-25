"""Unit tests for ``metrics/adherence.py``.

Small synthetic frames mimicking the ``io_adherent_time_raw`` table, no
warehouse. We verify the productive-slot filter, the ratio math, the
day/week/month aggregation + bucketing, the most-recent-dimension rule, and
the output contract.
"""

from __future__ import annotations

import datetime as dt

import pandas as pd

from adherence import (
    ADHERENCE_EXCLUDED_ACTIVITY_TYPES,
    IO_ADHERENCE_METRIC_SCHEMA,
    METRIC_NAME,
    compute_adherence,
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
        "adherent_minutes": 30.0,
    }
    base.update(overrides)
    return base


def make_raw(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame([make_slot(**r) for r in rows])


class TestComputeAdherence:
    def test_basic_ratio_daily(self):
        # two slots, 30/30 and 15/30 -> 45/60 = 75%
        out = compute_adherence(
            make_raw([
                {"slot_time": "09:00:00", "adherent_minutes": 30.0},
                {"slot_time": "09:30:00", "adherent_minutes": 15.0},
            ])
        )
        day = out[out["date_granularity"] == "day"]
        assert len(day) == 1
        row = day.iloc[0]
        assert row["numerator"] == 45.0
        assert row["denominator"] == 60.0
        assert row["metric_value"] == 75.0
        assert row["metric"] == METRIC_NAME
        assert row["date_reference"] == dt.date(2026, 5, 4)

    def test_excluded_activity_types_dropped(self):
        # lunch_break / time_off / shrinkage must not affect numerator or denom.
        rows = [{"slot_time": "09:00:00", "adherent_minutes": 30.0}]
        rows += [
            {"slot_time": "10:00:00", "activity_type_required": t,
             "adherent_minutes": 30.0}
            for t in ADHERENCE_EXCLUDED_ACTIVITY_TYPES
        ]
        out = compute_adherence(make_raw(rows))
        day = out[out["date_granularity"] == "day"].iloc[0]
        assert day["denominator"] == 30.0  # only the productive slot counts
        assert day["metric_value"] == 100.0

    def test_exclusion_is_case_insensitive(self):
        out = compute_adherence(
            make_raw([{"activity_type_required": "Shrinkage"}])
        )
        # the only slot is excluded -> empty result
        assert out.empty

    def test_all_granularities_emitted(self):
        out = compute_adherence(make_raw([{}]))
        assert set(out["date_granularity"]) == {
            "day", "week", "month", "quarter", "semester", "year"
        }

    def test_week_bucket_is_monday(self):
        # Wednesday 2026-05-06 -> week bucket Monday 2026-05-04
        out = compute_adherence(make_raw([{"date": dt.date(2026, 5, 6)}]))
        week = out[out["date_granularity"] == "week"].iloc[0]
        assert week["date_reference"] == dt.date(2026, 5, 4)

    def test_month_bucket_is_first_of_month(self):
        out = compute_adherence(make_raw([{"date": dt.date(2026, 5, 6)}]))
        month = out[out["date_granularity"] == "month"].iloc[0]
        assert month["date_reference"] == dt.date(2026, 5, 1)

    def test_weekly_aggregates_across_days(self):
        # Mon + Tue in the same week: 30/30 and 0/30 -> 30/60 = 50% weekly
        out = compute_adherence(
            make_raw([
                {"date": dt.date(2026, 5, 4), "adherent_minutes": 30.0},
                {"date": dt.date(2026, 5, 5), "adherent_minutes": 0.0},
            ])
        )
        week = out[out["date_granularity"] == "week"].iloc[0]
        assert week["numerator"] == 30.0
        assert week["denominator"] == 60.0
        assert week["metric_value"] == 50.0

    def test_dimensions_take_latest_value_in_bucket(self):
        # squad changes mid-month; monthly row should carry the later squad.
        out = compute_adherence(
            make_raw([
                {"date": dt.date(2026, 5, 4), "squad": "txn"},
                {"date": dt.date(2026, 5, 20), "squad": "cuenta"},
            ])
        )
        month = out[out["date_granularity"] == "month"].iloc[0]
        assert month["squad"] == "cuenta"

    def test_zero_denominator_yields_null_metric_value(self):
        # Construct a productive slot with 0 required minutes (degenerate).
        out = compute_adherence(
            make_raw([{"required_minutes": 0.0, "adherent_minutes": 0.0}])
        )
        day = out[out["date_granularity"] == "day"].iloc[0]
        assert pd.isna(day["metric_value"])

    def test_per_agent_separation(self):
        out = compute_adherence(
            make_raw([
                {"agent": "a.one", "adherent_minutes": 30.0},
                {"agent": "b.two", "adherent_minutes": 0.0},
            ])
        )
        day = out[out["date_granularity"] == "day"]
        assert set(day["agent"]) == {"a.one", "b.two"}
        assert day.set_index("agent").loc["a.one", "metric_value"] == 100.0
        assert day.set_index("agent").loc["b.two", "metric_value"] == 0.0

    def test_output_schema_and_column_order(self):
        out = compute_adherence(make_raw([{}]))
        assert list(out.columns) == [c for c, _ in IO_ADHERENCE_METRIC_SCHEMA]

    def test_empty_input_yields_empty_frame_with_schema(self):
        out = compute_adherence(make_raw([])[0:0])
        assert out.empty
        assert list(out.columns) == [c for c, _ in IO_ADHERENCE_METRIC_SCHEMA]
