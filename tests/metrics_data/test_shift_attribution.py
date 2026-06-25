"""Unit tests for ``metrics_data/shift_attribution.py``.

Pure-pandas tests for the night-shift re-attribution helper. They cover:

  * ``night_agent_months`` — selecting only ``shift == 'night'`` roster rows and
    normalizing ``snapshot_month`` to a tz-naive month-start.
  * ``shift_start_date`` — the noon-boundary roll-back, the 2026-07-01 cutover
    gate, the clamp that keeps the June 30 -> July 1 boundary shift on its legacy
    (split) attribution, and the no-op for morning / non-night agents.
"""

from __future__ import annotations

import datetime as dt

import pandas as pd

from shift_attribution import (
    NIGHT_SHIFT_CUTOVER,
    night_agent_months,
    shift_start_date,
)


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------


def make_agent_info(rows: list[dict]) -> pd.DataFrame:
    defaults = {
        "agent": "nyx.owl",
        "shift": "night",
        "snapshot_month": dt.date(2026, 7, 1),
    }
    return pd.DataFrame([{**defaults, **r} for r in rows])


def make_activity(rows: list[dict]) -> pd.DataFrame:
    """Frame with the three columns ``shift_start_date`` needs."""
    defaults = {
        "agent": "nyx.owl",
        "local_ts": pd.Timestamp("2026-07-05 22:00:00"),
        "date": dt.date(2026, 7, 5),
    }
    return pd.DataFrame([{**defaults, **r} for r in rows])


def _attribute(activity: pd.DataFrame, agent_info: pd.DataFrame) -> pd.Series:
    return shift_start_date(
        activity,
        agent_col="agent",
        local_ts_col="local_ts",
        calendar_date_col="date",
        night_months=night_agent_months(agent_info),
    )


# ---------------------------------------------------------------------------
# night_agent_months
# ---------------------------------------------------------------------------


class TestNightAgentMonths:
    def test_keeps_only_night_rows(self):
        out = night_agent_months(
            make_agent_info(
                [
                    {"agent": "a", "shift": "night"},
                    {"agent": "b", "shift": "morning"},
                    {"agent": "c", "shift": None},
                ]
            )
        )
        assert set(out["agent"]) == {"a"}
        assert out["is_night"].all()

    def test_case_insensitive_shift_label(self):
        out = night_agent_months(make_agent_info([{"agent": "a", "shift": "NIGHT"}]))
        assert len(out) == 1

    def test_normalizes_snapshot_month_to_month_start(self):
        out = night_agent_months(
            make_agent_info([{"snapshot_month": dt.date(2026, 7, 31)}])
        )
        assert out.iloc[0]["snapshot_month"] == pd.Timestamp("2026-07-01")

    def test_handles_tz_aware_snapshot_month(self):
        out = night_agent_months(
            make_agent_info([{"snapshot_month": pd.Timestamp("2026-07-01", tz="UTC")}])
        )
        assert out.iloc[0]["snapshot_month"] == pd.Timestamp("2026-07-01")
        assert out.iloc[0]["snapshot_month"].tz is None

    def test_dedups(self):
        out = night_agent_months(
            make_agent_info(
                [
                    {"agent": "a", "snapshot_month": dt.date(2026, 7, 1)},
                    {"agent": "a", "snapshot_month": dt.date(2026, 7, 15)},
                ]
            )
        )
        assert len(out) == 1

    def test_no_night_agents_returns_empty_typed_frame(self):
        out = night_agent_months(make_agent_info([{"shift": "morning"}]))
        assert out.empty
        assert list(out.columns) == ["agent", "snapshot_month", "is_night"]


# ---------------------------------------------------------------------------
# shift_start_date
# ---------------------------------------------------------------------------


class TestShiftStartDate:
    def test_evening_head_stays_on_start_day(self):
        # 22:00 on Jul 5 -> noon boundary keeps it on Jul 5.
        out = _attribute(
            make_activity([{"local_ts": pd.Timestamp("2026-07-05 22:00:00"),
                            "date": dt.date(2026, 7, 5)}]),
            make_agent_info([{}]),
        )
        assert out.iloc[0] == dt.date(2026, 7, 5)

    def test_early_morning_tail_rolls_back_to_start_day(self):
        # 03:00 on Jul 6 (calendar Jul 6) -> rolled back to Jul 5.
        out = _attribute(
            make_activity([{"local_ts": pd.Timestamp("2026-07-06 03:00:00"),
                            "date": dt.date(2026, 7, 6)}]),
            make_agent_info([{}]),
        )
        assert out.iloc[0] == dt.date(2026, 7, 5)

    def test_morning_agent_is_never_touched(self):
        out = _attribute(
            make_activity([{"local_ts": pd.Timestamp("2026-07-06 03:00:00"),
                            "date": dt.date(2026, 7, 6)}]),
            make_agent_info([{"shift": "morning"}]),
        )
        assert out.iloc[0] == dt.date(2026, 7, 6)

    def test_before_cutover_keeps_legacy_split(self):
        # Night tail on Jun 30 03:00 -> would roll to Jun 29, but it's pre-cutover.
        out = _attribute(
            make_activity([{"local_ts": pd.Timestamp("2026-06-30 03:00:00"),
                            "date": dt.date(2026, 6, 30)}]),
            make_agent_info([{"snapshot_month": dt.date(2026, 6, 1)}]),
        )
        assert out.iloc[0] == dt.date(2026, 6, 30)

    def test_july_1_boundary_tail_is_clamped(self):
        # Tail at 2026-07-01 03:00 would roll back to Jun 30 (< cutover); clamp
        # keeps it on its legacy calendar date (Jul 1) so June metrics are frozen.
        out = _attribute(
            make_activity([{"local_ts": pd.Timestamp("2026-07-01 03:00:00"),
                            "date": dt.date(2026, 7, 1)}]),
            make_agent_info([{}]),
        )
        assert out.iloc[0] == dt.date(2026, 7, 1)

    def test_unknown_night_month_is_left_join_miss(self):
        # Activity in a month with no night-roster row -> treated as non-night.
        out = _attribute(
            make_activity([{"local_ts": pd.Timestamp("2026-08-02 03:00:00"),
                            "date": dt.date(2026, 8, 2)}]),
            make_agent_info([{"snapshot_month": dt.date(2026, 7, 1)}]),
        )
        assert out.iloc[0] == dt.date(2026, 8, 2)

    def test_empty_frame_returns_empty_series(self):
        empty = make_activity([{}]).iloc[0:0]
        out = shift_start_date(
            empty,
            agent_col="agent",
            local_ts_col="local_ts",
            calendar_date_col="date",
            night_months=night_agent_months(make_agent_info([{}])),
        )
        assert out.empty

    def test_handles_tz_aware_local_ts(self):
        out = _attribute(
            make_activity(
                [{"local_ts": pd.Timestamp("2026-07-06 03:00:00", tz="UTC"),
                  "date": dt.date(2026, 7, 6)}]
            ),
            make_agent_info([{}]),
        )
        assert out.iloc[0] == dt.date(2026, 7, 5)

    def test_cutover_constant_is_july_1_2026(self):
        assert NIGHT_SHIFT_CUTOVER == pd.Timestamp("2026-07-01")
