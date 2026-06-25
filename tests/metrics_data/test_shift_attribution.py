"""Unit tests for ``metrics_data/shift_attribution.py`` (PySpark).

Tests for the night-shift re-attribution helper. They cover:

  * ``night_agent_months`` — selecting only ``shift == 'night'`` roster rows and
    normalizing ``snapshot_month`` to a month-start ``DATE``.
  * ``shift_start_date`` — the noon-boundary roll-back, the 2026-07-01 cutover
    gate, the clamp that keeps the June 30 -> July 1 boundary shift on its legacy
    (split) attribution, and the no-op for morning / non-night agents.
"""

from __future__ import annotations

import datetime as dt

from pyspark.sql import types as T

from shift_attribution import (
    NIGHT_SHIFT_CUTOVER,
    night_agent_months,
    shift_start_date,
)

# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------

_AGENT_INFO_SCHEMA = T.StructType(
    [
        T.StructField("agent", T.StringType()),
        T.StructField("shift", T.StringType()),
        T.StructField("snapshot_month", T.DateType()),
    ]
)

_ACTIVITY_SCHEMA = T.StructType(
    [
        T.StructField("agent", T.StringType()),
        T.StructField("local_ts", T.TimestampType()),
        T.StructField("date", T.DateType()),
    ]
)


def make_agent_info(spark, rows):
    defaults = {"agent": "nyx.owl", "shift": "night", "snapshot_month": dt.date(2026, 7, 1)}
    data = [{**defaults, **r} for r in rows]
    return spark.createDataFrame(
        [(r["agent"], r["shift"], r["snapshot_month"]) for r in data],
        _AGENT_INFO_SCHEMA,
    )


def make_activity(spark, rows):
    defaults = {
        "agent": "nyx.owl",
        "local_ts": dt.datetime(2026, 7, 5, 22, 0, 0),
        "date": dt.date(2026, 7, 5),
    }
    data = [{**defaults, **r} for r in rows]
    return spark.createDataFrame(
        [(r["agent"], r["local_ts"], r["date"]) for r in data],
        _ACTIVITY_SCHEMA,
    )


def _attribute(activity, agent_info):
    out = shift_start_date(
        activity,
        agent_col="agent",
        local_ts_col="local_ts",
        calendar_date_col="date",
        night_months=night_agent_months(agent_info),
    )
    return [r["date"] for r in out.collect()]


# ---------------------------------------------------------------------------
# night_agent_months
# ---------------------------------------------------------------------------


class TestNightAgentMonths:
    def test_keeps_only_night_rows(self, spark):
        out = night_agent_months(
            make_agent_info(
                spark,
                [
                    {"agent": "a", "shift": "night"},
                    {"agent": "b", "shift": "morning"},
                    {"agent": "c", "shift": None},
                ],
            )
        ).collect()
        assert {r["agent"] for r in out} == {"a"}
        assert all(r["is_night"] for r in out)

    def test_case_insensitive_shift_label(self, spark):
        out = night_agent_months(make_agent_info(spark, [{"agent": "a", "shift": "NIGHT"}]))
        assert out.count() == 1

    def test_normalizes_snapshot_month_to_month_start(self, spark):
        out = night_agent_months(
            make_agent_info(spark, [{"snapshot_month": dt.date(2026, 7, 31)}])
        ).collect()
        assert out[0]["snapshot_month"] == dt.date(2026, 7, 1)

    def test_dedups(self, spark):
        out = night_agent_months(
            make_agent_info(
                spark,
                [
                    {"agent": "a", "snapshot_month": dt.date(2026, 7, 1)},
                    {"agent": "a", "snapshot_month": dt.date(2026, 7, 15)},
                ],
            )
        )
        assert out.count() == 1

    def test_no_night_agents_returns_empty_typed_frame(self, spark):
        out = night_agent_months(make_agent_info(spark, [{"shift": "morning"}]))
        assert out.count() == 0
        assert out.columns == ["agent", "snapshot_month", "is_night"]


# ---------------------------------------------------------------------------
# shift_start_date
# ---------------------------------------------------------------------------


class TestShiftStartDate:
    def test_evening_head_stays_on_start_day(self, spark):
        out = _attribute(
            make_activity(
                spark,
                [{"local_ts": dt.datetime(2026, 7, 5, 22, 0, 0), "date": dt.date(2026, 7, 5)}],
            ),
            make_agent_info(spark, [{}]),
        )
        assert out[0] == dt.date(2026, 7, 5)

    def test_early_morning_tail_rolls_back_to_start_day(self, spark):
        out = _attribute(
            make_activity(
                spark,
                [{"local_ts": dt.datetime(2026, 7, 6, 3, 0, 0), "date": dt.date(2026, 7, 6)}],
            ),
            make_agent_info(spark, [{}]),
        )
        assert out[0] == dt.date(2026, 7, 5)

    def test_morning_agent_is_never_touched(self, spark):
        out = _attribute(
            make_activity(
                spark,
                [{"local_ts": dt.datetime(2026, 7, 6, 3, 0, 0), "date": dt.date(2026, 7, 6)}],
            ),
            make_agent_info(spark, [{"shift": "morning"}]),
        )
        assert out[0] == dt.date(2026, 7, 6)

    def test_before_cutover_keeps_legacy_split(self, spark):
        out = _attribute(
            make_activity(
                spark,
                [{"local_ts": dt.datetime(2026, 6, 30, 3, 0, 0), "date": dt.date(2026, 6, 30)}],
            ),
            make_agent_info(spark, [{"snapshot_month": dt.date(2026, 6, 1)}]),
        )
        assert out[0] == dt.date(2026, 6, 30)

    def test_july_1_boundary_tail_is_clamped(self, spark):
        out = _attribute(
            make_activity(
                spark,
                [{"local_ts": dt.datetime(2026, 7, 1, 3, 0, 0), "date": dt.date(2026, 7, 1)}],
            ),
            make_agent_info(spark, [{}]),
        )
        assert out[0] == dt.date(2026, 7, 1)

    def test_unknown_night_month_is_left_join_miss(self, spark):
        out = _attribute(
            make_activity(
                spark,
                [{"local_ts": dt.datetime(2026, 8, 2, 3, 0, 0), "date": dt.date(2026, 8, 2)}],
            ),
            make_agent_info(spark, [{"snapshot_month": dt.date(2026, 7, 1)}]),
        )
        assert out[0] == dt.date(2026, 8, 2)

    def test_empty_frame_returns_empty_result(self, spark):
        empty = make_activity(spark, [{}]).limit(0)
        out = shift_start_date(
            empty,
            agent_col="agent",
            local_ts_col="local_ts",
            calendar_date_col="date",
            night_months=night_agent_months(make_agent_info(spark, [{}])),
        )
        assert out.count() == 0

    def test_preserves_other_columns_and_order(self, spark):
        activity = make_activity(spark, [{}])
        out = shift_start_date(
            activity,
            agent_col="agent",
            local_ts_col="local_ts",
            calendar_date_col="date",
            night_months=night_agent_months(make_agent_info(spark, [{}])),
        )
        assert out.columns == activity.columns

    def test_cutover_constant_is_july_1_2026(self):
        assert NIGHT_SHIFT_CUTOVER == dt.date(2026, 7, 1)
