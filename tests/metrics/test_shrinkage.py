"""Unit tests for ``metrics/shrinkage.py`` (PySpark).

Small synthetic Spark frames mimicking the ``io_shrinkage_slots_raw`` table, no
warehouse. We verify the ratio math, the lunch_break exclusion, the pre/post-
2026-03-01 denominator rule, the day/week/month aggregation + bucketing, the
most-recent-dimension rule, the XForce/XPLead roll-ups, the output contract,
and the adjustment-tab handling (reclassify_dime_slots, drop_slot_windows for
exclusiones_generales/training/shadowing, apply_no_shrinkage).
"""

from __future__ import annotations

import datetime as dt

from pyspark.sql import types as T

from metric_utils import METRIC_COLUMNS
from shrinkage import (
    IO_SHRINKAGE_METRIC_SCHEMA,
    METRIC_NAME,
    XFORCE_METRIC_NAME,
    XPLEAD_METRIC_NAME,
    compute_shrinkage,
    compute_shrinkage_rollups,
)

# ---------------------------------------------------------------------------
# Builders — raw io_shrinkage_slots_raw frame
# ---------------------------------------------------------------------------

_RAW_SCHEMA = T.StructType(
    [
        T.StructField("agent", T.StringType()),
        T.StructField("xforce", T.StringType()),
        T.StructField("xplead", T.StringType()),
        T.StructField("team", T.StringType()),
        T.StructField("squad", T.StringType()),
        T.StructField("district", T.StringType()),
        T.StructField("shift", T.StringType()),
        T.StructField("date", T.DateType()),
        T.StructField("slot_time", T.StringType()),
        T.StructField("activity_type_required", T.StringType()),
        T.StructField("dimensioned_activity", T.StringType()),
        T.StructField("shrinkage_flag", T.IntegerType()),
        T.StructField("controllable_shrinkage_flag", T.IntegerType()),
        T.StructField("uncontrollable_shrinkage_flag", T.IntegerType()),
    ]
)


def make_raw(spark, rows):
    defaults = {
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
    data = [{**defaults, **r} for r in rows]
    return spark.createDataFrame(
        [tuple(r[f.name] for f in _RAW_SCHEMA.fields) for r in data], _RAW_SCHEMA
    )


# Adjustment-table builder. The sync step normalizes the Spanish sheet headers
# to snake_case; these are the columns the adjustments.manual helpers read.
_ADJ_SCHEMA = T.StructType(
    [
        T.StructField("agente", T.StringType()),
        T.StructField("equipo", T.StringType()),
        T.StructField("fecha_inicio", T.StringType()),
        T.StructField("fecha_fin", T.StringType()),
        T.StructField("hora_inicio", T.StringType()),
        T.StructField("hora_fin", T.StringType()),
        T.StructField("etiqueta_correcta", T.StringType()),
    ]
)


def make_adj(spark, rows):
    defaults = {
        "agente": "nuberto.lopez",
        "equipo": "Todos",
        "fecha_inicio": "2026-05-04",
        "fecha_fin": "2026-05-04",
        "hora_inicio": "00:00",
        "hora_fin": "23:59",
        "etiqueta_correcta": None,
    }
    data = [{**defaults, **r} for r in rows]
    return spark.createDataFrame(
        [tuple(r[f.name] for f in _ADJ_SCHEMA.fields) for r in data], _ADJ_SCHEMA
    )


def _by_gran(out, granularity="day"):
    return [r for r in out.collect() if r["date_granularity"] == granularity]


def _one(out, granularity="day"):
    rows = _by_gran(out, granularity)
    assert len(rows) == 1
    return rows[0]


# ---------------------------------------------------------------------------
# compute_shrinkage — ratio + denominator rule
# ---------------------------------------------------------------------------


class TestComputeShrinkage:
    def test_basic_ratio_daily(self, spark):
        # 1 shrinkage slot out of 4 required slots -> 25%.
        rows = [
            {"slot_time": "09:00:00", "activity_type_required": "shrinkage", "shrinkage_flag": 1},
            {"slot_time": "09:30:00", "activity_type_required": "available"},
            {"slot_time": "10:00:00", "activity_type_required": "oos"},
            {"slot_time": "10:30:00", "activity_type_required": "bko"},
        ]
        row = _one(compute_shrinkage(make_raw(spark, rows)))
        assert row["numerator"] == 1.0
        assert row["denominator"] == 4.0
        assert row["metric_value"] == 25.0
        assert row["metric"] == METRIC_NAME
        assert row["date_reference"] == dt.date(2026, 5, 4)

    def test_lunch_break_dropped_from_both_sides(self, spark):
        rows = [
            {"slot_time": "09:00:00", "activity_type_required": "shrinkage", "shrinkage_flag": 1},
            {"slot_time": "09:30:00", "activity_type_required": "available"},
            {"slot_time": "13:00:00", "activity_type_required": "lunch_break"},
        ]
        row = _one(compute_shrinkage(make_raw(spark, rows)))
        assert row["denominator"] == 2.0  # lunch_break excluded
        assert row["numerator"] == 1.0
        assert row["metric_value"] == 50.0

    def test_post_cutover_drops_time_off_from_denominator(self, spark):
        rows = [
            {"activity_type_required": "shrinkage", "shrinkage_flag": 1},
            {"activity_type_required": "available"},
            {"activity_type_required": "time_off"},
        ]
        row = _one(compute_shrinkage(make_raw(spark, rows)))
        assert row["denominator"] == 2.0
        assert row["metric_value"] == 50.0

    def test_post_cutover_keeps_dime_invalid_notation_in_denominator(self, spark):
        rows = [
            {"activity_type_required": "dime_invalid_notation", "dimensioned_activity": "Huddle", "shrinkage_flag": 1},
            {"activity_type_required": "available"},
        ]
        row = _one(compute_shrinkage(make_raw(spark, rows)))
        assert row["denominator"] == 2.0
        assert row["numerator"] == 1.0
        assert row["metric_value"] == 50.0

    def test_pre_cutover_drops_dime_invalid_notation_from_denominator(self, spark):
        d = dt.date(2026, 2, 10)
        rows = [
            {"date": d, "activity_type_required": "shrinkage", "shrinkage_flag": 1},
            {"date": d, "activity_type_required": "available"},
            {"date": d, "activity_type_required": "dime_invalid_notation", "dimensioned_activity": "Huddle"},
        ]
        row = _one(compute_shrinkage(make_raw(spark, rows)))
        assert row["denominator"] == 2.0
        assert row["numerator"] == 1.0
        assert row["metric_value"] == 50.0

    def test_pre_cutover_keeps_time_off_in_denominator(self, spark):
        d = dt.date(2026, 2, 10)
        rows = [
            {"date": d, "activity_type_required": "shrinkage", "shrinkage_flag": 1},
            {"date": d, "activity_type_required": "time_off"},
        ]
        row = _one(compute_shrinkage(make_raw(spark, rows)))
        assert row["denominator"] == 2.0
        assert row["metric_value"] == 50.0

    def test_zero_shrinkage_is_zero_percent(self, spark):
        rows = [
            {"activity_type_required": "available"},
            {"activity_type_required": "oos"},
        ]
        row = _one(compute_shrinkage(make_raw(spark, rows)))
        assert row["numerator"] == 0.0
        assert row["denominator"] == 2.0
        assert row["metric_value"] == 0.0

    def test_all_granularities_emitted(self, spark):
        out = compute_shrinkage(make_raw(spark, [{}]))
        grans = {r["date_granularity"] for r in out.collect()}
        assert grans == {"day", "week", "month", "quarter", "semester", "year"}

    def test_week_bucket_is_monday(self, spark):
        out = compute_shrinkage(make_raw(spark, [{"date": dt.date(2026, 5, 6)}]))
        week = _one(out, "week")
        assert week["date_reference"] == dt.date(2026, 5, 4)

    def test_weekly_aggregates_across_days(self, spark):
        rows = [
            {"date": dt.date(2026, 5, 4), "activity_type_required": "shrinkage", "shrinkage_flag": 1},
            {"date": dt.date(2026, 5, 4), "activity_type_required": "available"},
            {"date": dt.date(2026, 5, 5), "activity_type_required": "available"},
            {"date": dt.date(2026, 5, 5), "activity_type_required": "oos"},
        ]
        week = _one(compute_shrinkage(make_raw(spark, rows)), "week")
        assert week["numerator"] == 1.0
        assert week["denominator"] == 4.0
        assert week["metric_value"] == 25.0

    def test_dimensions_take_latest_value_in_bucket(self, spark):
        out = compute_shrinkage(
            make_raw(
                spark,
                [
                    {"date": dt.date(2026, 5, 4), "squad": "txn"},
                    {"date": dt.date(2026, 5, 20), "squad": "cuenta"},
                ],
            )
        )
        month = _one(out, "month")
        assert month["squad"] == "cuenta"

    def test_all_lunch_break_yields_empty(self, spark):
        out = compute_shrinkage(make_raw(spark, [{"activity_type_required": "lunch_break"}]))
        assert out.count() == 0

    def test_per_agent_separation(self, spark):
        out = compute_shrinkage(
            make_raw(
                spark,
                [
                    {"agent": "a.one", "activity_type_required": "shrinkage", "shrinkage_flag": 1},
                    {"agent": "b.two", "activity_type_required": "available"},
                ],
            )
        )
        day = {r["agent"]: r for r in _by_gran(out)}
        assert set(day) == {"a.one", "b.two"}
        assert day["a.one"]["metric_value"] == 100.0
        assert day["b.two"]["metric_value"] == 0.0

    def test_output_schema_and_column_order(self, spark):
        out = compute_shrinkage(make_raw(spark, [{}]))
        assert out.columns == [c for c, _ in IO_SHRINKAGE_METRIC_SCHEMA]

    def test_empty_input_yields_empty_frame_with_schema(self, spark):
        out = compute_shrinkage(spark.createDataFrame([], _RAW_SCHEMA))
        assert out.count() == 0
        assert out.columns == [c for c, _ in IO_SHRINKAGE_METRIC_SCHEMA]


# ---------------------------------------------------------------------------
# Adjustment-tab handling
# ---------------------------------------------------------------------------


class TestAdjustments:
    def test_drop_slot_windows_general_exclusion(self, spark):
        # Exclusiones Generales removes a matched (agent, date, window) slot from
        # BOTH numerator and denominator. Drop the 09:00 shrinkage slot -> only
        # the 09:30 available slot remains -> 0/1 = 0%.
        raw = make_raw(
            spark,
            [
                {"slot_time": "09:00:00", "activity_type_required": "shrinkage", "shrinkage_flag": 1},
                {"slot_time": "09:30:00", "activity_type_required": "available"},
            ],
        )
        adj = make_adj(spark, [{"hora_inicio": "09:00", "hora_fin": "09:30"}])
        row = _one(compute_shrinkage(raw, general_exclusions=adj))
        assert row["numerator"] == 0.0
        assert row["denominator"] == 1.0
        assert row["metric_value"] == 0.0

    def test_training_and_shadowing_drop_windows(self, spark):
        # Training + Shadowing tabs are additional drop_slot_windows sources.
        raw = make_raw(
            spark,
            [
                {"slot_time": "11:00:00", "activity_type_required": "available"},
                {"slot_time": "13:00:00", "activity_type_required": "available"},
                {"slot_time": "15:00:00", "activity_type_required": "shrinkage", "shrinkage_flag": 1},
            ],
        )
        training = make_adj(spark, [{"hora_inicio": "11:00", "hora_fin": "12:00"}])
        shadowing = make_adj(spark, [{"hora_inicio": "13:00", "hora_fin": "14:00"}])
        # 11:00 and 13:00 slots dropped -> only the 15:00 shrinkage slot remains.
        row = _one(compute_shrinkage(raw, training=training, shadowing=shadowing))
        assert row["numerator"] == 1.0
        assert row["denominator"] == 1.0
        assert row["metric_value"] == 100.0

    def test_apply_no_shrinkage_keeps_required_clears_numerator(self, spark):
        # No Shrinkage keeps the slot in the required base but clears the
        # shrinkage numerator flag. A 09:00 shrinkage slot becomes 0 numerator
        # but stays a required slot -> 0/2 = 0%.
        raw = make_raw(
            spark,
            [
                {"slot_time": "09:00:00", "activity_type_required": "shrinkage", "shrinkage_flag": 1, "controllable_shrinkage_flag": 1},
                {"slot_time": "09:30:00", "activity_type_required": "available"},
            ],
        )
        adj = make_adj(spark, [{"hora_inicio": "09:00", "hora_fin": "09:30"}])
        row = _one(compute_shrinkage(raw, no_shrinkage=adj))
        assert row["numerator"] == 0.0
        assert row["denominator"] == 2.0  # required slot preserved
        assert row["metric_value"] == 0.0

    def test_reclassify_dime_slots_relabels_and_reflags(self, spark):
        # Inconsistencias DIME relabels activity_type_required to the corrected
        # label and re-derives shrinkage_flag from it. Relabel an available slot
        # to 'shrinkage' -> it becomes the numerator. 1/2 = 50%.
        raw = make_raw(
            spark,
            [
                {"slot_time": "09:00:00", "activity_type_required": "available"},
                {"slot_time": "09:30:00", "activity_type_required": "available"},
            ],
        )
        adj = make_adj(
            spark,
            [{"hora_inicio": "09:00", "hora_fin": "09:30", "etiqueta_correcta": "shrinkage"}],
        )
        row = _one(compute_shrinkage(raw, dime_inconsistencies=adj))
        assert row["numerator"] == 1.0
        assert row["denominator"] == 2.0
        assert row["metric_value"] == 50.0


# ---------------------------------------------------------------------------
# Roll-ups (XForce / XPLead, slot-weighted)
# ---------------------------------------------------------------------------


class TestRollups:
    def _agent_metric(self, spark):
        # xf1: a1 has 1/4 shrinkage, a2 has 3/4 -> slot-weighted 4/8 = 50%.
        # xf2: b1 has 2/4 -> 50%.
        rows = []
        for agent, xf, acts in [
            ("a1", "xf1", ["shrinkage", "available", "oos", "bko"]),
            ("a2", "xf1", ["shrinkage", "shrinkage", "shrinkage", "available"]),
            ("b1", "xf2", ["shrinkage", "shrinkage", "available", "oos"]),
        ]:
            for i, act in enumerate(acts):
                rows.append(
                    {
                        "agent": agent,
                        "xforce": xf,
                        "xplead": "xp",
                        "activity_type_required": act,
                        "shrinkage_flag": 1 if act == "shrinkage" else 0,
                        "slot_time": f"{9 + i // 2:02d}:{(i % 2) * 30:02d}:00",
                    }
                )
        return compute_shrinkage(make_raw(spark, rows))

    def test_xforce_rollup_is_slot_weighted(self, spark):
        roll = compute_shrinkage_rollups(self._agent_metric(spark))
        day = {
            r["xforce"]: r
            for r in roll.collect()
            if r["metric"] == XFORCE_METRIC_NAME and r["date_granularity"] == "day"
        }
        assert day["xf1"]["numerator"] == 4.0  # 1 + 3
        assert day["xf1"]["denominator"] == 8.0  # 4 + 4
        assert day["xf1"]["metric_value"] == 50.0
        assert day["xf1"]["agent"] is None
        assert day["xf1"]["xplead"] == "xp"

    def test_xplead_rollup_aggregates_across_xforces(self, spark):
        roll = compute_shrinkage_rollups(self._agent_metric(spark))
        day = [
            r
            for r in roll.collect()
            if r["metric"] == XPLEAD_METRIC_NAME and r["date_granularity"] == "day"
        ]
        assert len(day) == 1
        r = day[0]
        assert r["xforce"] is None
        assert r["xplead"] == "xp"
        assert r["numerator"] == 6.0  # 1 + 3 + 2
        assert r["denominator"] == 12.0  # 4 + 4 + 4
        assert r["metric_value"] == 50.0

    def test_output_contract(self, spark):
        roll = compute_shrinkage_rollups(self._agent_metric(spark))
        assert roll.columns == list(METRIC_COLUMNS)
        assert {r["metric"] for r in roll.collect()} == {
            XFORCE_METRIC_NAME,
            XPLEAD_METRIC_NAME,
        }
        assert all(r["agent"] is None for r in roll.collect())

    def test_empty_input_yields_empty(self, spark):
        empty = compute_shrinkage(spark.createDataFrame([], _RAW_SCHEMA))
        roll = compute_shrinkage_rollups(empty)
        assert roll.count() == 0
        assert roll.columns == list(METRIC_COLUMNS)
