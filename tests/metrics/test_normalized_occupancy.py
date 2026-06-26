"""Unit tests for ``metrics/normalized_occupancy.py`` (PySpark).

Small synthetic Spark frames mimicking ``io_occupancy_time_raw``, no warehouse.
We verify the productive-slot filter, the agent occupancy ratio, the two-step
district+shift benchmark (mean of squad ratios), the NO = occupancy / benchmark
ratio, NULL-shift (content) handling, the nitza.zarza suppression, and the
output contract.
"""

from __future__ import annotations

import datetime as dt

from pyspark.sql import types as T

from normalized_occupancy import (
    EXCLUDED_ACTIVITY_TYPES,
    IO_NORMALIZED_OCCUPANCY_METRIC_SCHEMA,
    METRIC_NAME,
    compute_normalized_occupancy,
)

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
        T.StructField("required_minutes", T.DoubleType()),
        T.StructField("occupancy_minutes", T.DoubleType()),
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
        "date": dt.date(2026, 5, 4),  # a Monday
        "slot_time": "09:00:00",
        "activity_type_required": "available",
        "required_minutes": 30.0,
        "occupancy_minutes": 30.0,
    }
    data = [{**defaults, **r} for r in rows]
    return spark.createDataFrame(
        [tuple(r[f.name] for f in _RAW_SCHEMA.fields) for r in data], _RAW_SCHEMA
    )


def _by_agent(out, granularity="day"):
    return {
        r["agent"]: r
        for r in out.filter(out["date_granularity"] == granularity).collect()
    }


class TestComputeNormalizedOccupancy:
    def test_single_cohort_no_is_100(self, spark):
        out = compute_normalized_occupancy(
            make_raw(
                spark,
                [
                    {"slot_time": "09:00:00", "occupancy_minutes": 30.0},
                    {"slot_time": "09:30:00", "occupancy_minutes": 15.0},
                ],
            )
        )
        day = _by_agent(out)["nuberto.lopez"]
        # occupancy = 45/60 = 75%
        assert abs(day["numerator"] - 75.0) < 1e-9
        assert abs(day["denominator"] - 75.0) < 1e-9
        assert abs(day["metric_value"] - 100.0) < 1e-9
        assert day["metric"] == METRIC_NAME

    def test_benchmark_is_mean_of_squad_ratios(self, spark):
        # district csi / morning / May has two squads:
        #   txn:    a.one occ (30 + 0)/(30+30) = 0.5
        #   cuenta: b.two occ 30/30          = 1.0
        # benchmark = mean(0.5, 1.0) = 0.75 (75%).
        out = compute_normalized_occupancy(
            make_raw(
                spark,
                [
                    {"agent": "a.one", "squad": "txn", "slot_time": "09:00:00", "occupancy_minutes": 30.0},
                    {"agent": "a.one", "squad": "txn", "slot_time": "09:30:00", "occupancy_minutes": 0.0},
                    {"agent": "b.two", "squad": "cuenta", "slot_time": "09:00:00", "occupancy_minutes": 30.0},
                ],
            )
        )
        day = _by_agent(out)
        assert abs(day["a.one"]["denominator"] - 75.0) < 1e-9
        assert abs(day["b.two"]["denominator"] - 75.0) < 1e-9
        assert abs(day["a.one"]["numerator"] - 50.0) < 1e-9
        assert abs(day["a.one"]["metric_value"] - 50 / 75 * 100) < 1e-6
        assert abs(day["b.two"]["metric_value"] - 100 / 75 * 100) < 1e-6

    def test_excluded_activity_types_dropped(self, spark):
        rows = [{"slot_time": "09:00:00", "occupancy_minutes": 30.0}]
        rows += [
            {"slot_time": "10:00:00", "activity_type_required": t, "occupancy_minutes": 0.0}
            for t in EXCLUDED_ACTIVITY_TYPES
        ]
        out = compute_normalized_occupancy(make_raw(spark, rows))
        day = _by_agent(out)["nuberto.lopez"]
        assert abs(day["numerator"] - 100.0) < 1e-9
        assert abs(day["metric_value"] - 100.0) < 1e-9

    def test_exclusion_case_insensitive(self, spark):
        out = compute_normalized_occupancy(
            make_raw(spark, [{"activity_type_required": "Shrinkage"}])
        )
        assert out.count() == 0

    def test_null_shift_content_handled(self, spark):
        out = compute_normalized_occupancy(
            make_raw(
                spark,
                [
                    {"agent": "c.one", "team": "content", "squad": "enablement", "district": "content", "shift": None, "occupancy_minutes": 30.0},
                    {"agent": "c.two", "team": "content", "squad": "enablement", "district": "content", "shift": None, "occupancy_minutes": 30.0},
                ],
            )
        )
        day = _by_agent(out)
        assert set(day) == {"c.one", "c.two"}
        assert all(abs(r["metric_value"] - 100.0) < 1e-9 for r in day.values())

    def test_nitza_no_metric_suppressed_but_still_feeds_benchmark(self, spark):
        out = compute_normalized_occupancy(
            make_raw(
                spark,
                [
                    {"agent": "nitza.zarza", "occupancy_minutes": 30.0},
                    {"agent": "peer.agent", "occupancy_minutes": 15.0},
                ],
            )
        )
        day = _by_agent(out)
        assert "nitza.zarza" not in day
        # Benchmark still includes nitza: (30 + 15) / (30 + 30) = 75%.
        assert abs(day["peer.agent"]["denominator"] - 75.0) < 1e-9

    def test_nitza_not_suppressed_outside_window(self, spark):
        # June 2026 is outside the Apr-May suppression window -> she IS emitted.
        out = compute_normalized_occupancy(
            make_raw(spark, [{"agent": "nitza.zarza", "date": dt.date(2026, 6, 1)}])
        )
        assert "nitza.zarza" in _by_agent(out)

    def test_all_granularities_emitted(self, spark):
        out = compute_normalized_occupancy(make_raw(spark, [{}]))
        grans = {r["date_granularity"] for r in out.select("date_granularity").distinct().collect()}
        assert grans == {"day", "week", "month", "quarter", "semester", "year"}

    def test_week_bucket_is_monday(self, spark):
        out = compute_normalized_occupancy(make_raw(spark, [{"date": dt.date(2026, 5, 6)}]))
        week = out.filter(out["date_granularity"] == "week").collect()[0]
        assert week["date_reference"] == dt.date(2026, 5, 4)

    def test_dimensions_take_latest_value_in_bucket(self, spark):
        out = compute_normalized_occupancy(
            make_raw(
                spark,
                [
                    {"date": dt.date(2026, 5, 4), "squad": "txn"},
                    {"date": dt.date(2026, 5, 20), "squad": "cuenta"},
                ],
            )
        )
        month = out.filter(out["date_granularity"] == "month").collect()[0]
        assert month["squad"] == "cuenta"

    def test_output_schema_and_column_order(self, spark):
        out = compute_normalized_occupancy(make_raw(spark, [{}]))
        assert out.columns == [c for c, _ in IO_NORMALIZED_OCCUPANCY_METRIC_SCHEMA]

    def test_empty_input_yields_empty_frame_with_schema(self, spark):
        out = compute_normalized_occupancy(spark.createDataFrame([], _RAW_SCHEMA))
        assert out.count() == 0
        assert out.columns == [c for c, _ in IO_NORMALIZED_OCCUPANCY_METRIC_SCHEMA]
