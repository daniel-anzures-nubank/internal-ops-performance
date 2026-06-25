"""Unit tests for ``metrics/adherence.py`` (PySpark).

Small synthetic frames mimicking the ``io_adherent_time_raw`` table, built via
the session-scoped ``spark`` fixture. We verify the productive-slot filter, the
ratio math, the day/week/month aggregation + bucketing, the most-recent-dimension
rule, and the output contract.
"""

from __future__ import annotations

import datetime as dt

from pyspark.sql import functions as F
from pyspark.sql import types as T

from adherence import (
    ADHERENCE_EXCLUDED_ACTIVITY_TYPES,
    IO_ADHERENCE_METRIC_SCHEMA,
    METRIC_NAME,
    compute_adherence,
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
        T.StructField("adherent_minutes", T.DoubleType()),
    ]
)


def _slot(**overrides) -> dict:
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


def make_raw(spark, rows):
    data = [_slot(**r) for r in rows]
    return spark.createDataFrame(
        [tuple(r[f.name] for f in _RAW_SCHEMA.fields) for r in data], _RAW_SCHEMA
    )


def _gran(out, granularity):
    return out.filter(F.col("date_granularity") == granularity).collect()


class TestComputeAdherence:
    def test_basic_ratio_daily(self, spark):
        out = compute_adherence(
            make_raw(
                spark,
                [
                    {"slot_time": "09:00:00", "adherent_minutes": 30.0},
                    {"slot_time": "09:30:00", "adherent_minutes": 15.0},
                ],
            )
        )
        day = _gran(out, "day")
        assert len(day) == 1
        row = day[0]
        assert row["numerator"] == 45.0
        assert row["denominator"] == 60.0
        assert row["metric_value"] == 75.0
        assert row["metric"] == METRIC_NAME
        assert row["date_reference"] == dt.date(2026, 5, 4)

    def test_excluded_activity_types_dropped(self, spark):
        rows = [{"slot_time": "09:00:00", "adherent_minutes": 30.0}]
        rows += [
            {"slot_time": "10:00:00", "activity_type_required": t, "adherent_minutes": 30.0}
            for t in ADHERENCE_EXCLUDED_ACTIVITY_TYPES
        ]
        day = _gran(compute_adherence(make_raw(spark, rows)), "day")[0]
        assert day["denominator"] == 30.0
        assert day["metric_value"] == 100.0

    def test_exclusion_is_case_insensitive(self, spark):
        out = compute_adherence(make_raw(spark, [{"activity_type_required": "Shrinkage"}]))
        assert out.count() == 0

    def test_all_granularities_emitted(self, spark):
        out = compute_adherence(make_raw(spark, [{}]))
        grans = {r["date_granularity"] for r in out.collect()}
        assert grans == {"day", "week", "month", "quarter", "semester", "year"}

    def test_week_bucket_is_monday(self, spark):
        out = compute_adherence(make_raw(spark, [{"date": dt.date(2026, 5, 6)}]))
        assert _gran(out, "week")[0]["date_reference"] == dt.date(2026, 5, 4)

    def test_month_bucket_is_first_of_month(self, spark):
        out = compute_adherence(make_raw(spark, [{"date": dt.date(2026, 5, 6)}]))
        assert _gran(out, "month")[0]["date_reference"] == dt.date(2026, 5, 1)

    def test_weekly_aggregates_across_days(self, spark):
        out = compute_adherence(
            make_raw(
                spark,
                [
                    {"date": dt.date(2026, 5, 4), "adherent_minutes": 30.0},
                    {"date": dt.date(2026, 5, 5), "adherent_minutes": 0.0},
                ],
            )
        )
        week = _gran(out, "week")[0]
        assert week["numerator"] == 30.0
        assert week["denominator"] == 60.0
        assert week["metric_value"] == 50.0

    def test_dimensions_take_latest_value_in_bucket(self, spark):
        out = compute_adherence(
            make_raw(
                spark,
                [
                    {"date": dt.date(2026, 5, 4), "squad": "txn"},
                    {"date": dt.date(2026, 5, 20), "squad": "cuenta"},
                ],
            )
        )
        assert _gran(out, "month")[0]["squad"] == "cuenta"

    def test_zero_denominator_yields_null_metric_value(self, spark):
        out = compute_adherence(
            make_raw(spark, [{"required_minutes": 0.0, "adherent_minutes": 0.0}])
        )
        assert _gran(out, "day")[0]["metric_value"] is None

    def test_per_agent_separation(self, spark):
        out = compute_adherence(
            make_raw(
                spark,
                [
                    {"agent": "a.one", "adherent_minutes": 30.0},
                    {"agent": "b.two", "adherent_minutes": 0.0},
                ],
            )
        )
        day = {r["agent"]: r["metric_value"] for r in _gran(out, "day")}
        assert set(day) == {"a.one", "b.two"}
        assert day["a.one"] == 100.0
        assert day["b.two"] == 0.0

    def test_output_schema_and_column_order(self, spark):
        out = compute_adherence(make_raw(spark, [{}]))
        assert out.columns == [c for c, _ in IO_ADHERENCE_METRIC_SCHEMA]

    def test_empty_input_yields_empty_frame_with_schema(self, spark):
        out = compute_adherence(make_raw(spark, []))
        assert out.count() == 0
        assert out.columns == [c for c, _ in IO_ADHERENCE_METRIC_SCHEMA]
