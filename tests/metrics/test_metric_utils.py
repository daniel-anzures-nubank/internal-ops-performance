"""Unit tests for ``metrics/metric_utils.py`` (shared aggregation, PySpark).

Small synthetic Spark frames, no warehouse. We verify the Column-based
``bucket_date`` mapping per granularity and the ``aggregate_long`` sum + ratio
semantics (incl. the zero-denominator NULL and the output contract).
"""

from __future__ import annotations

import datetime as dt

from pyspark.sql import functions as F
from pyspark.sql import types as T

from metric_utils import METRIC_COLUMNS, aggregate_long, bucket_date


# ---------------------------------------------------------------------------
# bucket_date — Column-based, one date -> its period-bucket start
# ---------------------------------------------------------------------------


def _bucket(spark, d: dt.date, granularity: str) -> dt.date:
    df = spark.createDataFrame([(d,)], T.StructType([T.StructField("date", T.DateType())]))
    return df.select(bucket_date(F.col("date"), granularity).alias("b")).collect()[0]["b"]


class TestBucketDate:
    def test_day(self, spark):
        assert _bucket(spark, dt.date(2026, 5, 6), "day") == dt.date(2026, 5, 6)

    def test_week_is_monday(self, spark):
        assert _bucket(spark, dt.date(2026, 5, 6), "week") == dt.date(2026, 5, 4)

    def test_month_is_first(self, spark):
        assert _bucket(spark, dt.date(2026, 5, 6), "month") == dt.date(2026, 5, 1)

    def test_quarter_is_first_of_quarter(self, spark):
        assert _bucket(spark, dt.date(2026, 5, 6), "quarter") == dt.date(2026, 4, 1)

    def test_semester_first_half(self, spark):
        assert _bucket(spark, dt.date(2026, 5, 6), "semester") == dt.date(2026, 1, 1)

    def test_semester_second_half(self, spark):
        assert _bucket(spark, dt.date(2026, 9, 6), "semester") == dt.date(2026, 7, 1)

    def test_year_is_first_of_year(self, spark):
        assert _bucket(spark, dt.date(2026, 5, 6), "year") == dt.date(2026, 1, 1)

    def test_unknown_raises(self, spark):
        try:
            bucket_date(F.col("date"), "decade")
            assert False, "expected ValueError"
        except ValueError:
            pass


# ---------------------------------------------------------------------------
# aggregate_long — sum numerator/denominator + ratio per (agent, bucket)
# ---------------------------------------------------------------------------

_AGG_SCHEMA = T.StructType(
    [
        T.StructField("agent", T.StringType()),
        T.StructField("xforce", T.StringType()),
        T.StructField("xplead", T.StringType()),
        T.StructField("team", T.StringType()),
        T.StructField("squad", T.StringType()),
        T.StructField("district", T.StringType()),
        T.StructField("shift", T.StringType()),
        T.StructField("date", T.DateType()),
        T.StructField("num", T.DoubleType()),
        T.StructField("den", T.DoubleType()),
    ]
)


def make_df(spark, rows):
    defaults = {
        "agent": "a.one",
        "xforce": "x.f",
        "xplead": "x.p",
        "team": "core",
        "squad": "txn",
        "district": "csi",
        "shift": "morning",
        "date": dt.date(2026, 5, 6),  # a Wednesday
        "num": 30.0,
        "den": 30.0,
    }
    data = [{**defaults, **r} for r in rows]
    return spark.createDataFrame(
        [tuple(r[f.name] for f in _AGG_SCHEMA.fields) for r in data], _AGG_SCHEMA
    )


def _day(out):
    rows = out.filter(out["date_granularity"] == "day").collect()
    assert len(rows) == 1, f"expected one day row, got {len(rows)}"
    return rows[0]


class TestAggregateLong:
    def test_basic_sum_and_ratio(self, spark):
        out = aggregate_long(
            make_df(spark, [{"num": 30.0, "den": 30.0}, {"num": 15.0, "den": 30.0}]),
            numerator_col="num",
            denominator_col="den",
            metric_name="test",
        )
        day = _day(out)
        assert day["numerator"] == 45.0
        assert day["denominator"] == 60.0
        assert day["metric_value"] == 75.0
        assert day["metric"] == "test"

    def test_zero_denominator_is_null(self, spark):
        out = aggregate_long(
            make_df(spark, [{"num": 0.0, "den": 0.0}]),
            numerator_col="num",
            denominator_col="den",
            metric_name="test",
        )
        assert _day(out)["metric_value"] is None

    def test_columns_and_order(self, spark):
        out = aggregate_long(
            make_df(spark, [{}]),
            numerator_col="num",
            denominator_col="den",
            metric_name="test",
        )
        assert out.columns == list(METRIC_COLUMNS)

    def test_empty_input(self, spark):
        empty = spark.createDataFrame([], _AGG_SCHEMA)
        out = aggregate_long(
            empty,
            numerator_col="num",
            denominator_col="den",
            metric_name="test",
        )
        assert len(out.take(1)) == 0
        assert out.columns == list(METRIC_COLUMNS)
