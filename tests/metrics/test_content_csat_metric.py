"""Unit tests for ``metrics/content_csat_metric.py`` (PySpark).

Small synthetic Spark frames mimicking the ``io_content_csat_raw`` table, no
warehouse. We verify the CSAT ratio (SUM(promoters) / SUM(number_of_questions)
* 100), the day/week/month aggregation across responses, team scope, dimensions
(latest in bucket), and the output contract.
"""

from __future__ import annotations

import datetime as dt

from pyspark.sql import types as T

from content_csat_metric import (
    IO_CONTENT_CSAT_METRIC_SCHEMA,
    METRIC_NAME,
    compute_content_csat,
)

RATED = dt.date(2026, 5, 1)  # month rated

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
        T.StructField("target_squad", T.StringType()),
        T.StructField("requested_by", T.StringType()),
        T.StructField("survey_timestamp", T.TimestampType()),
        T.StructField("promoters", T.IntegerType()),
        T.StructField("number_of_questions", T.IntegerType()),
        T.StructField("csat_score", T.DoubleType()),
    ]
)


def make_raw(spark, rows):
    defaults = {
        "agent": "nuberto.lopez",
        "xforce": "nuliana.cruz",
        "xplead": "nuricio.diaz",
        "team": "content",
        "squad": "enablement",
        "district": "content",
        "shift": None,
        "date": RATED,
        "target_squad": "txn",
        "requested_by": "lead.one",
        "survey_timestamp": dt.datetime(2026, 5, 2, 10, 0, 0),
        "promoters": 8,
        "number_of_questions": 8,
        "csat_score": 1.0,
    }
    data = [{**defaults, **r} for r in rows]
    return spark.createDataFrame(
        [tuple(r[f.name] for f in _RAW_SCHEMA.fields) for r in data], _RAW_SCHEMA
    )


def _day_rows(out):
    return {
        r["agent"]: r
        for r in out.filter(out["date_granularity"] == "day").collect()
    }


def _one_day(out):
    rows = out.filter(out["date_granularity"] == "day").collect()
    assert len(rows) == 1, f"expected one day row, got {len(rows)}"
    return rows[0]


class TestComputeContentCsat:
    def test_single_response_ratio(self, spark):
        row = _one_day(compute_content_csat(make_raw(spark, [{"promoters": 6}])))
        assert row["numerator"] == 6.0
        assert row["denominator"] == 8.0
        assert row["metric_value"] == 75.0
        assert row["metric"] == METRIC_NAME

    def test_aggregates_across_responses(self, spark):
        # Legacy example: promoters [7,4,8,7,6] over 8 questions each -> 80%.
        rows = [{"promoters": p} for p in (7, 4, 8, 7, 6)]
        row = _one_day(compute_content_csat(make_raw(spark, rows)))
        assert row["numerator"] == 32.0
        assert row["denominator"] == 40.0
        assert row["metric_value"] == 80.0

    def test_perfect_and_zero(self, spark):
        out = compute_content_csat(
            make_raw(
                spark,
                [
                    {"agent": "a.one", "promoters": 8},
                    {"agent": "b.two", "promoters": 0},
                ],
            )
        )
        by_agent = _day_rows(out)
        assert by_agent["a.one"]["metric_value"] == 100.0
        assert by_agent["b.two"]["metric_value"] == 0.0

    def test_null_promoters_coerced_to_zero(self, spark):
        # A NULL promoters value is treated as 0 (matches legacy fillna(0)).
        out = compute_content_csat(
            make_raw(
                spark,
                [
                    {"promoters": None, "number_of_questions": 8},
                    {"promoters": 8, "number_of_questions": 8},
                ],
            )
        )
        row = _one_day(out)
        assert row["numerator"] == 8.0
        assert row["denominator"] == 16.0
        assert row["metric_value"] == 50.0

    def test_non_content_team_excluded(self, spark):
        out = compute_content_csat(
            make_raw(spark, [{"team": "core", "promoters": 8}])
        )
        assert len(out.take(1)) == 0

    def test_all_granularities_emitted(self, spark):
        out = compute_content_csat(make_raw(spark, [{}]))
        grans = {
            r["date_granularity"] for r in out.select("date_granularity").collect()
        }
        assert grans == {"day", "week", "month", "quarter", "semester", "year"}

    def test_monthly_aggregates_multiple_dates(self, spark):
        rows = [
            {"date": dt.date(2026, 5, 1), "promoters": 8},
            {"date": dt.date(2026, 5, 20), "promoters": 4},
        ]
        out = compute_content_csat(make_raw(spark, rows))
        month = out.filter(out["date_granularity"] == "month").collect()[0]
        assert month["numerator"] == 12.0
        assert month["denominator"] == 16.0
        assert month["metric_value"] == 75.0

    def test_dimensions_take_latest_value_in_bucket(self, spark):
        rows = [
            {"date": dt.date(2026, 5, 1), "xplead": "early.lead"},
            {"date": dt.date(2026, 5, 20), "xplead": "late.lead"},
        ]
        out = compute_content_csat(make_raw(spark, rows))
        month = out.filter(out["date_granularity"] == "month").collect()[0]
        assert month["xplead"] == "late.lead"

    def test_per_agent_separation(self, spark):
        rows = [
            {"agent": "a.one", "promoters": 8},
            {"agent": "b.two", "promoters": 4},
        ]
        by_agent = _day_rows(compute_content_csat(make_raw(spark, rows)))
        assert set(by_agent) == {"a.one", "b.two"}
        assert by_agent["a.one"]["metric_value"] == 100.0
        assert by_agent["b.two"]["metric_value"] == 50.0

    def test_output_schema_and_column_order(self, spark):
        out = compute_content_csat(make_raw(spark, [{}]))
        assert out.columns == [c for c, _ in IO_CONTENT_CSAT_METRIC_SCHEMA]

    def test_empty_input_yields_empty_frame_with_schema(self, spark):
        empty = spark.createDataFrame([], _RAW_SCHEMA)
        out = compute_content_csat(empty)
        assert len(out.take(1)) == 0
        assert out.columns == [c for c, _ in IO_CONTENT_CSAT_METRIC_SCHEMA]

    def test_all_non_content_yields_empty_frame_with_schema(self, spark):
        # Non-content rows are filtered out -> the empty metric path still
        # returns the contract-shaped frame.
        out = compute_content_csat(make_raw(spark, [{"team": "core"}]))
        assert len(out.take(1)) == 0
        assert out.columns == [c for c, _ in IO_CONTENT_CSAT_METRIC_SCHEMA]
