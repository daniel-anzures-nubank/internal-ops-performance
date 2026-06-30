"""Unit tests for ``metrics/wows_metric.py`` (PySpark).

Small synthetic Spark frames mimicking ``io_wows_raw``, no warehouse. WoWs is a
COUNT metric: ``metric_value`` is the number of distinct ``case_id`` per agent
per period (denominator carries the monthly target 5, not a ratio). We verify the
distinct count, that metric_value is the count (not numerator/denom*100), the
day/week/month/quarter/semester/year bucketing, team scope, per-agent separation,
dimensions taking the latest value in the bucket, the pre-cutover 2026-03-27
outage drop (kept post-cutover), and the output contract.
"""

from __future__ import annotations

import datetime as dt

from pyspark.sql import types as T

from wows_metric import (
    IO_WOWS_METRIC_SCHEMA,
    METRIC_NAME,
    MONTHLY_TARGET,
    compute_wows,
)

LOGGED = dt.date(2026, 5, 4)  # a Monday

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
        T.StructField("case_id", T.StringType()),
    ]
)


def make_raw(spark, rows):
    defaults = {
        "agent": "nuberto.lopez",
        "xforce": "nuliana.cruz",
        "xplead": "nuricio.diaz",
        "team": "social media",
        "squad": "social_es",
        "district": "social",
        "shift": "morning",
        "date": LOGGED,
        "case_id": "W1",
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


class TestComputeWows:
    def test_counts_distinct_case_ids(self, spark):
        row = _one_day(
            compute_wows(
                make_raw(
                    spark,
                    [{"case_id": "W1"}, {"case_id": "W2"}, {"case_id": "W3"}],
                )
            )
        )
        assert row["numerator"] == 3.0
        assert row["metric_value"] == 3.0
        assert row["denominator"] == float(MONTHLY_TARGET)
        assert row["metric"] == METRIC_NAME

    def test_duplicate_case_id_counted_once(self, spark):
        row = _one_day(
            compute_wows(
                make_raw(
                    spark,
                    [{"case_id": "W1"}, {"case_id": "W1"}, {"case_id": "W2"}],
                )
            )
        )
        assert row["metric_value"] == 2.0

    def test_metric_value_equals_numerator_not_ratio(self, spark):
        # WoWs is a count: metric_value must be the count, not numerator/den*100.
        row = _one_day(
            compute_wows(make_raw(spark, [{"case_id": f"W{i}"} for i in range(7)]))
        )
        assert row["metric_value"] == 7.0
        assert row["numerator"] == 7.0

    def test_weekly_aggregates_across_days(self, spark):
        out = compute_wows(
            make_raw(
                spark,
                [
                    {"date": dt.date(2026, 5, 4), "case_id": "W1"},
                    {"date": dt.date(2026, 5, 5), "case_id": "W2"},
                    {"date": dt.date(2026, 5, 6), "case_id": "W3"},
                ],
            )
        )
        week = out.filter(out["date_granularity"] == "week").collect()[0]
        assert week["date_reference"] == dt.date(2026, 5, 4)
        assert week["metric_value"] == 3.0

    def test_non_social_team_excluded(self, spark):
        out = compute_wows(make_raw(spark, [{"team": "core", "case_id": "W1"}]))
        assert len(out.take(1)) == 0

    def test_all_granularities_emitted(self, spark):
        out = compute_wows(make_raw(spark, [{}]))
        grans = {
            r["date_granularity"] for r in out.select("date_granularity").collect()
        }
        assert grans == {"day", "week", "month", "quarter", "semester", "year"}

    def test_per_agent_separation(self, spark):
        by_agent = _day_rows(
            compute_wows(
                make_raw(
                    spark,
                    [
                        {"agent": "a.one", "case_id": "W1"},
                        {"agent": "a.one", "case_id": "W2"},
                        {"agent": "b.two", "case_id": "W3"},
                    ],
                )
            )
        )
        assert set(by_agent) == {"a.one", "b.two"}
        assert by_agent["a.one"]["metric_value"] == 2.0
        assert by_agent["b.two"]["metric_value"] == 1.0

    def test_dimensions_take_latest_value_in_bucket(self, spark):
        out = compute_wows(
            make_raw(
                spark,
                [
                    {"date": dt.date(2026, 5, 4), "case_id": "W1", "squad": "social_es"},
                    {"date": dt.date(2026, 5, 20), "case_id": "W2", "squad": "social_pt"},
                ],
            )
        )
        month = out.filter(out["date_granularity"] == "month").collect()[0]
        assert month["squad"] == "social_pt"

    # --- outage-date exclusion (pre-cutover, SM-only) -----------------------

    def test_outage_date_dropped_pre_cutover(self, spark):
        # 2026-03-27 is dropped entirely (legacy general-access-problems day).
        out = compute_wows(
            make_raw(
                spark,
                [{"case_id": "W1", "date": dt.date(2026, 3, 27)}],
            )
        )
        assert len(out.take(1)) == 0

    def test_non_outage_date_kept(self, spark):
        # The day before the outage is unaffected.
        out = compute_wows(
            make_raw(
                spark,
                [{"case_id": "W1", "date": dt.date(2026, 3, 26)}],
            )
        )
        assert len(out.take(1)) > 0

    def test_outage_dropped_before_bucketing(self, spark):
        # The drop happens on the RAW rows before bucketing, so the week / month
        # counts exclude the outage WoW too (only the 03-26 WoW survives).
        out = compute_wows(
            make_raw(
                spark,
                [
                    {"case_id": "W1", "date": dt.date(2026, 3, 26)},
                    {"case_id": "W2", "date": dt.date(2026, 3, 27)},  # outage, dropped
                ],
            )
        )
        month = out.filter(out["date_granularity"] == "month").collect()[0]
        assert month["metric_value"] == 1.0

    def test_outage_date_kept_post_cutover(self, spark):
        # Same calendar day in a post-cutover year is NOT dropped (cutover-gated).
        out = compute_wows(
            make_raw(
                spark,
                [{"case_id": "W1", "date": dt.date(2027, 3, 27)}],
            )
        )
        assert len(out.take(1)) > 0

    # --- contract -----------------------------------------------------------

    def test_output_schema_and_column_order(self, spark):
        out = compute_wows(make_raw(spark, [{}]))
        assert out.columns == [c for c, _ in IO_WOWS_METRIC_SCHEMA]

    def test_empty_input_yields_empty_frame_with_schema(self, spark):
        empty = spark.createDataFrame([], _RAW_SCHEMA)
        out = compute_wows(empty)
        assert len(out.take(1)) == 0
        assert out.columns == [c for c, _ in IO_WOWS_METRIC_SCHEMA]

    def test_all_non_social_yields_empty_frame_with_schema(self, spark):
        # Non-SM rows are filtered out -> the empty metric path still returns the
        # contract-shaped frame.
        out = compute_wows(make_raw(spark, [{"team": "core"}]))
        assert len(out.take(1)) == 0
        assert out.columns == [c for c, _ in IO_WOWS_METRIC_SCHEMA]
