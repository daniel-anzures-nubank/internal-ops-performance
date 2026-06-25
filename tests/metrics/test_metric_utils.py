"""Unit tests for ``metrics/metric_utils.py`` (shared aggregation)."""

from __future__ import annotations

import datetime as dt

import pandas as pd

from metric_utils import METRIC_COLUMNS, aggregate_long, bucket_dates


def make_row(**overrides) -> dict:
    base = {
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
    base.update(overrides)
    return base


def make_df(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame([make_row(**r) for r in rows])


class TestBucketDates:
    def test_day(self):
        s = bucket_dates(pd.Series([dt.date(2026, 5, 6)]), "day")
        assert s.iloc[0] == pd.Timestamp(2026, 5, 6)

    def test_week_is_monday(self):
        s = bucket_dates(pd.Series([dt.date(2026, 5, 6)]), "week")
        assert s.iloc[0] == pd.Timestamp(2026, 5, 4)

    def test_month_is_first(self):
        s = bucket_dates(pd.Series([dt.date(2026, 5, 6)]), "month")
        assert s.iloc[0] == pd.Timestamp(2026, 5, 1)

    def test_quarter_is_first_of_quarter(self):
        s = bucket_dates(pd.Series([dt.date(2026, 5, 6)]), "quarter")
        assert s.iloc[0] == pd.Timestamp(2026, 4, 1)

    def test_semester_first_half(self):
        s = bucket_dates(pd.Series([dt.date(2026, 5, 6)]), "semester")
        assert s.iloc[0] == pd.Timestamp(2026, 1, 1)

    def test_semester_second_half(self):
        s = bucket_dates(pd.Series([dt.date(2026, 9, 6)]), "semester")
        assert s.iloc[0] == pd.Timestamp(2026, 7, 1)

    def test_year_is_first_of_year(self):
        s = bucket_dates(pd.Series([dt.date(2026, 5, 6)]), "year")
        assert s.iloc[0] == pd.Timestamp(2026, 1, 1)

    def test_unknown_raises(self):
        try:
            bucket_dates(pd.Series([dt.date(2026, 5, 6)]), "decade")
            assert False, "expected ValueError"
        except ValueError:
            pass


class TestAggregateLong:
    def test_basic_sum_and_ratio(self):
        out = aggregate_long(
            make_df([
                {"num": 30.0, "den": 30.0},
                {"num": 15.0, "den": 30.0},
            ]),
            numerator_col="num",
            denominator_col="den",
            metric_name="test",
        )
        day = out[out["date_granularity"] == "day"].iloc[0]
        assert day["numerator"] == 45.0
        assert day["denominator"] == 60.0
        assert day["metric_value"] == 75.0
        assert day["metric"] == "test"

    def test_zero_denominator_is_null(self):
        out = aggregate_long(
            make_df([{"num": 0.0, "den": 0.0}]),
            numerator_col="num",
            denominator_col="den",
            metric_name="test",
        )
        day = out[out["date_granularity"] == "day"].iloc[0]
        assert pd.isna(day["metric_value"])

    def test_columns_and_order(self):
        out = aggregate_long(
            make_df([{}]),
            numerator_col="num",
            denominator_col="den",
            metric_name="test",
        )
        assert list(out.columns) == list(METRIC_COLUMNS)

    def test_empty_input(self):
        out = aggregate_long(
            make_df([])[0:0],
            numerator_col="num",
            denominator_col="den",
            metric_name="test",
        )
        assert out.empty
        assert list(out.columns) == list(METRIC_COLUMNS)
