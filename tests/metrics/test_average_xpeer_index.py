"""Unit tests for ``metrics/average_xpeer_index.py`` (PySpark).

Small synthetic agent-level Xpeer Index frames. We verify the per-XForce mean,
NULL handling, the **deck** grouping (core+fraud merge into one ``main`` row; a
cross-deck xforce stays split), the week+month gate, the team=NULL output, the
numerator/denominator convention, and the output contract.
"""

from __future__ import annotations

import datetime as dt

from pyspark.sql import types as T

from metric_utils import METRIC_COLUMNS
from average_xpeer_index import (
    IO_AVERAGE_XPEER_INDEX_METRIC_SCHEMA,
    METRIC_NAME,
    compute_average_xpeer_index,
)

D = dt.date(2026, 5, 1)          # pre-cutover
POST = dt.date(2026, 7, 1)       # cutover onward

_SCHEMA = T.StructType(
    [
        T.StructField("agent", T.StringType()),
        T.StructField("xforce", T.StringType()),
        T.StructField("xplead", T.StringType()),
        T.StructField("team", T.StringType()),
        T.StructField("squad", T.StringType()),
        T.StructField("district", T.StringType()),
        T.StructField("shift", T.StringType()),
        T.StructField("date_reference", T.DateType()),
        T.StructField("date_granularity", T.StringType()),
        T.StructField("metric", T.StringType()),
        T.StructField("numerator", T.DoubleType()),
        T.StructField("denominator", T.DoubleType()),
        T.StructField("metric_value", T.DoubleType()),
    ]
)
_FIELDS = [f.name for f in _SCHEMA.fields]


def idx(agent, mv, *, xforce="xf", xplead="xp", team="core", dref=D, gran="month"):
    return {
        "agent": agent, "xforce": xforce, "xplead": xplead, "team": team,
        "squad": "sq", "district": "di", "shift": "morning",
        "date_reference": dref, "date_granularity": gran, "metric": "xpeer_index",
        "numerator": 0.0, "denominator": 0.0,
        "metric_value": None if mv is None else float(mv),
    }


def frame(spark, rows):
    data = [tuple(r[name] for name in _FIELDS) for r in rows]
    return spark.createDataFrame(data, _SCHEMA)


def rows(out):
    return out.collect()


def only(out):
    r = out.collect()
    assert len(r) == 1, f"expected 1 row, got {len(r)}"
    return r[0]


class TestAverage:
    def test_simple_mean(self, spark):
        out = compute_average_xpeer_index(
            frame(spark, [idx("a", 80.0), idx("b", 100.0), idx("c", 90.0)])
        )
        r = only(out)
        assert r["metric"] == METRIC_NAME
        assert abs(r["metric_value"] - 90.0) < 1e-9
        assert abs(r["numerator"] - 270.0) < 1e-9
        assert abs(r["denominator"] - 3.0) < 1e-9
        assert r["agent"] is None and r["squad"] is None and r["shift"] is None
        assert r["team"] is None  # legacy carries no team on this metric
        assert r["xforce"] == "xf" and r["xplead"] == "xp"

    def test_null_index_ignored(self, spark):
        out = compute_average_xpeer_index(
            frame(spark, [idx("a", 80.0), idx("b", None)])
        )
        r = only(out)
        assert abs(r["metric_value"] - 80.0) < 1e-9
        assert abs(r["denominator"] - 1.0) < 1e-9

    def test_separate_xforces(self, spark):
        out = compute_average_xpeer_index(
            frame(spark, [idx("a", 80.0, xforce="A"), idx("b", 60.0, xforce="B")])
        )
        by_xf = {r["xforce"]: r["metric_value"] for r in rows(out)}
        assert by_xf == {"A": 80.0, "B": 60.0}


class TestDeckGrouping:
    def test_core_and_fraud_merge_into_one_main_row(self, spark):
        # A cross-team xforce (core + fraud agents) is ONE main-deck row, the mean
        # over all of them — legacy main deck groups by (xforce, xplead) over both.
        out = compute_average_xpeer_index(
            frame(spark, [
                idx("a", 80.0, team="core", xforce="brenda"),
                idx("b", 100.0, team="fraud", xforce="brenda"),
            ])
        )
        r = only(out)
        assert abs(r["metric_value"] - 90.0) < 1e-9
        assert abs(r["denominator"] - 2.0) < 1e-9

    def test_cross_deck_xforce_stays_split(self, spark):
        # An xforce with core AND social-media agents must NOT be averaged
        # together (legacy keeps them in separate decks with very different
        # values) — two rows, one per deck.
        out = compute_average_xpeer_index(
            frame(spark, [
                idx("a", 96.0, team="core", xforce="marcela"),
                idx("b", 49.0, team="social media", xforce="marcela"),
            ])
        )
        vals = sorted(r["metric_value"] for r in rows(out))
        assert len(vals) == 2
        assert abs(vals[0] - 49.0) < 1e-9 and abs(vals[1] - 96.0) < 1e-9

    def test_null_team_support_squad_is_main(self, spark):
        # A NULL-team main-deck support agent joins the same 'main' deck as core.
        out = compute_average_xpeer_index(
            frame(spark, [
                idx("a", 80.0, team="core", xforce="x"),
                idx("b", 100.0, team=None, xforce="x"),
            ])
        )
        r = only(out)
        assert abs(r["metric_value"] - 90.0) < 1e-9
        assert abs(r["denominator"] - 2.0) < 1e-9


class TestGranularityGate:
    def test_pre_cutover_keeps_only_week_and_month(self, spark):
        out = compute_average_xpeer_index(
            frame(spark, [idx("a", 80.0, gran=g) for g in
                          ("day", "week", "month", "quarter", "semester", "year")])
        )
        assert {r["date_granularity"] for r in rows(out)} == {"week", "month"}

    def test_post_cutover_allows_all_granularities(self, spark):
        out = compute_average_xpeer_index(
            frame(spark, [idx("a", 80.0, dref=POST, gran=g) for g in
                          ("day", "week", "month", "quarter", "semester", "year")])
        )
        assert {r["date_granularity"] for r in rows(out)} == {
            "day", "week", "month", "quarter", "semester", "year"
        }


class TestContract:
    def test_output_contract(self, spark):
        out = compute_average_xpeer_index(frame(spark, [idx("a", 80.0)]))
        assert out.columns == list(METRIC_COLUMNS)
        assert [c for c, _ in IO_AVERAGE_XPEER_INDEX_METRIC_SCHEMA] == list(
            METRIC_COLUMNS
        )

    def test_empty_returns_empty(self, spark):
        out = compute_average_xpeer_index(frame(spark, []))
        assert len(out.take(1)) == 0
        assert out.columns == list(METRIC_COLUMNS)

    def test_all_null_returns_empty(self, spark):
        out = compute_average_xpeer_index(frame(spark, [idx("a", None)]))
        assert len(out.take(1)) == 0
