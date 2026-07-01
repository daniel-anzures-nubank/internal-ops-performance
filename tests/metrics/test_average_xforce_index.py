"""Unit tests for ``metrics/average_xforce_index.py`` (PySpark).

Small synthetic ``io_xforce_index_metric`` frames. We verify the per-XPLead
mean, the ``xforce_index`` metric filter, NULL handling, the **deck** grouping
(core+fraud merge into one ``main`` row; a cross-deck xplead stays split), the
week+month gate, the team=NULL output, the numerator/denominator convention, and
the output contract.
"""

from __future__ import annotations

import datetime as dt

from pyspark.sql import types as T

from metric_utils import METRIC_COLUMNS
from average_xforce_index import (
    IO_AVERAGE_XFORCE_INDEX_METRIC_SCHEMA,
    METRIC_NAME,
    compute_average_xforce_index,
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


def idx(xforce, mv, *, xplead="xp", team="core", dref=D, gran="month",
        metric="xforce_index"):
    return {
        "agent": None, "xforce": xforce, "xplead": xplead, "team": team,
        "squad": None, "district": None, "shift": None,
        "date_reference": dref, "date_granularity": gran, "metric": metric,
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
    def test_simple_mean_per_xplead(self, spark):
        out = compute_average_xforce_index(
            frame(spark, [idx("A", 80.0), idx("B", 100.0), idx("C", 90.0)])
        )
        r = only(out)
        assert r["metric"] == METRIC_NAME
        assert abs(r["metric_value"] - 90.0) < 1e-9
        assert abs(r["numerator"] - 270.0) < 1e-9
        assert abs(r["denominator"] - 3.0) < 1e-9
        assert r["xplead"] == "xp"
        assert r["agent"] is None and r["xforce"] is None and r["squad"] is None
        assert r["team"] is None  # legacy carries no team on this metric

    def test_separate_xpleads(self, spark):
        out = compute_average_xforce_index(
            frame(spark, [idx("A", 80.0, xplead="p1"), idx("B", 60.0, xplead="p2")])
        )
        by = {r["xplead"]: r["metric_value"] for r in rows(out)}
        assert abs(by["p1"] - 80.0) < 1e-9
        assert abs(by["p2"] - 60.0) < 1e-9

    def test_null_index_ignored(self, spark):
        out = compute_average_xforce_index(
            frame(spark, [idx("A", 80.0), idx("B", None)])
        )
        r = only(out)
        assert abs(r["metric_value"] - 80.0) < 1e-9
        assert abs(r["denominator"] - 1.0) < 1e-9

    def test_other_metrics_filtered_out(self, spark):
        out = compute_average_xforce_index(
            frame(spark, [idx("A", 80.0), idx("B", 0.0, metric="something_else")])
        )
        r = only(out)
        assert abs(r["metric_value"] - 80.0) < 1e-9
        assert abs(r["denominator"] - 1.0) < 1e-9


class TestDeckGrouping:
    def test_core_and_fraud_merge_into_one_main_row(self, spark):
        # A cross-team xplead (core + fraud xforces) is ONE main-deck row, the
        # mean over all of them — legacy's main notebook averages the XForce
        # Index over both core and fraud xforces of an xplead.
        out = compute_average_xforce_index(
            frame(spark, [
                idx("A", 80.0, team="core", xplead="brenda"),
                idx("B", 100.0, team="fraud", xplead="brenda"),
            ])
        )
        r = only(out)
        assert abs(r["metric_value"] - 90.0) < 1e-9
        assert abs(r["denominator"] - 2.0) < 1e-9

    def test_cross_deck_xplead_stays_split(self, spark):
        # An xplead with core AND social-media xforces must NOT be averaged
        # together (legacy keeps them in separate decks with very different
        # values) — two rows, one per deck.
        out = compute_average_xforce_index(
            frame(spark, [
                idx("A", 96.0, team="core", xplead="marcela"),
                idx("B", 49.0, team="social media", xplead="marcela"),
            ])
        )
        vals = sorted(r["metric_value"] for r in rows(out))
        assert len(vals) == 2
        assert abs(vals[0] - 49.0) < 1e-9 and abs(vals[1] - 96.0) < 1e-9

    def test_null_team_support_squad_is_main(self, spark):
        # A NULL-team main-deck support xforce joins the same 'main' deck as core.
        out = compute_average_xforce_index(
            frame(spark, [
                idx("A", 80.0, team="core", xplead="x"),
                idx("B", 100.0, team=None, xplead="x"),
            ])
        )
        r = only(out)
        assert abs(r["metric_value"] - 90.0) < 1e-9
        assert abs(r["denominator"] - 2.0) < 1e-9

    def test_all_decks_included(self, spark):
        # Distinct xpleads on each deck each survive as their own row.
        out = compute_average_xforce_index(
            frame(spark, [
                idx("c", 80.0, team="core", xplead="pc"),
                idx("f", 80.0, team="fraud", xplead="pf"),
                idx("s", 80.0, team="social media", xplead="ps"),
                idx("ct", 80.0, team="content", xplead="pct"),
            ])
        )
        assert {r["xplead"] for r in rows(out)} == {"pc", "pf", "ps", "pct"}


class TestGranularityGate:
    def test_pre_cutover_keeps_only_week_and_month(self, spark):
        out = compute_average_xforce_index(
            frame(spark, [idx("A", 80.0, gran=g) for g in
                          ("day", "week", "month", "quarter", "semester", "year")])
        )
        assert {r["date_granularity"] for r in rows(out)} == {"week", "month"}

    def test_post_cutover_allows_all_granularities(self, spark):
        out = compute_average_xforce_index(
            frame(spark, [idx("A", 80.0, dref=POST, gran=g) for g in
                          ("day", "week", "month", "quarter", "semester", "year")])
        )
        assert {r["date_granularity"] for r in rows(out)} == {
            "day", "week", "month", "quarter", "semester", "year"
        }


class TestContract:
    def test_output_contract(self, spark):
        out = compute_average_xforce_index(frame(spark, [idx("A", 80.0)]))
        assert out.columns == list(METRIC_COLUMNS)
        assert [c for c, _ in IO_AVERAGE_XFORCE_INDEX_METRIC_SCHEMA] == list(
            METRIC_COLUMNS
        )

    def test_empty_returns_empty(self, spark):
        out = compute_average_xforce_index(frame(spark, []))
        assert len(out.take(1)) == 0
        assert out.columns == list(METRIC_COLUMNS)

    def test_all_null_returns_empty(self, spark):
        out = compute_average_xforce_index(frame(spark, [idx("A", None)]))
        assert len(out.take(1)) == 0
