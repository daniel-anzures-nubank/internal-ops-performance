"""Unit tests for ``metrics/nuvinhos_performance.py`` (PySpark).

Small synthetic agent-level Xpeer Index + tenure frames (no warehouse). We
verify, for the pre-2026-07-01 legacy era:

* the Nuvinho window classification (change-month + 2);
* the **two-level** cohort aggregation (inner AVG per full cohort incl. the
  nuvinho flag, outer AVG per roll-up key);
* the per-deck **ELSE NULL** (main / content) vs **ELSE 0** (social media)
  split, which deflates the SM ratio differently from main;
* the per-deck **roll-up gating** (main XForce + squad + district; SM all three;
  content XForce single-level + degenerate squad/district; Core never emits
  squad/district on its own — it is folded into the ``main`` deck);
* **deck grouping** (core + fraud merge into one ``main`` XForce row);
* the **week + month** scope and the **2025-12-01 floor**;
* Content's degenerate (no-Nuvinhos) result;
* the post-cutover corrected flat-mean formula;
* the output contract.
"""

from __future__ import annotations

import datetime as dt

from pyspark.sql import types as T

from metric_utils import METRIC_COLUMNS
from nuvinhos_performance import (
    IO_NUVINHOS_PERFORMANCE_METRIC_SCHEMA,
    METRIC_DISTRICT,
    METRIC_SQUAD,
    METRIC_XFORCE,
    compute_nuvinhos_performance,
)

MONTH = dt.date(2026, 5, 1)        # pre-cutover
POST = dt.date(2026, 7, 1)         # cutover onward
NUV_CHANGE = dt.date(2026, 5, 10)  # last_change in May -> nuvinho in May
OLD_CHANGE = dt.date(2026, 1, 5)   # last_change in Jan -> old in May

_IDX_SCHEMA = T.StructType(
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
_IDX_FIELDS = [f.name for f in _IDX_SCHEMA.fields]

_TEN_SCHEMA = T.StructType(
    [
        T.StructField("agent", T.StringType()),
        T.StructField("snapshot_month", T.DateType()),
        T.StructField("last_change_date", T.DateType()),
    ]
)
_TEN_FIELDS = [f.name for f in _TEN_SCHEMA.fields]


def idx_row(agent, mv, *, xforce="xf", xplead="xp", team="core", squad="sq",
            district="di", dref=MONTH, gran="month", metric="xpeer_index"):
    return {
        "agent": agent, "xforce": xforce, "xplead": xplead, "team": team,
        "squad": squad, "district": district, "shift": "morning",
        "date_reference": dref, "date_granularity": gran, "metric": metric,
        "numerator": None if mv is None else float(mv),
        "denominator": 100.0,
        "metric_value": None if mv is None else float(mv),
    }


def tenure_row(agent, last_change, *, snap=MONTH):
    return {"agent": agent, "snapshot_month": snap, "last_change_date": last_change}


def idx_frame(spark, rows):
    data = [tuple(r[name] for name in _IDX_FIELDS) for r in rows]
    return spark.createDataFrame(data, _IDX_SCHEMA)


def ten_frame(spark, rows):
    data = [tuple(r[name] for name in _TEN_FIELDS) for r in rows]
    return spark.createDataFrame(data, _TEN_SCHEMA)


def collect(out):
    return out.collect()


def metric_rows(out, metric):
    return [r for r in out.collect() if r["metric"] == metric]


def one_xforce(out):
    rs = metric_rows(out, METRIC_XFORCE)
    assert len(rs) == 1, f"expected 1 XForce row, got {len(rs)}"
    return rs[0]


# --------------------------------------------------------------------------- #
class TestClassification:
    def test_recent_change_is_nuvinho(self, spark):
        out = compute_nuvinhos_performance(
            idx_frame(spark, [idx_row("new", 80), idx_row("old", 100)]),
            ten_frame(spark, [
                tenure_row("new", NUV_CHANGE),
                tenure_row("old", OLD_CHANGE),
            ]),
        )
        r = one_xforce(out)
        assert abs(r["numerator"] - 80.0) < 1e-9   # mean nuvinho index
        assert abs(r["denominator"] - 100.0) < 1e-9  # mean old index
        assert abs(r["metric_value"] - 80.0) < 1e-9  # 80 / 100 * 100

    def test_window_is_change_month_plus_two(self, spark):
        def run(month):
            out = compute_nuvinhos_performance(
                idx_frame(spark, [
                    idx_row("n", 90, dref=month),
                    idx_row("o", 100, dref=month),
                ]),
                ten_frame(spark, [
                    tenure_row("n", dt.date(2026, 3, 15), snap=month),
                    tenure_row("o", dt.date(2025, 1, 1), snap=month),
                ]),
            )
            return one_xforce(out)

        # May = March + 2 -> still nuvinho; ratio 90 / 100.
        r_may = run(dt.date(2026, 5, 1))
        assert abs(r_may["metric_value"] - 90.0) < 1e-9
        # June = March + 3 -> 'n' is now old; no nuvinhos in this XForce cohort.
        # ELSE NULL -> numerator AVG over NULLs = NULL -> metric_value NULL.
        r_jun = run(dt.date(2026, 6, 1))
        assert r_jun["numerator"] is None
        assert r_jun["metric_value"] is None

    def test_null_tenure_is_old(self, spark):
        out = compute_nuvinhos_performance(
            idx_frame(spark, [idx_row("a", 90), idx_row("b", 80)]),
            ten_frame(spark, [tenure_row("a", None), tenure_row("b", None)]),
        )
        r = one_xforce(out)
        # No nuvinhos -> numerator NULL (ELSE NULL); old mean = (90+80)/2 = 85.
        assert r["numerator"] is None
        assert abs(r["denominator"] - 85.0) < 1e-9
        assert r["metric_value"] is None


class TestTwoLevelAndElseClause:
    def _data(self):
        # squad A: one nuvinho only (no old). squad B: one nuvinho + one old.
        rows = [
            idx_row("nA", 80, squad="A", district="dA"),
            idx_row("nB", 60, squad="B", district="dB"),
            idx_row("oB", 100, squad="B", district="dB"),
        ]
        ten = [
            tenure_row("nA", NUV_CHANGE),
            tenure_row("nB", NUV_CHANGE),
            tenure_row("oB", OLD_CHANGE),
        ]
        return rows, ten

    def test_main_xforce_else_null_two_level(self, spark):
        # ELSE NULL: outer AVG(nuv) over {80, 60} = 70; AVG(old) over {100} = 100.
        rows, ten = self._data()
        out = compute_nuvinhos_performance(
            idx_frame(spark, [dict(r, team="core") for r in rows]),
            ten_frame(spark, ten),
        )
        r = one_xforce(out)
        assert abs(r["numerator"] - 70.0) < 1e-9
        assert abs(r["denominator"] - 100.0) < 1e-9
        assert abs(r["metric_value"] - 70.0) < 1e-9

    def test_sm_xforce_else_zero_two_level(self, spark):
        # ELSE 0: the opposite-flag cohort contributes a real 0 to the outer AVG.
        # nuv cohorts: A(80,0), B(60,0); old cohort: B(0,100).
        #   AVG(nuv) over {80, 60, 0} = 46.666...
        #   AVG(old) over {0, 0, 100} = 33.333...
        #   metric_value = 46.667 / 33.333 * 100 = 140.0
        rows, ten = self._data()
        out = compute_nuvinhos_performance(
            idx_frame(spark, [dict(r, team="social media") for r in rows]),
            ten_frame(spark, ten),
        )
        r = one_xforce(out)
        assert abs(r["numerator"] - (140.0 / 3)) < 1e-6
        assert abs(r["denominator"] - (100.0 / 3)) < 1e-6
        assert abs(r["metric_value"] - 140.0) < 1e-6


class TestRollupGating:
    def _mixed(self, spark, team):
        rows = [idx_row("n", 80, team=team), idx_row("o", 100, team=team)]
        ten = [tenure_row("n", NUV_CHANGE), tenure_row("o", OLD_CHANGE)]
        return compute_nuvinhos_performance(idx_frame(spark, rows), ten_frame(spark, ten))

    def test_main_emits_all_three_rollups(self, spark):
        out = self._mixed(spark, "core")
        assert {r["metric"] for r in collect(out)} == {
            METRIC_XFORCE, METRIC_SQUAD, METRIC_DISTRICT
        }

    def test_sm_emits_all_three_rollups(self, spark):
        out = self._mixed(spark, "social media")
        assert {r["metric"] for r in collect(out)} == {
            METRIC_XFORCE, METRIC_SQUAD, METRIC_DISTRICT
        }

    def test_squad_and_district_keys(self, spark):
        out = self._mixed(spark, "core")
        sq = metric_rows(out, METRIC_SQUAD)[0]
        assert sq["squad"] == "sq" and sq["xforce"] is None and sq["district"] is None
        di = metric_rows(out, METRIC_DISTRICT)[0]
        assert di["district"] == "di" and di["squad"] is None and di["xforce"] is None

    def test_core_and_fraud_merge_into_one_main_xforce(self, spark):
        # A cross-team XForce (core + fraud) is ONE main-deck row — deck grouping,
        # not team. Both nuvinhos -> mean(80, 60) = 70; both old -> mean(100, 90).
        rows = [
            idx_row("n_core", 80, team="core", xforce="brenda", squad="A", district="dA"),
            idx_row("n_fraud", 60, team="fraud", xforce="brenda", squad="B", district="dB"),
            idx_row("o_core", 100, team="core", xforce="brenda", squad="A", district="dA"),
            idx_row("o_fraud", 90, team="fraud", xforce="brenda", squad="B", district="dB"),
        ]
        ten = [
            tenure_row("n_core", NUV_CHANGE), tenure_row("n_fraud", NUV_CHANGE),
            tenure_row("o_core", OLD_CHANGE), tenure_row("o_fraud", OLD_CHANGE),
        ]
        out = compute_nuvinhos_performance(idx_frame(spark, rows), ten_frame(spark, ten))
        r = one_xforce(out)
        assert abs(r["numerator"] - 70.0) < 1e-9       # mean(80, 60)
        assert abs(r["denominator"] - 95.0) < 1e-9      # mean(100, 90)


class TestContent:
    def test_content_single_level_no_nuvinhos(self, spark):
        # Content tenure (last_change_date NULL for content) -> everyone old, so
        # the single-level XForce numerator is NULL -> metric_value NULL.
        rows = [
            idx_row("a", 90, team="content", xforce="cx"),
            idx_row("b", 80, team="content", xforce="cx"),
        ]
        ten = [tenure_row("a", None), tenure_row("b", None)]
        out = compute_nuvinhos_performance(idx_frame(spark, rows), ten_frame(spark, ten))
        r = one_xforce(out)
        assert r["numerator"] is None and r["metric_value"] is None
        assert abs(r["denominator"] - 85.0) < 1e-9   # old mean passes through

    def test_content_squad_district_degenerate_null_key(self, spark):
        # Content squad/district are a single NULL-keyed row each (from the XForce
        # output): denominator = COUNT(DISTINCT agent) = 0 (agent NULL there).
        rows = [
            idx_row("a", 90, team="content", xforce="cx"),
            idx_row("b", 80, team="content", xforce="cx"),
        ]
        ten = [tenure_row("a", None), tenure_row("b", None)]
        out = compute_nuvinhos_performance(idx_frame(spark, rows), ten_frame(spark, ten))
        sq = metric_rows(out, METRIC_SQUAD)
        di = metric_rows(out, METRIC_DISTRICT)
        assert len(sq) == 1 and sq[0]["squad"] is None
        assert sq[0]["denominator"] == 0.0
        assert len(di) == 1 and di[0]["district"] is None


class TestScope:
    def test_pre_cutover_keeps_only_week_and_month(self, spark):
        rows, ten = [], [tenure_row("n", NUV_CHANGE), tenure_row("o", OLD_CHANGE)]
        for g in ("day", "week", "month", "quarter", "semester", "year"):
            rows += [idx_row("n", 80, gran=g), idx_row("o", 100, gran=g)]
        out = compute_nuvinhos_performance(idx_frame(spark, rows), ten_frame(spark, ten))
        assert {r["date_granularity"] for r in collect(out)} == {"week", "month"}

    def test_pre_2025_12_floor_drops_early_buckets(self, spark):
        rows = [
            idx_row("n", 80, dref=dt.date(2025, 11, 1)),
            idx_row("o", 100, dref=dt.date(2025, 11, 1)),
        ]
        ten = [
            tenure_row("n", NUV_CHANGE, snap=dt.date(2025, 11, 1)),
            tenure_row("o", OLD_CHANGE, snap=dt.date(2025, 11, 1)),
        ]
        out = compute_nuvinhos_performance(idx_frame(spark, rows), ten_frame(spark, ten))
        assert len(out.take(1)) == 0

    def test_non_xpeer_metric_ignored(self, spark):
        rows = [idx_row("n", 80, metric="other"), idx_row("o", 100, metric="other")]
        ten = [tenure_row("n", NUV_CHANGE), tenure_row("o", OLD_CHANGE)]
        out = compute_nuvinhos_performance(idx_frame(spark, rows), ten_frame(spark, ten))
        assert len(out.take(1)) == 0


class TestPostCutover:
    def test_post_cutover_flat_mean_all_grains(self, spark):
        # From 2026-07-01: corrected flat mean(Index|nuvinho)/mean(Index|old),
        # all six granularities allowed.
        rows, ten = [], [
            tenure_row("n", dt.date(2026, 7, 10), snap=POST),
            tenure_row("o", dt.date(2026, 1, 1), snap=POST),
        ]
        for g in ("day", "week", "month", "quarter", "semester", "year"):
            rows += [idx_row("n", 80, dref=POST, gran=g),
                     idx_row("o", 100, dref=POST, gran=g)]
        out = compute_nuvinhos_performance(idx_frame(spark, rows), ten_frame(spark, ten))
        grains = {r["date_granularity"] for r in metric_rows(out, METRIC_XFORCE)}
        assert grains == {"day", "week", "month", "quarter", "semester", "year"}
        r = [r for r in metric_rows(out, METRIC_XFORCE)
             if r["date_granularity"] == "month"][0]
        assert abs(r["metric_value"] - 80.0) < 1e-9


class TestContract:
    def test_output_contract(self, spark):
        out = compute_nuvinhos_performance(
            idx_frame(spark, [idx_row("n", 80), idx_row("o", 100)]),
            ten_frame(spark, [tenure_row("n", NUV_CHANGE), tenure_row("o", OLD_CHANGE)]),
        )
        assert out.columns == list(METRIC_COLUMNS)
        assert [c for c, _ in IO_NUVINHOS_PERFORMANCE_METRIC_SCHEMA] == list(METRIC_COLUMNS)
        r = one_xforce(out)
        assert r["agent"] is None and r["shift"] is None and r["team"] is None
        assert r["squad"] is None and r["district"] is None  # XForce roll-up

    def test_empty_returns_empty(self, spark):
        out = compute_nuvinhos_performance(
            idx_frame(spark, []), ten_frame(spark, [])
        )
        assert len(out.take(1)) == 0
        assert out.columns == list(METRIC_COLUMNS)
