"""Unit tests for ``metrics/xforce_index.py`` (PySpark).

Small synthetic component frames (no warehouse). We verify the four component
transforms (shrinkage fold — with the May/June-2026 23% target; the xpeers
on-target 90-100 rescale; raw avg_idx; improved fold), the XForce-weighted
shrinkage roll-up, and the five build-plan parity fixes that replace the old
presence-based gating with legacy's explicit DATE rule:

* Fixes #1-#3 — the 4th component is added by a DATE rule
  (``date_reference < 2026-05-01 AND NOT david-April``), NOT by an improved row
  being present: a pre-May bucket with no improved value is still 4-component
  (improved folds to 0); the david.fernandez >= 2026-04-01 carve-out drops it to
  3; non-david Core April stays 4.
* Fix #4 — only week + month grains survive pre-cutover.
* Fix #5 / deck grouping — shrinkage rolls up by a synthetic deck (core+fraud →
  one ``main`` row; a cross-deck xforce stays split); ``team`` is emitted NULL.

``improved_benchmarks`` is DEFERRED, so a ``None`` improved input must still
produce the legacy component COUNT (the date rule), with the improved numerator
folded to 0.
"""

from __future__ import annotations

import datetime as dt

from pyspark.sql import types as T

from metric_utils import METRIC_COLUMNS
from xforce_index import (
    IO_XFORCE_INDEX_METRIC_SCHEMA,
    METRIC_NAME,
    compute_xforce_index,
)

FEB = dt.date(2026, 2, 1)          # pre-May  -> 4-component
APR = dt.date(2026, 4, 1)          # pre-May  -> 4-component (david carve-out -> 3)
MAY = dt.date(2026, 5, 1)          # >= cutoff -> 3-component; 23% shrinkage target
JUN = dt.date(2026, 6, 1)          # 23% shrinkage target (month)
MAY_WEEK = dt.date(2026, 5, 4)     # a Monday in May -> keeps the 20% target
POST = dt.date(2026, 7, 1)         # cutover onward (all grains)

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


def shrink(agent, num, den, *, xforce="xf", xplead="xp", team="core",
           dref=FEB, gran="month"):
    return {
        "agent": agent, "xforce": xforce, "xplead": xplead, "team": team,
        "squad": "sq", "district": "di", "shift": "morning",
        "date_reference": dref, "date_granularity": gran, "metric": "shrinkage",
        "numerator": float(num), "denominator": float(den),
        "metric_value": (num / den * 100) if den else None,
    }


def xrow(metric, mv, *, xforce="xf", xplead="xp", team="core", dref=FEB,
         gran="month"):
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


def xit(spark, mv, **kw):
    return frame(spark, [xrow("xpeers_in_target", mv, **kw)])


def avg(spark, mv, **kw):
    return frame(spark, [xrow("average_xpeer_index", mv, **kw)])


def imp(spark, mv, **kw):
    return frame(spark, [xrow("improved_benchmark_xforce", mv, **kw)])


def rows(out):
    return out.collect()


def one(out):
    r = out.collect()
    assert len(r) == 1, f"expected 1 row, got {len(r)}"
    return r[0]


class TestComponentTransforms:
    def test_shrinkage_at_or_below_20_is_100(self, spark):
        # 10% shrinkage -> 100; xit/avg 0; Feb -> 4-component (improved folds 0).
        out = compute_xforce_index(
            frame(spark, [shrink("a", 10, 100)]),
            xit(spark, 0), avg(spark, 0), None,
        )
        r = one(out)
        assert abs(r["numerator"] - 100.0) < 1e-9
        assert abs(r["denominator"] - 400.0) < 1e-9  # date rule -> 4

    def test_shrinkage_above_20_folds_to_120_minus(self, spark):
        # 30% shrinkage -> 120 - 30 = 90.
        out = compute_xforce_index(
            frame(spark, [shrink("a", 30, 100)]),
            xit(spark, 0), avg(spark, 0), None,
        )
        assert abs(one(out)["numerator"] - 90.0) < 1e-9

    def test_shrinkage_weighted_across_agents(self, spark):
        # (5/50) + (15/50) -> 20/100 = 20% -> <=20 -> 100.
        out = compute_xforce_index(
            frame(spark, [shrink("a", 5, 50), shrink("b", 15, 50)]),
            xit(spark, 0), avg(spark, 0), None,
        )
        assert abs(one(out)["numerator"] - 100.0) < 1e-9

    def test_below_70_xit_and_avg_raw(self, spark):
        # shrinkage 10% -> 100; xit 50 (< 70 -> raw); avg 90 raw. Feb -> 4-comp.
        out = compute_xforce_index(
            frame(spark, [shrink("a", 10, 100)]),
            xit(spark, 50), avg(spark, 90), None,
        )
        r = one(out)
        assert abs(r["numerator"] - (100 + 50 + 90)) < 1e-9
        assert abs(r["metric_value"] - ((240 / 400) * 100)) < 1e-9

    def test_missing_xit_avg_coalesce_to_zero(self, spark):
        out = compute_xforce_index(
            frame(spark, [shrink("a", 10, 100)]),
            None, None, None,
        )
        r = one(out)
        assert abs(r["numerator"] - 100.0) < 1e-9  # only shrinkage component

    def test_improved_at_or_above_60_is_100(self, spark):
        out = compute_xforce_index(
            frame(spark, [shrink("a", 10, 100)]),
            xit(spark, 0), avg(spark, 0), imp(spark, 60),
        )
        r = one(out)
        assert abs(r["denominator"] - 400.0) < 1e-9
        assert abs(r["numerator"] - (100 + 0 + 0 + 100)) < 1e-9

    def test_improved_below_60_divided_by_0_6(self, spark):
        # improved 30 -> 30 / 0.6 = 50.
        out = compute_xforce_index(
            frame(spark, [shrink("a", 10, 100)]),
            xit(spark, 0), avg(spark, 0), imp(spark, 30),
        )
        assert abs(one(out)["numerator"] - (100 + 50)) < 1e-9

    def test_only_improved_benchmark_xforce_metric_counts(self, spark):
        # squad/district improved rows are ignored; the xforce improved is absent
        # -> Feb still 4-component (date rule), improved folds to 0.
        ben = frame(spark, [
            xrow("improved_benchmark_squad", 100),
            xrow("improved_benchmark_district", 100),
        ])
        out = compute_xforce_index(
            frame(spark, [shrink("a", 10, 100)]),
            xit(spark, 0), avg(spark, 0), ben,
        )
        r = one(out)
        assert abs(r["denominator"] - 400.0) < 1e-9
        assert abs(r["numerator"] - 100.0) < 1e-9  # improved folds to 0


class TestXpeersInTargetRescale:
    # xit component: >= 70 -> 90 + (xit-70)/3 (90..100); < 70 -> raw. (xit=0 avg=0,
    # so numerator == the xit component; Feb -> 4-comp, improved folds to 0.)
    def _num(self, spark, xit_mv):
        out = compute_xforce_index(
            frame(spark, [shrink("a", 100, 100)]),  # 100% shrinkage -> 20 (>20 fold)
            xit(spark, xit_mv), avg(spark, 0), None,
        )
        # shrinkage 100% -> 120-100 = 20; so xit component = numerator - 20.
        return one(out)["numerator"] - 20.0

    def test_on_target_70_maps_to_90(self, spark):
        assert abs(self._num(spark, 70) - 90.0) < 1e-9

    def test_on_target_85_maps_to_95(self, spark):
        assert abs(self._num(spark, 85) - 95.0) < 1e-9

    def test_on_target_100_maps_to_100(self, spark):
        assert abs(self._num(spark, 100) - 100.0) < 1e-9

    def test_just_below_70_is_raw(self, spark):
        assert abs(self._num(spark, 69) - 69.0) < 1e-9

    def test_null_xit_is_zero(self, spark):
        assert abs(self._num(spark, None) - 0.0) < 1e-9


class TestShrinkageTarget:
    # May/June-2026 MONTH buckets use a 23% target; everything else 20%.
    # (xit=0 avg=0 -> numerator == the shrinkage component; MAY/JUN -> 3-comp.)
    def _num(self, spark, pct, dref, gran="month"):
        out = compute_xforce_index(
            frame(spark, [shrink("a", pct, 100, dref=dref, gran=gran)]),
            xit(spark, 0, dref=dref, gran=gran), avg(spark, 0, dref=dref, gran=gran),
            None,
        )
        return one(out)["numerator"]

    def test_may_month_22pct_within_relaxed_target(self, spark):
        # 22% <= 23 -> 100 (would be 98 under the 20% target).
        assert abs(self._num(spark, 22, MAY) - 100.0) < 1e-9

    def test_may_month_above_23_folds_from_123(self, spark):
        # 25% > 23 -> (100 + 23) - 25 = 98.
        assert abs(self._num(spark, 25, MAY) - 98.0) < 1e-9

    def test_june_month_23pct_is_100(self, spark):
        assert abs(self._num(spark, 23, JUN) - 100.0) < 1e-9

    def test_may_week_keeps_20pct_target(self, spark):
        # A May Monday is NOT date_reference 2026-05-01 -> 20% target -> 120-22 = 98.
        assert abs(self._num(spark, 22, MAY_WEEK, gran="week") - 98.0) < 1e-9

    def test_april_month_keeps_20pct_target(self, spark):
        assert abs(self._num(spark, 22, APR) - 98.0) < 1e-9


class TestDateBasedGating:
    def test_pre_may_is_four_component_without_improved_row(self, spark):
        # Fix #1: 4-component by DATE rule even when no improved row is present.
        out = compute_xforce_index(
            frame(spark, [shrink("a", 10, 100, dref=FEB)]),
            xit(spark, 0, dref=FEB), avg(spark, 0, dref=FEB), None,
        )
        assert abs(one(out)["denominator"] - 400.0) < 1e-9

    def test_may_onward_is_three_component(self, spark):
        out = compute_xforce_index(
            frame(spark, [shrink("a", 10, 100, dref=MAY)]),
            xit(spark, 0, dref=MAY), avg(spark, 0, dref=MAY), None,
        )
        assert abs(one(out)["denominator"] - 300.0) < 1e-9

    def test_non_david_core_april_stays_four_component(self, spark):
        # Fix #3: a flat 2026-05 cutoff (not a per-team Core>=April removal).
        out = compute_xforce_index(
            frame(spark, [shrink("a", 10, 100, dref=APR, xplead="someone")]),
            xit(spark, 0, dref=APR, xplead="someone"),
            avg(spark, 0, dref=APR, xplead="someone"),
            None,
        )
        assert abs(one(out)["denominator"] - 400.0) < 1e-9

    def test_david_fernandez_april_carveout_drops_to_three(self, spark):
        out = compute_xforce_index(
            frame(spark, [shrink("a", 10, 100, dref=APR, xplead="david.fernandez")]),
            xit(spark, 0, dref=APR, xplead="david.fernandez"),
            avg(spark, 0, dref=APR, xplead="david.fernandez"),
            imp(spark, 60, dref=APR, xplead="david.fernandez"),
        )
        r = one(out)
        assert abs(r["denominator"] - 300.0) < 1e-9
        # improved is excluded from BOTH numerator and divisor.
        assert abs(r["numerator"] - (100 + 0 + 0)) < 1e-9

    def test_david_fernandez_pre_april_keeps_four_component(self, spark):
        # The carve-out only fires from 2026-04-01; Feb david is still 4.
        out = compute_xforce_index(
            frame(spark, [shrink("a", 10, 100, dref=FEB, xplead="david.fernandez")]),
            xit(spark, 0, dref=FEB, xplead="david.fernandez"),
            avg(spark, 0, dref=FEB, xplead="david.fernandez"),
            imp(spark, 60, dref=FEB, xplead="david.fernandez"),
        )
        assert abs(one(out)["denominator"] - 400.0) < 1e-9

    def test_date_rule_applies_to_weekly_grain(self, spark):
        # Fix #2: weekly pre-May buckets are 4-component even with no weekly
        # improved row (the upstream improved table is month-only).
        wk = dt.date(2026, 2, 2)  # a Monday in February
        out = compute_xforce_index(
            frame(spark, [shrink("a", 10, 100, dref=wk, gran="week")]),
            xit(spark, 0, dref=wk, gran="week"),
            avg(spark, 0, dref=wk, gran="week"),
            None,
        )
        assert abs(one(out)["denominator"] - 400.0) < 1e-9


class TestGranularityScope:
    def test_pre_cutover_keeps_only_week_and_month(self, spark):
        # Fix #4: day/quarter/semester/year dropped before the cutover.
        sh = [shrink("a", 10, 100, gran=g) for g in
              ("day", "week", "month", "quarter", "semester", "year")]
        x = frame(spark, [xrow("xpeers_in_target", 0, gran=g) for g in
                          ("day", "week", "month", "quarter", "semester", "year")])
        a = frame(spark, [xrow("average_xpeer_index", 0, gran=g) for g in
                          ("day", "week", "month", "quarter", "semester", "year")])
        out = compute_xforce_index(frame(spark, sh), x, a, None)
        assert {r["date_granularity"] for r in rows(out)} == {"week", "month"}

    def test_post_cutover_allows_all_granularities(self, spark):
        sh = [shrink("a", 10, 100, dref=POST, gran=g) for g in
              ("day", "week", "month", "quarter", "semester", "year")]
        out = compute_xforce_index(frame(spark, sh), None, None, None)
        assert {r["date_granularity"] for r in rows(out)} == {
            "day", "week", "month", "quarter", "semester", "year"
        }
        # post-cutover -> 3-component (date_reference >= 2026-05-01).
        assert all(abs(r["denominator"] - 300.0) < 1e-9 for r in rows(out))


class TestDeckGrouping:
    def test_core_and_fraud_merge_into_one_main_row(self, spark):
        # A cross-TEAM xforce (core + fraud agents) is ONE 'main' deck row: the
        # shrinkage is summed across both, not split per team.
        out = compute_xforce_index(
            frame(spark, [
                shrink("a", 10, 50, team="core", xforce="brenda"),
                shrink("b", 10, 50, team="fraud", xforce="brenda"),
            ]),
            xit(spark, 0, xforce="brenda"), avg(spark, 0, xforce="brenda"), None,
        )
        r = one(out)
        # 20/100 = 20% -> <=20 -> 100 (single merged shrinkage component).
        assert abs(r["numerator"] - 100.0) < 1e-9
        assert r["team"] is None  # team emitted NULL (deck is a grouping device)

    def test_null_team_support_squad_joins_main(self, spark):
        out = compute_xforce_index(
            frame(spark, [
                shrink("a", 10, 50, team="core", xforce="x"),
                shrink("b", 10, 50, team=None, xforce="x"),
            ]),
            xit(spark, 0, xforce="x"), avg(spark, 0, xforce="x"), None,
        )
        r = one(out)
        assert abs(r["numerator"] - 100.0) < 1e-9

    def test_cross_deck_xforce_stays_split(self, spark):
        # An xforce with core AND social-media agents must NOT be merged: two
        # rows (one per deck), each with its own shrinkage.
        out = compute_xforce_index(
            frame(spark, [
                shrink("a", 5, 100, team="core", xforce="marcela"),       # 5% -> 100
                shrink("b", 50, 100, team="social media", xforce="marcela"),  # 50% -> 70
            ]),
            None, None, None,
        )
        comps = sorted(r["numerator"] for r in rows(out))
        assert len(comps) == 2
        assert abs(comps[0] - 70.0) < 1e-9 and abs(comps[1] - 100.0) < 1e-9

    def test_team_emitted_null(self, spark):
        out = compute_xforce_index(
            frame(spark, [shrink("a", 10, 100, team="social media", xforce="s")]),
            None, None, None,
        )
        assert one(out)["team"] is None


class TestContract:
    def test_output_contract(self, spark):
        out = compute_xforce_index(
            frame(spark, [shrink("a", 10, 100)]),
            xit(spark, 80), avg(spark, 90), None,
        )
        assert out.columns == list(METRIC_COLUMNS)
        assert [c for c, _ in IO_XFORCE_INDEX_METRIC_SCHEMA] == list(METRIC_COLUMNS)
        r = one(out)
        assert r["metric"] == METRIC_NAME
        assert r["xforce"] == "xf" and r["xplead"] == "xp"
        assert r["agent"] is None and r["squad"] is None and r["shift"] is None

    def test_empty_shrinkage_returns_empty(self, spark):
        out = compute_xforce_index(
            frame(spark, []), xit(spark, 80), avg(spark, 90), None,
        )
        assert len(out.take(1)) == 0
        assert out.columns == list(METRIC_COLUMNS)

    def test_no_agent_shrinkage_rows_returns_empty(self, spark):
        # Only roll-up (shrinkage_xforce) rows, no agent 'shrinkage' rows.
        out = compute_xforce_index(
            frame(spark, [xrow("shrinkage_xforce", 10)]),
            xit(spark, 80), avg(spark, 90), None,
        )
        assert len(out.take(1)) == 0

    def test_driven_by_shrinkage(self, spark):
        # An xforce present only in xpeers (not shrinkage) does not appear.
        out = compute_xforce_index(
            frame(spark, [shrink("a", 10, 100, xforce="A")]),
            xit(spark, 80, xforce="B"), None, None,
        )
        assert {r["xforce"] for r in rows(out)} == {"A"}

    def test_improved_none_is_handled_gracefully(self, spark):
        # Deferred improved input: build still produces rows (count via date rule).
        # shrinkage 10% -> 100; xit 85 -> 95 (on-target rescale); avg 90. MAY -> 3.
        out = compute_xforce_index(
            frame(spark, [shrink("a", 10, 100, dref=MAY)]),
            xit(spark, 85, dref=MAY), avg(spark, 90, dref=MAY), None,
        )
        r = one(out)
        assert abs(r["denominator"] - 300.0) < 1e-9
        assert abs(r["metric_value"] - 95.0) < 1e-9  # (100 + 95 + 90) / 300 * 100
