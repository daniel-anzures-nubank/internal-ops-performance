"""Unit tests for ``metrics/xpeers_in_target.py`` (PySpark).

Small synthetic agent-level component metric frames. We verify the per-component
target thresholds, the targets-achieved / total-targets ratio, the team-specific
component sets (Core/Fraud NTPJ vs SM tNPS+WoWs), and the four parity fixes:

* Fix #1 — only week + month grains survive pre-cutover;
* Fix #2 — the per-grain/team era boundary on the RAW ``date_reference``
  (a January *weekly* bucket already carries Quality for SM and the XPLead grain,
  but NOT for the Core/Fraud XForce grain);
* Fix #3 — Core/Fraud do NOT coalesce a missing NTPJ (the row is kept with NULL
  value), while Social Media coalesces every component to 0;
* Fix #4 — Social-Media-only degenerate squad/district roll-ups.
"""

from __future__ import annotations

import datetime as dt

from pyspark.sql import types as T

from metric_utils import METRIC_COLUMNS
from xpeers_in_target import (
    DISTRICT_METRIC_NAME,
    IO_XPEERS_IN_TARGET_METRIC_SCHEMA,
    METRIC_NAME,
    SQUAD_METRIC_NAME,
    XPLEAD_DISTRICT_METRIC_NAME,
    XPLEAD_METRIC_NAME,
    XPLEAD_SQUAD_METRIC_NAME,
    compute_xpeers_in_target,
    compute_xpeers_in_target_xplead,
)

MAR = dt.date(2026, 5, 1)        # a clean "March-onward" month (all components)
FEB = dt.date(2026, 2, 1)
JAN = dt.date(2026, 1, 1)
JAN_WEEK = dt.date(2026, 1, 5)   # a Monday in January
FEB_WEEK = dt.date(2026, 2, 2)   # a Monday in February

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


def m(metric, agent, mv, *, xforce="xf", xplead="xp", team="core", dref=MAR, gran="month"):
    return {
        "agent": agent, "xforce": xforce, "xplead": xplead, "team": team,
        "squad": "sq", "district": "di", "shift": "morning",
        "date_reference": dref, "date_granularity": gran, "metric": metric,
        "numerator": 0.0, "denominator": 100.0,
        "metric_value": None if mv is None else float(mv),
    }


def frame(spark, rows):
    data = [tuple(r[name] for name in _FIELDS) for r in rows]
    return spark.createDataFrame(data, _SCHEMA)


def rows(out):
    return out.collect()


def grain_rows(out, metric):
    return [r for r in out.collect() if r["metric"] == metric]


def one(out, metric=METRIC_NAME):
    r = grain_rows(out, metric)
    assert len(r) == 1, f"expected 1 {metric} row, got {len(r)}"
    return r[0]


# --------------------------------------------------------------------------- #
class TestCoreFraud:
    def test_full_targets_ratio(self, spark):
        # a: adh 96(✓) ntpj 90(✓) nocc 110(✓) qa 96(✓) -> 4/4
        # b: adh 90(✗) ntpj 130(✗) nocc 90(✗) qa 80(✗) -> 0/4 ; 4 / 8 -> 50%
        out = compute_xpeers_in_target(
            adherence=frame(spark, [m("adherence", "a", 96), m("adherence", "b", 90)]),
            ntpj=frame(spark, [m("ntpj", "a", 90), m("ntpj", "b", 130)]),
            normalized_occupancy=frame(spark, [m("normalized_occupancy", "a", 110),
                                               m("normalized_occupancy", "b", 90)]),
            quality=frame(spark, [m("quality", "a", 96), m("quality", "b", 80)]),
        )
        r = one(out)
        assert r["metric"] == METRIC_NAME
        assert abs(r["numerator"] - 4.0) < 1e-9
        assert abs(r["denominator"] - 8.0) < 1e-9
        assert abs(r["metric_value"] - 50.0) < 1e-9

    def test_ntpj_threshold_is_le_100(self, spark):
        out = compute_xpeers_in_target(
            adherence=frame(spark, [m("adherence", "a", 50), m("adherence", "b", 50)]),
            ntpj=frame(spark, [m("ntpj", "a", 100), m("ntpj", "b", 100.1)]),
        )
        r = one(out)  # adherence 0/2 ; ntpj 1/2 -> 1/4
        assert abs(r["numerator"] - 1.0) < 1e-9
        assert abs(r["denominator"] - 4.0) < 1e-9

    def test_null_metric_value_counts_in_denominator_only(self, spark):
        # ntpj present for both so the CF row is numeric (Fix #3); a NULL adherence
        # metric_value still counts in the denominator but fails the numerator.
        out = compute_xpeers_in_target(
            adherence=frame(spark, [m("adherence", "a", 96),
                                    m("adherence", "b", None)]),
            ntpj=frame(spark, [m("ntpj", "a", 90), m("ntpj", "b", 90)]),
        )
        r = one(out)  # adherence: in=1 (a) tot=2 ; ntpj: in=2 tot=2 -> 3 / 4
        assert abs(r["denominator"] - 4.0) < 1e-9  # both agents counted twice
        assert abs(r["numerator"] - 3.0) < 1e-9    # adh 'a' + ntpj both


class TestEra:
    def test_cf_xforce_january_month_is_adherence_plus_ntpj(self, spark):
        out = compute_xpeers_in_target(
            adherence=frame(spark, [m("adherence", "a", 96, dref=JAN)]),
            ntpj=frame(spark, [m("ntpj", "a", 90, dref=JAN)]),
            normalized_occupancy=frame(spark, [m("normalized_occupancy", "a", 110, dref=JAN)]),
            quality=frame(spark, [m("quality", "a", 96, dref=JAN)]),
        )
        assert abs(one(out)["denominator"] - 2.0) < 1e-9  # adh + ntpj only

    def test_cf_xforce_february_adds_quality(self, spark):
        out = compute_xpeers_in_target(
            adherence=frame(spark, [m("adherence", "a", 96, dref=FEB)]),
            ntpj=frame(spark, [m("ntpj", "a", 90, dref=FEB)]),
            quality=frame(spark, [m("quality", "a", 96, dref=FEB)]),
        )
        assert abs(one(out)["denominator"] - 3.0) < 1e-9  # adh + ntpj + qa

    def test_cf_xforce_march_adds_nocc(self, spark):
        out = compute_xpeers_in_target(
            adherence=frame(spark, [m("adherence", "a", 96)]),
            ntpj=frame(spark, [m("ntpj", "a", 90)]),
            normalized_occupancy=frame(spark, [m("normalized_occupancy", "a", 110)]),
            quality=frame(spark, [m("quality", "a", 96)]),
        )
        assert abs(one(out)["denominator"] - 4.0) < 1e-9

    def test_cf_xforce_january_WEEK_excludes_quality(self, spark):
        # Fix #2: Core/Fraud XForce uses the month-anchored >= Feb-1 boundary, so a
        # January weekly bucket still EXCLUDES quality (den = adh + ntpj).
        out = compute_xpeers_in_target(
            adherence=frame(spark, [m("adherence", "a", 96, dref=JAN_WEEK, gran="week")]),
            ntpj=frame(spark, [m("ntpj", "a", 90, dref=JAN_WEEK, gran="week")]),
            quality=frame(spark, [m("quality", "a", 96, dref=JAN_WEEK, gran="week")]),
        )
        assert abs(one(out)["denominator"] - 2.0) < 1e-9

    def test_sm_january_WEEK_includes_quality(self, spark):
        # Fix #2: Social Media uses the > Jan-1 boundary on the raw date_reference,
        # so EVERY January weekly bucket already includes quality.
        out = compute_xpeers_in_target(
            adherence=frame(spark, [m("adherence", "a", 96, dref=JAN_WEEK, gran="week",
                                      team="social media")]),
            tnps=frame(spark, [m("tnps", "a", 90, dref=JAN_WEEK, gran="week",
                                 team="social media")]),
            wows=frame(spark, [m("wows", "a", 6, dref=JAN_WEEK, gran="week",
                                 team="social media")]),
            quality=frame(spark, [m("quality", "a", 96, dref=JAN_WEEK, gran="week",
                                    team="social media")]),
        )
        # adh + tnps + wows + quality = 4 (NO not yet — needs > Feb-1).
        assert abs(one(out)["denominator"] - 4.0) < 1e-9

    def test_xplead_cf_january_WEEK_includes_quality(self, spark):
        # Fix #2: the XPLead grain (even for Core/Fraud) uses > Jan-1, so a January
        # weekly XPLead bucket includes quality (den = adh + ntpj + qa).
        out = compute_xpeers_in_target_xplead(
            adherence=frame(spark, [m("adherence", "a", 96, dref=JAN_WEEK, gran="week")]),
            ntpj=frame(spark, [m("ntpj", "a", 90, dref=JAN_WEEK, gran="week")]),
            quality=frame(spark, [m("quality", "a", 96, dref=JAN_WEEK, gran="week")]),
        )
        assert abs(one(out, XPLEAD_METRIC_NAME)["denominator"] - 3.0) < 1e-9


class TestSocialMedia:
    def test_sm_uses_tnps_and_wows_not_ntpj(self, spark):
        out = compute_xpeers_in_target(
            adherence=frame(spark, [m("adherence", "a", 96, team="social media")]),
            ntpj=frame(spark, [m("ntpj", "a", 90, team="social media")]),  # ignored
            normalized_occupancy=frame(spark, [m("normalized_occupancy", "a", 110, team="social media")]),
            quality=frame(spark, [m("quality", "a", 96, team="social media")]),
            tnps=frame(spark, [m("tnps", "a", 90, team="social media")]),
            wows=frame(spark, [m("wows", "a", 6, team="social media")]),
        )
        r = one(out)  # adh + tnps + wows + qa + nocc = 5, all pass
        assert abs(r["denominator"] - 5.0) < 1e-9
        assert abs(r["numerator"] - 5.0) < 1e-9

    def test_tnps_and_wows_thresholds(self, spark):
        out = compute_xpeers_in_target(
            adherence=frame(spark, [m("adherence", "a", 50, dref=JAN, team="social media"),
                                    m("adherence", "b", 50, dref=JAN, team="social media")]),
            tnps=frame(spark, [m("tnps", "a", 88, dref=JAN, team="social media"),
                               m("tnps", "b", 87, dref=JAN, team="social media")]),
            wows=frame(spark, [m("wows", "a", 5, dref=JAN, team="social media"),
                               m("wows", "b", 4, dref=JAN, team="social media")]),
        )
        r = one(out)  # adh 0/2, tnps 1/2, wows 1/2 -> 2 / 6
        assert abs(r["numerator"] - 2.0) < 1e-9
        assert abs(r["denominator"] - 6.0) < 1e-9


class TestFix3Coalescing:
    def test_cf_missing_ntpj_yields_null_row_kept(self, spark):
        # Core/Fraud do NOT coalesce ntpj: a missing ntpj match -> NULL
        # numerator/denominator/metric_value, but the row is STILL emitted.
        out = compute_xpeers_in_target(
            adherence=frame(spark, [m("adherence", "a", 96)]),
            # no ntpj source at all
        )
        r = one(out)
        assert r["numerator"] is None
        assert r["denominator"] is None
        assert r["metric_value"] is None

    def test_sm_missing_optional_components_stay_numeric(self, spark):
        # Social Media coalesces everything: missing quality/nocc -> 0, never NULL.
        out = compute_xpeers_in_target(
            adherence=frame(spark, [m("adherence", "a", 96, dref=JAN, team="social media")]),
            tnps=frame(spark, [m("tnps", "a", 90, dref=JAN, team="social media")]),
            wows=frame(spark, [m("wows", "a", 6, dref=JAN, team="social media")]),
        )
        r = one(out)
        assert r["metric_value"] is not None
        assert abs(r["denominator"] - 3.0) < 1e-9  # adh + tnps + wows


class TestFix4SMRollups:
    def test_sm_xforce_squad_and_district_rollups(self, spark):
        # Two SM xforces (Jan, adh+tnps+wows): A all-pass 3/3, B all-fail 0/3.
        # The degenerate squad/district roll-ups sum both -> 3 / 6 -> 50%.
        out = compute_xpeers_in_target(
            adherence=frame(spark, [
                m("adherence", "a", 96, dref=JAN, team="social media", xforce="A"),
                m("adherence", "b", 50, dref=JAN, team="social media", xforce="B")]),
            tnps=frame(spark, [
                m("tnps", "a", 90, dref=JAN, team="social media", xforce="A"),
                m("tnps", "b", 80, dref=JAN, team="social media", xforce="B")]),
            wows=frame(spark, [
                m("wows", "a", 6, dref=JAN, team="social media", xforce="A"),
                m("wows", "b", 4, dref=JAN, team="social media", xforce="B")]),
        )
        for metric in (SQUAD_METRIC_NAME, DISTRICT_METRIC_NAME):
            r = one(out, metric)
            assert r["xforce"] is None and r["squad"] is None and r["district"] is None
            assert abs(r["numerator"] - 3.0) < 1e-9
            assert abs(r["denominator"] - 6.0) < 1e-9
            assert abs(r["metric_value"] - 50.0) < 1e-9

    def test_core_fraud_has_no_squad_district_rollups(self, spark):
        out = compute_xpeers_in_target(
            adherence=frame(spark, [m("adherence", "a", 96)]),
            ntpj=frame(spark, [m("ntpj", "a", 90)]),
        )
        metrics = {r["metric"] for r in rows(out)}
        assert SQUAD_METRIC_NAME not in metrics
        assert DISTRICT_METRIC_NAME not in metrics

    def test_xplead_sm_rollups(self, spark):
        out = compute_xpeers_in_target_xplead(
            adherence=frame(spark, [
                m("adherence", "a", 96, dref=JAN, team="social media", xplead="p1"),
                m("adherence", "b", 50, dref=JAN, team="social media", xplead="p2")]),
            tnps=frame(spark, [
                m("tnps", "a", 90, dref=JAN, team="social media", xplead="p1"),
                m("tnps", "b", 80, dref=JAN, team="social media", xplead="p2")]),
            wows=frame(spark, [
                m("wows", "a", 6, dref=JAN, team="social media", xplead="p1"),
                m("wows", "b", 4, dref=JAN, team="social media", xplead="p2")]),
        )
        for metric in (XPLEAD_SQUAD_METRIC_NAME, XPLEAD_DISTRICT_METRIC_NAME):
            r = one(out, metric)
            assert abs(r["numerator"] - 3.0) < 1e-9
            assert abs(r["denominator"] - 6.0) < 1e-9


class TestFix1Granularity:
    def test_only_week_and_month_survive(self, spark):
        out = compute_xpeers_in_target(
            adherence=frame(spark, [m("adherence", "a", 96, gran=g) for g in
                                    ("day", "week", "month", "quarter", "semester", "year")]),
            ntpj=frame(spark, [m("ntpj", "a", 90, gran=g) for g in
                               ("day", "week", "month", "quarter", "semester", "year")]),
        )
        assert {r["date_granularity"] for r in rows(out)} == {"week", "month"}


class TestDeck:
    def test_core_and_fraud_merge_into_one_main_row(self, spark):
        # A cross-team XForce (core + fraud agents) is ONE main-deck row — legacy
        # groups the main deck by (xforce, xplead) over both teams.
        # a (core): adh 96(✓) ntpj 90(✓) -> 2/2 ; b (fraud): adh 90(✗) ntpj 130(✗) -> 0/2
        out = compute_xpeers_in_target(
            adherence=frame(spark, [m("adherence", "a", 96, team="core", xforce="x"),
                                    m("adherence", "b", 90, team="fraud", xforce="x")]),
            ntpj=frame(spark, [m("ntpj", "a", 90, team="core", xforce="x"),
                               m("ntpj", "b", 130, team="fraud", xforce="x")]),
        )
        r = one(out)
        assert r["team"] is None
        assert abs(r["numerator"] - 2.0) < 1e-9
        assert abs(r["denominator"] - 4.0) < 1e-9
        assert abs(r["metric_value"] - 50.0) < 1e-9

    def test_cross_deck_xforce_stays_split(self, spark):
        # An XForce with core AND social-media agents must NOT merge (different
        # decks/notebooks) — two rows for the same xforce.
        out = compute_xpeers_in_target(
            adherence=frame(spark, [
                m("adherence", "a", 96, team="core", xforce="x"),
                m("adherence", "b", 96, team="social media", xforce="x")]),
            ntpj=frame(spark, [m("ntpj", "a", 90, team="core", xforce="x")]),
            tnps=frame(spark, [m("tnps", "a", 90, team="social media", xforce="x")]),
            wows=frame(spark, [m("wows", "a", 6, team="social media", xforce="x")]),
        )
        xf_rows = grain_rows(out, METRIC_NAME)
        assert len(xf_rows) == 2  # one main, one sm


class TestGeneral:
    def test_content_excluded(self, spark):
        out = compute_xpeers_in_target(
            adherence=frame(spark, [m("adherence", "a", 96, team="content", xforce="ct"),
                                    m("adherence", "b", 96, team="core", xforce="cf")]),
            ntpj=frame(spark, [m("ntpj", "b", 90, team="core", xforce="cf")]),
        )
        assert {r["xforce"] for r in grain_rows(out, METRIC_NAME)} == {"cf"}

    def test_driver_is_adherence(self, spark):
        out = compute_xpeers_in_target(
            adherence=frame(spark, [m("adherence", "a", 96, xforce="A")]),
            ntpj=frame(spark, [m("ntpj", "a", 90, xforce="A"),
                               m("ntpj", "z", 90, xforce="B")]),
        )
        assert {r["xforce"] for r in grain_rows(out, METRIC_NAME)} == {"A"}

    def test_pre_2026_dropped(self, spark):
        out = compute_xpeers_in_target(
            adherence=frame(spark, [m("adherence", "a", 96, dref=dt.date(2025, 12, 1))]),
        )
        assert len(out.take(1)) == 0

    def test_output_contract(self, spark):
        out = compute_xpeers_in_target(
            adherence=frame(spark, [m("adherence", "a", 96)]),
            ntpj=frame(spark, [m("ntpj", "a", 90)]),
        )
        assert out.columns == list(METRIC_COLUMNS)
        assert [c for c, _ in IO_XPEERS_IN_TARGET_METRIC_SCHEMA] == list(METRIC_COLUMNS)
        r = one(out)
        assert r["agent"] is None and r["squad"] is None and r["shift"] is None
        assert r["xforce"] == "xf" and r["team"] is None  # legacy carries no team

    def test_empty_returns_empty(self, spark):
        out = compute_xpeers_in_target(adherence=frame(spark, []))
        assert len(out.take(1)) == 0
        assert out.columns == list(METRIC_COLUMNS)


class TestXPLead:
    def test_rolls_agents_across_xforces_into_one_xplead(self, spark):
        out = compute_xpeers_in_target_xplead(
            adherence=frame(spark, [m("adherence", "a", 96, xforce="xf1"),
                                    m("adherence", "b", 90, xforce="xf2")]),
            ntpj=frame(spark, [m("ntpj", "a", 90, xforce="xf1"),
                               m("ntpj", "b", 130, xforce="xf2")]),
        )
        r = one(out, XPLEAD_METRIC_NAME)
        assert r["xforce"] is None and r["xplead"] == "xp"
        assert abs(r["numerator"] - 2.0) < 1e-9
        assert abs(r["denominator"] - 4.0) < 1e-9
        assert abs(r["metric_value"] - 50.0) < 1e-9

    def test_separate_xpleads_stay_separate(self, spark):
        out = compute_xpeers_in_target_xplead(
            adherence=frame(spark, [m("adherence", "a", 96, xplead="p1"),
                                    m("adherence", "b", 96, xplead="p2")]),
            ntpj=frame(spark, [m("ntpj", "a", 90, xplead="p1"),
                               m("ntpj", "b", 90, xplead="p2")]),
        )
        assert {r["xplead"] for r in grain_rows(out, XPLEAD_METRIC_NAME)} == {"p1", "p2"}

    def test_empty_returns_empty(self, spark):
        out = compute_xpeers_in_target_xplead(adherence=frame(spark, []))
        assert len(out.take(1)) == 0
        assert out.columns == list(METRIC_COLUMNS)
