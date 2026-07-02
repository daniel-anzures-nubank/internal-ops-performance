"""Unit tests for ``metrics/xpeers_in_target.py`` (PySpark).

Small synthetic agent-level component metric frames. We verify the per-component
target thresholds, the targets-achieved / total-targets ratio, the team-specific
component sets (Core/Fraud NTPJ vs SM tNPS+WoWs vs Content SLA-NTPJ+CSAT), and
the parity fixes:

* Fix #1 — only week + month grains survive pre-cutover;
* Fix #2 — the per-grain/team era boundary on the RAW ``date_reference``
  (a January *weekly* bucket already carries Quality for SM and the XPLead grain,
  but NOT for the Core/Fraud XForce grain);
* Fix #3 — Core/Fraud do NOT coalesce a missing NTPJ (the row is kept with NULL
  value), while Social Media / Content coalesce every component to 0;
* Fix #4 — SM/Content degenerate squad/district roll-ups (SM sums counts;
  Content averages the grain rows' metric_value with a constant-0 denominator).

Content specifics: rows only from Feb 2026 (the legacy save filter drops
Jan-2026), NTPJ is the higher-is-better SLA metric (``>= 95``; XPLead grain
``>= 100`` pre-cutover — the legacy quirk), NTPJ + NOcc join from March, and the
XPLead grain is month-only pre-cutover.
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
            # SM roll-up rows carry the deck group label so they stay
            # distinguishable from Content's same-named rows.
            assert r["xforce"] is None
            assert r["squad"] == "social" and r["district"] == "social"
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


class TestContent:
    def test_march_onward_component_set_and_ratio(self, spark):
        # a: adh 96(✓) ntpj 96(✓ SLA >=95) nocc 110(✓) csat 96(✓) -> 4/4
        # b: adh 90(✗) ntpj 90(✗)          nocc 90(✗)  csat 80(✗) -> 0/4 ; 4/8 -> 50%
        out = compute_xpeers_in_target(
            adherence=frame(spark, [m("adherence", "a", 96, team="content"),
                                    m("adherence", "b", 90, team="content")]),
            ntpj=frame(spark, [m("ntpj", "a", 96, team="content"),
                               m("ntpj", "b", 90, team="content")]),
            normalized_occupancy=frame(spark, [
                m("normalized_occupancy", "a", 110, team="content"),
                m("normalized_occupancy", "b", 90, team="content")]),
            content_csat=frame(spark, [m("content_csat", "a", 96, team="content"),
                                       m("content_csat", "b", 80, team="content")]),
        )
        r = one(out)
        assert abs(r["numerator"] - 4.0) < 1e-9
        assert abs(r["denominator"] - 8.0) < 1e-9
        assert abs(r["metric_value"] - 50.0) < 1e-9

    def test_ntpj_threshold_is_ge_95_sla_direction(self, spark):
        # Content NTPJ is higher-is-better: 95(✓) 94.9(✗) 100(✓).
        out = compute_xpeers_in_target(
            adherence=frame(spark, [m("adherence", a, 50, team="content")
                                    for a in ("a", "b", "c")]),
            ntpj=frame(spark, [m("ntpj", "a", 95, team="content"),
                               m("ntpj", "b", 94.9, team="content"),
                               m("ntpj", "c", 100, team="content")]),
        )
        r = one(out)  # adh 0/3 ; ntpj 2/3 -> 2/6
        assert abs(r["numerator"] - 2.0) < 1e-9
        assert abs(r["denominator"] - 6.0) < 1e-9

    def test_february_is_adherence_plus_csat_only(self, spark):
        # NTPJ + NOcc join only from March; CSAT is in from the start.
        out = compute_xpeers_in_target(
            adherence=frame(spark, [m("adherence", "a", 96, team="content", dref=FEB)]),
            ntpj=frame(spark, [m("ntpj", "a", 96, team="content", dref=FEB)]),
            normalized_occupancy=frame(spark, [
                m("normalized_occupancy", "a", 110, team="content", dref=FEB)]),
            content_csat=frame(spark, [m("content_csat", "a", 96, team="content", dref=FEB)]),
        )
        assert abs(one(out)["denominator"] - 2.0) < 1e-9  # adh + csat

    def test_january_2026_dropped(self, spark):
        # The legacy Content deck's save filter permanently drops Jan-2026.
        out = compute_xpeers_in_target(
            adherence=frame(spark, [
                m("adherence", "a", 96, team="content", dref=JAN),
                m("adherence", "a", 96, team="content", dref=JAN_WEEK, gran="week")]),
            ntpj=frame(spark, [m("ntpj", "a", 96, team="content", dref=JAN)]),
        )
        assert len(out.take(1)) == 0

    def test_missing_csat_coalesces_to_zero_not_null(self, spark):
        # Content coalesces every component (SM-style) — no NULL rows.
        out = compute_xpeers_in_target(
            adherence=frame(spark, [m("adherence", "a", 96, team="content", dref=FEB)]),
        )
        r = one(out)  # adherence only: 1/1
        assert abs(r["numerator"] - 1.0) < 1e-9
        assert abs(r["denominator"] - 1.0) < 1e-9
        assert abs(r["metric_value"] - 100.0) < 1e-9

    def test_xplead_month_only_pre_cutover(self, spark):
        out = compute_xpeers_in_target_xplead(
            adherence=frame(spark, [
                m("adherence", "a", 96, team="content", dref=FEB),
                m("adherence", "a", 96, team="content", dref=FEB_WEEK, gran="week")]),
        )
        assert {r["date_granularity"] for r in grain_rows(out, XPLEAD_METRIC_NAME)} == {
            "month"
        }

    def test_xplead_ntpj_ge_100_legacy_quirk(self, spark):
        # Pre-cutover the Content XPLead grain flags NTPJ >= 100 (not >= 95):
        # 96 fails, 100 passes.
        out = compute_xpeers_in_target_xplead(
            adherence=frame(spark, [m("adherence", a, 50, team="content")
                                    for a in ("a", "b")]),
            ntpj=frame(spark, [m("ntpj", "a", 96, team="content"),
                               m("ntpj", "b", 100, team="content")]),
        )
        r = one(out, XPLEAD_METRIC_NAME)  # adh 0/2 ; ntpj 1/2 -> 1/4
        assert abs(r["numerator"] - 1.0) < 1e-9
        assert abs(r["denominator"] - 4.0) < 1e-9

    def test_rollups_average_metric_value_with_zero_denominator(self, spark):
        # Content roll-ups (legacy L5641-5730): numerator = SUM(metric_value),
        # denominator = COUNT(DISTINCT agent) = 0, metric_value = AVG.
        # A all-pass (mv 100) + B all-fail (mv 0) -> num 100, den 0, mv 50.
        out = compute_xpeers_in_target(
            adherence=frame(spark, [
                m("adherence", "a", 96, team="content", dref=FEB, xforce="A"),
                m("adherence", "b", 50, team="content", dref=FEB, xforce="B")]),
            content_csat=frame(spark, [
                m("content_csat", "a", 96, team="content", dref=FEB, xforce="A"),
                m("content_csat", "b", 80, team="content", dref=FEB, xforce="B")]),
        )
        for metric in (SQUAD_METRIC_NAME, DISTRICT_METRIC_NAME):
            r = one(out, metric)
            assert r["xforce"] is None
            assert r["squad"] == "enablement" and r["district"] == "content"
            assert abs(r["numerator"] - 100.0) < 1e-9
            assert abs(r["denominator"] - 0.0) < 1e-9
            assert abs(r["metric_value"] - 50.0) < 1e-9

    def test_content_and_core_same_xforce_stay_split(self, spark):
        # Content is its own deck — a shared xforce name does not merge.
        out = compute_xpeers_in_target(
            adherence=frame(spark, [m("adherence", "a", 96, team="content", xforce="x"),
                                    m("adherence", "b", 96, team="core", xforce="x")]),
            ntpj=frame(spark, [m("ntpj", "a", 96, team="content", xforce="x"),
                               m("ntpj", "b", 90, team="core", xforce="x")]),
        )
        assert len(grain_rows(out, METRIC_NAME)) == 2


class TestGeneral:

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
