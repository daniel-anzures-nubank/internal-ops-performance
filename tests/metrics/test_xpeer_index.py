"""Unit tests for ``metrics/xpeer_index.py`` (PySpark).

Small synthetic ``io_*_metric`` frames (no warehouse). We verify the component
transforms (NTPJ fold, NO truncation, WoWs fold), the team- and era-dependent
composition (Core/Fraud vs Content vs Social Media; Jan/Feb/March cutovers), the
adherence-as-driver rule, the team guard (a NULL team is a main-deck support
squad and gets the Core/Fraud roster; only an unexpected NON-NULL team falls
through to Adherence-only), and the output contract — plus the two parity fixes:

* **Fix #1** — pre-cutover output is restricted to week + month grain.
* **Fix #2** — the December-2025 ISO weekly bucket (Monday 2025-12-29) is KEPT
  and the weekly era is classified by the RAW ``date_reference`` boundaries
  (not the month of the Monday).
"""

from __future__ import annotations

import datetime as dt

from pyspark.sql import types as T

from metric_utils import METRIC_COLUMNS
from xpeer_index import IO_XPEER_INDEX_METRIC_SCHEMA, METRIC_NAME, compute_xpeer_index

MAR = dt.date(2026, 5, 1)  # March-or-later era (full composition)
FEB = dt.date(2026, 2, 1)
JAN = dt.date(2026, 1, 1)

# The component inputs are already-aggregated tidy `io_*_metric` frames.
_METRIC_SCHEMA = T.StructType(
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

_FIELDS = [f.name for f in _METRIC_SCHEMA.fields]


def _row(metric, agent, mv, dref=MAR, gran="month", team="core", **dims):
    base = {
        "agent": agent,
        "xforce": "x.force",
        "xplead": "x.plead",
        "team": team,
        "squad": "sq",
        "district": "di",
        "shift": "morning",
        "date_reference": dref,
        "date_granularity": gran,
        "metric": metric,
        "numerator": float(mv),
        "denominator": 100.0,
        "metric_value": float(mv),
    }
    base.update(dims)
    return base


def frame(spark, rows):
    """Build a tidy `io_*_metric` Spark frame from `_row` dicts (or empty)."""
    data = [tuple(r[name] for name in _FIELDS) for r in rows]
    return spark.createDataFrame(data, _METRIC_SCHEMA)


def f(spark, metric, *rows):
    """Convenience: a frame of `_row(...)` dicts for one metric."""
    return frame(spark, list(rows))


def only(out, agent):
    sub = [r for r in out.collect() if r["agent"] == agent]
    assert len(sub) == 1, f"expected 1 row for {agent}, got {len(sub)}"
    return sub[0]


def month_rows(out):
    return out.filter(out["date_granularity"] == "month").collect()


# --------------------------------------------------------------------------- #
# Core / Fraud
# --------------------------------------------------------------------------- #
class TestCoreFraud:
    def test_full_mar_composition(self, spark):
        # adh 90, ntpj 80 -> fold 100, nocc 95 -> 95, quality 88. mean = 93.25
        out = compute_xpeer_index(
            adherence=frame(spark, [_row("adherence", "a", 90, team="core")]),
            ntpj=frame(spark, [_row("ntpj", "a", 80, team="core")]),
            normalized_occupancy=frame(spark, [_row("normalized_occupancy", "a", 95)]),
            quality=frame(spark, [_row("quality", "a", 88)]),
        )
        r = only(out, "a")
        assert r["metric"] == METRIC_NAME
        assert abs(r["numerator"] - (90 + 100 + 95 + 88)) < 1e-9
        assert abs(r["denominator"] - 400.0) < 1e-9
        assert abs(r["metric_value"] - 93.25) < 1e-9

    def test_ntpj_fold_above_100(self, spark):
        # ntpj 130 -> 200 - 130 = 70
        out = compute_xpeer_index(
            adherence=frame(spark, [_row("adherence", "a", 100)]),
            ntpj=frame(spark, [_row("ntpj", "a", 130)]),
            normalized_occupancy=frame(spark, [_row("normalized_occupancy", "a", 100)]),
            quality=frame(spark, [_row("quality", "a", 100)]),
        )
        r = only(out, "a")
        assert abs(r["numerator"] - (100 + 70 + 100 + 100)) < 1e-9

    def test_ntpj_above_200_is_zero(self, spark):
        out = compute_xpeer_index(
            adherence=frame(spark, [_row("adherence", "a", 100)]),
            ntpj=frame(spark, [_row("ntpj", "a", 250)]),
            normalized_occupancy=frame(spark, [_row("normalized_occupancy", "a", 100)]),
            quality=frame(spark, [_row("quality", "a", 100)]),
        )
        assert abs(only(out, "a")["numerator"] - (100 + 0 + 100 + 100)) < 1e-9

    def test_nocc_truncated(self, spark):
        # nocc 120 -> 100
        out = compute_xpeer_index(
            adherence=frame(spark, [_row("adherence", "a", 100)]),
            ntpj=frame(spark, [_row("ntpj", "a", 100)]),
            normalized_occupancy=frame(spark, [_row("normalized_occupancy", "a", 120)]),
            quality=frame(spark, [_row("quality", "a", 100)]),
        )
        assert abs(only(out, "a")["numerator"] - 400.0) < 1e-9

    def test_january_only_adherence_ntpj(self, spark):
        # Jan: quality & NO excluded even when present. (adh + ntpj) / 2
        out = compute_xpeer_index(
            adherence=frame(spark, [_row("adherence", "a", 90, dref=JAN)]),
            ntpj=frame(spark, [_row("ntpj", "a", 80, dref=JAN)]),
            normalized_occupancy=frame(spark, [_row("normalized_occupancy", "a", 95, dref=JAN)]),
            quality=frame(spark, [_row("quality", "a", 88, dref=JAN)]),
        )
        r = only(out, "a")
        assert abs(r["denominator"] - 200.0) < 1e-9
        assert abs(r["metric_value"] - 95.0) < 1e-9  # (90 + 100) / 2

    def test_february_adds_quality_not_nocc(self, spark):
        out = compute_xpeer_index(
            adherence=frame(spark, [_row("adherence", "a", 90, dref=FEB)]),
            ntpj=frame(spark, [_row("ntpj", "a", 80, dref=FEB)]),
            normalized_occupancy=frame(spark, [_row("normalized_occupancy", "a", 95, dref=FEB)]),
            quality=frame(spark, [_row("quality", "a", 88, dref=FEB)]),
        )
        r = only(out, "a")
        assert abs(r["denominator"] - 300.0) < 1e-9  # adh + ntpj + quality
        assert abs(r["numerator"] - (90 + 100 + 88)) < 1e-9

    def test_march_missing_quality_drops_term(self, spark):
        out = compute_xpeer_index(
            adherence=frame(spark, [_row("adherence", "a", 90)]),
            ntpj=frame(spark, [_row("ntpj", "a", 80)]),
            normalized_occupancy=frame(spark, [_row("normalized_occupancy", "a", 95)]),
            quality=frame(spark, []),  # no quality
        )
        r = only(out, "a")
        assert abs(r["denominator"] - 300.0) < 1e-9  # adh + ntpj + nocc
        assert abs(r["metric_value"] - (90 + 100 + 95) / 3) < 1e-9

    def test_nitza_april_may_no_component_carveout(self, spark):
        out = compute_xpeer_index(
            adherence=frame(spark, [
                _row("adherence", "nitza.zarza", 90, dref=dt.date(2026, 4, 1))
            ]),
            ntpj=frame(spark, [
                _row("ntpj", "nitza.zarza", 100, dref=dt.date(2026, 4, 1))
            ]),
            normalized_occupancy=frame(spark, [
                _row("normalized_occupancy", "nitza.zarza", 10, dref=dt.date(2026, 4, 1))
            ]),
            quality=frame(spark, [
                _row("quality", "nitza.zarza", 80, dref=dt.date(2026, 4, 1))
            ]),
        )
        r = only(out, "nitza.zarza")
        assert abs(r["numerator"] - (90 + 100 + 80)) < 1e-9
        assert abs(r["denominator"] - 300.0) < 1e-9
        assert abs(r["metric_value"] - 90.0) < 1e-9


# --------------------------------------------------------------------------- #
# Content (CSAT is the quality term; quality only from March)
# --------------------------------------------------------------------------- #
class TestContent:
    def test_mar_full_uses_csat(self, spark):
        # adh 90, ntpj 130 -> 70, nocc 120 -> 100, csat 80. mean = 85
        out = compute_xpeer_index(
            adherence=frame(spark, [_row("adherence", "a", 90, team="content")]),
            ntpj=frame(spark, [_row("ntpj", "a", 130, team="content")]),
            normalized_occupancy=frame(spark, [_row("normalized_occupancy", "a", 120, team="content")]),
            content_csat=frame(spark, [_row("content_csat", "a", 80, team="content")]),
        )
        r = only(out, "a")
        assert abs(r["denominator"] - 400.0) < 1e-9
        assert abs(r["metric_value"] - 85.0) < 1e-9

    def test_february_is_adherence_only(self, spark):
        # Real Content has NO ntpj rows before March, so legacy Feb Content is
        # Adherence-only (den 100, not 200). nocc & csat are also era-gated out.
        # (Verified: legacy _content index_agent Feb = 17 rows, all den=100.)
        out = compute_xpeer_index(
            adherence=frame(spark, [_row("adherence", "a", 90, dref=FEB, team="content")]),
            normalized_occupancy=frame(spark, [_row("normalized_occupancy", "a", 100, dref=FEB, team="content")]),
            content_csat=frame(spark, [_row("content_csat", "a", 50, dref=FEB, team="content")]),
        )
        r = only(out, "a")
        assert abs(r["denominator"] - 100.0) < 1e-9
        assert abs(r["metric_value"] - 90.0) < 1e-9

    def test_content_ntpj_present_only_drops_when_absent(self, spark):
        # Content NTPJ is present-only: a March Content agent with no ntpj row
        # drops it from BOTH numerator and denominator (den 300 = adh+nocc+csat),
        # unlike Core/Fraud where NTPJ is a fixed divisor term.
        out = compute_xpeer_index(
            adherence=frame(spark, [_row("adherence", "a", 90, team="content")]),
            normalized_occupancy=frame(spark, [_row("normalized_occupancy", "a", 100, team="content")]),
            content_csat=frame(spark, [_row("content_csat", "a", 80, team="content")]),
        )
        r = only(out, "a")
        assert abs(r["denominator"] - 300.0) < 1e-9
        assert abs(r["metric_value"] - (90 + 100 + 80) / 3) < 1e-9

    def test_content_ignores_playvox_quality(self, spark):
        # A stray Playvox quality row must NOT feed Content (it uses CSAT).
        out = compute_xpeer_index(
            adherence=frame(spark, [_row("adherence", "a", 90, team="content")]),
            ntpj=frame(spark, [_row("ntpj", "a", 100, team="content")]),
            normalized_occupancy=frame(spark, [_row("normalized_occupancy", "a", 100, team="content")]),
            quality=frame(spark, [_row("quality", "a", 0, team="content")]),  # ignored
            content_csat=frame(spark, [_row("content_csat", "a", 80, team="content")]),
        )
        r = only(out, "a")
        assert abs(r["metric_value"] - (90 + 100 + 100 + 80) / 4) < 1e-9


# --------------------------------------------------------------------------- #
# Social Media (WoWs + tNPS instead of NTPJ; SM excludes NTPJ)
# --------------------------------------------------------------------------- #
class TestSocialMedia:
    def test_mar_full_five_terms(self, spark):
        # adh 90, wows 5 -> 100, tnps 40, quality 80, nocc 120 -> 100. mean = 82
        out = compute_xpeer_index(
            adherence=frame(spark, [_row("adherence", "a", 90, team="social media")]),
            normalized_occupancy=frame(spark, [_row("normalized_occupancy", "a", 120, team="social media")]),
            quality=frame(spark, [_row("quality", "a", 80, team="social media")]),
            tnps=frame(spark, [_row("tnps", "a", 40, team="social media")]),
            wows=frame(spark, [_row("wows", "a", 5, team="social media")]),
        )
        r = only(out, "a")
        assert abs(r["denominator"] - 500.0) < 1e-9
        assert abs(r["metric_value"] - 82.0) < 1e-9

    def test_sm_excludes_ntpj(self, spark):
        # A stray NTPJ row for an SM agent must NOT enter the index.
        out = compute_xpeer_index(
            adherence=frame(spark, [_row("adherence", "a", 90, dref=JAN, team="social media")]),
            ntpj=frame(spark, [_row("ntpj", "a", 0, dref=JAN, team="social media")]),
            wows=frame(spark, [_row("wows", "a", 5, dref=JAN, team="social media")]),
        )
        r = only(out, "a")
        assert abs(r["denominator"] - 200.0) < 1e-9  # adh + wows only
        assert abs(r["metric_value"] - (90 + 100) / 2) < 1e-9

    def test_wows_fold_below_target(self, spark):
        # wows count 4 -> 4/5*100 = 80; Jan -> only adh + wows (no tnps here)
        out = compute_xpeer_index(
            adherence=frame(spark, [_row("adherence", "a", 90, dref=JAN, team="social media")]),
            wows=frame(spark, [_row("wows", "a", 4, dref=JAN, team="social media")]),
        )
        r = only(out, "a")
        assert abs(r["denominator"] - 200.0) < 1e-9
        assert abs(r["metric_value"] - (90 + 80) / 2) < 1e-9

    def test_negative_tnps_lowers_index(self, spark):
        # Jan with tnps present: (adh + wows + tnps) / 3, tnps may be negative.
        out = compute_xpeer_index(
            adherence=frame(spark, [_row("adherence", "a", 90, dref=JAN, team="social media")]),
            tnps=frame(spark, [_row("tnps", "a", -30, dref=JAN, team="social media")]),
            wows=frame(spark, [_row("wows", "a", 5, dref=JAN, team="social media")]),
        )
        r = only(out, "a")
        assert abs(r["denominator"] - 300.0) < 1e-9
        assert abs(r["metric_value"] - (90 + 100 - 30) / 3) < 1e-9

    def test_missing_tnps_drops_term(self, spark):
        out = compute_xpeer_index(
            adherence=frame(spark, [_row("adherence", "a", 90, dref=JAN, team="social media")]),
            wows=frame(spark, [_row("wows", "a", 5, dref=JAN, team="social media")]),
        )
        assert abs(only(out, "a")["denominator"] - 200.0) < 1e-9


# --------------------------------------------------------------------------- #
# Fix #1 — pre-cutover restricted to week + month grain
# --------------------------------------------------------------------------- #
class TestGranularityGate:
    def test_pre_cutover_keeps_only_week_and_month(self, spark):
        rows_for = lambda metric: frame(spark, [
            _row(metric, "a", 90, dref=JAN, gran=g)
            for g in ("day", "week", "month", "quarter", "semester", "year")
        ])
        out = compute_xpeer_index(
            adherence=rows_for("adherence"),
            ntpj=rows_for("ntpj"),
        )
        grans = {r["date_granularity"] for r in out.collect()}
        assert grans == {"week", "month"}

    def test_post_cutover_allows_all_granularities(self, spark):
        post = dt.date(2026, 7, 1)
        rows_for = lambda metric: frame(spark, [
            _row(metric, "a", 90, dref=post, gran=g)
            for g in ("day", "week", "month", "quarter", "semester", "year")
        ])
        out = compute_xpeer_index(
            adherence=rows_for("adherence"),
            ntpj=rows_for("ntpj"),
        )
        grans = {r["date_granularity"] for r in out.collect()}
        assert grans == {"day", "week", "month", "quarter", "semester", "year"}


# --------------------------------------------------------------------------- #
# Fix #2 — December-2025 weekly bucket / raw-date era classification
# --------------------------------------------------------------------------- #
class TestWeeklyEra:
    def test_dec_2025_weekly_bucket_is_kept_and_is_jan_era(self, spark):
        # Monday 2025-12-29 ISO weekly bucket: month-anchor (2025-12) would drop
        # it, but legacy KEEPS it with Jan era -> (adh + ntpj) / 2 only.
        wk = dt.date(2025, 12, 29)
        out = compute_xpeer_index(
            adherence=frame(spark, [_row("adherence", "a", 90, dref=wk, gran="week")]),
            ntpj=frame(spark, [_row("ntpj", "a", 80, dref=wk, gran="week")]),
            normalized_occupancy=frame(spark, [_row("normalized_occupancy", "a", 95, dref=wk, gran="week")]),
            quality=frame(spark, [_row("quality", "a", 88, dref=wk, gran="week")]),
        )
        r = only(out, "a")
        assert abs(r["denominator"] - 200.0) < 1e-9  # Jan era: adh + ntpj only
        assert abs(r["metric_value"] - 95.0) < 1e-9  # (90 + 100) / 2

    def test_weekly_era_classified_by_raw_monday_feb(self, spark):
        # A Monday inside Feb (e.g. 2026-02-02) -> Feb era: + quality, not NO.
        wk = dt.date(2026, 2, 2)
        out = compute_xpeer_index(
            adherence=frame(spark, [_row("adherence", "a", 90, dref=wk, gran="week")]),
            ntpj=frame(spark, [_row("ntpj", "a", 80, dref=wk, gran="week")]),
            normalized_occupancy=frame(spark, [_row("normalized_occupancy", "a", 95, dref=wk, gran="week")]),
            quality=frame(spark, [_row("quality", "a", 88, dref=wk, gran="week")]),
        )
        r = only(out, "a")
        assert abs(r["denominator"] - 300.0) < 1e-9  # adh + ntpj + quality

    def test_weekly_era_classified_by_raw_monday_mar(self, spark):
        # A Monday on/after 2026-03-01 -> Mar+ era: + NO + quality.
        wk = dt.date(2026, 3, 2)
        out = compute_xpeer_index(
            adherence=frame(spark, [_row("adherence", "a", 90, dref=wk, gran="week")]),
            ntpj=frame(spark, [_row("ntpj", "a", 80, dref=wk, gran="week")]),
            normalized_occupancy=frame(spark, [_row("normalized_occupancy", "a", 95, dref=wk, gran="week")]),
            quality=frame(spark, [_row("quality", "a", 88, dref=wk, gran="week")]),
        )
        r = only(out, "a")
        assert abs(r["denominator"] - 400.0) < 1e-9  # adh + ntpj + nocc + quality

    def test_month_grain_jan_unaffected_by_week_classification(self, spark):
        # The Dec-2025-floor / raw-Monday rule is week-only; a 2026-01 month
        # bucket still truncs to its month and gets the Jan era (adh + ntpj).
        out = compute_xpeer_index(
            adherence=frame(spark, [_row("adherence", "a", 90, dref=JAN, gran="month")]),
            ntpj=frame(spark, [_row("ntpj", "a", 80, dref=JAN, gran="month")]),
            normalized_occupancy=frame(spark, [_row("normalized_occupancy", "a", 95, dref=JAN, gran="month")]),
            quality=frame(spark, [_row("quality", "a", 88, dref=JAN, gran="month")]),
        )
        r = only(out, "a")
        assert abs(r["denominator"] - 200.0) < 1e-9
        assert abs(r["metric_value"] - 95.0) < 1e-9


# --------------------------------------------------------------------------- #
# Cross-cutting behaviour
# --------------------------------------------------------------------------- #
class TestGeneral:
    def test_adherence_is_the_driver(self, spark):
        # Agent with NTPJ but no Adherence row must not appear.
        out = compute_xpeer_index(
            adherence=frame(spark, [_row("adherence", "a", 90)]),
            ntpj=frame(spark, [_row("ntpj", "a", 90), _row("ntpj", "b", 90)]),
        )
        assert {r["agent"] for r in out.collect()} == {"a"}

    def test_pre_2026_month_filtered_out(self, spark):
        out = compute_xpeer_index(
            adherence=frame(spark, [_row("adherence", "a", 90, dref=dt.date(2025, 12, 1), gran="month")]),
            ntpj=frame(spark, [_row("ntpj", "a", 90, dref=dt.date(2025, 12, 1), gran="month")]),
        )
        assert len(out.take(1)) == 0

    def test_unknown_non_null_team_gets_no_core_fraud_composition(self, spark):
        # An unexpected NON-NULL team must NOT get NTPJ/NO/Quality; only the
        # always-on Adherence term -> denominator 100. (NULL is handled
        # separately — it is a known main-deck support squad, see below.)
        out = compute_xpeer_index(
            adherence=frame(spark, [_row("adherence", "a", 90, team="mystery")]),
            ntpj=frame(spark, [_row("ntpj", "a", 0, team="mystery")]),
            normalized_occupancy=frame(spark, [_row("normalized_occupancy", "a", 0, team="mystery")]),
            quality=frame(spark, [_row("quality", "a", 0, team="mystery")]),
        )
        r = only(out, "a")
        assert abs(r["denominator"] - 100.0) < 1e-9
        assert abs(r["metric_value"] - 90.0) < 1e-9

    def test_null_team_gets_core_fraud_composition(self, spark):
        # A NULL team is a main-deck support-squad agent (quality / planning /
        # enablement / idsec) that legacy keeps with team = NULL and scores with
        # the full Core/Fraud roster. Verified: every NULL-team adherence agent
        # is in the legacy CF deck (40/40), with den 200/300/400, never 100.
        out = compute_xpeer_index(
            adherence=frame(spark, [_row("adherence", "a", 90, team=None)]),
            ntpj=frame(spark, [_row("ntpj", "a", 100, team=None)]),
            normalized_occupancy=frame(spark, [_row("normalized_occupancy", "a", 90, team=None)]),
        )
        r = only(out, "a")
        assert abs(r["denominator"] - 300.0) < 1e-9  # adh + ntpj (fixed) + nocc (Mar)
        assert abs(r["metric_value"] - (90 + 100 + 90) / 3) < 1e-9

    def test_core_fraud_ntpj_is_a_fixed_divisor(self, spark):
        # Core/Fraud count NTPJ in the denominator even with no ntpj row (it
        # folds to 0): a Jan CF agent with only adherence -> den 200, not 100.
        out = compute_xpeer_index(
            adherence=frame(spark, [_row("adherence", "a", 90, dref=JAN, team="core")]),
        )
        r = only(out, "a")
        assert abs(r["denominator"] - 200.0) < 1e-9  # adh + ntpj (fixed, folds 0)
        assert abs(r["metric_value"] - 45.0) < 1e-9  # (90 + 0) / 2

    def test_fraud_team_gets_core_fraud_composition(self, spark):
        out = compute_xpeer_index(
            adherence=frame(spark, [_row("adherence", "a", 90, team="fraud")]),
            ntpj=frame(spark, [_row("ntpj", "a", 100, team="fraud")]),
            normalized_occupancy=frame(spark, [_row("normalized_occupancy", "a", 90, team="fraud")]),
            quality=frame(spark, [_row("quality", "a", 88, team="fraud")]),
        )
        r = only(out, "a")
        assert abs(r["denominator"] - 400.0) < 1e-9  # adh + ntpj + nocc + quality

    def test_empty_adherence_returns_empty(self, spark):
        out = compute_xpeer_index(adherence=frame(spark, []))
        assert len(out.take(1)) == 0
        assert out.columns == list(METRIC_COLUMNS)

    def test_output_contract(self, spark):
        out = compute_xpeer_index(
            adherence=frame(spark, [_row("adherence", "a", 90)]),
            ntpj=frame(spark, [_row("ntpj", "a", 90)]),
            normalized_occupancy=frame(spark, [_row("normalized_occupancy", "a", 90)]),
            quality=frame(spark, [_row("quality", "a", 90)]),
        )
        assert out.columns == list(METRIC_COLUMNS)
        schema_cols = [c for c, _ in IO_XPEER_INDEX_METRIC_SCHEMA]
        assert schema_cols == list(METRIC_COLUMNS)
        r = only(out, "a")
        assert r["team"] == "core"
        assert r["xforce"] == "x.force"
