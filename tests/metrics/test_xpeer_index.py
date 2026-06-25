"""Unit tests for ``metrics/xpeer_index.py``.

Small synthetic ``io_*_metric`` frames (no warehouse). We verify the component
transforms (NTPJ fold, NO truncation, WoWs fold), the team- and era-dependent
composition (Core/Fraud vs Content vs Social Media; Jan/Feb/March cutovers), the
end-of-period era anchoring for multi-month buckets, the adherence-as-driver
rule, the pre-2026 floor, and the output contract.
"""

from __future__ import annotations

import datetime as dt

import pandas as pd

from metric_utils import METRIC_COLUMNS
from xpeer_index import IO_XPEER_INDEX_METRIC_SCHEMA, METRIC_NAME, compute_xpeer_index

MAR = dt.date(2026, 5, 1)  # March-or-later era (full composition)
FEB = dt.date(2026, 2, 1)
JAN = dt.date(2026, 1, 1)


def row(metric, agent, mv, dref=MAR, gran="month", team="core", **dims):
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


def frame(rows):
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=list(METRIC_COLUMNS))


def only(out, agent):
    sub = out[out["agent"] == agent]
    assert len(sub) == 1, f"expected 1 row for {agent}, got {len(sub)}"
    return sub.iloc[0]


# --------------------------------------------------------------------------- #
# Core / Fraud
# --------------------------------------------------------------------------- #
class TestCoreFraud:
    def test_full_mar_composition(self):
        # adh 90, ntpj 80 -> fold 100, nocc 95 -> 95, quality 88. mean = 93.25
        out = compute_xpeer_index(
            adherence=frame([row("adherence", "a", 90, team="core")]),
            ntpj=frame([row("ntpj", "a", 80, team="core")]),
            normalized_occupancy=frame([row("normalized_occupancy", "a", 95)]),
            quality=frame([row("quality", "a", 88)]),
        )
        r = only(out, "a")
        assert r["metric"] == METRIC_NAME
        assert abs(r["numerator"] - (90 + 100 + 95 + 88)) < 1e-9
        assert abs(r["denominator"] - 400.0) < 1e-9
        assert abs(r["metric_value"] - 93.25) < 1e-9

    def test_ntpj_fold_above_100(self):
        # ntpj 130 -> 200 - 130 = 70
        out = compute_xpeer_index(
            adherence=frame([row("adherence", "a", 100)]),
            ntpj=frame([row("ntpj", "a", 130)]),
            normalized_occupancy=frame([row("normalized_occupancy", "a", 100)]),
            quality=frame([row("quality", "a", 100)]),
        )
        r = only(out, "a")
        assert abs(r["numerator"] - (100 + 70 + 100 + 100)) < 1e-9

    def test_ntpj_above_200_is_zero(self):
        out = compute_xpeer_index(
            adherence=frame([row("adherence", "a", 100)]),
            ntpj=frame([row("ntpj", "a", 250)]),
            normalized_occupancy=frame([row("normalized_occupancy", "a", 100)]),
            quality=frame([row("quality", "a", 100)]),
        )
        assert abs(only(out, "a")["numerator"] - (100 + 0 + 100 + 100)) < 1e-9

    def test_nocc_truncated(self):
        # nocc 120 -> 100
        out = compute_xpeer_index(
            adherence=frame([row("adherence", "a", 100)]),
            ntpj=frame([row("ntpj", "a", 100)]),
            normalized_occupancy=frame([row("normalized_occupancy", "a", 120)]),
            quality=frame([row("quality", "a", 100)]),
        )
        assert abs(only(out, "a")["numerator"] - 400.0) < 1e-9

    def test_january_only_adherence_ntpj(self):
        # Jan: quality & NO excluded even when present. (adh + ntpj) / 2
        out = compute_xpeer_index(
            adherence=frame([row("adherence", "a", 90, dref=JAN)]),
            ntpj=frame([row("ntpj", "a", 80, dref=JAN)]),
            normalized_occupancy=frame([row("normalized_occupancy", "a", 95, dref=JAN)]),
            quality=frame([row("quality", "a", 88, dref=JAN)]),
        )
        r = only(out, "a")
        assert abs(r["denominator"] - 200.0) < 1e-9
        assert abs(r["metric_value"] - 95.0) < 1e-9  # (90 + 100) / 2

    def test_february_adds_quality_not_nocc(self):
        out = compute_xpeer_index(
            adherence=frame([row("adherence", "a", 90, dref=FEB)]),
            ntpj=frame([row("ntpj", "a", 80, dref=FEB)]),
            normalized_occupancy=frame([row("normalized_occupancy", "a", 95, dref=FEB)]),
            quality=frame([row("quality", "a", 88, dref=FEB)]),
        )
        r = only(out, "a")
        assert abs(r["denominator"] - 300.0) < 1e-9  # adh + ntpj + quality
        assert abs(r["numerator"] - (90 + 100 + 88)) < 1e-9

    def test_march_missing_quality_drops_term(self):
        out = compute_xpeer_index(
            adherence=frame([row("adherence", "a", 90)]),
            ntpj=frame([row("ntpj", "a", 80)]),
            normalized_occupancy=frame([row("normalized_occupancy", "a", 95)]),
            quality=frame([]),  # no quality
        )
        r = only(out, "a")
        assert abs(r["denominator"] - 300.0) < 1e-9  # adh + ntpj + nocc
        assert abs(r["metric_value"] - (90 + 100 + 95) / 3) < 1e-9

    def test_nitza_april_may_no_component_carveout(self):
        out = compute_xpeer_index(
            adherence=frame([
                row("adherence", "nitza.zarza", 90, dref=dt.date(2026, 4, 1))
            ]),
            ntpj=frame([
                row("ntpj", "nitza.zarza", 100, dref=dt.date(2026, 4, 1))
            ]),
            normalized_occupancy=frame([
                row(
                    "normalized_occupancy",
                    "nitza.zarza",
                    10,
                    dref=dt.date(2026, 4, 1),
                )
            ]),
            quality=frame([
                row("quality", "nitza.zarza", 80, dref=dt.date(2026, 4, 1))
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
    def test_mar_full_uses_csat(self):
        # adh 90, ntpj 130 -> 70, nocc 120 -> 100, csat 80. mean = 85
        out = compute_xpeer_index(
            adherence=frame([row("adherence", "a", 90, team="content")]),
            ntpj=frame([row("ntpj", "a", 130, team="content")]),
            normalized_occupancy=frame([row("normalized_occupancy", "a", 120, team="content")]),
            content_csat=frame([row("content_csat", "a", 80, team="content")]),
        )
        r = only(out, "a")
        assert abs(r["denominator"] - 400.0) < 1e-9
        assert abs(r["metric_value"] - 85.0) < 1e-9

    def test_february_excludes_csat_and_nocc(self):
        # Content Jan & Feb = (adh + ntpj) / 2 only.
        out = compute_xpeer_index(
            adherence=frame([row("adherence", "a", 90, dref=FEB, team="content")]),
            ntpj=frame([row("ntpj", "a", 100, dref=FEB, team="content")]),
            normalized_occupancy=frame([row("normalized_occupancy", "a", 100, dref=FEB, team="content")]),
            content_csat=frame([row("content_csat", "a", 50, dref=FEB, team="content")]),
        )
        r = only(out, "a")
        assert abs(r["denominator"] - 200.0) < 1e-9
        assert abs(r["metric_value"] - 95.0) < 1e-9  # (90 + 100) / 2

    def test_content_ignores_playvox_quality(self):
        # A stray Playvox quality row must NOT feed Content (it uses CSAT).
        out = compute_xpeer_index(
            adherence=frame([row("adherence", "a", 90, team="content")]),
            ntpj=frame([row("ntpj", "a", 100, team="content")]),
            normalized_occupancy=frame([row("normalized_occupancy", "a", 100, team="content")]),
            quality=frame([row("quality", "a", 0, team="content")]),  # ignored
            content_csat=frame([row("content_csat", "a", 80, team="content")]),
        )
        r = only(out, "a")
        assert abs(r["metric_value"] - (90 + 100 + 100 + 80) / 4) < 1e-9


# --------------------------------------------------------------------------- #
# Social Media (WoWs + tNPS instead of NTPJ)
# --------------------------------------------------------------------------- #
class TestSocialMedia:
    def test_mar_full_five_terms(self):
        # adh 90, wows 5 -> 100, tnps 40, quality 80, nocc 120 -> 100. mean = 82
        out = compute_xpeer_index(
            adherence=frame([row("adherence", "a", 90, team="social media")]),
            normalized_occupancy=frame([row("normalized_occupancy", "a", 120, team="social media")]),
            quality=frame([row("quality", "a", 80, team="social media")]),
            tnps=frame([row("tnps", "a", 40, team="social media")]),
            wows=frame([row("wows", "a", 5, team="social media")]),
        )
        r = only(out, "a")
        assert abs(r["denominator"] - 500.0) < 1e-9
        assert abs(r["metric_value"] - 82.0) < 1e-9

    def test_wows_fold_below_target(self):
        # wows count 4 -> 4/5*100 = 80; Jan -> only adh + wows (no tnps here)
        out = compute_xpeer_index(
            adherence=frame([row("adherence", "a", 90, dref=JAN, team="social media")]),
            wows=frame([row("wows", "a", 4, dref=JAN, team="social media")]),
        )
        r = only(out, "a")
        assert abs(r["denominator"] - 200.0) < 1e-9
        assert abs(r["metric_value"] - (90 + 80) / 2) < 1e-9

    def test_negative_tnps_lowers_index(self):
        # Jan with tnps present: (adh + wows + tnps) / 3, tnps may be negative.
        out = compute_xpeer_index(
            adherence=frame([row("adherence", "a", 90, dref=JAN, team="social media")]),
            tnps=frame([row("tnps", "a", -30, dref=JAN, team="social media")]),
            wows=frame([row("wows", "a", 5, dref=JAN, team="social media")]),
        )
        r = only(out, "a")
        assert abs(r["denominator"] - 300.0) < 1e-9
        assert abs(r["metric_value"] - (90 + 100 - 30) / 3) < 1e-9

    def test_missing_tnps_drops_term(self):
        out = compute_xpeer_index(
            adherence=frame([row("adherence", "a", 90, dref=JAN, team="social media")]),
            wows=frame([row("wows", "a", 5, dref=JAN, team="social media")]),
        )
        assert abs(only(out, "a")["denominator"] - 200.0) < 1e-9


# --------------------------------------------------------------------------- #
# Cross-cutting behaviour
# --------------------------------------------------------------------------- #
class TestGeneral:
    def test_adherence_is_the_driver(self):
        # Agent with NTPJ but no Adherence row must not appear.
        out = compute_xpeer_index(
            adherence=frame([row("adherence", "a", 90)]),
            ntpj=frame([row("ntpj", "a", 90), row("ntpj", "b", 90)]),
        )
        assert set(out["agent"]) == {"a"}

    def test_quarter_bucket_anchors_on_end_month(self):
        # Q1 bucket (date_reference Jan 1) ends in March -> NO is included.
        out = compute_xpeer_index(
            adherence=frame([row("adherence", "a", 90, dref=JAN, gran="quarter")]),
            ntpj=frame([row("ntpj", "a", 100, dref=JAN, gran="quarter")]),
            normalized_occupancy=frame([row("normalized_occupancy", "a", 90, dref=JAN, gran="quarter")]),
        )
        r = only(out, "a")
        assert abs(r["denominator"] - 300.0) < 1e-9  # adh + ntpj + nocc
        assert abs(r["metric_value"] - (90 + 100 + 90) / 3) < 1e-9

    def test_pre_2026_filtered_out(self):
        out = compute_xpeer_index(
            adherence=frame([row("adherence", "a", 90, dref=dt.date(2025, 12, 1))]),
            ntpj=frame([row("ntpj", "a", 90, dref=dt.date(2025, 12, 1))]),
        )
        assert out.empty

    def test_unknown_team_uses_core_fraud_formula(self):
        out = compute_xpeer_index(
            adherence=frame([row("adherence", "a", 90, team="mystery")]),
            ntpj=frame([row("ntpj", "a", 100, team="mystery")]),
            normalized_occupancy=frame([row("normalized_occupancy", "a", 90, team="mystery")]),
        )
        r = only(out, "a")
        assert abs(r["denominator"] - 300.0) < 1e-9

    def test_empty_adherence_returns_empty(self):
        out = compute_xpeer_index(adherence=frame([]))
        assert out.empty
        assert list(out.columns) == list(METRIC_COLUMNS)

    def test_output_contract(self):
        out = compute_xpeer_index(
            adherence=frame([row("adherence", "a", 90)]),
            ntpj=frame([row("ntpj", "a", 90)]),
            normalized_occupancy=frame([row("normalized_occupancy", "a", 90)]),
            quality=frame([row("quality", "a", 90)]),
        )
        assert list(out.columns) == list(METRIC_COLUMNS)
        schema_cols = [c for c, _ in IO_XPEER_INDEX_METRIC_SCHEMA]
        assert schema_cols == list(METRIC_COLUMNS)
        r = only(out, "a")
        assert r["team"] == "core"
        assert r["xforce"] == "x.force"
