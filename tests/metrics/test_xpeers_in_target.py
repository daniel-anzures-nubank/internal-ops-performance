"""Unit tests for ``metrics/xpeers_in_target.py``.

Small synthetic agent-level metric frames (no warehouse). We verify the
per-component target thresholds, the targets-achieved / total-targets ratio, the
team-specific component sets (Core/Fraud NTPJ vs SM tNPS+WoWs), the Feb/March
era cutovers, Content exclusion, and the output contract.
"""

from __future__ import annotations

import datetime as dt

import pandas as pd

from metric_utils import METRIC_COLUMNS
from xpeers_in_target import (
    IO_XPEERS_IN_TARGET_METRIC_SCHEMA,
    METRIC_NAME,
    XPLEAD_METRIC_NAME,
    compute_xpeers_in_target,
    compute_xpeers_in_target_xplead,
)

MAR = dt.date(2026, 5, 1)
FEB = dt.date(2026, 2, 1)
JAN = dt.date(2026, 1, 1)


def m(metric, agent, mv, *, xforce="xf", xplead="xp", team="core", dref=MAR, gran="month"):
    return {
        "agent": agent,
        "xforce": xforce,
        "xplead": xplead,
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


def frame(rows):
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=list(METRIC_COLUMNS))


def one(out):
    assert len(out) == 1, f"expected 1 row, got {len(out)}"
    return out.iloc[0]


# --------------------------------------------------------------------------- #
class TestCoreFraud:
    def test_full_targets_ratio(self):
        # 2 agents. a: adh 96(✓) ntpj 90(✓) nocc 110(✓) qa 96(✓) -> 4/4
        #           b: adh 90(✗) ntpj 130(✗) nocc 90(✗) qa 80(✗) -> 0/4
        # total targets = 8, achieved = 4 -> 50%
        out = compute_xpeers_in_target(
            adherence=frame([m("adherence", "a", 96), m("adherence", "b", 90)]),
            ntpj=frame([m("ntpj", "a", 90), m("ntpj", "b", 130)]),
            normalized_occupancy=frame([m("normalized_occupancy", "a", 110),
                                        m("normalized_occupancy", "b", 90)]),
            quality=frame([m("quality", "a", 96), m("quality", "b", 80)]),
        )
        r = one(out)
        assert r["metric"] == METRIC_NAME
        assert abs(r["numerator"] - 4.0) < 1e-9
        assert abs(r["denominator"] - 8.0) < 1e-9
        assert abs(r["metric_value"] - 50.0) < 1e-9

    def test_ntpj_threshold_is_le_100(self):
        # ntpj 100 passes (<=100); 100.1 fails.
        out = compute_xpeers_in_target(
            adherence=frame([m("adherence", "a", 50), m("adherence", "b", 50)]),
            ntpj=frame([m("ntpj", "a", 100), m("ntpj", "b", 100.1)]),
        )
        r = one(out)
        # adherence: 0/2 ; ntpj: 1/2 -> 1/4
        assert abs(r["numerator"] - 1.0) < 1e-9
        assert abs(r["denominator"] - 4.0) < 1e-9

    def test_null_metric_value_counts_in_denominator_only(self):
        out = compute_xpeers_in_target(
            adherence=frame([m("adherence", "a", 96),
                             {**m("adherence", "b", 0), "metric_value": None}]),
        )
        r = one(out)
        assert abs(r["denominator"] - 2.0) < 1e-9  # both agents counted
        assert abs(r["numerator"] - 1.0) < 1e-9    # only 'a' achieved


class TestEra:
    def test_january_excludes_quality_and_nocc(self):
        out = compute_xpeers_in_target(
            adherence=frame([m("adherence", "a", 96, dref=JAN)]),
            ntpj=frame([m("ntpj", "a", 90, dref=JAN)]),
            normalized_occupancy=frame([m("normalized_occupancy", "a", 110, dref=JAN)]),
            quality=frame([m("quality", "a", 96, dref=JAN)]),
        )
        r = one(out)
        assert abs(r["denominator"] - 2.0) < 1e-9  # adh + ntpj only

    def test_february_adds_quality_not_nocc(self):
        out = compute_xpeers_in_target(
            adherence=frame([m("adherence", "a", 96, dref=FEB)]),
            ntpj=frame([m("ntpj", "a", 90, dref=FEB)]),
            normalized_occupancy=frame([m("normalized_occupancy", "a", 110, dref=FEB)]),
            quality=frame([m("quality", "a", 96, dref=FEB)]),
        )
        r = one(out)
        assert abs(r["denominator"] - 3.0) < 1e-9  # adh + ntpj + qa

    def test_march_adds_nocc(self):
        out = compute_xpeers_in_target(
            adherence=frame([m("adherence", "a", 96)]),
            ntpj=frame([m("ntpj", "a", 90)]),
            normalized_occupancy=frame([m("normalized_occupancy", "a", 110)]),
            quality=frame([m("quality", "a", 96)]),
        )
        r = one(out)
        assert abs(r["denominator"] - 4.0) < 1e-9


class TestSocialMedia:
    def test_sm_uses_tnps_and_wows_not_ntpj(self):
        # SM March: adh + tnps + wows + qa + nocc = 5 targets.
        out = compute_xpeers_in_target(
            adherence=frame([m("adherence", "a", 96, team="social media")]),
            ntpj=frame([m("ntpj", "a", 90, team="social media")]),  # ignored for SM
            normalized_occupancy=frame([m("normalized_occupancy", "a", 110, team="social media")]),
            quality=frame([m("quality", "a", 96, team="social media")]),
            tnps=frame([m("tnps", "a", 90, team="social media")]),
            wows=frame([m("wows", "a", 6, team="social media")]),
        )
        r = one(out)
        assert abs(r["denominator"] - 5.0) < 1e-9
        assert abs(r["numerator"] - 5.0) < 1e-9  # all pass (tnps>=88, wows>=5)

    def test_tnps_and_wows_thresholds(self):
        # tnps 88 passes, 87 fails; wows 5 passes, 4 fails. Jan (adh+tnps+wows).
        out = compute_xpeers_in_target(
            adherence=frame([m("adherence", "a", 50, dref=JAN, team="social media"),
                             m("adherence", "b", 50, dref=JAN, team="social media")]),
            tnps=frame([m("tnps", "a", 88, dref=JAN, team="social media"),
                        m("tnps", "b", 87, dref=JAN, team="social media")]),
            wows=frame([m("wows", "a", 5, dref=JAN, team="social media"),
                        m("wows", "b", 4, dref=JAN, team="social media")]),
        )
        r = one(out)
        # adh 0/2, tnps 1/2, wows 1/2 -> 2 / 6
        assert abs(r["numerator"] - 2.0) < 1e-9
        assert abs(r["denominator"] - 6.0) < 1e-9


class TestGeneral:
    def test_content_excluded(self):
        out = compute_xpeers_in_target(
            adherence=frame([m("adherence", "a", 96, team="content"),
                             m("adherence", "b", 96, team="core", xforce="cf")]),
            ntpj=frame([m("ntpj", "b", 90, team="core", xforce="cf")]),
        )
        assert set(out["team"]) == {"core"}

    def test_driver_is_adherence(self):
        # XForce present only in ntpj (no adherence) must not appear.
        out = compute_xpeers_in_target(
            adherence=frame([m("adherence", "a", 96, xforce="A")]),
            ntpj=frame([m("ntpj", "a", 90, xforce="A"),
                        m("ntpj", "z", 90, xforce="B")]),
        )
        assert set(out["xforce"]) == {"A"}

    def test_pre_2026_dropped(self):
        out = compute_xpeers_in_target(
            adherence=frame([m("adherence", "a", 96, dref=dt.date(2025, 12, 1))]),
        )
        assert out.empty

    def test_output_contract(self):
        out = compute_xpeers_in_target(
            adherence=frame([m("adherence", "a", 96)]),
            ntpj=frame([m("ntpj", "a", 90)]),
        )
        assert list(out.columns) == list(METRIC_COLUMNS)
        assert [c for c, _ in IO_XPEERS_IN_TARGET_METRIC_SCHEMA] == list(METRIC_COLUMNS)
        r = one(out)
        assert r["agent"] is None and r["squad"] is None and r["shift"] is None
        assert r["xforce"] == "xf" and r["team"] == "core"

    def test_empty_returns_empty(self):
        out = compute_xpeers_in_target(adherence=frame([]))
        assert out.empty
        assert list(out.columns) == list(METRIC_COLUMNS)


class TestXPLead:
    def test_rolls_agents_across_xforces_into_one_xplead(self):
        # Two agents under the same XPLead but different XForces.
        # a: adh 96(✓) ntpj 90(✓) -> 2/2 ; b: adh 90(✗) ntpj 130(✗) -> 0/2
        # XPLead total = 4 targets, achieved = 2 -> 50%
        out = compute_xpeers_in_target_xplead(
            adherence=frame([m("adherence", "a", 96, xforce="xf1"),
                             m("adherence", "b", 90, xforce="xf2")]),
            ntpj=frame([m("ntpj", "a", 90, xforce="xf1"),
                        m("ntpj", "b", 130, xforce="xf2")]),
        )
        r = one(out)
        assert r["metric"] == XPLEAD_METRIC_NAME
        assert r["xforce"] is None
        assert r["xplead"] == "xp"
        assert abs(r["numerator"] - 2.0) < 1e-9
        assert abs(r["denominator"] - 4.0) < 1e-9
        assert abs(r["metric_value"] - 50.0) < 1e-9

    def test_separate_xpleads_stay_separate(self):
        out = compute_xpeers_in_target_xplead(
            adherence=frame([m("adherence", "a", 96, xplead="p1"),
                             m("adherence", "b", 96, xplead="p2")]),
            ntpj=frame([m("ntpj", "a", 90, xplead="p1"),
                        m("ntpj", "b", 90, xplead="p2")]),
        )
        assert set(out["xplead"]) == {"p1", "p2"}
        assert set(out["metric"]) == {XPLEAD_METRIC_NAME}

    def test_content_excluded(self):
        out = compute_xpeers_in_target_xplead(
            adherence=frame([m("adherence", "a", 96, team="content", xplead="pc"),
                             m("adherence", "b", 96, team="core", xplead="pk")]),
            ntpj=frame([m("ntpj", "b", 90, team="core", xplead="pk")]),
        )
        assert set(out["team"]) == {"core"}

    def test_output_contract(self):
        out = compute_xpeers_in_target_xplead(
            adherence=frame([m("adherence", "a", 96)]),
            ntpj=frame([m("ntpj", "a", 90)]),
        )
        assert list(out.columns) == list(METRIC_COLUMNS)
        r = one(out)
        assert r["agent"] is None and r["xforce"] is None
        assert r["squad"] is None and r["shift"] is None

    def test_empty_returns_empty(self):
        out = compute_xpeers_in_target_xplead(adherence=frame([]))
        assert out.empty
        assert list(out.columns) == list(METRIC_COLUMNS)
