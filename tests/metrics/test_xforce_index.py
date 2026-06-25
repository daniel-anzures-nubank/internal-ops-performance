"""Unit tests for ``metrics/xforce_index.py``.

Synthetic component frames (no warehouse). We verify the component transforms
(shrinkage fold, improved fold), the XForce-weighted shrinkage roll-up, the
3- vs 4-component mean, the improved-benchmark presence rule (which encodes the
SM/Content exclusion and the Core/Fraud cutovers), and the output contract.
"""

from __future__ import annotations

import datetime as dt

import pandas as pd

from metric_utils import METRIC_COLUMNS
from xforce_index import (
    IO_XFORCE_INDEX_METRIC_SCHEMA,
    METRIC_NAME,
    compute_xforce_index,
)

D = dt.date(2026, 2, 1)


def shrink_row(agent, num, den, *, xforce="xf", xplead="xp", team="core",
               dref=D, gran="month"):
    return {
        "agent": agent, "xforce": xforce, "xplead": xplead, "team": team,
        "squad": "sq", "district": "di", "shift": "morning",
        "date_reference": dref, "date_granularity": gran, "metric": "shrinkage",
        "numerator": float(num), "denominator": float(den),
        "metric_value": (num / den * 100) if den else None,
    }


def xforce_row(metric, mv, *, xforce="xf", xplead="xp", team="core", dref=D,
               gran="month"):
    return {
        "agent": None, "xforce": xforce, "xplead": xplead, "team": team,
        "squad": None, "district": None, "shift": None,
        "date_reference": dref, "date_granularity": gran, "metric": metric,
        "numerator": 0.0, "denominator": 0.0, "metric_value": float(mv),
    }


def frame(rows):
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=list(METRIC_COLUMNS))


def one(out):
    assert len(out) == 1, f"expected 1 row, got {len(out)}"
    return out.iloc[0]


class TestComponents:
    def test_three_component_mean(self):
        # shrinkage 10% -> 100; xit 80; avg_idx 90. No improved -> mean of 3.
        out = compute_xforce_index(
            shrinkage=frame([shrink_row("a", 10, 100)]),  # 10% shrinkage
            xpeers_in_target=frame([xforce_row("xpeers_in_target", 80)]),
            average_xpeer_index=frame([xforce_row("average_xpeer_index", 90)]),
            improved_benchmarks=None,
        )
        r = one(out)
        assert r["metric"] == METRIC_NAME
        assert abs(r["denominator"] - 300.0) < 1e-9
        assert abs(r["numerator"] - (100 + 80 + 90)) < 1e-9
        assert abs(r["metric_value"] - 90.0) < 1e-9
        assert r["agent"] is None and r["squad"] is None

    def test_shrinkage_above_20_fold(self):
        # 30% shrinkage -> 120 - 30 = 90 component.
        out = compute_xforce_index(
            shrinkage=frame([shrink_row("a", 30, 100)]),
            xpeers_in_target=frame([xforce_row("xpeers_in_target", 90)]),
            average_xpeer_index=frame([xforce_row("average_xpeer_index", 90)]),
        )
        r = one(out)
        assert abs(r["numerator"] - (90 + 90 + 90)) < 1e-9
        assert abs(r["metric_value"] - 90.0) < 1e-9

    def test_shrinkage_weighted_across_agents(self):
        # Two agents: (5/50) and (15/50) -> 20/100 = 20% -> <=20 -> 100.
        out = compute_xforce_index(
            shrinkage=frame([shrink_row("a", 5, 50), shrink_row("b", 15, 50)]),
            xpeers_in_target=frame([xforce_row("xpeers_in_target", 0)]),
            average_xpeer_index=frame([xforce_row("average_xpeer_index", 0)]),
        )
        r = one(out)
        assert abs(r["numerator"] - 100.0) < 1e-9  # only shrinkage component

    def test_missing_components_coalesce_zero(self):
        out = compute_xforce_index(
            shrinkage=frame([shrink_row("a", 10, 100)]),  # -> 100
            xpeers_in_target=frame([]),
            average_xpeer_index=frame([]),
        )
        r = one(out)
        assert abs(r["numerator"] - 100.0) < 1e-9
        assert abs(r["metric_value"] - (100 / 3)) < 1e-9


class TestImprovedComponent:
    def test_four_components_when_improved_present(self):
        out = compute_xforce_index(
            shrinkage=frame([shrink_row("a", 10, 100)]),       # 100
            xpeers_in_target=frame([xforce_row("xpeers_in_target", 60)]),
            average_xpeer_index=frame([xforce_row("average_xpeer_index", 80)]),
            improved_benchmarks=frame([
                xforce_row("improved_benchmark_xforce", 60),   # >=60 -> 100
            ]),
        )
        r = one(out)
        assert abs(r["denominator"] - 400.0) < 1e-9
        assert abs(r["numerator"] - (100 + 60 + 80 + 100)) < 1e-9
        assert abs(r["metric_value"] - 85.0) < 1e-9

    def test_david_fernandez_april_carveout_removes_improved_from_index(self):
        out = compute_xforce_index(
            shrinkage=frame([
                shrink_row(
                    "a",
                    10,
                    100,
                    xplead="david.fernandez",
                    dref=dt.date(2026, 4, 1),
                )
            ]),
            xpeers_in_target=frame([
                xforce_row(
                    "xpeers_in_target",
                    60,
                    xplead="david.fernandez",
                    dref=dt.date(2026, 4, 1),
                )
            ]),
            average_xpeer_index=frame([
                xforce_row(
                    "average_xpeer_index",
                    80,
                    xplead="david.fernandez",
                    dref=dt.date(2026, 4, 1),
                )
            ]),
            improved_benchmarks=frame([
                xforce_row(
                    "improved_benchmark_xforce",
                    60,
                    xplead="david.fernandez",
                    dref=dt.date(2026, 4, 1),
                )
            ]),
        )

        r = one(out)
        assert abs(r["denominator"] - 300.0) < 1e-9
        assert abs(r["numerator"] - (100 + 60 + 80)) < 1e-9
        assert abs(r["metric_value"] - 80.0) < 1e-9

    def test_improved_below_60_fold(self):
        # improved 30 -> 30/0.6 = 50.
        out = compute_xforce_index(
            shrinkage=frame([shrink_row("a", 10, 100)]),
            xpeers_in_target=frame([xforce_row("xpeers_in_target", 0)]),
            average_xpeer_index=frame([xforce_row("average_xpeer_index", 0)]),
            improved_benchmarks=frame([
                xforce_row("improved_benchmark_xforce", 30),
            ]),
        )
        r = one(out)
        assert abs(r["denominator"] - 400.0) < 1e-9
        assert abs(r["numerator"] - (100 + 0 + 0 + 50)) < 1e-9

    def test_other_improved_metrics_ignored(self):
        # Only improved_benchmark_xforce counts; squad/district rows ignored.
        out = compute_xforce_index(
            shrinkage=frame([shrink_row("a", 10, 100)]),
            xpeers_in_target=frame([xforce_row("xpeers_in_target", 0)]),
            average_xpeer_index=frame([xforce_row("average_xpeer_index", 0)]),
            improved_benchmarks=frame([
                xforce_row("improved_benchmark_squad", 100),
                xforce_row("improved_benchmark_district", 100),
            ]),
        )
        r = one(out)
        assert abs(r["denominator"] - 300.0) < 1e-9  # no xforce improved row

    def test_sm_three_components_even_with_no_improved(self):
        out = compute_xforce_index(
            shrinkage=frame([shrink_row("a", 10, 100, team="social media",
                                        xforce="s")]),
            xpeers_in_target=frame([xforce_row("xpeers_in_target", 90,
                                               team="social media", xforce="s")]),
            average_xpeer_index=frame([xforce_row("average_xpeer_index", 90,
                                                  team="social media", xforce="s")]),
            improved_benchmarks=None,
        )
        r = one(out)
        assert r["team"] == "social media"
        assert abs(r["denominator"] - 300.0) < 1e-9


class TestContract:
    def test_output_contract(self):
        out = compute_xforce_index(
            shrinkage=frame([shrink_row("a", 10, 100)]),
            xpeers_in_target=frame([xforce_row("xpeers_in_target", 80)]),
            average_xpeer_index=frame([xforce_row("average_xpeer_index", 90)]),
        )
        assert list(out.columns) == list(METRIC_COLUMNS)
        assert [c for c, _ in IO_XFORCE_INDEX_METRIC_SCHEMA] == list(METRIC_COLUMNS)
        r = one(out)
        assert r["xforce"] == "xf" and r["xplead"] == "xp" and r["team"] == "core"

    def test_empty_shrinkage_returns_empty(self):
        out = compute_xforce_index(
            shrinkage=frame([]),
            xpeers_in_target=frame([xforce_row("xpeers_in_target", 80)]),
            average_xpeer_index=frame([xforce_row("average_xpeer_index", 90)]),
        )
        assert out.empty
        assert list(out.columns) == list(METRIC_COLUMNS)

    def test_driven_by_shrinkage(self):
        # XForce present only in xpeers (not shrinkage) does not appear.
        out = compute_xforce_index(
            shrinkage=frame([shrink_row("a", 10, 100, xforce="A")]),
            xpeers_in_target=frame([xforce_row("xpeers_in_target", 80, xforce="B")]),
            average_xpeer_index=frame([]),
        )
        assert set(out["xforce"]) == {"A"}
