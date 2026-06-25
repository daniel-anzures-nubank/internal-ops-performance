"""Unit tests for ``metrics/nuvinhos_performance.py``.

Small synthetic Xpeer Index + tenure frames (no warehouse). We verify the
Nuvinho window classification, the Nuvinho-vs-old ratio, the two-level cohort
averaging, the three roll-ups (XForce / squad / district), NULL-tenure handling,
the no-old-agents edge, and the output contract.
"""

from __future__ import annotations

import datetime as dt

import pandas as pd

from metric_utils import METRIC_COLUMNS
from nuvinhos_performance import (
    IO_NUVINHOS_PERFORMANCE_METRIC_SCHEMA,
    METRIC_DISTRICT,
    METRIC_SQUAD,
    METRIC_XFORCE,
    compute_nuvinhos_performance,
)

MONTH = dt.date(2026, 5, 1)


def idx_row(agent, mv, *, xforce="xf", xplead="xp", team="core", squad="sq",
            district="di", dref=MONTH, gran="month"):
    return {
        "agent": agent,
        "xforce": xforce,
        "xplead": xplead,
        "team": team,
        "squad": squad,
        "district": district,
        "shift": "morning",
        "date_reference": dref,
        "date_granularity": gran,
        "metric": "xpeer_index",
        "numerator": float(mv),
        "denominator": 100.0,
        "metric_value": float(mv),
    }


def tenure_row(agent, last_change, *, snap=MONTH):
    return {
        "agent": agent,
        "snapshot_month": snap,
        "last_change_date": last_change,
    }


def idx_frame(rows):
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=list(METRIC_COLUMNS))


def ten_frame(rows):
    return pd.DataFrame(
        rows or [], columns=["agent", "snapshot_month", "last_change_date"]
    )


def xforce(out):
    return out[out["metric"] == METRIC_XFORCE]


# --------------------------------------------------------------------------- #
class TestClassification:
    def test_recent_hire_is_nuvinho(self):
        # last_change in May -> nuvinho in May. old hired in Jan.
        out = compute_nuvinhos_performance(
            idx_frame([
                idx_row("new", 80),
                idx_row("old", 100),
            ]),
            ten_frame([
                tenure_row("new", dt.date(2026, 5, 10)),
                tenure_row("old", dt.date(2026, 1, 5)),
            ]),
        )
        r = xforce(out).iloc[0]
        # Flat averages over agents: mean nuvinho index / mean old index.
        assert abs(r["numerator"] - 80.0) < 1e-9
        assert abs(r["denominator"] - 100.0) < 1e-9
        assert abs(r["metric_value"] - 80.0) < 1e-9  # 80 / 100 * 100

    def test_window_includes_change_month_plus_two(self):
        # change in March -> nuvinho for March, April, May; not February or June.
        def run(month):
            out = compute_nuvinhos_performance(
                idx_frame([
                    idx_row("n", 90, dref=month),
                    idx_row("o", 100, dref=month),
                ]),
                ten_frame([
                    tenure_row("n", dt.date(2026, 3, 15), snap=month),
                    tenure_row("o", dt.date(2025, 1, 1), snap=month),
                ]),
            )
            return xforce(out).iloc[0]

        # within window (May = March + 2) -> nuvinho present, ratio 90 / 100.
        r_may = run(dt.date(2026, 5, 1))
        assert abs(r_may["metric_value"] - 90.0) < 1e-9
        # June = March + 3 -> 'n' is now old; no nuvinhos -> numerator 0, mv 0.
        r_jun = run(dt.date(2026, 6, 1))
        assert abs(r_jun["numerator"] - 0.0) < 1e-9
        assert abs(r_jun["metric_value"] - 0.0) < 1e-9

    def test_null_tenure_is_old(self):
        out = compute_nuvinhos_performance(
            idx_frame([idx_row("a", 90), idx_row("b", 80)]),
            ten_frame([
                tenure_row("a", None),
                tenure_row("b", None),
            ]),
        )
        r = xforce(out).iloc[0]
        assert abs(r["numerator"] - 0.0) < 1e-9  # no nuvinhos
        assert abs(r["denominator"] - 85.0) < 1e-9  # avg old (90, 80)
        assert r["metric_value"] == 0.0


class TestRollups:
    def test_three_metrics_emitted(self):
        out = compute_nuvinhos_performance(
            idx_frame([idx_row("n", 80), idx_row("o", 100)]),
            ten_frame([
                tenure_row("n", dt.date(2026, 5, 1)),
                tenure_row("o", dt.date(2026, 1, 1)),
            ]),
        )
        assert set(out["metric"]) == {METRIC_XFORCE, METRIC_SQUAD, METRIC_DISTRICT}

    def test_squad_and_district_dims(self):
        out = compute_nuvinhos_performance(
            idx_frame([idx_row("n", 80), idx_row("o", 100)]),
            ten_frame([
                tenure_row("n", dt.date(2026, 5, 1)),
                tenure_row("o", dt.date(2026, 1, 1)),
            ]),
        )
        sq = out[out["metric"] == METRIC_SQUAD].iloc[0]
        assert sq["squad"] == "sq"
        assert sq["xforce"] is None and sq["district"] is None
        di = out[out["metric"] == METRIC_DISTRICT].iloc[0]
        assert di["district"] == "di"
        assert di["squad"] is None and di["xforce"] is None

    def test_flat_average_across_squads(self):
        # Two squads under one xforce; the XForce roll-up is a FLAT average over
        # all agents, regardless of how they split across squads:
        #   numerator   = mean(80, 60) = 70   (the two Nuvinhos)
        #   denominator = mean(100, 60) = 80  (the two old agents)
        #   metric_value = 70 / 80 * 100 = 87.5%
        rows = [
            idx_row("n1", 80, squad="A"), idx_row("o1", 100, squad="A"),
            idx_row("n2", 60, squad="B"), idx_row("o2", 60, squad="B"),
        ]
        ten = [
            tenure_row("n1", dt.date(2026, 5, 1)), tenure_row("o1", dt.date(2026, 1, 1)),
            tenure_row("n2", dt.date(2026, 5, 1)), tenure_row("o2", dt.date(2026, 1, 1)),
        ]
        out = compute_nuvinhos_performance(idx_frame(rows), ten_frame(ten))
        r = xforce(out).iloc[0]
        assert abs(r["numerator"] - 70.0) < 1e-9
        assert abs(r["denominator"] - 80.0) < 1e-9
        assert abs(r["metric_value"] - 87.5) < 1e-9

    def test_xforce_unaffected_by_cohort_counts(self):
        # 1 Nuvinho squad vs many old squads must NOT dilute (the legacy bug).
        rows = [idx_row("n", 90, squad="A")]
        ten = [tenure_row("n", dt.date(2026, 5, 1))]
        for i in range(5):
            rows.append(idx_row(f"o{i}", 90, squad=f"S{i}"))
            ten.append(tenure_row(f"o{i}", dt.date(2026, 1, 1)))
        out = compute_nuvinhos_performance(idx_frame(rows), ten_frame(ten))
        r = xforce(out).iloc[0]
        assert abs(r["numerator"] - 90.0) < 1e-9
        assert abs(r["denominator"] - 90.0) < 1e-9
        assert abs(r["metric_value"] - 100.0) < 1e-9


class TestGeneral:
    def test_all_teams_present(self):
        rows, ten = [], []
        for t, xf in [("core", "x1"), ("fraud", "x2"),
                      ("social media", "x3"), ("content", "x4")]:
            rows += [idx_row("n_" + t, 80, team=t, xforce=xf),
                     idx_row("o_" + t, 100, team=t, xforce=xf)]
            ten += [tenure_row("n_" + t, dt.date(2026, 5, 1)),
                    tenure_row("o_" + t, dt.date(2026, 1, 1))]
        out = compute_nuvinhos_performance(idx_frame(rows), ten_frame(ten))
        assert set(xforce(out)["team"]) == {"core", "fraud", "social media", "content"}

    def test_granularities_pass_through(self):
        rows, ten = [], []
        for g in ("day", "week", "month", "quarter", "semester", "year"):
            rows += [idx_row("n", 80, gran=g), idx_row("o", 100, gran=g)]
        ten = [tenure_row("n", dt.date(2026, 5, 1)), tenure_row("o", dt.date(2026, 1, 1))]
        out = compute_nuvinhos_performance(idx_frame(rows), ten_frame(ten))
        assert set(xforce(out)["date_granularity"]) == {
            "day", "week", "month", "quarter", "semester", "year"
        }

    def test_empty_returns_empty(self):
        out = compute_nuvinhos_performance(idx_frame([]), ten_frame([]))
        assert out.empty
        assert list(out.columns) == list(METRIC_COLUMNS)

    def test_output_contract(self):
        out = compute_nuvinhos_performance(
            idx_frame([idx_row("n", 80), idx_row("o", 100)]),
            ten_frame([
                tenure_row("n", dt.date(2026, 5, 1)),
                tenure_row("o", dt.date(2026, 1, 1)),
            ]),
        )
        assert list(out.columns) == list(METRIC_COLUMNS)
        assert [c for c, _ in IO_NUVINHOS_PERFORMANCE_METRIC_SCHEMA] == list(METRIC_COLUMNS)
        r = xforce(out).iloc[0]
        assert r["agent"] is None and r["shift"] is None
        assert r["squad"] is None and r["district"] is None  # XForce roll-up
