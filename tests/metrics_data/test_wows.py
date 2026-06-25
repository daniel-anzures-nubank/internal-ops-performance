"""Unit tests for ``metrics_data/wows.py``.

Small synthetic pandas frames, no warehouse, sub-second runs.

wows is a RAW dataset: one row per Social-Media WoW experience (no count, no
target). We verify the unresolved-agent filter, the roster join that attaches
the standardized dimensions (agent, xforce, xplead, team, squad, district,
shift), and the output contract.
"""

from __future__ import annotations

import datetime as dt

import pandas as pd

from wows import (
    IO_WOWS_SCHEMA,
    WOWS_OUT_OF_SCOPE_SQUADS,
    compute_wows,
)

_BRAZIL_NUBANK_DOMAIN = ".".join(["nubank", "com", "br"])


def _mock_email(local: str, domain: str = _BRAZIL_NUBANK_DOMAIN) -> str:
    return f"{local}@{domain}"


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------


def make_wow_row(**overrides) -> dict:
    base = {
        "agent": "jane.doe",
        "agent_email": _mock_email("jane.doe"),
        "case_id": "2070635",
        "date": dt.date(2026, 5, 15),
    }
    base.update(overrides)
    return base


def make_wows(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame([make_wow_row(**r) for r in rows])


def make_roster(rows: list[dict]) -> pd.DataFrame:
    defaults = {
        "agent": "jane.doe",
        "actor_id": "actor-1",
        "xforce": "lead.one",
        "xplead": "boss.one",
        "team": "social media",
        "squad": "social",
        "squad_district": "social",
        "status": "active",
        "shift": "morning",
        "snapshot_date": dt.date(2026, 5, 31),
        "snapshot_month": dt.date(2026, 5, 1),
        "hire_start_date": dt.date(2025, 1, 15),
        "last_change_date": dt.date(2025, 1, 15),
    }
    return pd.DataFrame([{**defaults, **r} for r in rows])


# ---------------------------------------------------------------------------
# compute_wows — end-to-end (one row per WoW experience)
# ---------------------------------------------------------------------------


class TestComputeWows:
    def test_one_wow_one_row(self):
        out = compute_wows(make_roster([{}]), make_wows([{"case_id": "777"}]))
        assert len(out) == 1
        row = out.iloc[0]
        assert row["agent"] == "jane.doe"
        assert row["date"] == dt.date(2026, 5, 15)
        assert row["case_id"] == "777"
        assert row["team"] == "social media"
        assert row["squad"] == "social"
        assert row["district"] == "social"
        assert row["shift"] == "morning"

    def test_two_wows_two_rows(self):
        out = compute_wows(
            make_roster([{}]),
            make_wows([{"case_id": "1001"}, {"case_id": "1002"}]),
        )
        assert len(out) == 2
        assert sorted(out["case_id"]) == ["1001", "1002"]

    def test_no_dedup_of_repeated_case_id(self):
        # Raw grain: repeated case_id rows are kept; metrics counts DISTINCT.
        out = compute_wows(
            make_roster([{}]),
            make_wows([{"case_id": "dup"}, {"case_id": "dup"}]),
        )
        assert len(out) == 2

    def test_unresolved_agent_dropped(self):
        out = compute_wows(make_roster([{}]), make_wows([{"agent": ""}]))
        assert out.empty

    def test_inactive_agent_dropped(self):
        out = compute_wows(make_roster([{"status": "inactive"}]), make_wows([{}]))
        assert out.empty

    def test_null_squad_agent_dropped(self):
        out = compute_wows(make_roster([{"squad": None}]), make_wows([{}]))
        assert out.empty

    def test_no_roster_match_dropped(self):
        out = compute_wows(
            make_roster([{"agent": "someone.else"}]), make_wows([{}])
        )
        assert out.empty

    def test_uses_natural_snapshot_month(self):
        out = compute_wows(
            make_roster(
                [
                    {"snapshot_month": dt.date(2026, 3, 1), "squad": "social"},
                    {"snapshot_month": dt.date(2026, 4, 1), "squad": "social_b"},
                ]
            ),
            make_wows(
                [
                    {"case_id": "mar", "date": dt.date(2026, 3, 10)},
                    {"case_id": "apr", "date": dt.date(2026, 4, 10)},
                ]
            ),
        )
        assert len(out) == 2
        squads_by_case = dict(zip(out["case_id"], out["squad"]))
        assert squads_by_case["mar"] == "social"
        assert squads_by_case["apr"] == "social_b"

    def test_outage_date_not_filtered(self):
        # The legacy 2026-03-27 outage exclusion is a metrics-layer concern.
        out = compute_wows(
            make_roster([{"snapshot_month": dt.date(2026, 3, 1)}]),
            make_wows([{"date": dt.date(2026, 3, 27)}]),
        )
        assert len(out) == 1

    def test_out_of_scope_squads_constant_is_empty(self):
        assert WOWS_OUT_OF_SCOPE_SQUADS == ()

    def test_output_schema_and_column_order(self):
        out = compute_wows(make_roster([{}]), make_wows([{}]))
        assert list(out.columns) == [c for c, _ in IO_WOWS_SCHEMA]

    def test_empty_input_yields_empty_frame_with_schema(self):
        out = compute_wows(make_roster([{}]), make_wows([])[0:0])
        assert out.empty
        assert list(out.columns) == [c for c, _ in IO_WOWS_SCHEMA]
