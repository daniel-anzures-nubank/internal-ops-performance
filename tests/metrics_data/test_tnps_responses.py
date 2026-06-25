"""Unit tests for ``metrics_data/tnps_responses.py``.

Small synthetic pandas frames, no warehouse, sub-second runs.

tnps_responses is a RAW dataset: one row per Social-Media tNPS survey response
(no classification, no NPS aggregation). We verify the human-agent filter, the
roster join that attaches the standardized dimensions
(agent, xforce, xplead, team, squad, district, shift), and the output contract.
"""

from __future__ import annotations

import datetime as dt

import pandas as pd
import pytest

from tnps_responses import (
    IO_TNPS_RESPONSES_SCHEMA,
    TNPS_OUT_OF_SCOPE_SQUADS,
    compute_tnps_responses,
)

_MEXICO_NUBANK_DOMAIN = ".".join(["nubank", "com", "mx"])


def _mock_email(local: str, domain: str = _MEXICO_NUBANK_DOMAIN) -> str:
    return f"{local}@{domain}"


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------


def make_tnps_row(**overrides) -> dict:
    base = {
        "agent": "jane.doe",
        "agent_email_id": _mock_email("jane.doe"),
        "case_number": "1001",
        "date": dt.date(2026, 5, 15),
        "survey_response_date": dt.date(2026, 5, 15),
        "case_closure_time": dt.date(2026, 5, 15),
        "survey_score": 10,
    }
    base.update(overrides)
    return base


def make_tnps(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame([make_tnps_row(**r) for r in rows])


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
# compute_tnps_responses — end-to-end (one row per survey response)
# ---------------------------------------------------------------------------


class TestComputeTnpsResponses:
    def test_one_response_one_row(self):
        out = compute_tnps_responses(make_roster([{}]), make_tnps([{"survey_score": 9}]))
        assert len(out) == 1
        row = out.iloc[0]
        assert row["agent"] == "jane.doe"
        assert row["date"] == dt.date(2026, 5, 15)
        assert row["case_number"] == "1001"
        assert int(row["survey_score"]) == 9
        assert row["team"] == "social media"
        assert row["squad"] == "social"
        assert row["district"] == "social"
        assert row["shift"] == "morning"

    def test_two_responses_two_rows(self):
        out = compute_tnps_responses(
            make_roster([{}]),
            make_tnps(
                [
                    {"case_number": "1001", "survey_score": 10},
                    {"case_number": "1002", "survey_score": 4},
                ]
            ),
        )
        assert len(out) == 2
        assert sorted(out["case_number"]) == ["1001", "1002"]

    def test_no_classification_or_aggregation(self):
        # Raw scores are preserved verbatim (no promoter/detractor mapping).
        out = compute_tnps_responses(
            make_roster([{}]),
            make_tnps([{"case_number": "c", "survey_score": 7}]),
        )
        assert int(out.iloc[0]["survey_score"]) == 7

    def test_null_score_kept(self):
        out = compute_tnps_responses(
            make_roster([{}]),
            make_tnps([{"survey_score": None}]),
        )
        assert len(out) == 1
        assert pd.isna(out.iloc[0]["survey_score"])

    def test_unattributed_agent_dropped(self):
        out = compute_tnps_responses(
            make_roster([{}]),
            make_tnps([{"agent": ""}]),
        )
        assert out.empty

    def test_inactive_agent_dropped(self):
        out = compute_tnps_responses(
            make_roster([{"status": "inactive"}]),
            make_tnps([{}]),
        )
        assert out.empty

    def test_null_squad_agent_dropped(self):
        out = compute_tnps_responses(
            make_roster([{"squad": None}]),
            make_tnps([{}]),
        )
        assert out.empty

    def test_no_roster_match_dropped(self):
        out = compute_tnps_responses(
            make_roster([{"agent": "someone.else"}]),
            make_tnps([{}]),
        )
        assert out.empty

    def test_uses_natural_snapshot_month(self):
        out = compute_tnps_responses(
            make_roster(
                [
                    {"snapshot_month": dt.date(2026, 3, 1), "squad": "social"},
                    {"snapshot_month": dt.date(2026, 4, 1), "squad": "social_b"},
                ]
            ),
            make_tnps(
                [
                    {"case_number": "mar", "date": dt.date(2026, 3, 10)},
                    {"case_number": "apr", "date": dt.date(2026, 4, 10)},
                ]
            ),
        )
        assert len(out) == 2
        squads_by_case = dict(zip(out["case_number"], out["squad"]))
        assert squads_by_case["mar"] == "social"
        assert squads_by_case["apr"] == "social_b"

    def test_outage_date_not_filtered(self):
        # The legacy 2026-03-27 outage exclusion is a metrics-layer concern.
        out = compute_tnps_responses(
            make_roster([{"snapshot_month": dt.date(2026, 3, 1)}]),
            make_tnps([{"date": dt.date(2026, 3, 27)}]),
        )
        assert len(out) == 1

    def test_validity_window_not_applied(self):
        # survey_response_date far after closure is kept (deferred to metrics).
        out = compute_tnps_responses(
            make_roster([{}]),
            make_tnps(
                [
                    {
                        "date": dt.date(2026, 5, 15),
                        "survey_response_date": dt.date(2026, 5, 30),
                    }
                ]
            ),
        )
        assert len(out) == 1

    def test_out_of_scope_squads_constant_is_empty(self):
        assert TNPS_OUT_OF_SCOPE_SQUADS == ()

    def test_output_schema_and_column_order(self):
        out = compute_tnps_responses(make_roster([{}]), make_tnps([{}]))
        assert list(out.columns) == [c for c, _ in IO_TNPS_RESPONSES_SCHEMA]

    def test_empty_input_yields_empty_frame_with_schema(self):
        out = compute_tnps_responses(make_roster([{}]), make_tnps([])[0:0])
        assert out.empty
        assert list(out.columns) == [c for c, _ in IO_TNPS_RESPONSES_SCHEMA]
