"""Unit tests for ``metrics_data/content_csat.py``.

Small synthetic pandas frames, no warehouse, sub-second runs.

content_csat is a RAW dataset: one row per Content CSAT survey response fanned
out to each content agent serving the rated ``target_squad``. We verify the
per-response promoter count / csat_score, the target_squad normalization, the
target_squad-based fan-out join, and the output contract.
"""

from __future__ import annotations

import datetime as dt

import pandas as pd

from content_csat import (
    IO_CONTENT_CSAT_SCHEMA,
    NUMBER_OF_QUESTIONS,
    compute_content_csat,
)

_MEXICO_NUBANK_DOMAIN = ".".join(["nubank", "com", "mx"])


def _mock_email(local: str, domain: str = _MEXICO_NUBANK_DOMAIN) -> str:
    return f"{local}@{domain}"


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------


def make_csat_row(**overrides) -> dict:
    # Default: all 8 questions = 5 (all promoters).
    base = {
        "survey_timestamp": dt.datetime(2026, 4, 9, 15, 14, 11),
        "date_reference": dt.datetime(2026, 3, 9, 15, 14, 11),
        "requested_by": "julio.duran",
        "email_address": _mock_email("julio.duran"),
        "squad": "TXN",
        "mes": "Marzo 2026",
        "facilidad": 5,
        "comprension": 5,
        "comunicacion": 5,
        "calidad": 5,
        "tiempo": 5,
        "manejo_de_cambios": 5,
        "expectativas": 5,
        "aportacion_estrategica": 5,
        "nps": 9,
    }
    base.update(overrides)
    return base


def make_csat(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame([make_csat_row(**r) for r in rows])


def make_roster(rows: list[dict]) -> pd.DataFrame:
    defaults = {
        "agent": "alejandra.erazo",
        "actor_id": "actor-1",
        "xforce": "karina.gonzalez",
        "xplead": "alejandra.mota",
        "team": "content",
        "squad": "enablement",
        "squad_district": "content",
        "status": "active",
        "shift": None,
        "snapshot_date": dt.date(2026, 3, 1),
        "snapshot_month": dt.date(2026, 3, 1),
        "hire_start_date": None,
        "last_change_date": None,
        "target_squad": "txn",
    }
    return pd.DataFrame([{**defaults, **r} for r in rows])


# ---------------------------------------------------------------------------
# compute_content_csat — end-to-end
# ---------------------------------------------------------------------------


class TestComputeContentCsat:
    def test_all_promoters(self):
        out = compute_content_csat(make_roster([{}]), make_csat([{}]))
        assert len(out) == 1
        row = out.iloc[0]
        assert row["promoters"] == 8
        assert row["number_of_questions"] == NUMBER_OF_QUESTIONS == 8
        assert row["csat_score"] == 1.0
        assert row["agent"] == "alejandra.erazo"
        assert row["team"] == "content"
        assert row["squad"] == "enablement"
        assert row["district"] == "content"
        assert row["target_squad"] == "txn"
        assert row["date"] == dt.date(2026, 3, 9)

    def test_promoter_threshold(self):
        # scores >= 4 are promoters; 3 is not.
        out = compute_content_csat(
            make_roster([{}]),
            make_csat([{
                "facilidad": 4, "comprension": 3, "comunicacion": 5,
                "calidad": 1, "tiempo": 4, "manejo_de_cambios": 2,
                "expectativas": 4, "aportacion_estrategica": 5,
            }]),
        )
        # promoters: 4,_,5,_,4,_,4,5 -> facilidad,comunicacion,tiempo,expectativas,aportacion = 5
        assert out.iloc[0]["promoters"] == 5
        assert abs(out.iloc[0]["csat_score"] - 5 / 8) < 1e-9

    def test_null_score_not_promoter(self):
        out = compute_content_csat(
            make_roster([{}]),
            make_csat([{"facilidad": None}]),
        )
        assert out.iloc[0]["promoters"] == 7

    def test_emi_label_normalizes(self):
        out = compute_content_csat(
            make_roster([{"target_squad": "emi_general"}]),
            make_csat([{"squad": "E.M.I."}]),
        )
        assert len(out) == 1
        assert out.iloc[0]["target_squad"] == "emi_general"

    def test_general_label_normalizes(self):
        out = compute_content_csat(
            make_roster([{"target_squad": "emi_general"}]),
            make_csat([{
                "squad": "GENERAL (CHANNEL SOLUTIONS, PLANNING, SERVICE EXCELLENCE, QA, OPS DEFENSE)"
            }]),
        )
        assert len(out) == 1
        assert out.iloc[0]["target_squad"] == "emi_general"

    def test_fan_out_to_all_serving_agents(self):
        # One response, two content agents serving TXN -> two rows.
        roster = make_roster([
            {"agent": "alejandra.erazo", "target_squad": "txn"},
            {"agent": "aura.olvera", "target_squad": "txn"},
        ])
        out = compute_content_csat(roster, make_csat([{"squad": "TXN"}]))
        assert len(out) == 2
        assert set(out["agent"]) == {"alejandra.erazo", "aura.olvera"}

    def test_no_match_for_other_target_squad(self):
        out = compute_content_csat(
            make_roster([{"target_squad": "idsec"}]),
            make_csat([{"squad": "TXN"}]),
        )
        assert out.empty

    def test_inactive_agent_dropped(self):
        out = compute_content_csat(
            make_roster([{"status": "inactive"}]),
            make_csat([{}]),
        )
        assert out.empty

    def test_null_target_squad_roster_dropped(self):
        # A non-content (BDX) roster row has NULL target_squad and must not match.
        out = compute_content_csat(
            make_roster([{"target_squad": None}]),
            make_csat([{}]),
        )
        assert out.empty

    def test_month_attribution_join(self):
        # A March-rated response only matches the March roster row.
        roster = make_roster([
            {"snapshot_month": dt.date(2026, 3, 1), "agent": "march.agent"},
            {"snapshot_month": dt.date(2026, 4, 1), "agent": "april.agent"},
        ])
        out = compute_content_csat(roster, make_csat([{}]))  # date_reference March
        assert set(out["agent"]) == {"march.agent"}

    def test_aggregation_deferred(self):
        # Two responses for the same agent stay as two rows (no per-agent rollup).
        out = compute_content_csat(
            make_roster([{}]),
            make_csat([
                {"requested_by": "a", "survey_timestamp": dt.datetime(2026, 4, 9, 1)},
                {"requested_by": "b", "survey_timestamp": dt.datetime(2026, 4, 9, 2)},
            ]),
        )
        assert len(out) == 2

    def test_output_schema_and_column_order(self):
        out = compute_content_csat(make_roster([{}]), make_csat([{}]))
        assert list(out.columns) == [c for c, _ in IO_CONTENT_CSAT_SCHEMA]

    def test_empty_input_yields_empty_frame_with_schema(self):
        out = compute_content_csat(make_roster([{}]), make_csat([])[0:0])
        assert out.empty
        assert list(out.columns) == [c for c, _ in IO_CONTENT_CSAT_SCHEMA]
