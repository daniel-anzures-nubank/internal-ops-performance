"""Unit tests for ``metrics_data/quality_evaluations.py``.

Small synthetic pandas frames, no warehouse, sub-second runs.

quality_evaluations is a RAW dataset: one row per individual QA evaluation
(no per-day aggregation). Two sources unioned: Playvox (all teams) and Sprinklr
SM (Social Media, >= 2026-05-01). We verify the source filters, the
per-evaluation shaping, the Sprinklr cutover + ``source`` tagging, and the
roster join that attaches the standardized dimensions (agent, xforce, xplead,
squad, district, shift).
"""

from __future__ import annotations

import datetime as dt

import pandas as pd
import pytest

from quality_evaluations import (
    IO_QUALITY_EVALUATIONS_SCHEMA,
    PLAYVOX_TEAM_NAME_EXCLUSIONS,
    QUALITY_OUT_OF_SCOPE_SQUADS,
    SOURCE_PLAYVOX,
    SOURCE_SPRINKLR_SM,
    SPRINKLR_SM_CUTOVER,
    _is_nubank_email,
    build_evaluations,
    compute_quality_evaluations,
    filter_playvox,
)

_NUBANK_DOMAIN = ".".join(["nu", "com", "mx"])
_EXTERNAL_DOMAIN = "example.com"


def _mock_email(local: str, domain: str = _NUBANK_DOMAIN) -> str:
    return f"{local}@{domain}"


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------


def make_playvox_row(**overrides) -> dict:
    base = {
        "evaluation_id": "p-1",
        "agent": "jane.doe",
        "agent_email": _mock_email("jane.doe"),
        "team_name": "CREDIT",
        "scorecard_id": "sc-abc",
        "qa_score": 0.85,
        "created_at": dt.datetime(2026, 4, 15, 10, 30),
        "updated_at": dt.datetime(2026, 4, 15, 11, 0),
    }
    base.update(overrides)
    return base


def make_playvox(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame([make_playvox_row(**r) for r in rows])


def make_sprinklr_row(**overrides) -> dict:
    # Shape mirrors extractors/sprinklr_sm_evaluations.sql output (the columns
    # build_evaluations consumes). Default date is on/after the SM cutover.
    base = {
        "evaluation_id": "sm-1",
        "agent": "jane.doe",
        "qa_score": 90.0,
        "team_name": "SM",
        "created_at": dt.datetime(2026, 5, 15, 9, 0),
    }
    base.update(overrides)
    return base


def make_sprinklr(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame([make_sprinklr_row(**r) for r in rows])


def make_roster(rows: list[dict]) -> pd.DataFrame:
    defaults = {
        "agent": "jane.doe",
        "actor_id": "actor-1",
        "xforce": "lead.one",
        "xplead": "boss.one",
        "team": "core",
        "squad": "core",
        "squad_district": "northeast",
        "status": "active",
        "shift": "morning",
        "snapshot_date": dt.date(2026, 4, 30),
        "snapshot_month": dt.date(2026, 4, 1),
        "hire_start_date": dt.date(2025, 1, 15),
        "last_change_date": dt.date(2025, 1, 15),
    }
    return pd.DataFrame([{**defaults, **r} for r in rows])


# ---------------------------------------------------------------------------
# _is_nubank_email
# ---------------------------------------------------------------------------


class TestIsNubankEmail:
    @pytest.mark.parametrize(
        "email",
        [
            _mock_email("jane.doe"),
            _mock_email("jose.luis"),
            _mock_email("maria.elena2"),
            _mock_email("j.d"),
        ],
    )
    def test_matches_canonical_nubank_emails(self, email):
        assert _is_nubank_email(email)

    @pytest.mark.parametrize(
        "email",
        [
            _mock_email("jane.doe", _EXTERNAL_DOMAIN),
            _mock_email("jane.doe", "consorcio.example.com"),
            _mock_email("jane"),
            _mock_email("jane.doe.smith"),
            _mock_email("jane.doe", "nu-mx.example.com"),
        ],
    )
    def test_rejects_non_nubank(self, email):
        assert not _is_nubank_email(email)

    def test_uppercase_tolerated(self):
        assert _is_nubank_email(_mock_email("JANE.DOE"))

    @pytest.mark.parametrize("bad", [None, float("nan"), 0, 12, [], {}])
    def test_null_and_non_string_safe(self, bad):
        assert not _is_nubank_email(bad)


# ---------------------------------------------------------------------------
# filter_playvox
# ---------------------------------------------------------------------------


class TestFilterPlayvox:
    def test_keeps_a_well_formed_row(self):
        out = filter_playvox(make_playvox([{}]))
        assert len(out) == 1

    @pytest.mark.parametrize("team", list(PLAYVOX_TEAM_NAME_EXCLUSIONS))
    def test_drops_excluded_teams(self, team):
        out = filter_playvox(make_playvox([{"team_name": team}]))
        assert out.empty

    def test_drops_non_nubank_email(self):
        out = filter_playvox(
            make_playvox([{"agent_email": _mock_email("jane.doe", _EXTERNAL_DOMAIN)}])
        )
        assert out.empty

    def test_does_not_apply_scorecard_blacklist(self):
        legacy_blacklist = [
            "68def79b3f83da8cc9cb5299",
            "6812b3e46abeabb0653d197e",
        ]
        out = filter_playvox(
            make_playvox(
                [
                    {"evaluation_id": f"e{i}", "scorecard_id": sc}
                    for i, sc in enumerate(legacy_blacklist)
                ]
            )
        )
        assert len(out) == len(legacy_blacklist)

    def test_empty_input_returns_empty(self):
        out = filter_playvox(make_playvox([])[0:0])
        assert out.empty


# ---------------------------------------------------------------------------
# build_evaluations
# ---------------------------------------------------------------------------


class TestBuildEvaluations:
    def test_keeps_all_playvox_rows(self):
        out = build_evaluations(
            make_playvox([{"evaluation_id": "p-1"}, {"evaluation_id": "p-2"}]),
        )
        assert sorted(out["evaluation_id"]) == ["p-1", "p-2"]

    def test_derives_date_from_created_at(self):
        out = build_evaluations(
            make_playvox([{"created_at": dt.datetime(2026, 4, 15, 23, 59)}]),
        )
        assert out.iloc[0]["date"] == dt.date(2026, 4, 15)

    def test_drops_empty_string_agent(self):
        out = build_evaluations(
            make_playvox([{"evaluation_id": "p-1", "agent": ""}]),
        )
        assert out.empty

    def test_empty_input_returns_empty_with_columns(self):
        out = build_evaluations(make_playvox([])[0:0])
        assert out.empty
        assert "date" in out.columns


# ---------------------------------------------------------------------------
# compute_quality_evaluations — end-to-end (one row per evaluation)
# ---------------------------------------------------------------------------


class TestComputeQualityEvaluations:
    def test_one_evaluation_one_row(self):
        out = compute_quality_evaluations(
            agent_info=make_roster([{}]),
            playvox=make_playvox([{"qa_score": 0.8}]),
        )
        assert len(out) == 1
        row = out.iloc[0]
        assert row["agent"] == "jane.doe"
        assert row["date"] == dt.date(2026, 4, 15)
        assert pytest.approx(float(row["qa_score"])) == 0.8
        assert row["evaluation_id"] == "p-1"
        assert row["squad"] == "core"
        assert row["district"] == "northeast"
        assert row["shift"] == "morning"

    def test_two_evaluations_same_day_two_rows(self):
        out = compute_quality_evaluations(
            agent_info=make_roster([{}]),
            playvox=make_playvox(
                [
                    {"evaluation_id": "p-1", "qa_score": 0.6},
                    {"evaluation_id": "p-2", "qa_score": 1.0},
                ]
            ),
        )
        assert len(out) == 2
        assert sorted(out["evaluation_id"]) == ["p-1", "p-2"]

    def test_inactive_agent_dropped(self):
        out = compute_quality_evaluations(
            agent_info=make_roster([{"status": "inactive"}]),
            playvox=make_playvox([{}]),        )
        assert out.empty

    def test_out_of_scope_squads_constant_is_empty(self):
        assert QUALITY_OUT_OF_SCOPE_SQUADS == ()

    @pytest.mark.parametrize("squad", ["social", "content"])
    def test_social_and_content_squads_kept(self, squad):
        out = compute_quality_evaluations(
            agent_info=make_roster([{"squad": squad}]),
            playvox=make_playvox([{}]),        )
        assert len(out) == 1
        assert out.iloc[0]["squad"] == squad

    def test_null_squad_agent_dropped(self):
        out = compute_quality_evaluations(
            agent_info=make_roster([{"squad": None}]),
            playvox=make_playvox([{}]),        )
        assert out.empty

    def test_team_name_blacklist_applied(self):
        out = compute_quality_evaluations(
            agent_info=make_roster([{}]),
            playvox=make_playvox([{"team_name": "REGULATORY SOLUTIONS"}]),        )
        assert out.empty

    def test_non_nubank_email_dropped(self):
        out = compute_quality_evaluations(
            agent_info=make_roster([{}]),
            playvox=make_playvox(
                [{"agent_email": _mock_email("jane.doe", _EXTERNAL_DOMAIN)}]
            ),        )
        assert out.empty

    def test_no_roster_match_dropped(self):
        out = compute_quality_evaluations(
            agent_info=make_roster([{"agent": "someone.else"}]),
            playvox=make_playvox([{}]),        )
        assert out.empty

    def test_uses_natural_snapshot_month(self):
        out = compute_quality_evaluations(
            agent_info=make_roster(
                [
                    {"snapshot_month": dt.date(2026, 3, 1), "squad": "core"},
                    {"snapshot_month": dt.date(2026, 4, 1), "squad": "credit"},
                ]
            ),
            playvox=make_playvox(
                [
                    {"evaluation_id": "p-mar", "created_at": dt.datetime(2026, 3, 15)},
                    {"evaluation_id": "p-apr", "created_at": dt.datetime(2026, 4, 15)},
                ]
            ),        )
        assert len(out) == 2
        squads_by_date = dict(zip(out["date"], out["squad"]))
        assert squads_by_date[dt.date(2026, 3, 15)] == "core"
        assert squads_by_date[dt.date(2026, 4, 15)] == "credit"

    def test_outage_dates_not_filtered(self):
        out = compute_quality_evaluations(
            agent_info=make_roster(
                [
                    {"snapshot_month": dt.date(2026, 3, 1)},
                    {"snapshot_month": dt.date(2026, 4, 1)},
                ]
            ),
            playvox=make_playvox(
                [
                    {"evaluation_id": "p-1", "created_at": dt.datetime(2026, 3, 27)},
                    {"evaluation_id": "p-2", "created_at": dt.datetime(2026, 4, 9)},
                ]
            ),        )
        assert sorted(out["date"]) == [dt.date(2026, 3, 27), dt.date(2026, 4, 9)]

    def test_output_schema_and_column_order(self):
        out = compute_quality_evaluations(
            agent_info=make_roster([{}]),
            playvox=make_playvox([{}]),        )
        assert list(out.columns) == [c for c, _ in IO_QUALITY_EVALUATIONS_SCHEMA]

    def test_empty_inputs_yield_empty_frame_with_schema(self):
        out = compute_quality_evaluations(
            agent_info=make_roster([{}]),
            playvox=make_playvox([])[0:0],        )
        assert out.empty
        assert list(out.columns) == [c for c, _ in IO_QUALITY_EVALUATIONS_SCHEMA]

    def test_playvox_rows_tagged_source_playvox(self):
        out = compute_quality_evaluations(
            agent_info=make_roster([{}]),
            playvox=make_playvox([{}]),
        )
        assert (out["source"] == SOURCE_PLAYVOX).all()

    def test_no_sprinklr_arg_is_playvox_only(self):
        # Backwards-compat: omitting sprinklr_sm yields Playvox-only output.
        out = compute_quality_evaluations(
            agent_info=make_roster([{}]),
            playvox=make_playvox([{}]),
        )
        assert len(out) == 1
        assert set(out["source"]) == {SOURCE_PLAYVOX}


# ---------------------------------------------------------------------------
# Sprinklr SM union — cutover + source provenance
# ---------------------------------------------------------------------------


class TestSprinklrSmUnion:
    def test_union_adds_rows_tagged_source(self):
        out = compute_quality_evaluations(
            agent_info=make_roster([{"snapshot_month": dt.date(2026, 5, 1)}]),
            playvox=make_playvox(
                [{"evaluation_id": "p-1", "created_at": dt.datetime(2026, 5, 10)}]
            ),
            sprinklr_sm=make_sprinklr(
                [{"evaluation_id": "sm-1", "created_at": dt.datetime(2026, 5, 15)}]
            ),
        )
        assert len(out) == 2
        by_id = dict(zip(out["evaluation_id"], out["source"]))
        assert by_id["p-1"] == SOURCE_PLAYVOX
        assert by_id["sm-1"] == SOURCE_SPRINKLR_SM

    def test_sprinklr_qa_score_preserved(self):
        out = compute_quality_evaluations(
            agent_info=make_roster([{"snapshot_month": dt.date(2026, 5, 1)}]),
            playvox=make_playvox([])[0:0],
            sprinklr_sm=make_sprinklr([{"qa_score": 87.5}]),
        )
        assert len(out) == 1
        assert pytest.approx(float(out.iloc[0]["qa_score"])) == 87.5
        assert out.iloc[0]["source"] == SOURCE_SPRINKLR_SM

    def test_before_cutover_dropped(self):
        # An April Sprinklr row is dropped even though the roster has April.
        out = compute_quality_evaluations(
            agent_info=make_roster(
                [
                    {"snapshot_month": dt.date(2026, 4, 1)},
                    {"snapshot_month": dt.date(2026, 5, 1)},
                ]
            ),
            playvox=make_playvox([])[0:0],
            sprinklr_sm=make_sprinklr(
                [
                    {"evaluation_id": "sm-apr", "created_at": dt.datetime(2026, 4, 20)},
                    {"evaluation_id": "sm-may", "created_at": dt.datetime(2026, 5, 2)},
                ]
            ),
        )
        assert list(out["evaluation_id"]) == ["sm-may"]

    def test_cutover_boundary_is_inclusive(self):
        out = compute_quality_evaluations(
            agent_info=make_roster([{"snapshot_month": dt.date(2026, 5, 1)}]),
            playvox=make_playvox([])[0:0],
            sprinklr_sm=make_sprinklr(
                [{"evaluation_id": "sm-edge", "created_at": dt.datetime(2026, 5, 1)}]
            ),
        )
        assert list(out["evaluation_id"]) == ["sm-edge"]
        assert SPRINKLR_SM_CUTOVER == pd.Timestamp("2026-05-01")

    def test_empty_sprinklr_is_noop(self):
        out = compute_quality_evaluations(
            agent_info=make_roster([{}]),
            playvox=make_playvox([{}]),
            sprinklr_sm=make_sprinklr([])[0:0],
        )
        assert len(out) == 1
        assert set(out["source"]) == {SOURCE_PLAYVOX}

    def test_sprinklr_unmatched_roster_dropped(self):
        # Sprinklr agent not in the (active) roster → inner join drops it.
        out = compute_quality_evaluations(
            agent_info=make_roster(
                [{"agent": "someone.else", "snapshot_month": dt.date(2026, 5, 1)}]
            ),
            playvox=make_playvox([])[0:0],
            sprinklr_sm=make_sprinklr([{"agent": "jane.doe"}]),
        )
        assert out.empty
