"""Unit tests for ``metrics_data/shrinkage_slots.py``.

Small synthetic pandas frames, no warehouse, sub-second runs.

shrinkage_slots is a RAW per-slot dataset: one row per DIME slot for active
agents, flagged with ``shrinkage_flag`` / ``controllable_shrinkage_flag`` /
``uncontrollable_shrinkage_flag``. The DIME filter is minimal (non-null
``activity_type_required`` only); business exclusions move to the metrics
layer.
"""

from __future__ import annotations

import datetime as dt

import pandas as pd
import pytest

from shrinkage_slots import (
    IO_SHRINKAGE_SLOTS_SCHEMA,
    SHRINKAGE_FORMULA_CUTOVER,
    SHRINKAGE_MEETING_LEAVE_DIMENSIONED_ACTIVITIES,
    SHRINKAGE_OUT_OF_SCOPE_SQUADS,
    classify_slots,
    compute_shrinkage_slots,
    filter_dime,
)


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------

PRE_CUTOVER_DATE = dt.date(2026, 2, 15)
POST_CUTOVER_DATE = dt.date(2026, 4, 15)


def make_dime_row(**overrides) -> dict:
    base = {
        "agent": "jane.doe",
        "date": POST_CUTOVER_DATE,
        "squad": "core",
        "affiliation": "nubank",
        "activity_type_required": "chat",
        "shuffle_status_required": "available",
        "dimensioned_activity": "Chat",
        "local_timestamp_dime_slot_starts_at": dt.datetime(2026, 4, 15, 6, 0, 0),
        "slot_start_local_unix": 0,
        "slot_end_local_unix": 1800,
    }
    base.update(overrides)
    return base


def make_dime(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame([make_dime_row(**r) for r in rows])


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
# filter_dime — minimal (non-null activity_type_required only)
# ---------------------------------------------------------------------------


class TestFilterDime:
    def test_keeps_a_well_formed_row(self):
        assert len(filter_dime(make_dime([{}]))) == 1

    def test_drops_null_activity_type_required(self):
        assert filter_dime(make_dime([{"activity_type_required": None}])).empty

    @pytest.mark.parametrize("squad", ["planning", "quality", "wfm", "enablement"])
    def test_keeps_formerly_excluded_squads(self, squad):
        # Raw table: no squad business exclusions anymore.
        assert len(filter_dime(make_dime([{"squad": squad}]))) == 1

    @pytest.mark.parametrize(
        "activity", ["dime_invalid_notation", "time_off", "shrinkage"]
    )
    def test_keeps_all_non_null_activity_types(self, activity):
        assert len(filter_dime(make_dime([{"activity_type_required": activity}]))) == 1


# ---------------------------------------------------------------------------
# classify_slots — shrinkage rule + controllable/uncontrollable split
# ---------------------------------------------------------------------------


class TestClassifyShrinkageFlag:
    def test_pre_cutover_shrinkage_activity_is_shrinkage(self):
        out = classify_slots(
            make_dime([{"date": PRE_CUTOVER_DATE, "activity_type_required": "shrinkage"}])
        )
        assert int(out.iloc[0]["shrinkage_flag"]) == 1

    def test_pre_cutover_invalid_notation_meeting_not_shrinkage(self):
        out = classify_slots(
            make_dime(
                [
                    {
                        "date": PRE_CUTOVER_DATE,
                        "activity_type_required": "dime_invalid_notation",
                        "dimensioned_activity": "Mouring",
                    }
                ]
            )
        )
        assert int(out.iloc[0]["shrinkage_flag"]) == 0

    def test_post_cutover_shrinkage_activity_is_shrinkage(self):
        out = classify_slots(
            make_dime([{"date": POST_CUTOVER_DATE, "activity_type_required": "shrinkage"}])
        )
        assert int(out.iloc[0]["shrinkage_flag"]) == 1

    @pytest.mark.parametrize(
        "meeting_leave", list(SHRINKAGE_MEETING_LEAVE_DIMENSIONED_ACTIVITIES)
    )
    def test_post_cutover_invalid_notation_meeting_is_shrinkage(self, meeting_leave):
        out = classify_slots(
            make_dime(
                [
                    {
                        "date": POST_CUTOVER_DATE,
                        "activity_type_required": "dime_invalid_notation",
                        "dimensioned_activity": meeting_leave,
                    }
                ]
            )
        )
        assert int(out.iloc[0]["shrinkage_flag"]) == 1

    def test_post_cutover_invalid_notation_non_meeting_not_shrinkage(self):
        out = classify_slots(
            make_dime(
                [
                    {
                        "date": POST_CUTOVER_DATE,
                        "activity_type_required": "dime_invalid_notation",
                        "dimensioned_activity": "SomethingElse",
                    }
                ]
            )
        )
        assert int(out.iloc[0]["shrinkage_flag"]) == 0

    def test_normal_work_is_never_shrinkage(self):
        out = classify_slots(
            make_dime(
                [
                    {"date": PRE_CUTOVER_DATE, "activity_type_required": "chat"},
                    {"date": POST_CUTOVER_DATE, "activity_type_required": "email"},
                ]
            )
        )
        assert out["shrinkage_flag"].sum() == 0

    def test_cutover_inclusive_on_post_side(self):
        out = classify_slots(
            make_dime(
                [
                    {
                        "date": SHRINKAGE_FORMULA_CUTOVER,
                        "activity_type_required": "dime_invalid_notation",
                        "dimensioned_activity": "Mouring",
                    }
                ]
            )
        )
        assert int(out.iloc[0]["shrinkage_flag"]) == 1


class TestClassifyControlSplit:
    def test_licencia_is_uncontrollable(self):
        # Licencia is in the meeting/leave set, so an invalid-notation slot
        # with Licencia is shrinkage post-cutover, and uncontrollable.
        out = classify_slots(
            make_dime(
                [
                    {
                        "date": POST_CUTOVER_DATE,
                        "activity_type_required": "dime_invalid_notation",
                        "dimensioned_activity": "Licencia",
                    }
                ]
            )
        )
        row = out.iloc[0]
        assert int(row["shrinkage_flag"]) == 1
        assert int(row["uncontrollable_shrinkage_flag"]) == 1
        assert int(row["controllable_shrinkage_flag"]) == 0

    def test_lowercase_licencia_is_uncontrollable(self):
        out = classify_slots(
            make_dime(
                [{"activity_type_required": "shrinkage", "dimensioned_activity": "licencia"}]
            )
        )
        assert int(out.iloc[0]["uncontrollable_shrinkage_flag"]) == 1

    def test_skr_lcnc_is_uncontrollable(self):
        out = classify_slots(
            make_dime(
                [{"activity_type_required": "shrinkage", "dimensioned_activity": "SKR_LCNC"}]
            )
        )
        row = out.iloc[0]
        assert int(row["shrinkage_flag"]) == 1
        assert int(row["uncontrollable_shrinkage_flag"]) == 1
        assert int(row["controllable_shrinkage_flag"]) == 0

    def test_other_shrinkage_is_controllable(self):
        out = classify_slots(
            make_dime(
                [{"activity_type_required": "shrinkage", "dimensioned_activity": "Pausa"}]
            )
        )
        row = out.iloc[0]
        assert int(row["shrinkage_flag"]) == 1
        assert int(row["controllable_shrinkage_flag"]) == 1
        assert int(row["uncontrollable_shrinkage_flag"]) == 0

    def test_non_shrinkage_all_flags_zero(self):
        out = classify_slots(make_dime([{"activity_type_required": "chat"}]))
        row = out.iloc[0]
        assert int(row["shrinkage_flag"]) == 0
        assert int(row["controllable_shrinkage_flag"]) == 0
        assert int(row["uncontrollable_shrinkage_flag"]) == 0

    def test_control_split_sums_to_shrinkage_flag(self):
        out = classify_slots(
            make_dime(
                [
                    {"activity_type_required": "shrinkage", "dimensioned_activity": "Licencia"},
                    {"activity_type_required": "shrinkage", "dimensioned_activity": "Pausa"},
                    {"activity_type_required": "chat", "dimensioned_activity": "Chat"},
                ]
            )
        )
        combined = (
            out["controllable_shrinkage_flag"] + out["uncontrollable_shrinkage_flag"]
        )
        assert combined.tolist() == out["shrinkage_flag"].tolist()


# ---------------------------------------------------------------------------
# compute_shrinkage_slots — end-to-end (one row per slot)
# ---------------------------------------------------------------------------


class TestComputeShrinkageSlots:
    def test_basic_end_to_end_one_row_per_slot(self):
        roster = make_roster([{}])
        dime = make_dime(
            [
                {"activity_type_required": "shrinkage", "dimensioned_activity": "Pausa"},
                {
                    "activity_type_required": "chat",
                    "dimensioned_activity": "Chat",
                    "slot_start_local_unix": 1800,
                },
            ]
        )
        out = compute_shrinkage_slots(roster, dime)
        assert len(out) == 2
        assert int(out["shrinkage_flag"].sum()) == 1
        assert out.iloc[0]["district"] == "northeast"
        assert out.iloc[0]["shift"] == "morning"

    def test_output_column_order_matches_schema(self):
        out = compute_shrinkage_slots(make_roster([{}]), make_dime([{}]))
        assert list(out.columns) == [c for c, _ in IO_SHRINKAGE_SLOTS_SCHEMA]

    def test_drops_inactive_agent(self):
        out = compute_shrinkage_slots(
            make_roster([{"status": "inactive"}]),
            make_dime([{"activity_type_required": "shrinkage"}]),
        )
        assert out.empty

    def test_out_of_scope_squads_constant_is_empty(self):
        assert SHRINKAGE_OUT_OF_SCOPE_SQUADS == ()

    @pytest.mark.parametrize("squad", ["social", "content"])
    def test_social_and_content_squads_kept(self, squad):
        out = compute_shrinkage_slots(
            make_roster([{"squad": squad}]),
            make_dime([{"activity_type_required": "shrinkage"}]),
        )
        assert len(out) == 1
        assert out.iloc[0]["squad"] == squad

    def test_uncontrollable_flag_end_to_end(self):
        roster = make_roster([{}])
        dime = make_dime(
            [{"activity_type_required": "shrinkage", "dimensioned_activity": "SKR_LCNC"}]
        )
        out = compute_shrinkage_slots(roster, dime)
        assert int(out.iloc[0]["uncontrollable_shrinkage_flag"]) == 1
        assert int(out.iloc[0]["controllable_shrinkage_flag"]) == 0

    def test_handles_tz_aware_snapshot_month(self):
        roster = make_roster([{}])
        roster["snapshot_month"] = pd.to_datetime(
            roster["snapshot_month"]
        ).dt.tz_localize("UTC")
        dime = make_dime([{"activity_type_required": "shrinkage"}])
        out = compute_shrinkage_slots(roster, dime)
        assert len(out) == 1
        assert int(out.iloc[0]["shrinkage_flag"]) == 1

    def test_handles_empty_dime(self):
        roster = make_roster([{}])
        empty_dime = pd.DataFrame(columns=list(make_dime([{}]).columns))
        out = compute_shrinkage_slots(roster, empty_dime)
        assert out.empty


def test_schema_matches_output_columns():
    out = compute_shrinkage_slots(make_roster([{}]), make_dime([{}]))
    assert list(out.columns) == [c for c, _ in IO_SHRINKAGE_SLOTS_SCHEMA]


# ---------------------------------------------------------------------------
# Night-shift attribution (>= 2026-07-01)
# ---------------------------------------------------------------------------


class TestNightShiftAttribution:
    @staticmethod
    def _local_unix(ts: str) -> int:
        return int(pd.Timestamp(ts).value // 10**9)

    def _night_slot(self, day: dt.date, ts: str) -> pd.DataFrame:
        u = self._local_unix(ts)
        return make_dime(
            [
                {
                    "date": day,
                    "local_timestamp_dime_slot_starts_at": pd.Timestamp(ts),
                    "slot_start_local_unix": u,
                    "slot_end_local_unix": u + 1800,
                }
            ]
        )

    def test_night_tail_attributed_to_shift_start_day(self):
        roster = make_roster(
            [{"shift": "night", "snapshot_month": dt.date(2026, 7, 1),
              "snapshot_date": dt.date(2026, 7, 31)}]
        )
        dime = self._night_slot(dt.date(2026, 7, 6), "2026-07-06 03:00:00")
        out = compute_shrinkage_slots(roster, dime)
        assert len(out) == 1
        assert out.iloc[0]["date"] == dt.date(2026, 7, 5)
        assert out.iloc[0]["slot_time"] == "03:00:00"

    def test_morning_agent_not_re_attributed(self):
        roster = make_roster(
            [{"shift": "morning", "snapshot_month": dt.date(2026, 7, 1),
              "snapshot_date": dt.date(2026, 7, 31)}]
        )
        dime = self._night_slot(dt.date(2026, 7, 6), "2026-07-06 03:00:00")
        out = compute_shrinkage_slots(roster, dime)
        assert len(out) == 1
        assert out.iloc[0]["date"] == dt.date(2026, 7, 6)
