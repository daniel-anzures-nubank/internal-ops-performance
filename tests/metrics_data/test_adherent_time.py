"""Unit tests for ``metrics_data/adherent_time.py``.

These tests use small, hand-crafted pandas frames. They never touch
Databricks — every check exercises pure-pandas transformation logic, which
makes them fast (sub-second).

adherent_time is now a RAW dataset: ``filter_dime`` keeps every slot with a
non-null ``activity_type_required`` (business exclusions move to the metrics
layer), and the output is one row per DIME slot with ``adherent_minutes`` and
``required_minutes`` plus the standardized dimensions
(agent, xforce, xplead, squad, district, shift).
"""

from __future__ import annotations

import datetime as dt

import pandas as pd
import pytest

from adherent_time import (
    CORE_OUT_OF_SCOPE_SQUADS,
    MEXICO_UTC_OFFSET_SECONDS,
    SLOT_DURATION_SECONDS,
    compute_adherent_time,
    compute_slot_adherence,
    filter_dime,
    filter_productivity,
)


# ---------------------------------------------------------------------------
# Builders for synthetic input frames
# ---------------------------------------------------------------------------

D = dt.date(2026, 5, 18)

_DAY_SECONDS = 86400
SLOT_06_LOCAL = (
    int(dt.datetime(2026, 5, 18, 12, 0, 0).timestamp()) // _DAY_SECONDS
) * _DAY_SECONDS + 6 * 3600
# 06:00 local → 12:00 UTC. We use UTC unix for productivity rows.
SLOT_06_UTC = SLOT_06_LOCAL + MEXICO_UTC_OFFSET_SECONDS


def make_dime_row(**overrides) -> dict:
    """One synthetic DIME-slot row with sensible defaults (passes the filter)."""
    base = {
        "agent": "jane.doe",
        "date": D,
        "squad": "core",
        "affiliation": "nubank",
        "activity_type_required": "shuffle",
        "shuffle_status_required": "available",
        "dimensioned_activity": "Chat",
        "local_timestamp_dime_slot_starts_at": dt.datetime(2026, 5, 18, 6, 0, 0),
        "slot_start_local_unix": SLOT_06_LOCAL,
        "slot_end_local_unix": SLOT_06_LOCAL + SLOT_DURATION_SECONDS,
    }
    base.update(overrides)
    return base


def make_dime(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame([make_dime_row(**r) for r in rows])


def make_prod_row(**overrides) -> dict:
    """One synthetic productivity row (defaults are a 30-min available shift)."""
    base = {
        "agent": "jane.doe",
        "actor_id": "actor-1",
        "timestamp": pd.Timestamp("2026-05-18 12:00:00"),
        "next_event_time": pd.Timestamp("2026-05-18 12:30:00"),
        "activity_start_unix": SLOT_06_UTC,
        "activity_end_unix": SLOT_06_UTC + 1800,
        "raw_status": "available",
        "inferred_status": "available",
        "channel": None,
        "active_jobs": 0,
        "level_3": None,
    }
    base.update(overrides)
    return base


def make_prod(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame([make_prod_row(**r) for r in rows])


def make_roster(rows: list[dict]) -> pd.DataFrame:
    """One synthetic roster row per agent×month (defaults are active core)."""
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
        "snapshot_date": dt.date(2026, 5, 31),
        "snapshot_month": dt.date(2026, 5, 1),
        "hire_start_date": dt.date(2025, 1, 15),
        "last_change_date": dt.date(2025, 1, 15),
    }
    return pd.DataFrame([{**defaults, **r} for r in rows])


# ---------------------------------------------------------------------------
# filter_dime — minimal raw slot universe
# ---------------------------------------------------------------------------


class TestFilterDime:
    def test_keeps_well_formed_row(self):
        out = filter_dime(make_dime([{}]))
        assert len(out) == 1

    def test_drops_null_activity_type(self):
        out = filter_dime(make_dime([{"activity_type_required": None}]))
        assert out.empty

    @pytest.mark.parametrize("act", ["lunch_break", "time_off", "shrinkage"])
    def test_keeps_formerly_excluded_activity_types(self, act):
        # Raw table: these exclusions now belong to the metrics layer.
        out = filter_dime(make_dime([{"activity_type_required": act}]))
        assert len(out) == 1

    @pytest.mark.parametrize("dim", ["Mouring", "Licencia", "Vacacion"])
    def test_keeps_formerly_excluded_dimensioned_activities(self, dim):
        out = filter_dime(make_dime([{"dimensioned_activity": dim}]))
        assert len(out) == 1

    @pytest.mark.parametrize("squad", ["wfm", "credit_evolution", "dote"])
    def test_keeps_formerly_excluded_squads(self, squad):
        out = filter_dime(make_dime([{"squad": squad}]))
        assert len(out) == 1

    def test_keeps_null_squad(self):
        out = filter_dime(make_dime([{"squad": None}]))
        assert len(out) == 1

    def test_adds_utc_unix_columns_with_six_hour_offset(self):
        out = filter_dime(make_dime([{}]))
        assert out.iloc[0]["slot_start"] == SLOT_06_LOCAL + MEXICO_UTC_OFFSET_SECONDS
        assert out.iloc[0]["slot_end"] == out.iloc[0]["slot_start"] + SLOT_DURATION_SECONDS


# ---------------------------------------------------------------------------
# filter_productivity
# ---------------------------------------------------------------------------


class TestFilterProductivity:
    @pytest.mark.parametrize("status", ["available", "oos", "training"])
    def test_keeps_connected_status(self, status):
        out = filter_productivity(make_prod([{"inferred_status": status}]))
        assert len(out) == 1

    def test_keeps_paused_with_jobs(self):
        out = filter_productivity(
            make_prod([{"inferred_status": "pause", "level_3": "paused_with_jobs"}])
        )
        assert len(out) == 1

    def test_drops_plain_paused(self):
        out = filter_productivity(
            make_prod([{"inferred_status": "pause", "level_3": "paused"}])
        )
        assert out.empty

    def test_keeps_active_jobs_positive_regardless_of_status(self):
        out = filter_productivity(
            make_prod([{"inferred_status": "lunch_break", "active_jobs": 1}])
        )
        assert len(out) == 1

    def test_drops_unknown_status_with_no_active_jobs(self):
        out = filter_productivity(
            make_prod([{"inferred_status": "weird_status", "active_jobs": 0}])
        )
        assert out.empty

    def test_keeps_null_status_after_jan_22_2026(self):
        out = filter_productivity(
            make_prod(
                [{"inferred_status": None, "timestamp": pd.Timestamp("2026-02-15 12:00:00")}]
            )
        )
        assert len(out) == 1

    def test_drops_null_status_before_jan_22_2026(self):
        out = filter_productivity(
            make_prod(
                [{"inferred_status": None, "timestamp": pd.Timestamp("2026-01-10 12:00:00")}]
            )
        )
        assert out.empty

    def test_handles_tz_aware_utc_timestamps_from_warehouse(self):
        out = filter_productivity(
            make_prod(
                [
                    {
                        "inferred_status": None,
                        "timestamp": pd.Timestamp("2026-02-15 12:00:00", tz="UTC"),
                    }
                ]
            )
        )
        assert len(out) == 1


# ---------------------------------------------------------------------------
# compute_slot_adherence — the overlap math + LEFT-JOIN behavior
# ---------------------------------------------------------------------------


class TestComputeSlotAdherence:
    def _one_slot(self, **dime_overrides) -> pd.DataFrame:
        return filter_dime(make_dime([dime_overrides]))

    def test_activity_fully_inside_slot(self):
        slots = self._one_slot()
        prod = filter_productivity(
            make_prod(
                [
                    {
                        "activity_start_unix": SLOT_06_UTC + 300,
                        "activity_end_unix": SLOT_06_UTC + 900,
                    }
                ]
            )
        )
        result = compute_slot_adherence(slots, prod)
        assert len(result) == 1
        assert result.iloc[0]["adherent_time_final"] == 600

    def test_activity_spans_slot(self):
        slots = self._one_slot()
        prod = filter_productivity(
            make_prod(
                [
                    {
                        "activity_start_unix": SLOT_06_UTC - 3600,
                        "activity_end_unix": SLOT_06_UTC + 5400,
                    }
                ]
            )
        )
        result = compute_slot_adherence(slots, prod)
        assert result.iloc[0]["adherent_time_final"] == SLOT_DURATION_SECONDS

    def test_activity_ends_inside_slot(self):
        slots = self._one_slot()
        prod = filter_productivity(
            make_prod(
                [
                    {
                        "activity_start_unix": SLOT_06_UTC - 300,
                        "activity_end_unix": SLOT_06_UTC + 600,
                    }
                ]
            )
        )
        result = compute_slot_adherence(slots, prod)
        assert result.iloc[0]["adherent_time_final"] == 600

    def test_activity_starts_inside_slot(self):
        slots = self._one_slot()
        prod = filter_productivity(
            make_prod(
                [
                    {
                        "activity_start_unix": SLOT_06_UTC + 1200,
                        "activity_end_unix": SLOT_06_UTC + 2400,
                    }
                ]
            )
        )
        result = compute_slot_adherence(slots, prod)
        assert result.iloc[0]["adherent_time_final"] == 600

    def test_no_overlap_still_returns_slot_with_zero(self):
        slots = self._one_slot()
        prod = filter_productivity(
            make_prod(
                [
                    {
                        "activity_start_unix": SLOT_06_UTC + 9999,
                        "activity_end_unix": SLOT_06_UTC + 20000,
                    }
                ]
            )
        )
        result = compute_slot_adherence(slots, prod)
        assert len(result) == 1
        assert result.iloc[0]["adherent_time_final"] == 0

    def test_multiple_productivity_rows_sum_then_cap(self):
        slots = self._one_slot()
        prod = filter_productivity(
            make_prod(
                [
                    {
                        "activity_start_unix": SLOT_06_UTC,
                        "activity_end_unix": SLOT_06_UTC + 720,
                    },
                    {
                        "activity_start_unix": SLOT_06_UTC + 720,
                        "activity_end_unix": SLOT_06_UTC + 1440,
                        "timestamp": pd.Timestamp("2026-05-18 12:12:00"),
                        "next_event_time": pd.Timestamp("2026-05-18 12:24:00"),
                    },
                    {
                        "activity_start_unix": SLOT_06_UTC + 1440,
                        "activity_end_unix": SLOT_06_UTC + 2160,
                        "timestamp": pd.Timestamp("2026-05-18 12:24:00"),
                        "next_event_time": pd.Timestamp("2026-05-18 12:36:00"),
                    },
                ]
            )
        )
        result = compute_slot_adherence(slots, prod)
        assert result.iloc[0]["adherent_time_final"] == SLOT_DURATION_SECONDS

    def test_empty_productivity_returns_all_slots_with_zero(self):
        slots = self._one_slot()
        prod = pd.DataFrame(columns=list(make_prod([{}]).columns))
        result = compute_slot_adherence(slots, prod)
        assert len(result) == 1
        assert result.iloc[0]["adherent_time_final"] == 0


# ---------------------------------------------------------------------------
# compute_adherent_time — end-to-end small synthetic pipeline
# ---------------------------------------------------------------------------


class TestComputeAdherentTimeEndToEnd:
    def test_one_agent_one_day_one_slot_full_adherent(self):
        dime = make_dime([{}])
        prod = make_prod([{}])  # exactly covers the slot
        roster = make_roster([{}])

        out = compute_adherent_time(roster, dime, prod)

        assert len(out) == 1
        row = out.iloc[0]
        assert row["agent"] == "jane.doe"
        assert row["xforce"] == "lead.one"
        assert row["squad"] == "core"
        assert row["district"] == "northeast"
        assert row["shift"] == "morning"
        assert row["slot_time"] == "06:00:00"
        assert row["activity_type_required"] == "shuffle"
        assert row["adherent_minutes"] == SLOT_DURATION_SECONDS / 60.0
        assert row["required_minutes"] == SLOT_DURATION_SECONDS / 60.0

    def test_column_order_matches_schema(self):
        out = compute_adherent_time(make_roster([{}]), make_dime([{}]), make_prod([{}]))
        assert list(out.columns) == [
            "agent",
            "xforce",
            "xplead",
            "team",
            "squad",
            "district",
            "shift",
            "date",
            "slot_time",
            "activity_type_required",
            "required_minutes",
            "adherent_minutes",
        ]

    def test_no_productivity_yields_zero_adherent_full_required(self):
        dime = make_dime([{}])
        prod = pd.DataFrame(columns=list(make_prod([{}]).columns))
        roster = make_roster([{}])

        out = compute_adherent_time(roster, dime, prod)

        assert len(out) == 1
        assert out.iloc[0]["adherent_minutes"] == 0.0
        assert out.iloc[0]["required_minutes"] == SLOT_DURATION_SECONDS / 60.0

    def test_inactive_agent_dropped(self):
        dime = make_dime([{}])
        prod = make_prod([{}])
        roster = make_roster([{"status": "inactive"}])

        out = compute_adherent_time(roster, dime, prod)
        assert out.empty

    def test_out_of_scope_squads_constant_is_empty(self):
        assert CORE_OUT_OF_SCOPE_SQUADS == ()

    @pytest.mark.parametrize("squad", ["social", "content"])
    def test_social_and_content_squads_kept(self, squad):
        dime = make_dime([{}])
        prod = make_prod([{}])
        roster = make_roster([{"squad": squad}])

        out = compute_adherent_time(roster, dime, prod)
        assert len(out) == 1
        assert out.iloc[0]["squad"] == squad

    def test_each_slot_emits_its_own_row(self):
        dime = make_dime(
            [
                {},
                {
                    "local_timestamp_dime_slot_starts_at": dt.datetime(
                        2026, 5, 18, 6, 30, 0
                    ),
                    "slot_start_local_unix": SLOT_06_LOCAL + 1800,
                    "slot_end_local_unix": SLOT_06_LOCAL + 3600,
                },
            ]
        )
        prod = pd.DataFrame(columns=list(make_prod([{}]).columns))
        roster = make_roster([{}])

        out = compute_adherent_time(roster, dime, prod)
        assert len(out) == 2
        assert (out["required_minutes"] == SLOT_DURATION_SECONDS / 60.0).all()
        assert (out["adherent_minutes"] == 0.0).all()
        assert sorted(out["slot_time"].tolist()) == ["06:00:00", "06:30:00"]
        assert out["required_minutes"].sum() == 2 * SLOT_DURATION_SECONDS / 60.0

    def test_handles_tz_aware_roster_snapshot_month(self):
        dime = make_dime([{}])
        prod = make_prod([{}])
        roster = make_roster([{"snapshot_month": pd.Timestamp("2026-05-01", tz="UTC")}])
        out = compute_adherent_time(roster, dime, prod)
        assert len(out) == 1
        assert out.iloc[0]["adherent_minutes"] == SLOT_DURATION_SECONDS / 60.0

    def test_night_tail_attributed_to_shift_start_day(self):
        # Night agent, slot at Jul 6 03:00 local -> attributed to Jul 5.
        ts = pd.Timestamp("2026-07-06 03:00:00")
        local_unix = int(ts.value // 10**9)
        dime = make_dime(
            [
                {
                    "date": dt.date(2026, 7, 6),
                    "local_timestamp_dime_slot_starts_at": ts,
                    "slot_start_local_unix": local_unix,
                    "slot_end_local_unix": local_unix + SLOT_DURATION_SECONDS,
                }
            ]
        )
        prod = pd.DataFrame(columns=list(make_prod([{}]).columns))
        roster = make_roster(
            [{"shift": "night", "snapshot_month": dt.date(2026, 7, 1),
              "snapshot_date": dt.date(2026, 7, 31)}]
        )
        out = compute_adherent_time(roster, dime, prod)
        assert len(out) == 1
        assert out.iloc[0]["date"] == dt.date(2026, 7, 5)
        assert out.iloc[0]["slot_time"] == "03:00:00"

    def test_night_tail_before_cutover_keeps_calendar_day(self):
        ts = pd.Timestamp("2026-06-30 03:00:00")
        local_unix = int(ts.value // 10**9)
        dime = make_dime(
            [
                {
                    "date": dt.date(2026, 6, 30),
                    "local_timestamp_dime_slot_starts_at": ts,
                    "slot_start_local_unix": local_unix,
                    "slot_end_local_unix": local_unix + SLOT_DURATION_SECONDS,
                }
            ]
        )
        prod = pd.DataFrame(columns=list(make_prod([{}]).columns))
        roster = make_roster(
            [{"shift": "night", "snapshot_month": dt.date(2026, 6, 1),
              "snapshot_date": dt.date(2026, 6, 30)}]
        )
        out = compute_adherent_time(roster, dime, prod)
        assert len(out) == 1
        assert out.iloc[0]["date"] == dt.date(2026, 6, 30)

    def test_uses_month_specific_roster_snapshot(self):
        dime = make_dime([{}])
        prod = make_prod([{}])
        roster = make_roster(
            [
                {
                    "snapshot_month": dt.date(2026, 4, 1),
                    "snapshot_date": dt.date(2026, 4, 30),
                    "squad": "wrong-april",
                },
                {
                    "snapshot_month": dt.date(2026, 5, 1),
                    "snapshot_date": dt.date(2026, 5, 31),
                    "squad": "correct-may",
                },
            ]
        )

        out = compute_adherent_time(roster, dime, prod)
        assert len(out) == 1
        assert out.iloc[0]["squad"] == "correct-may"
