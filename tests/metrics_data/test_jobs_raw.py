"""Unit tests for ``metrics_data/jobs_raw.py``.

Small synthetic pandas frames, no warehouse, sub-second runs.

jobs_raw is a RAW per-job feed: one row per individual job (shuffle + OOS),
ALL shuffle statuses kept, with raw start/end times, the derived job_id, and a
``required_activity_on_day_flag`` computed from the NTPJ DIME definition of
"scheduled". No aggregation, no expected-duration benchmark (those move to the
metrics layer).
"""

from __future__ import annotations

import datetime as dt

import pandas as pd
import pytest

from jobs_raw import (
    DIME_ACTIVITY_TYPE_EXCLUSIONS,
    DIME_SQUAD_EXCLUSIONS,
    IO_JOBS_RAW_SCHEMA,
    NTPJ_OUT_OF_SCOPE_SQUADS,
    _clean_oos_job_classification,
    _shuffle_job_id,
    build_jobs_union,
    build_oos_jobs_raw,
    build_shuffle_jobs_raw,
    compute_jobs_raw,
    compute_required_activities,
    filter_dime,
)


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------

D = dt.date(2026, 5, 18)


def make_shuffle_row(**overrides) -> dict:
    base = {
        "agent": "jane.doe",
        "actor_affiliation": "nubank",
        "date": D,
        "job_type": "manual",
        "activity_type": "chat",
        "status": "finished",
        "net_time_spent_seconds": 600,
        "local_start_time": dt.datetime(2026, 5, 18, 6, 0, 0),
        "local_stop_time": dt.datetime(2026, 5, 18, 6, 10, 0),
        "activity_start_unix": 0,
        "activity_end_unix": 600,
    }
    base.update(overrides)
    return base


def make_shuffle(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame([make_shuffle_row(**r) for r in rows])


def make_oos_row(**overrides) -> dict:
    base = {
        "agent": "jane.doe",
        "date": D,
        "job_classification": "support_ticket",
        "net_time_spent_seconds": 900,
        "local_start_date": dt.datetime(2026, 5, 18, 7, 0, 0),
        "local_stop_date": dt.datetime(2026, 5, 18, 7, 15, 0),
        "activity_start_unix": 0,
        "activity_end_unix": 900,
        "squad": "core",
        "comment": "",
    }
    base.update(overrides)
    return base


def make_oos(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame([make_oos_row(**r) for r in rows])


def make_dime_row(**overrides) -> dict:
    base = {
        "agent": "jane.doe",
        "date": D,
        "squad": "core",
        "affiliation": "nubank",
        "activity_type_required": "chat",
        "shuffle_status_required": "available",
        "dimensioned_activity": "Chat",
        "local_timestamp_dime_slot_starts_at": dt.datetime(2026, 5, 18, 6, 0, 0),
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
        "snapshot_date": dt.date(2026, 5, 31),
        "snapshot_month": dt.date(2026, 5, 1),
        "hire_start_date": dt.date(2025, 1, 15),
        "last_change_date": dt.date(2025, 1, 15),
    }
    return pd.DataFrame([{**defaults, **r} for r in rows])


# ---------------------------------------------------------------------------
# job_id construction
# ---------------------------------------------------------------------------


class TestShuffleJobId:
    def test_email_includes_job_type(self):
        out = _shuffle_job_id(
            pd.Series(["email"]), pd.Series(["voice"]), pd.Series(["finished"])
        )
        assert out.tolist() == ["email - voice - finished"]

    def test_backoffice_uses_bko_prefix_and_job_type(self):
        out = _shuffle_job_id(
            pd.Series(["backoffice"]), pd.Series(["manual"]), pd.Series(["finished"])
        )
        assert out.tolist() == ["bko - manual - finished"]

    def test_other_activity_types_omit_job_type(self):
        out = _shuffle_job_id(
            pd.Series(["chat", "voice"]),
            pd.Series(["something", "ignored"]),
            pd.Series(["finished", "transferred"]),
        )
        assert out.tolist() == ["chat - finished", "voice - transferred"]


class TestCleanOosJobClassification:
    def test_passes_through_non_content_squad_unchanged(self):
        out = _clean_oos_job_classification(
            pd.Series(["Some Classification (OOS_CONT)"]), pd.Series(["core"])
        )
        assert out.tolist() == ["Some Classification (OOS_CONT)"]

    def test_strips_oos_cont_and_lowercases_for_content_squad(self):
        out = _clean_oos_job_classification(
            pd.Series(["Publish Bug (OOS_CONT)"]),
            pd.Series(["mx_content"]),
        )
        assert out.tolist() == ["publish_bug"]


# ---------------------------------------------------------------------------
# build_shuffle_jobs_raw / build_oos_jobs_raw / build_jobs_union
# ---------------------------------------------------------------------------


class TestBuildShuffleJobsRaw:
    def test_one_row_per_job_all_statuses_kept(self):
        out = build_shuffle_jobs_raw(
            make_shuffle(
                [
                    {"status": "finished"},
                    {"status": "transferred"},
                    {"status": "skipped"},
                    {"status": "cancelled"},
                ]
            )
        )
        assert len(out) == 4

    def test_maps_start_end_and_duration(self):
        out = build_shuffle_jobs_raw(make_shuffle([{}]))
        row = out.iloc[0]
        assert row["start_time"] == dt.datetime(2026, 5, 18, 6, 0, 0)
        assert row["end_time"] == dt.datetime(2026, 5, 18, 6, 10, 0)
        assert row["duration_seconds"] == 600
        assert row["job_id"] == "chat - finished"

    def test_empty_returns_shaped_frame(self):
        out = build_shuffle_jobs_raw(make_shuffle([]))
        assert out.empty


class TestBuildOosJobsRaw:
    def test_synthesizes_activity_type_and_status(self):
        out = build_oos_jobs_raw(make_oos([{}]))
        row = out.iloc[0]
        assert row["activity_type"] == "oos"
        assert row["status"] == "finished"
        assert row["job_id"] == "oos - support_ticket"
        assert row["start_time"] == dt.datetime(2026, 5, 18, 7, 0, 0)
        assert row["duration_seconds"] == 900

    def test_content_squad_classification_cleaned(self):
        out = build_oos_jobs_raw(
            make_oos([{"squad": "mx_content", "job_classification": "Publish Bug (OOS_CONT)"}])
        )
        assert out.iloc[0]["job_id"] == "oos - publish_bug"


class TestBuildJobsUnion:
    def test_concats_shuffle_and_oos(self):
        out = build_jobs_union(make_shuffle([{}]), make_oos([{}]))
        assert len(out) == 2
        assert set(out["activity_type"]) == {"chat", "oos"}


# ---------------------------------------------------------------------------
# filter_dime — NTPJ filter (used for the required flag)
# ---------------------------------------------------------------------------


class TestFilterDime:
    def test_keeps_well_formed_row(self):
        out = filter_dime(make_dime([{}]))
        assert len(out) == 1

    @pytest.mark.parametrize("bad", list(DIME_ACTIVITY_TYPE_EXCLUSIONS))
    def test_drops_excluded_activity_types(self, bad):
        out = filter_dime(make_dime([{"activity_type_required": bad}]))
        assert out.empty

    @pytest.mark.parametrize("bad", list(DIME_SQUAD_EXCLUSIONS))
    def test_drops_excluded_squads(self, bad):
        out = filter_dime(make_dime([{"squad": bad}]))
        assert out.empty

    def test_drops_null_activity_type(self):
        out = filter_dime(make_dime([{"activity_type_required": None}]))
        assert out.empty

    def test_drops_null_squad(self):
        out = filter_dime(make_dime([{"squad": None}]))
        assert out.empty

    @pytest.mark.parametrize("bad", ["pause", "training", "lunch", None])
    def test_drops_invalid_shuffle_status(self, bad):
        out = filter_dime(make_dime([{"shuffle_status_required": bad}]))
        assert out.empty

    @pytest.mark.parametrize("good", ["available", "oos"])
    def test_keeps_available_and_oos_shuffle_status(self, good):
        out = filter_dime(make_dime([{"shuffle_status_required": good}]))
        assert len(out) == 1

    def test_keeps_meeting_dimensioned_activity(self):
        out = filter_dime(make_dime([{"dimensioned_activity": "Mouring"}]))
        assert len(out) == 1


# ---------------------------------------------------------------------------
# compute_required_activities
# ---------------------------------------------------------------------------


class TestComputeRequiredActivities:
    def test_distinct_agent_date_activity(self):
        out = compute_required_activities(
            filter_dime(
                pd.DataFrame(
                    [
                        make_dime_row(activity_type_required="chat"),
                        make_dime_row(
                            activity_type_required="chat",
                            slot_start_local_unix=1800,
                            slot_end_local_unix=3600,
                        ),
                        make_dime_row(
                            activity_type_required="email",
                            slot_start_local_unix=3600,
                            slot_end_local_unix=5400,
                        ),
                    ]
                )
            )
        )
        assert set(out["activity_type"]) == {"chat", "email"}
        assert (out["required_flag"] == 1).all()


# ---------------------------------------------------------------------------
# compute_jobs_raw — end to end
# ---------------------------------------------------------------------------


class TestComputeJobsRaw:
    def test_basic_end_to_end_chat_path(self):
        roster = make_roster([{}])
        dime = make_dime([{}])  # chat slot scheduled
        shuffle = make_shuffle([{"net_time_spent_seconds": 100}])
        out = compute_jobs_raw(roster, dime, shuffle, make_oos([]))
        assert len(out) == 1
        row = out.iloc[0]
        assert row["agent"] == "jane.doe"
        assert row["job_id"] == "chat - finished"
        assert row["activity_type"] == "chat"
        assert row["status"] == "finished"
        assert row["duration_seconds"] == 100
        assert row["required_activity_on_day_flag"] == 1
        assert row["district"] == "northeast"
        assert row["shift"] == "morning"

    def test_two_jobs_two_rows(self):
        roster = make_roster([{}])
        dime = make_dime([{}])
        shuffle = make_shuffle(
            [{"net_time_spent_seconds": 100}, {"net_time_spent_seconds": 200}]
        )
        out = compute_jobs_raw(roster, dime, shuffle, make_oos([]))
        assert len(out) == 2

    def test_job_without_required_activity_kept_with_flag_zero(self):
        # OOS job on a day the agent only had chat scheduled. Unlike legacy
        # NTPJ (which dropped it), jobs_raw keeps it with flag 0.
        roster = make_roster([{}])
        dime = make_dime([{"activity_type_required": "chat"}])
        out = compute_jobs_raw(
            roster, dime, make_shuffle([]), make_oos([{"job_classification": "ad_hoc"}])
        )
        assert len(out) == 1
        assert out.iloc[0]["activity_type"] == "oos"
        assert out.iloc[0]["required_activity_on_day_flag"] == 0

    @pytest.mark.parametrize("status", ["finished", "transferred", "skipped", "cancelled"])
    def test_all_shuffle_statuses_kept(self, status):
        roster = make_roster([{}])
        dime = make_dime([{}])
        out = compute_jobs_raw(
            roster, dime, make_shuffle([{"status": status}]), make_oos([])
        )
        assert len(out) == 1
        assert out.iloc[0]["status"] == status

    def test_drops_inactive_agents(self):
        roster = make_roster([{"status": "inactive"}])
        out = compute_jobs_raw(roster, make_dime([{}]), make_shuffle([{}]), make_oos([]))
        assert out.empty

    def test_out_of_scope_squads_constant_is_empty(self):
        assert NTPJ_OUT_OF_SCOPE_SQUADS == ()

    @pytest.mark.parametrize("squad", ["social", "content"])
    def test_social_and_content_squads_kept(self, squad):
        roster = make_roster([{"squad": squad}])
        out = compute_jobs_raw(roster, make_dime([{}]), make_shuffle([{}]), make_oos([]))
        assert len(out) == 1
        assert out.iloc[0]["squad"] == squad

    def test_output_columns_match_schema(self):
        roster = make_roster([{}])
        out = compute_jobs_raw(roster, make_dime([{}]), make_shuffle([{}]), make_oos([]))
        assert list(out.columns) == [c for c, _ in IO_JOBS_RAW_SCHEMA]

    def test_joins_correct_month_of_roster(self):
        roster = pd.concat(
            [
                make_roster(
                    [
                        {
                            "snapshot_date": dt.date(2026, 4, 30),
                            "snapshot_month": dt.date(2026, 4, 1),
                            "squad": "credit",
                            "squad_district": "old_district",
                        }
                    ]
                ),
                make_roster(
                    [
                        {
                            "snapshot_date": dt.date(2026, 5, 31),
                            "snapshot_month": dt.date(2026, 5, 1),
                            "squad": "core",
                            "squad_district": "new_district",
                        }
                    ]
                ),
            ],
            ignore_index=True,
        )
        april_day = dt.date(2026, 4, 15)
        dime = make_dime(
            [
                {
                    "date": april_day,
                    "local_timestamp_dime_slot_starts_at": dt.datetime(
                        2026, 4, 15, 6, 0, 0
                    ),
                }
            ]
        )
        shuffle = make_shuffle(
            [
                {
                    "date": april_day,
                    "local_start_time": dt.datetime(2026, 4, 15, 6, 0, 0),
                    "local_stop_time": dt.datetime(2026, 4, 15, 6, 10, 0),
                }
            ]
        )
        out = compute_jobs_raw(roster, dime, shuffle, make_oos([]))
        assert len(out) == 1
        assert out.iloc[0]["squad"] == "credit"
        assert out.iloc[0]["district"] == "old_district"

    def test_handles_tz_aware_roster_snapshot_month(self):
        roster = make_roster([{"snapshot_month": pd.Timestamp("2026-05-01", tz="UTC")}])
        out = compute_jobs_raw(roster, make_dime([{}]), make_shuffle([{}]), make_oos([]))
        assert len(out) == 1


# ---------------------------------------------------------------------------
# Night-shift attribution (>= 2026-07-01): both jobs and the DIME required-set
# roll the early-morning tail back to the day the shift started, so the
# required-flag join stays aligned.
# ---------------------------------------------------------------------------


def _local_unix(ts: str) -> int:
    """Naive local wall-clock -> unix seconds (UTC-interpreted, as the modules treat slot_start_local_unix)."""
    return int(pd.Timestamp(ts).value // 10**9)


class TestNightShiftAttribution:
    def _night_roster(self, **over):
        return make_roster(
            [
                {
                    "shift": "night",
                    "snapshot_month": dt.date(2026, 7, 1),
                    "snapshot_date": dt.date(2026, 7, 31),
                    **over,
                }
            ]
        )

    def _tail_dime(self, day: dt.date, ts: str):
        return make_dime(
            [
                {
                    "date": day,
                    "activity_type_required": "chat",
                    "local_timestamp_dime_slot_starts_at": pd.Timestamp(ts),
                    "slot_start_local_unix": _local_unix(ts),
                    "slot_end_local_unix": _local_unix(ts) + 1800,
                }
            ]
        )

    def _tail_shuffle(self, day: dt.date, ts: str):
        start = pd.Timestamp(ts)
        return make_shuffle(
            [
                {
                    "date": day,
                    "activity_type": "chat",
                    "local_start_time": start,
                    "local_stop_time": start + pd.Timedelta(minutes=10),
                }
            ]
        )

    def test_tail_rolls_back_to_shift_start_day_and_flag_stays_one(self):
        # Night agent, early-morning tail on Jul 6 03:00 -> attributed to Jul 5.
        # DIME required slot (also at Jul 6 03:00) rolls back identically, so the
        # required-flag join still matches.
        roster = self._night_roster()
        dime = self._tail_dime(dt.date(2026, 7, 6), "2026-07-06 03:00:00")
        shuffle = self._tail_shuffle(dt.date(2026, 7, 6), "2026-07-06 03:00:00")
        out = compute_jobs_raw(roster, dime, shuffle, make_oos([]))
        assert len(out) == 1
        assert out.iloc[0]["date"] == dt.date(2026, 7, 5)
        assert out.iloc[0]["required_activity_on_day_flag"] == 1

    def test_morning_agent_tail_not_re_attributed(self):
        roster = self._night_roster(shift="morning")
        dime = self._tail_dime(dt.date(2026, 7, 6), "2026-07-06 03:00:00")
        shuffle = self._tail_shuffle(dt.date(2026, 7, 6), "2026-07-06 03:00:00")
        out = compute_jobs_raw(roster, dime, shuffle, make_oos([]))
        assert len(out) == 1
        assert out.iloc[0]["date"] == dt.date(2026, 7, 6)

    def test_before_cutover_keeps_legacy_calendar_day(self):
        roster = self._night_roster(
            snapshot_month=dt.date(2026, 6, 1), snapshot_date=dt.date(2026, 6, 30)
        )
        dime = self._tail_dime(dt.date(2026, 6, 30), "2026-06-30 03:00:00")
        shuffle = self._tail_shuffle(dt.date(2026, 6, 30), "2026-06-30 03:00:00")
        out = compute_jobs_raw(roster, dime, shuffle, make_oos([]))
        assert len(out) == 1
        assert out.iloc[0]["date"] == dt.date(2026, 6, 30)
