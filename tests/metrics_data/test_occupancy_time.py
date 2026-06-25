"""Unit tests for ``metrics_data/occupancy_time.py``.

Small synthetic pandas frames, no warehouse, sub-second runs.

occupancy_time is now a RAW dataset: ``filter_dime`` keeps every slot with a
non-null ``activity_type_required`` (business exclusions move to the metrics
layer) but STILL applies the systemic reclassifications (Control MC /
xMC Debit Fraud / dime_invalid_notation -> 'oos') because those are part of the
occupancy matching logic. There is no monthly benchmark anymore (it moved to
the metrics layer); output is one row per slot with ``occupancy_minutes`` and
``required_minutes``.
"""

from __future__ import annotations

import datetime as dt

import pandas as pd
import pytest

from occupancy_time import (
    DIMENSIONED_ACTIVITY_TO_OOS,
    IO_OCCUPANCY_TIME_SCHEMA,
    NOCC_OUT_OF_SCOPE_SQUADS,
    SHUFFLE_OCCUPIED_STATUSES,
    SLOT_DURATION_SECONDS,
    build_jobs_union,
    compute_occupancy_time,
    compute_slot_occupancy,
    filter_dime,
)


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------

D = dt.date(2026, 5, 18)
SLOT_BASE = 1_716_000_000  # any int; only start/end deltas matter


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
        "slot_start_local_unix": SLOT_BASE,
        "slot_end_local_unix": SLOT_BASE + SLOT_DURATION_SECONDS,
    }
    base.update(overrides)
    return base


def make_dime(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame([make_dime_row(**r) for r in rows])


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
        "activity_start_unix": SLOT_BASE,
        "activity_end_unix": SLOT_BASE + 600,
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
        "local_start_date": dt.datetime(2026, 5, 18, 6, 5, 0),
        "local_stop_date": dt.datetime(2026, 5, 18, 6, 20, 0),
        "activity_start_unix": SLOT_BASE + 300,
        "activity_end_unix": SLOT_BASE + 1200,
        "squad": "core",
        "comment": "",
    }
    base.update(overrides)
    return base


def make_oos(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame([make_oos_row(**r) for r in rows])


def make_sm_row(**overrides) -> dict:
    base = {
        "agent": "jane.doe",
        "date": D,
        "net_time_spent_seconds": 600,
        "case_assignment_time": dt.datetime(2026, 5, 18, 6, 0, 0),
        "case_unassignment_time": dt.datetime(2026, 5, 18, 6, 10, 0),
        "activity_start_unix": SLOT_BASE,
        "activity_end_unix": SLOT_BASE + 600,
    }
    base.update(overrides)
    return base


def make_sm(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame([make_sm_row(**r) for r in rows])


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
# filter_dime — minimal raw universe + systemic reclassifications
# ---------------------------------------------------------------------------


class TestFilterDime:
    def test_keeps_a_well_formed_row(self):
        out = filter_dime(make_dime([{}]))
        assert len(out) == 1

    def test_drops_null_activity_type_required(self):
        out = filter_dime(make_dime([{"activity_type_required": None}]))
        assert out.empty

    @pytest.mark.parametrize("act", ["lunch_break", "time_off", "shrinkage"])
    def test_keeps_formerly_excluded_activity_types(self, act):
        out = filter_dime(make_dime([{"activity_type_required": act}]))
        assert len(out) == 1

    @pytest.mark.parametrize("dim", ["Mouring", "Licencia", "Vacacion"])
    def test_keeps_formerly_excluded_dimensioned_activities(self, dim):
        out = filter_dime(make_dime([{"dimensioned_activity": dim}]))
        assert len(out) == 1

    @pytest.mark.parametrize("squad", ["wfm", "credit_evolution", "dote", "social"])
    def test_keeps_formerly_excluded_squads(self, squad):
        out = filter_dime(make_dime([{"squad": squad}]))
        assert len(out) == 1

    def test_keeps_null_squad(self):
        out = filter_dime(make_dime([{"squad": None}]))
        assert len(out) == 1

    @pytest.mark.parametrize("dim_act", list(DIMENSIONED_ACTIVITY_TO_OOS))
    def test_systemic_fraud_reclassification(self, dim_act):
        out = filter_dime(
            make_dime(
                [{"dimensioned_activity": dim_act, "activity_type_required": "chat"}]
            )
        )
        assert len(out) == 1
        assert out.iloc[0]["activity_type_required"] == "oos"

    def test_systemic_invalid_notation_reclassification(self):
        out = filter_dime(
            make_dime([{"activity_type_required": "dime_invalid_notation"}])
        )
        assert len(out) == 1
        assert out.iloc[0]["activity_type_required"] == "oos"

    def test_does_not_apply_per_agent_timeoff_reclassification(self):
        out = filter_dime(
            make_dime(
                [
                    {
                        "agent": "maria.reyes",
                        "date": dt.date(2026, 2, 15),
                        "activity_type_required": "chat",
                    }
                ]
            )
        )
        assert len(out) == 1
        assert out.iloc[0]["activity_type_required"] == "chat"


# ---------------------------------------------------------------------------
# build_jobs_union
# ---------------------------------------------------------------------------


class TestBuildJobsUnion:
    @pytest.mark.parametrize("status", list(SHUFFLE_OCCUPIED_STATUSES))
    def test_keeps_all_three_occupied_statuses(self, status):
        out = build_jobs_union(make_shuffle([{"status": status}]), make_oos([]))
        assert len(out) == 1

    def test_drops_other_statuses(self):
        out = build_jobs_union(make_shuffle([{"status": "cancelled"}]), make_oos([]))
        assert out.empty

    def test_synthesizes_activity_type_oos_on_oos_side(self):
        out = build_jobs_union(make_shuffle([]), make_oos([{}]))
        assert len(out) == 1
        assert out.iloc[0]["activity_type"] == "oos"

    def test_concats_shuffle_and_oos(self):
        out = build_jobs_union(make_shuffle([{}]), make_oos([{}]))
        assert sorted(out["activity_type"].tolist()) == ["chat", "oos"]

    def test_sm_jobs_synthesized_as_oos(self):
        out = build_jobs_union(make_shuffle([]), make_oos([]), make_sm([{}]))
        assert len(out) == 1
        assert out.iloc[0]["activity_type"] == "oos"

    def test_sm_jobs_optional_default_none(self):
        # Existing two-arg callers keep working (no SM jobs).
        out = build_jobs_union(make_shuffle([{}]), make_oos([]))
        assert len(out) == 1

    def test_concats_all_three_sources(self):
        out = build_jobs_union(make_shuffle([{}]), make_oos([{}]), make_sm([{}]))
        assert sorted(out["activity_type"].tolist()) == ["chat", "oos", "oos"]

    def test_luis_contreras_content_oos_plus_two_hour_correction(self):
        out = build_jobs_union(
            make_shuffle([]),
            make_oos([
                {
                    "agent": "luis.contreras",
                    "date": dt.date(2026, 3, 8),
                    "local_start_date": dt.datetime(2026, 3, 8, 23, 30),
                    "local_stop_date": dt.datetime(2026, 3, 8, 23, 45),
                    "activity_start_unix": 1_000,
                    "activity_end_unix": 1_900,
                    "squad": "content_content",
                }
            ]),
        )

        row = out.iloc[0]
        assert row["activity_start_unix"] == 1_000 + 2 * 3600
        assert row["activity_end_unix"] == 1_900 + 2 * 3600
        assert row["date"] == dt.date(2026, 3, 9)

    def test_luis_contreras_content_oos_plus_one_hour_correction(self):
        out = build_jobs_union(
            make_shuffle([]),
            make_oos([
                {
                    "agent": "luis.contreras",
                    "date": dt.date(2026, 3, 9),
                    "local_start_date": dt.datetime(2026, 3, 9, 10, 0),
                    "local_stop_date": dt.datetime(2026, 3, 9, 10, 15),
                    "activity_start_unix": 1_000,
                    "activity_end_unix": 1_900,
                    "squad": "content_content",
                }
            ]),
        )

        row = out.iloc[0]
        assert row["activity_start_unix"] == 1_000 + 3600
        assert row["activity_end_unix"] == 1_900 + 3600

    def test_luis_contreras_non_content_oos_not_corrected(self):
        out = build_jobs_union(
            make_shuffle([]),
            make_oos([
                {
                    "agent": "luis.contreras",
                    "date": dt.date(2026, 3, 9),
                    "local_start_date": dt.datetime(2026, 3, 9, 10, 0),
                    "local_stop_date": dt.datetime(2026, 3, 9, 10, 15),
                    "activity_start_unix": 1_000,
                    "activity_end_unix": 1_900,
                    "squad": "core",
                }
            ]),
        )

        row = out.iloc[0]
        assert row["activity_start_unix"] == 1_000
        assert row["activity_end_unix"] == 1_900


# ---------------------------------------------------------------------------
# compute_slot_occupancy — the heart of the metric
# ---------------------------------------------------------------------------


class TestComputeSlotOccupancy:
    def _slot_only(self):
        return filter_dime(make_dime([{}]))

    def _make_job(self, start_offset: int, end_offset: int, activity_type: str = "chat"):
        return make_shuffle(
            [
                {
                    "activity_type": activity_type,
                    "activity_start_unix": SLOT_BASE + start_offset,
                    "activity_end_unix": SLOT_BASE + end_offset,
                }
            ]
        )

    def test_single_matching_job_fully_inside_slot(self):
        slots = self._slot_only()
        jobs = build_jobs_union(self._make_job(0, 600), make_oos([]))
        out = compute_slot_occupancy(slots, jobs)
        assert len(out) == 1
        assert int(out.iloc[0]["occupancy_time"]) == 600

    def test_mismatched_activity_yields_zero(self):
        slots = self._slot_only()
        jobs = build_jobs_union(
            self._make_job(0, 600, activity_type="email"), make_oos([])
        )
        out = compute_slot_occupancy(slots, jobs)
        assert int(out.iloc[0]["occupancy_time"]) == 0

    def test_job_clipped_to_slot_start(self):
        slots = self._slot_only()
        jobs = build_jobs_union(self._make_job(-300, 600), make_oos([]))
        out = compute_slot_occupancy(slots, jobs)
        assert int(out.iloc[0]["occupancy_time"]) == 600

    def test_job_clipped_to_slot_end(self):
        slots = self._slot_only()
        jobs = build_jobs_union(self._make_job(1500, 2400), make_oos([]))
        out = compute_slot_occupancy(slots, jobs)
        assert int(out.iloc[0]["occupancy_time"]) == 300

    def test_job_fully_swallows_slot(self):
        slots = self._slot_only()
        jobs = build_jobs_union(self._make_job(-300, 2400), make_oos([]))
        out = compute_slot_occupancy(slots, jobs)
        assert int(out.iloc[0]["occupancy_time"]) == 1800

    def test_two_non_overlapping_jobs_sum(self):
        slots = self._slot_only()
        jobs = build_jobs_union(
            make_shuffle(
                [
                    make_shuffle_row(
                        activity_start_unix=SLOT_BASE,
                        activity_end_unix=SLOT_BASE + 600,
                    ),
                    make_shuffle_row(
                        activity_start_unix=SLOT_BASE + 900,
                        activity_end_unix=SLOT_BASE + 1500,
                    ),
                ]
            ),
            make_oos([]),
        )
        out = compute_slot_occupancy(slots, jobs)
        assert int(out.iloc[0]["occupancy_time"]) == 1200

    def test_overlapping_jobs_dedup_no_double_count(self):
        slots = self._slot_only()
        jobs = build_jobs_union(
            make_shuffle(
                [
                    make_shuffle_row(
                        activity_start_unix=SLOT_BASE,
                        activity_end_unix=SLOT_BASE + 1200,
                    ),
                    make_shuffle_row(
                        activity_start_unix=SLOT_BASE + 600,
                        activity_end_unix=SLOT_BASE + 1500,
                    ),
                ]
            ),
            make_oos([]),
        )
        out = compute_slot_occupancy(slots, jobs)
        assert int(out.iloc[0]["occupancy_time"]) == 1500

    def test_dedup_only_within_same_activity_type_partition(self):
        slots = self._slot_only()
        jobs = build_jobs_union(
            make_shuffle(
                [
                    make_shuffle_row(
                        activity_type="chat",
                        activity_start_unix=SLOT_BASE,
                        activity_end_unix=SLOT_BASE + 1200,
                    ),
                    make_shuffle_row(
                        activity_type="email",
                        activity_start_unix=SLOT_BASE + 600,
                        activity_end_unix=SLOT_BASE + 1500,
                    ),
                ]
            ),
            make_oos([]),
        )
        out = compute_slot_occupancy(slots, jobs)
        assert int(out.iloc[0]["occupancy_time"]) == 1200

    def test_no_overlap_job_dropped(self):
        slots = self._slot_only()
        jobs = build_jobs_union(self._make_job(1800, 2400), make_oos([]))
        out = compute_slot_occupancy(slots, jobs)
        assert len(out) == 1
        assert int(out.iloc[0]["occupancy_time"]) == 0

    def test_slot_with_no_matching_jobs_kept_with_zero(self):
        slots = self._slot_only()
        empty_jobs = build_jobs_union(make_shuffle([]), make_oos([]))
        out = compute_slot_occupancy(slots, empty_jobs)
        assert len(out) == 1
        assert int(out.iloc[0]["occupancy_time"]) == 0

    def test_occupancy_time_capped_at_slot_duration(self):
        slots = self._slot_only()
        jobs = build_jobs_union(
            make_shuffle(
                [
                    make_shuffle_row(
                        activity_start_unix=SLOT_BASE,
                        activity_end_unix=SLOT_BASE + 600,
                    ),
                    make_shuffle_row(
                        activity_start_unix=SLOT_BASE + 600,
                        activity_end_unix=SLOT_BASE + 1200,
                    ),
                    make_shuffle_row(
                        activity_start_unix=SLOT_BASE + 1200,
                        activity_end_unix=SLOT_BASE + 1800,
                    ),
                ]
            ),
            make_oos([]),
        )
        out = compute_slot_occupancy(slots, jobs)
        assert int(out.iloc[0]["occupancy_time"]) == 1800

    def test_oos_activity_matches_oos_slot(self):
        slots = filter_dime(make_dime([{"activity_type_required": "oos"}]))
        jobs = build_jobs_union(
            make_shuffle([]),
            make_oos(
                [
                    {
                        "activity_start_unix": SLOT_BASE + 300,
                        "activity_end_unix": SLOT_BASE + 900,
                    }
                ]
            ),
        )
        out = compute_slot_occupancy(slots, jobs)
        assert int(out.iloc[0]["occupancy_time"]) == 600


# ---------------------------------------------------------------------------
# compute_occupancy_time — end-to-end
# ---------------------------------------------------------------------------


def _baseline_inputs():
    roster = make_roster([{}])
    dime = make_dime([{}])
    shuffle = make_shuffle(
        [{"activity_start_unix": SLOT_BASE, "activity_end_unix": SLOT_BASE + 600}]
    )
    oos = make_oos([])
    return roster, dime, shuffle, oos


class TestComputeOccupancyTime:
    def test_basic_end_to_end_shape(self):
        roster, dime, shuffle, oos = _baseline_inputs()
        out = compute_occupancy_time(roster, dime, shuffle, oos)
        assert len(out) == 1
        row = out.iloc[0]
        assert row["agent"] == "jane.doe"
        assert row["occupancy_minutes"] == 600 / 60.0
        assert row["required_minutes"] == SLOT_DURATION_SECONDS / 60.0
        assert row["activity_type_required"] == "chat"
        assert row["squad"] == "core"
        assert row["district"] == "northeast"
        assert row["shift"] == "morning"

    def test_output_column_order_matches_schema(self):
        roster, dime, shuffle, oos = _baseline_inputs()
        out = compute_occupancy_time(roster, dime, shuffle, oos)
        assert list(out.columns) == [c for c, _ in IO_OCCUPANCY_TIME_SCHEMA]

    def test_drops_inactive_agent(self):
        roster = make_roster([{"status": "inactive"}])
        _, dime, shuffle, oos = _baseline_inputs()
        out = compute_occupancy_time(roster, dime, shuffle, oos)
        assert out.empty

    @pytest.mark.parametrize("squad", ["social", "content"])
    def test_social_and_content_squads_kept(self, squad):
        roster = make_roster([{"squad": squad}])
        _, dime, shuffle, oos = _baseline_inputs()
        out = compute_occupancy_time(roster, dime, shuffle, oos)
        assert len(out) == 1
        assert out.iloc[0]["squad"] == squad

    def test_keeps_slot_with_no_matching_jobs(self):
        roster, dime, _, _ = _baseline_inputs()
        out = compute_occupancy_time(roster, dime, make_shuffle([]), make_oos([]))
        assert len(out) == 1
        assert out.iloc[0]["occupancy_minutes"] == 0.0

    def test_sm_jobs_populate_social_occupancy(self):
        # Social agent with an 'oos' DIME slot and no shuffle/taskmaster jobs:
        # occupancy must come from the Sprinklr SM case (10 min overlap).
        roster = make_roster([{"squad": "social", "team": "social media"}])
        dime = make_dime([{"activity_type_required": "oos"}])
        out = compute_occupancy_time(
            roster, dime, make_shuffle([]), make_oos([]), make_sm([{}])
        )
        assert len(out) == 1
        row = out.iloc[0]
        assert row["squad"] == "social"
        assert row["team"] == "social media"
        assert row["occupancy_minutes"] == 600 / 60.0

    def test_social_slot_zero_without_sm_jobs(self):
        # Same social 'oos' slot, but no SM jobs supplied -> occupancy 0.
        roster = make_roster([{"squad": "social", "team": "social media"}])
        dime = make_dime([{"activity_type_required": "oos"}])
        out = compute_occupancy_time(roster, dime, make_shuffle([]), make_oos([]))
        assert len(out) == 1
        assert out.iloc[0]["occupancy_minutes"] == 0.0

    def test_handles_tz_aware_snapshot_month(self):
        roster = make_roster([{}])
        roster["snapshot_month"] = pd.to_datetime(
            roster["snapshot_month"]
        ).dt.tz_localize("UTC")
        _, dime, shuffle, oos = _baseline_inputs()
        out = compute_occupancy_time(roster, dime, shuffle, oos)
        assert len(out) == 1

    def test_handles_empty_inputs(self):
        out = compute_occupancy_time(
            make_roster([{}]),
            pd.DataFrame(columns=list(make_dime([{}]).columns)),
            make_shuffle([]),
            make_oos([]),
        )
        assert out.empty


# ---------------------------------------------------------------------------
# Schema sanity
# ---------------------------------------------------------------------------


def test_schema_matches_output_columns():
    roster, dime, shuffle, oos = _baseline_inputs()
    out = compute_occupancy_time(roster, dime, shuffle, oos)
    assert list(out.columns) == [c for c, _ in IO_OCCUPANCY_TIME_SCHEMA]


def test_out_of_scope_squads_constants():
    assert NOCC_OUT_OF_SCOPE_SQUADS == ()


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
                    "slot_end_local_unix": u + SLOT_DURATION_SECONDS,
                }
            ]
        )

    def test_night_tail_attributed_to_shift_start_day(self):
        roster = make_roster(
            [{"shift": "night", "snapshot_month": dt.date(2026, 7, 1),
              "snapshot_date": dt.date(2026, 7, 31)}]
        )
        dime = self._night_slot(dt.date(2026, 7, 6), "2026-07-06 03:00:00")
        out = compute_occupancy_time(roster, dime, make_shuffle([]), make_oos([]))
        assert len(out) == 1
        assert out.iloc[0]["date"] == dt.date(2026, 7, 5)
        assert out.iloc[0]["slot_time"] == "03:00:00"

    def test_morning_agent_not_re_attributed(self):
        roster = make_roster(
            [{"shift": "morning", "snapshot_month": dt.date(2026, 7, 1),
              "snapshot_date": dt.date(2026, 7, 31)}]
        )
        dime = self._night_slot(dt.date(2026, 7, 6), "2026-07-06 03:00:00")
        out = compute_occupancy_time(roster, dime, make_shuffle([]), make_oos([]))
        assert len(out) == 1
        assert out.iloc[0]["date"] == dt.date(2026, 7, 6)
