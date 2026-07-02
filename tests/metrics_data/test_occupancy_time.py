"""Unit tests for ``metrics_data/occupancy_time.py`` (PySpark).

These tests build small, hand-crafted Spark DataFrames via the session-scoped
``spark`` fixture (see ``tests/conftest.py``). They never touch Databricks —
every check exercises the pure Spark transformation logic.

occupancy_time is a RAW dataset: ``filter_dime`` keeps every slot with a
non-null ``activity_type_required`` and applies the systemic reclassifications
(Control MC / xMC Debit Fraud / dime_invalid_notation -> 'oos') plus the fixed
legacy DIME filters (meeting/leave dimensioned_activity drop and the
wfm/credit_evolution/dote DIME-squad drop). ``social`` DIME slots are KEPT on
all dates — Social-Media occupancy is sourced from Sprinklr ``sm_jobs`` and is
intentionally ON for the whole history (a documented divergence from legacy).
Pre-cutover (< 2026-07-01) SM slots reproduce the legacy SM deck's scoring
quirks byte-for-byte: empty slot = 1800, the no-dedup occupied sum,
``dime_invalid_notation`` slots dropped (not reclassified), and only
social/social_social DIME slots in scope for SM-team agents.
There is no monthly benchmark (it moved to the metrics layer); output is one
row per slot with ``occupancy_minutes`` and ``required_minutes``.
"""

from __future__ import annotations

import datetime as dt

import pytest
from pyspark.sql import types as T

from occupancy_time import (
    DIMENSIONED_ACTIVITY_TO_OOS,
    IO_OCCUPANCY_TIME_SCHEMA,
    MEETING_LEAVE_DIMENSIONED_ACTIVITIES,
    NOCC_DIME_SQUAD_EXCLUSIONS,
    NOCC_OUT_OF_SCOPE_SQUADS,
    SHUFFLE_OCCUPIED_STATUSES,
    SLOT_DURATION_SECONDS,
    SM_DIME_SQUADS,
    SM_FULL_CREDIT_EXCLUDED_ACTIVITY_TYPES,
    build_jobs_union,
    compute_occupancy_time,
    compute_slot_occupancy,
    filter_dime,
)

# Former Social-Media cutover date (2026-07-01). The SM-occupancy gate was
# removed (social occupancy is now ON for all dates — Sprinklr-sourced, a
# documented divergence from legacy), but several tests still exercise a
# post-July date to confirm social occupancy works there too.
POST_JULY = dt.date(2026, 7, 1)

# ---------------------------------------------------------------------------
# Builders for synthetic input frames
# ---------------------------------------------------------------------------

D = dt.date(2026, 5, 18)  # pre-cutover (< 2026-07-01)
SLOT_BASE = 1_716_000_000  # any int; only start/end deltas matter


_DIME_SCHEMA = T.StructType(
    [
        T.StructField("agent", T.StringType()),
        T.StructField("date", T.DateType()),
        T.StructField("squad", T.StringType()),
        T.StructField("affiliation", T.StringType()),
        T.StructField("activity_type_required", T.StringType()),
        T.StructField("shuffle_status_required", T.StringType()),
        T.StructField("dimensioned_activity", T.StringType()),
        T.StructField("local_timestamp_dime_slot_starts_at", T.TimestampType()),
        T.StructField("slot_start_local_unix", T.LongType()),
        T.StructField("slot_end_local_unix", T.LongType()),
    ]
)

_SHUFFLE_SCHEMA = T.StructType(
    [
        T.StructField("agent", T.StringType()),
        T.StructField("actor_affiliation", T.StringType()),
        T.StructField("date", T.DateType()),
        T.StructField("job_type", T.StringType()),
        T.StructField("activity_type", T.StringType()),
        T.StructField("status", T.StringType()),
        T.StructField("net_time_spent_seconds", T.LongType()),
        T.StructField("local_start_time", T.TimestampType()),
        T.StructField("local_stop_time", T.TimestampType()),
        T.StructField("activity_start_unix", T.LongType()),
        T.StructField("activity_end_unix", T.LongType()),
    ]
)

_OOS_SCHEMA = T.StructType(
    [
        T.StructField("agent", T.StringType()),
        T.StructField("date", T.DateType()),
        T.StructField("job_classification", T.StringType()),
        T.StructField("net_time_spent_seconds", T.LongType()),
        T.StructField("local_start_date", T.TimestampType()),
        T.StructField("local_stop_date", T.TimestampType()),
        T.StructField("activity_start_unix", T.LongType()),
        T.StructField("activity_end_unix", T.LongType()),
        T.StructField("squad", T.StringType()),
        T.StructField("comment", T.StringType()),
    ]
)

_SM_SCHEMA = T.StructType(
    [
        T.StructField("agent", T.StringType()),
        T.StructField("date", T.DateType()),
        T.StructField("net_time_spent_seconds", T.LongType()),
        T.StructField("case_assignment_time", T.TimestampType()),
        T.StructField("case_unassignment_time", T.TimestampType()),
        T.StructField("activity_start_unix", T.LongType()),
        T.StructField("activity_end_unix", T.LongType()),
    ]
)

_ROSTER_SCHEMA = T.StructType(
    [
        T.StructField("agent", T.StringType()),
        T.StructField("actor_id", T.StringType()),
        T.StructField("xforce", T.StringType()),
        T.StructField("xplead", T.StringType()),
        T.StructField("team", T.StringType()),
        T.StructField("squad", T.StringType()),
        T.StructField("squad_district", T.StringType()),
        T.StructField("status", T.StringType()),
        T.StructField("shift", T.StringType()),
        T.StructField("snapshot_date", T.DateType()),
        T.StructField("snapshot_month", T.DateType()),
        T.StructField("hire_start_date", T.DateType()),
        T.StructField("last_change_date", T.DateType()),
    ]
)


def _rows_to_df(spark, schema, rows, defaults):
    data = [{**defaults, **r} for r in rows]
    return spark.createDataFrame(
        [tuple(r[f.name] for f in schema.fields) for r in data], schema
    )


def make_dime(spark, rows):
    defaults = {
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
    return _rows_to_df(spark, _DIME_SCHEMA, rows, defaults)


def make_shuffle(spark, rows):
    defaults = {
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
    return _rows_to_df(spark, _SHUFFLE_SCHEMA, rows, defaults)


def make_oos(spark, rows):
    defaults = {
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
    return _rows_to_df(spark, _OOS_SCHEMA, rows, defaults)


def make_sm(spark, rows):
    defaults = {
        "agent": "jane.doe",
        "date": D,
        "net_time_spent_seconds": 600,
        "case_assignment_time": dt.datetime(2026, 5, 18, 6, 0, 0),
        "case_unassignment_time": dt.datetime(2026, 5, 18, 6, 10, 0),
        "activity_start_unix": SLOT_BASE,
        "activity_end_unix": SLOT_BASE + 600,
    }
    return _rows_to_df(spark, _SM_SCHEMA, rows, defaults)


def empty_shuffle(spark):
    return spark.createDataFrame([], _SHUFFLE_SCHEMA)


def empty_oos(spark):
    return spark.createDataFrame([], _OOS_SCHEMA)


def make_roster(spark, rows):
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
    return _rows_to_df(spark, _ROSTER_SCHEMA, rows, defaults)


# ---------------------------------------------------------------------------
# filter_dime — minimal raw universe + systemic reclassifications + fixed filters
# ---------------------------------------------------------------------------


class TestFilterDime:
    def test_keeps_a_well_formed_row(self, spark):
        assert filter_dime(make_dime(spark, [{}])).count() == 1

    def test_drops_null_activity_type_required(self, spark):
        assert filter_dime(make_dime(spark, [{"activity_type_required": None}])).count() == 0

    @pytest.mark.parametrize("act", ["lunch_break", "time_off", "shrinkage"])
    def test_keeps_formerly_excluded_activity_types(self, spark, act):
        # Activity-type exclusions move to the metrics layer.
        assert filter_dime(make_dime(spark, [{"activity_type_required": act}])).count() == 1

    @pytest.mark.parametrize("dim_act", list(MEETING_LEAVE_DIMENSIONED_ACTIVITIES))
    def test_drops_meeting_leave_dimensioned_activity(self, spark, dim_act):
        # Fixed DIME filter (all dates): leave/meeting slots are excluded.
        assert filter_dime(make_dime(spark, [{"dimensioned_activity": dim_act}])).count() == 0

    def test_keeps_null_dimensioned_activity(self, spark):
        assert filter_dime(make_dime(spark, [{"dimensioned_activity": None}])).count() == 1

    @pytest.mark.parametrize("squad", ["wfm", "credit_evolution", "dote"])
    def test_drops_dime_squad_exclusions(self, spark, squad):
        # Fixed DIME filter (all dates): operational/WFM squads excluded.
        assert filter_dime(make_dime(spark, [{"squad": squad}])).count() == 0

    def test_drops_null_dime_squad(self, spark):
        assert filter_dime(make_dime(spark, [{"squad": None}])).count() == 0

    def test_keeps_social_dime_squad_before_july(self, spark):
        # Social DIME slots are kept on ALL dates now (date D is May 2026):
        # Social-Media occupancy is Sprinklr-sourced and intentionally ON for the
        # whole history — a documented divergence from legacy.
        assert filter_dime(make_dime(spark, [{"squad": "social"}])).count() == 1

    def test_keeps_social_dime_squad_post_july(self, spark):
        # Still kept on/after the former cutover date.
        out = filter_dime(make_dime(spark, [{"squad": "social", "date": POST_JULY}]))
        assert out.count() == 1

    def test_social_not_in_exclusion_constant(self, spark):
        # social was intentionally removed from the NOcc DIME-squad exclusions.
        assert "social" not in NOCC_DIME_SQUAD_EXCLUSIONS

    @pytest.mark.parametrize("dim_act", list(DIMENSIONED_ACTIVITY_TO_OOS))
    def test_systemic_fraud_reclassification(self, spark, dim_act):
        out = filter_dime(
            make_dime(spark, [{"dimensioned_activity": dim_act, "activity_type_required": "chat"}])
        ).collect()
        assert len(out) == 1
        assert out[0]["activity_type_required"] == "oos"

    def test_systemic_invalid_notation_reclassification(self, spark):
        # Non-SM DIME squad (default 'core'): the reclassify behavior is
        # unchanged on all dates.
        out = filter_dime(
            make_dime(spark, [{"activity_type_required": "dime_invalid_notation"}])
        ).collect()
        assert len(out) == 1
        assert out[0]["activity_type_required"] == "oos"

    @pytest.mark.parametrize("squad", list(SM_DIME_SQUADS))
    def test_sm_pre_cutover_invalid_notation_dropped_not_reclassified(self, spark, squad):
        # Legacy SM scoring (quirk 3): the SM deck's DIME filter (line 1064)
        # removes `dime_invalid_notation` slots from the universe entirely, so
        # pre-cutover SM slots (date D is May 2026) are dropped, never
        # reclassified to 'oos'.
        out = filter_dime(
            make_dime(
                spark,
                [{"squad": squad, "activity_type_required": "dime_invalid_notation"}],
            )
        )
        assert out.count() == 0

    def test_sm_post_cutover_invalid_notation_still_reclassified(self, spark):
        # On/after the cutover the corrected behavior applies to SM squads too:
        # the slot is kept and reclassified like everyone else's.
        out = filter_dime(
            make_dime(
                spark,
                [
                    {
                        "squad": "social",
                        "date": POST_JULY,
                        "activity_type_required": "dime_invalid_notation",
                    }
                ],
            )
        ).collect()
        assert len(out) == 1
        assert out[0]["activity_type_required"] == "oos"

    def test_does_not_apply_per_agent_timeoff_reclassification(self, spark):
        out = filter_dime(
            make_dime(
                spark,
                [{"agent": "maria.reyes", "date": dt.date(2026, 2, 15), "activity_type_required": "chat"}],
            )
        ).collect()
        assert len(out) == 1
        assert out[0]["activity_type_required"] == "chat"


# ---------------------------------------------------------------------------
# build_jobs_union
# ---------------------------------------------------------------------------


class TestBuildJobsUnion:
    @pytest.mark.parametrize("status", list(SHUFFLE_OCCUPIED_STATUSES))
    def test_keeps_all_three_occupied_statuses(self, spark, status):
        out = build_jobs_union(make_shuffle(spark, [{"status": status}]), empty_oos(spark))
        assert out.count() == 1

    def test_drops_other_statuses(self, spark):
        out = build_jobs_union(make_shuffle(spark, [{"status": "cancelled"}]), empty_oos(spark))
        assert out.count() == 0

    def test_synthesizes_activity_type_oos_on_oos_side(self, spark):
        out = build_jobs_union(empty_shuffle(spark), make_oos(spark, [{}])).collect()
        assert len(out) == 1
        assert out[0]["activity_type"] == "oos"

    def test_concats_shuffle_and_oos(self, spark):
        out = build_jobs_union(make_shuffle(spark, [{}]), make_oos(spark, [{}]))
        assert sorted(r["activity_type"] for r in out.collect()) == ["chat", "oos"]

    def test_sm_jobs_synthesized_as_oos(self, spark):
        out = build_jobs_union(empty_shuffle(spark), empty_oos(spark), make_sm(spark, [{}])).collect()
        assert len(out) == 1
        assert out[0]["activity_type"] == "oos"

    def test_sm_jobs_optional_default_none(self, spark):
        out = build_jobs_union(make_shuffle(spark, [{}]), empty_oos(spark))
        assert out.count() == 1

    def test_concats_all_three_sources(self, spark):
        out = build_jobs_union(make_shuffle(spark, [{}]), make_oos(spark, [{}]), make_sm(spark, [{}]))
        assert sorted(r["activity_type"] for r in out.collect()) == ["chat", "oos", "oos"]

    def test_luis_contreras_content_oos_plus_two_hour_correction(self, spark):
        out = build_jobs_union(
            empty_shuffle(spark),
            make_oos(
                spark,
                [
                    {
                        "agent": "luis.contreras",
                        "date": dt.date(2026, 3, 8),
                        "local_start_date": dt.datetime(2026, 3, 8, 23, 30),
                        "local_stop_date": dt.datetime(2026, 3, 8, 23, 45),
                        "activity_start_unix": 1_000,
                        "activity_end_unix": 1_900,
                        "squad": "content_content",
                    }
                ],
            ),
        ).collect()
        row = out[0]
        assert row["activity_start_unix"] == 1_000 + 2 * 3600
        assert row["activity_end_unix"] == 1_900 + 2 * 3600

    def test_luis_contreras_content_oos_plus_one_hour_correction(self, spark):
        out = build_jobs_union(
            empty_shuffle(spark),
            make_oos(
                spark,
                [
                    {
                        "agent": "luis.contreras",
                        "date": dt.date(2026, 3, 9),
                        "local_start_date": dt.datetime(2026, 3, 9, 10, 0),
                        "local_stop_date": dt.datetime(2026, 3, 9, 10, 15),
                        "activity_start_unix": 1_000,
                        "activity_end_unix": 1_900,
                        "squad": "content_content",
                    }
                ],
            ),
        ).collect()
        row = out[0]
        assert row["activity_start_unix"] == 1_000 + 3600
        assert row["activity_end_unix"] == 1_900 + 3600

    def test_luis_contreras_non_content_oos_not_corrected(self, spark):
        out = build_jobs_union(
            empty_shuffle(spark),
            make_oos(
                spark,
                [
                    {
                        "agent": "luis.contreras",
                        "date": dt.date(2026, 3, 9),
                        "local_start_date": dt.datetime(2026, 3, 9, 10, 0),
                        "local_stop_date": dt.datetime(2026, 3, 9, 10, 15),
                        "activity_start_unix": 1_000,
                        "activity_end_unix": 1_900,
                        "squad": "core",
                    }
                ],
            ),
        ).collect()
        row = out[0]
        assert row["activity_start_unix"] == 1_000
        assert row["activity_end_unix"] == 1_900


# ---------------------------------------------------------------------------
# compute_slot_occupancy — the heart of the metric
# ---------------------------------------------------------------------------


class TestComputeSlotOccupancy:
    def _slot_only(self, spark):
        return filter_dime(make_dime(spark, [{}]))

    def _job(self, spark, start_offset, end_offset, activity_type="chat"):
        return make_shuffle(
            spark,
            [
                {
                    "activity_type": activity_type,
                    "activity_start_unix": SLOT_BASE + start_offset,
                    "activity_end_unix": SLOT_BASE + end_offset,
                }
            ],
        )

    def _occ(self, slots, jobs):
        rows = compute_slot_occupancy(slots, jobs).collect()
        assert len(rows) == 1
        return int(rows[0]["occupancy_time"])

    def test_single_matching_job_fully_inside_slot(self, spark):
        jobs = build_jobs_union(self._job(spark, 0, 600), empty_oos(spark))
        assert self._occ(self._slot_only(spark), jobs) == 600

    def test_mismatched_activity_yields_zero(self, spark):
        jobs = build_jobs_union(self._job(spark, 0, 600, "email"), empty_oos(spark))
        assert self._occ(self._slot_only(spark), jobs) == 0

    def test_job_clipped_to_slot_start(self, spark):
        jobs = build_jobs_union(self._job(spark, -300, 600), empty_oos(spark))
        assert self._occ(self._slot_only(spark), jobs) == 600

    def test_job_clipped_to_slot_end(self, spark):
        jobs = build_jobs_union(self._job(spark, 1500, 2400), empty_oos(spark))
        assert self._occ(self._slot_only(spark), jobs) == 300

    def test_job_fully_swallows_slot(self, spark):
        jobs = build_jobs_union(self._job(spark, -300, 2400), empty_oos(spark))
        assert self._occ(self._slot_only(spark), jobs) == 1800

    def test_two_non_overlapping_jobs_sum(self, spark):
        jobs = build_jobs_union(
            make_shuffle(
                spark,
                [
                    {"activity_start_unix": SLOT_BASE, "activity_end_unix": SLOT_BASE + 600},
                    {"activity_start_unix": SLOT_BASE + 900, "activity_end_unix": SLOT_BASE + 1500},
                ],
            ),
            empty_oos(spark),
        )
        assert self._occ(self._slot_only(spark), jobs) == 1200

    def test_overlapping_jobs_dedup_no_double_count(self, spark):
        jobs = build_jobs_union(
            make_shuffle(
                spark,
                [
                    {"activity_start_unix": SLOT_BASE, "activity_end_unix": SLOT_BASE + 1200},
                    {"activity_start_unix": SLOT_BASE + 600, "activity_end_unix": SLOT_BASE + 1500},
                ],
            ),
            empty_oos(spark),
        )
        assert self._occ(self._slot_only(spark), jobs) == 1500

    def test_dedup_only_within_same_activity_type_partition(self, spark):
        jobs = build_jobs_union(
            make_shuffle(
                spark,
                [
                    {"activity_type": "chat", "activity_start_unix": SLOT_BASE, "activity_end_unix": SLOT_BASE + 1200},
                    {"activity_type": "email", "activity_start_unix": SLOT_BASE + 600, "activity_end_unix": SLOT_BASE + 1500},
                ],
            ),
            empty_oos(spark),
        )
        # only the matching ('chat') partition counts -> 1200
        assert self._occ(self._slot_only(spark), jobs) == 1200

    def test_no_overlap_job_dropped(self, spark):
        jobs = build_jobs_union(self._job(spark, 1800, 2400), empty_oos(spark))
        assert self._occ(self._slot_only(spark), jobs) == 0

    def test_slot_with_no_matching_jobs_kept_with_zero(self, spark):
        empty = build_jobs_union(empty_shuffle(spark), empty_oos(spark))
        assert self._occ(self._slot_only(spark), empty) == 0

    def test_occupancy_time_capped_at_slot_duration(self, spark):
        jobs = build_jobs_union(
            make_shuffle(
                spark,
                [
                    {"activity_start_unix": SLOT_BASE, "activity_end_unix": SLOT_BASE + 600},
                    {"activity_start_unix": SLOT_BASE + 600, "activity_end_unix": SLOT_BASE + 1200},
                    {"activity_start_unix": SLOT_BASE + 1200, "activity_end_unix": SLOT_BASE + 1800},
                ],
            ),
            empty_oos(spark),
        )
        assert self._occ(self._slot_only(spark), jobs) == 1800

    def test_oos_activity_matches_oos_slot(self, spark):
        slots = filter_dime(make_dime(spark, [{"activity_type_required": "oos"}]))
        jobs = build_jobs_union(
            empty_shuffle(spark),
            make_oos(
                spark,
                [{"activity_start_unix": SLOT_BASE + 300, "activity_end_unix": SLOT_BASE + 900}],
            ),
        )
        assert self._occ(slots, jobs) == 600


# ---------------------------------------------------------------------------
# Social-Media empty-slot full credit (pre-cutover parity quirk)
# ---------------------------------------------------------------------------


class TestSmEmptySlotFullCredit:
    """Legacy SM deck quirk: a pre-cutover SM slot with NO overlapping
    matching-activity Sprinklr case earns the full 1800 s (legacy
    ``ELSE 1800`` fall-through, lines 1129 + 1189/1223 of the SM Temp Fix
    notebook). Corrected (0) from 2026-07-01 onward."""

    def _sm_slot(self, spark, **overrides):
        row = {"squad": "social", "activity_type_required": "oos", **overrides}
        return filter_dime(make_dime(spark, [row]))

    def _no_jobs(self, spark):
        return build_jobs_union(empty_shuffle(spark), empty_oos(spark))

    def _occ(self, slots, jobs):
        rows = compute_slot_occupancy(slots, jobs).collect()
        assert len(rows) == 1
        return int(rows[0]["occupancy_time"])

    @pytest.mark.parametrize("squad", list(SM_DIME_SQUADS))
    def test_empty_sm_slot_full_credit_before_cutover(self, spark, squad):
        # (a) Date D is May 2026 (< 2026-07-01): the empty slot falls through
        # legacy's ELSE 1800 -> fully occupied.
        slots = self._sm_slot(spark, squad=squad)
        assert self._occ(slots, self._no_jobs(spark)) == SLOT_DURATION_SECONDS

    def test_empty_sm_slot_zero_on_after_cutover(self, spark):
        # (b) Corrected behavior from 2026-07-01: an empty slot is 0.
        slots = self._sm_slot(spark, date=POST_JULY)
        assert self._occ(slots, self._no_jobs(spark)) == 0

    def test_partial_overlap_keeps_actual_seconds_before_cutover(self, spark):
        # (c) Legacy's WHEN branch: a matching overlapping case keeps its
        # actual overlap seconds — the 1800 default only catches slots with NO
        # matching case (SUM(CASE WHEN activity_occuped = 1 THEN duration END)
        # is non-NULL as soon as one matching case overlaps).
        sm = make_sm(
            spark,
            [{"activity_start_unix": SLOT_BASE, "activity_end_unix": SLOT_BASE + 600}],
        )
        jobs = build_jobs_union(empty_shuffle(spark), empty_oos(spark), sm)
        assert self._occ(self._sm_slot(spark), jobs) == 600

    def test_non_sm_empty_slot_stays_zero_before_cutover(self, spark):
        # (d) Non-SM slot (default DIME squad 'core') with no overlap: unchanged.
        slots = filter_dime(make_dime(spark, [{}]))
        assert self._occ(slots, self._no_jobs(spark)) == 0

    def test_only_non_matching_overlap_still_gets_full_credit(self, spark):
        # Legacy's SUM(CASE WHEN activity_occuped = 1 THEN duration END) is
        # NULL when overlapping jobs exist but none match the slot's activity
        # type, so that slot ALSO falls through to ELSE 1800.
        chat_job = make_shuffle(
            spark,
            [
                {
                    "activity_type": "chat",
                    "activity_start_unix": SLOT_BASE,
                    "activity_end_unix": SLOT_BASE + 600,
                }
            ],
        )
        jobs = build_jobs_union(chat_job, empty_oos(spark))
        assert self._occ(self._sm_slot(spark), jobs) == SLOT_DURATION_SECONDS

    @pytest.mark.parametrize("act", ["lunch_break", "time_off", "shrinkage"])
    def test_non_productive_sm_slot_not_credited(self, spark, act):
        # Legacy's own DIME filter (lines 1064/1079) drops these before the
        # occupancy CASE, so they never earn the 1800 default.
        slots = self._sm_slot(spark, activity_type_required=act)
        assert self._occ(slots, self._no_jobs(spark)) == 0

    def test_invalid_notation_sm_slot_dropped_from_universe(self, spark):
        # Legacy drops `dime_invalid_notation` slots outright (line 1064).
        # Pre-cutover SM slots now reproduce that drop in filter_dime (legacy
        # SM scoring, quirk 3) instead of reclassifying to 'oos', so the slot
        # never reaches the occupancy calc — and can never earn the 1800
        # default.
        slots = self._sm_slot(spark, activity_type_required="dime_invalid_notation")
        assert slots.count() == 0
        assert compute_slot_occupancy(slots, self._no_jobs(spark)).count() == 0

    def test_excluded_activity_types_constant_matches_legacy_filter(self):
        assert SM_FULL_CREDIT_EXCLUDED_ACTIVITY_TYPES == (
            "lunch_break",
            "dime_invalid_notation",
            "time_off",
            "shrinkage",
        )

    def test_end_to_end_empty_sm_slot_full_credit(self, spark):
        # Through compute_occupancy_time: a pre-cutover social slot with no SM
        # case at all lands with the full 30 occupied minutes.
        roster = make_roster(spark, [{"squad": "social", "team": "social media"}])
        dime = make_dime(
            spark, [{"squad": "social", "activity_type_required": "oos"}]
        )
        out = compute_occupancy_time(
            roster, dime, empty_shuffle(spark), empty_oos(spark)
        ).collect()
        assert len(out) == 1
        assert out[0]["occupancy_minutes"] == SLOT_DURATION_SECONDS / 60.0


# ---------------------------------------------------------------------------
# Legacy SM no-dedup occupied sum (pre-cutover parity quirk)
# ---------------------------------------------------------------------------


class TestSmNoDedupOccupiedSum:
    """Legacy SM deck quirk: no interval dedup — ``occupancy_agg`` sums the
    RAW clipped job durations per slot (lines 1123-1135 of the SM Temp Fix
    notebook), capped at 1800 downstream (lines 1189/1223):
    ``occ = LEAST(Σ clip(job), 1800)``. Reproduced for SM pre-cutover slots
    with a matching overlap; non-SM slots (all dates) and SM slots on/after
    2026-07-01 keep the ``prev_max_end`` interval dedup."""

    def _sm_slot(self, spark, **overrides):
        row = {"squad": "social", "activity_type_required": "oos", **overrides}
        return filter_dime(make_dime(spark, [row]))

    def _occ(self, slots, jobs):
        rows = compute_slot_occupancy(slots, jobs).collect()
        assert len(rows) == 1
        return int(rows[0]["occupancy_time"])

    def _overlapping_sm_cases(self, spark, day=D):
        # [0, 1200) + [600, 1500): dedup (union coverage) = 1500; raw no-dedup
        # sum = 1200 + 900 = 2100 -> capped at 1800.
        return make_sm(
            spark,
            [
                {"date": day, "activity_start_unix": SLOT_BASE, "activity_end_unix": SLOT_BASE + 1200},
                {"date": day, "activity_start_unix": SLOT_BASE + 600, "activity_end_unix": SLOT_BASE + 1500},
            ],
        )

    def test_sm_pre_cutover_overlap_sums_without_dedup(self, spark):
        # (a) two overlapping cases [0,600) + [300,900): dedup would give the
        # 900 s union; legacy's raw sum gives 600 + 600 = 1200 s.
        sm = make_sm(
            spark,
            [
                {"activity_start_unix": SLOT_BASE, "activity_end_unix": SLOT_BASE + 600},
                {"activity_start_unix": SLOT_BASE + 300, "activity_end_unix": SLOT_BASE + 900},
            ],
        )
        jobs = build_jobs_union(empty_shuffle(spark), empty_oos(spark), sm)
        assert self._occ(self._sm_slot(spark), jobs) == 1200

    def test_sm_pre_cutover_no_dedup_sum_capped_at_1800(self, spark):
        # (a) raw sum 2100 s exceeds the slot -> LEAST(..., 1800).
        jobs = build_jobs_union(
            empty_shuffle(spark), empty_oos(spark), self._overlapping_sm_cases(spark)
        )
        assert self._occ(self._sm_slot(spark), jobs) == SLOT_DURATION_SECONDS

    def test_non_sm_slot_still_dedups(self, spark):
        # (b) the same overlap pattern on a non-SM slot (default DIME squad
        # 'core') keeps the union coverage: 1500 s, not 2100 -> 1800.
        jobs = build_jobs_union(
            make_shuffle(
                spark,
                [
                    {"activity_start_unix": SLOT_BASE, "activity_end_unix": SLOT_BASE + 1200},
                    {"activity_start_unix": SLOT_BASE + 600, "activity_end_unix": SLOT_BASE + 1500},
                ],
            ),
            empty_oos(spark),
        )
        slots = filter_dime(make_dime(spark, [{}]))
        assert self._occ(slots, jobs) == 1500

    def test_sm_post_cutover_slot_still_dedups(self, spark):
        # (b) SM slot on/after the cutover: corrected behavior -> dedup (1500).
        slots = self._sm_slot(spark, date=POST_JULY)
        jobs = build_jobs_union(
            empty_shuffle(spark),
            empty_oos(spark),
            self._overlapping_sm_cases(spark, day=POST_JULY),
        )
        assert self._occ(slots, jobs) == 1500


# ---------------------------------------------------------------------------
# Legacy SM slot scope — social DIME squads only (pre-cutover parity quirk)
# ---------------------------------------------------------------------------


class TestSmNonSocialDimeSlotScope:
    """Legacy SM deck quirk: only ``agent_dime_squad IN ('social',
    'social_social')`` slots are scored (line 1065 of the SM Temp Fix
    notebook), so an SM-team agent's slots from other DIME squads are out of
    scope before 2026-07-01. The performance team is only known after the
    roster join, so the drop happens there; non-SM teams keep all their slots
    and post-cutover SM keeps them too."""

    def _run(self, spark, *, team, dime_squad, day, month):
        roster = make_roster(
            spark,
            [
                {
                    "squad": "social" if team == "social media" else "core",
                    "team": team,
                    "snapshot_month": month,
                    "snapshot_date": dt.date(month.year, month.month, 28),
                }
            ],
        )
        dime = make_dime(spark, [{"squad": dime_squad, "date": day}])
        return compute_occupancy_time(
            roster, dime, empty_shuffle(spark), empty_oos(spark)
        ).collect()

    def test_sm_team_non_social_dime_slot_dropped_pre_cutover(self, spark):
        # (d) SM-team agent with a 'collections' DIME slot in May 2026: legacy
        # never scored it -> dropped post-roster-join.
        out = self._run(
            spark, team="social media", dime_squad="collections",
            day=D, month=dt.date(2026, 5, 1),
        )
        assert len(out) == 0

    def test_sm_team_social_dime_slot_kept_pre_cutover(self, spark):
        # The social DIME slot itself stays in scope pre-cutover.
        out = self._run(
            spark, team="social media", dime_squad="social",
            day=D, month=dt.date(2026, 5, 1),
        )
        assert len(out) == 1

    def test_sm_team_non_social_dime_slot_kept_post_cutover(self, spark):
        # (d) On/after the cutover the corrected behavior applies: the SM-team
        # agent's non-social DIME slot is kept.
        out = self._run(
            spark, team="social media", dime_squad="collections",
            day=POST_JULY, month=dt.date(2026, 7, 1),
        )
        assert len(out) == 1

    def test_non_sm_team_non_social_dime_slot_kept_pre_cutover(self, spark):
        # Invariant: non-SM teams keep all their (non-excluded) DIME slots on
        # all dates — byte-identical to the pre-fix behavior.
        out = self._run(
            spark, team="core", dime_squad="collections",
            day=D, month=dt.date(2026, 5, 1),
        )
        assert len(out) == 1

    def test_flag_column_does_not_leak_into_output(self, spark):
        out = self._run(
            spark, team="core", dime_squad="collections",
            day=D, month=dt.date(2026, 5, 1),
        )
        assert "non_sm_dime_squad_pre_cutover" not in out[0].asDict()


# ---------------------------------------------------------------------------
# compute_occupancy_time — end-to-end
# ---------------------------------------------------------------------------


def _baseline_inputs(spark):
    roster = make_roster(spark, [{}])
    dime = make_dime(spark, [{}])
    shuffle = make_shuffle(
        spark, [{"activity_start_unix": SLOT_BASE, "activity_end_unix": SLOT_BASE + 600}]
    )
    oos = empty_oos(spark)
    return roster, dime, shuffle, oos


class TestComputeOccupancyTime:
    def test_basic_end_to_end_shape(self, spark):
        roster, dime, shuffle, oos = _baseline_inputs(spark)
        out = compute_occupancy_time(roster, dime, shuffle, oos).collect()
        assert len(out) == 1
        row = out[0]
        assert row["agent"] == "jane.doe"
        assert row["occupancy_minutes"] == 600 / 60.0
        assert row["required_minutes"] == SLOT_DURATION_SECONDS / 60.0
        assert row["activity_type_required"] == "chat"
        assert row["squad"] == "core"
        assert row["district"] == "northeast"
        assert row["shift"] == "morning"
        assert row["slot_time"] == "06:00:00"

    def test_output_column_order_matches_schema(self, spark):
        roster, dime, shuffle, oos = _baseline_inputs(spark)
        out = compute_occupancy_time(roster, dime, shuffle, oos)
        assert out.columns == [c for c, _ in IO_OCCUPANCY_TIME_SCHEMA]

    def test_drops_inactive_agent(self, spark):
        roster = make_roster(spark, [{"status": "inactive"}])
        _, dime, shuffle, oos = _baseline_inputs(spark)
        assert compute_occupancy_time(roster, dime, shuffle, oos).count() == 0

    def test_content_squad_kept(self, spark):
        # Roster squad 'content' is in scope (DIME squad is separate); use a
        # non-excluded DIME squad on the slot.
        roster = make_roster(spark, [{"squad": "content"}])
        _, dime, shuffle, oos = _baseline_inputs(spark)
        out = compute_occupancy_time(roster, dime, shuffle, oos).collect()
        assert len(out) == 1
        assert out[0]["squad"] == "content"

    def test_keeps_slot_with_no_matching_jobs(self, spark):
        roster, dime, _, _ = _baseline_inputs(spark)
        out = compute_occupancy_time(roster, dime, empty_shuffle(spark), empty_oos(spark)).collect()
        assert len(out) == 1
        assert out[0]["occupancy_minutes"] == 0.0

    def test_sm_jobs_populate_social_occupancy_post_july(self, spark):
        # On/after the former cutover: social agent with an 'oos' DIME slot (DIME
        # squad 'social' kept) and no shuffle/taskmaster jobs gets occupancy from
        # the Sprinklr SM case (10 min overlap).
        cut = POST_JULY
        u = int(dt.datetime(cut.year, cut.month, cut.day, 6, 0, 0, tzinfo=dt.timezone.utc).timestamp())
        roster = make_roster(
            spark,
            [{"squad": "social", "team": "social media", "snapshot_month": cut, "snapshot_date": dt.date(2026, 7, 31)}],
        )
        dime = make_dime(
            spark,
            [
                {
                    "squad": "social",
                    "activity_type_required": "oos",
                    "date": cut,
                    "local_timestamp_dime_slot_starts_at": dt.datetime(cut.year, cut.month, cut.day, 6, 0, 0),
                    "slot_start_local_unix": u,
                    "slot_end_local_unix": u + SLOT_DURATION_SECONDS,
                }
            ],
        )
        sm = make_sm(
            spark,
            [{"date": cut, "activity_start_unix": u, "activity_end_unix": u + 600}],
        )
        out = compute_occupancy_time(
            roster, dime, empty_shuffle(spark), empty_oos(spark), sm
        ).collect()
        assert len(out) == 1
        assert out[0]["squad"] == "social"
        assert out[0]["occupancy_minutes"] == 600 / 60.0

    def test_sm_jobs_populate_social_occupancy_before_july(self, spark):
        # Social-Media occupancy is now ON for all dates (Sprinklr source). Even
        # pre-July (date D is May 2026), a social DIME slot is KEPT and the SM
        # case populates its occupancy — a documented divergence from legacy,
        # which dropped social slots and had no Sprinklr source.
        u = SLOT_BASE
        roster = make_roster(spark, [{"squad": "social", "team": "social media"}])
        dime = make_dime(
            spark,
            [
                {
                    "squad": "social",
                    "activity_type_required": "oos",
                    "slot_start_local_unix": u,
                    "slot_end_local_unix": u + SLOT_DURATION_SECONDS,
                }
            ],
        )
        sm = make_sm(
            spark, [{"activity_start_unix": u, "activity_end_unix": u + 600}]
        )
        out = compute_occupancy_time(
            roster, dime, empty_shuffle(spark), empty_oos(spark), sm
        ).collect()
        assert len(out) == 1
        assert out[0]["squad"] == "social"
        assert out[0]["occupancy_minutes"] == 600 / 60.0

    def test_handles_empty_inputs(self, spark):
        empty_dime = spark.createDataFrame([], _DIME_SCHEMA)
        out = compute_occupancy_time(
            make_roster(spark, [{}]), empty_dime, empty_shuffle(spark), empty_oos(spark)
        )
        assert out.count() == 0

    def test_uses_month_specific_roster_snapshot(self, spark):
        roster = make_roster(
            spark,
            [
                {"snapshot_month": dt.date(2026, 4, 1), "snapshot_date": dt.date(2026, 4, 30), "squad": "wrong-april"},
                {"snapshot_month": dt.date(2026, 5, 1), "snapshot_date": dt.date(2026, 5, 31), "squad": "correct-may"},
            ],
        )
        _, dime, shuffle, oos = _baseline_inputs(spark)
        out = compute_occupancy_time(roster, dime, shuffle, oos).collect()
        assert len(out) == 1
        assert out[0]["squad"] == "correct-may"

    def test_duplicate_roster_rows_do_not_fan_out_slots(self, spark):
        # Content/enablement agents get ≥2 rows per (agent, snapshot_month) from
        # agent_information's content branch (cross-join of multiple Google-Sheet
        # rows against every month, differing only in target_squad). Those rows
        # are identical on every dimension occupancy selects. The roster join must
        # NOT fan out: one slot must stay one row with occupancy <= 30 min, not
        # double to 60. The roster is deduped to one row per (agent, month) first.
        roster = make_roster(
            spark,
            [
                {"agent": "omar.ramirez", "squad": "content", "team": "content"},
                {"agent": "omar.ramirez", "squad": "content", "team": "content"},
            ],
        )
        dime = make_dime(spark, [{"agent": "omar.ramirez"}])
        shuffle = make_shuffle(
            spark,
            [
                {
                    "agent": "omar.ramirez",
                    "activity_start_unix": SLOT_BASE,
                    "activity_end_unix": SLOT_BASE + 600,
                }
            ],
        )
        out = compute_occupancy_time(roster, dime, shuffle, empty_oos(spark)).collect()
        # Exactly one row per slot (no doubling), occupancy is the single 10-min
        # value (600 / 60), not 20, and never exceeds the 30-min slot.
        assert len(out) == 1
        assert out[0]["occupancy_minutes"] == 600 / 60.0
        assert out[0]["occupancy_minutes"] <= SLOT_DURATION_SECONDS / 60.0

    def test_duplicate_dime_squad_slots_collapse_and_cap(self, spark):
        # The same physical slot (agent, date, slot_start, activity_type) can
        # appear under >1 DIME agent_dime_squad — most often because
        # append_missing_dime_slots re-adds a now-backfilled slot under a
        # different squad label (e.g. "Content" vs "content_content"). Jobs match
        # on agent/date/time (not squad), so each duplicate carries identical
        # occupancy. They MUST collapse to one row per slot, and total occupancy
        # is capped at the slot length AFTER summing (legacy
        # normalized_occupancy_final does LEAST(SUM(occupancy_time), 1800)).
        dime = make_dime(
            spark,
            [
                {"agent": "omar.ramirez", "squad": "content_content", "activity_type_required": "oos"},
                {"agent": "omar.ramirez", "squad": "content", "activity_type_required": "oos"},
            ],
        )
        roster = make_roster(spark, [{"agent": "omar.ramirez", "squad": "content", "team": "content"}])
        # A job that fully covers the 30-min slot, so each duplicate row would be
        # 1800s; summed = 3600s, which must cap to 1800s (30 min), not 60.
        shuffle = make_shuffle(
            spark,
            [
                {
                    "agent": "omar.ramirez",
                    "activity_type": "oos",
                    "activity_start_unix": SLOT_BASE,
                    "activity_end_unix": SLOT_BASE + SLOT_DURATION_SECONDS,
                }
            ],
        )
        out = compute_occupancy_time(roster, dime, shuffle, empty_oos(spark)).collect()
        assert len(out) == 1
        assert out[0]["occupancy_minutes"] == SLOT_DURATION_SECONDS / 60.0  # 30, not 60


def test_out_of_scope_squads_constant_is_empty():
    assert NOCC_OUT_OF_SCOPE_SQUADS == ()


# ---------------------------------------------------------------------------
# Night-shift attribution (>= 2026-07-01)
# ---------------------------------------------------------------------------


class TestNightShiftAttribution:
    @staticmethod
    def _local_unix(ts: str) -> int:
        return int(dt.datetime.fromisoformat(ts).replace(tzinfo=dt.timezone.utc).timestamp())

    def _night_slot(self, spark, day, ts):
        u = self._local_unix(ts)
        return make_dime(
            spark,
            [
                {
                    "date": day,
                    "local_timestamp_dime_slot_starts_at": dt.datetime.fromisoformat(ts),
                    "slot_start_local_unix": u,
                    "slot_end_local_unix": u + SLOT_DURATION_SECONDS,
                }
            ],
        )

    def test_night_tail_attributed_to_shift_start_day(self, spark):
        roster = make_roster(
            spark,
            [{"shift": "night", "snapshot_month": dt.date(2026, 7, 1), "snapshot_date": dt.date(2026, 7, 31)}],
        )
        dime = self._night_slot(spark, dt.date(2026, 7, 6), "2026-07-06 03:00:00")
        out = compute_occupancy_time(roster, dime, empty_shuffle(spark), empty_oos(spark)).collect()
        assert len(out) == 1
        assert out[0]["date"] == dt.date(2026, 7, 5)
        assert out[0]["slot_time"] == "03:00:00"

    def test_morning_agent_not_re_attributed(self, spark):
        roster = make_roster(
            spark,
            [{"shift": "morning", "snapshot_month": dt.date(2026, 7, 1), "snapshot_date": dt.date(2026, 7, 31)}],
        )
        dime = self._night_slot(spark, dt.date(2026, 7, 6), "2026-07-06 03:00:00")
        out = compute_occupancy_time(roster, dime, empty_shuffle(spark), empty_oos(spark)).collect()
        assert len(out) == 1
        assert out[0]["date"] == dt.date(2026, 7, 6)
