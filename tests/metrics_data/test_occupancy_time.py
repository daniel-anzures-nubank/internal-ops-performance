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
        out = filter_dime(
            make_dime(spark, [{"activity_type_required": "dime_invalid_notation"}])
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
