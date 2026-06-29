"""Unit tests for ``metrics_data/jobs_raw.py`` (PySpark).

Small hand-crafted Spark DataFrames via the session-scoped ``spark`` fixture
(see ``tests/conftest.py``). No warehouse.

jobs_raw is a RAW per-job feed: one row per individual job (shuffle + OOS),
ALL shuffle statuses kept, with raw start/end times, the derived job_id, the
roster attribution (LEFT-joined — every job survives so the benchmark pool is
complete), and a ``required_activity_on_day_flag`` computed from the NTPJ DIME
definition of "scheduled". No aggregation, no expected-duration benchmark
(those move to the metrics layer).
"""

from __future__ import annotations

import datetime as dt

import pytest
from pyspark.sql import types as T

from jobs_raw import (
    DIME_ACTIVITY_TYPE_EXCLUSIONS,
    DIME_SQUAD_EXCLUSIONS,
    IO_JOBS_RAW_SCHEMA,
    NTPJ_OUT_OF_SCOPE_SQUADS,
    build_jobs_union,
    build_oos_jobs_raw,
    build_shuffle_jobs_raw,
    compute_jobs_raw,
    compute_required_activities,
    filter_dime,
)

D = dt.date(2026, 5, 18)


# ---------------------------------------------------------------------------
# Builders for synthetic input frames
# ---------------------------------------------------------------------------

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
        "activity_start_unix": 0,
        "activity_end_unix": 600,
    }
    return _rows_to_df(spark, _SHUFFLE_SCHEMA, rows, defaults)


def make_oos(spark, rows):
    defaults = {
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
    return _rows_to_df(spark, _OOS_SCHEMA, rows, defaults)


def empty_oos(spark):
    return spark.createDataFrame([], _OOS_SCHEMA)


def empty_shuffle(spark):
    return spark.createDataFrame([], _SHUFFLE_SCHEMA)


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
        "slot_start_local_unix": 0,
        "slot_end_local_unix": 1800,
    }
    return _rows_to_df(spark, _DIME_SCHEMA, rows, defaults)


def empty_dime(spark):
    return spark.createDataFrame([], _DIME_SCHEMA)


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
# build_shuffle_jobs_raw / build_oos_jobs_raw / build_jobs_union
# ---------------------------------------------------------------------------


class TestBuildShuffleJobsRaw:
    def test_one_row_per_job_all_statuses_kept(self, spark):
        out = build_shuffle_jobs_raw(
            make_shuffle(
                spark,
                [
                    {"status": "finished"},
                    {"status": "transferred"},
                    {"status": "skipped"},
                    {"status": "cancelled"},
                ],
            )
        )
        assert out.count() == 4

    def test_maps_start_end_and_duration_and_job_id(self, spark):
        row = build_shuffle_jobs_raw(make_shuffle(spark, [{}])).collect()[0]
        assert row["start_time"] == dt.datetime(2026, 5, 18, 6, 0, 0)
        assert row["end_time"] == dt.datetime(2026, 5, 18, 6, 10, 0)
        assert row["duration_seconds"] == 600
        assert row["job_id"] == "chat - finished"

    @pytest.mark.parametrize(
        "activity_type,job_type,status,expected",
        [
            ("email", "voice", "finished", "email - voice - finished"),
            ("backoffice", "manual", "finished", "bko - manual - finished"),
            ("chat", "ignored", "finished", "chat - finished"),
            ("voice", "ignored", "transferred", "voice - transferred"),
        ],
    )
    def test_job_id_construction(self, spark, activity_type, job_type, status, expected):
        out = build_shuffle_jobs_raw(
            make_shuffle(
                spark,
                [{"activity_type": activity_type, "job_type": job_type, "status": status}],
            )
        ).collect()
        assert out[0]["job_id"] == expected

    def test_empty_returns_empty(self, spark):
        assert build_shuffle_jobs_raw(empty_shuffle(spark)).count() == 0


class TestBuildOosJobsRaw:
    def test_synthesizes_activity_type_and_status(self, spark):
        row = build_oos_jobs_raw(make_oos(spark, [{}])).collect()[0]
        assert row["activity_type"] == "oos"
        assert row["status"] == "finished"
        assert row["job_id"] == "oos - support_ticket"
        assert row["start_time"] == dt.datetime(2026, 5, 18, 7, 0, 0)
        assert row["duration_seconds"] == 900

    def test_passes_through_non_content_squad_unchanged(self, spark):
        out = build_oos_jobs_raw(
            make_oos(
                spark,
                [{"squad": "core", "job_classification": "Some Classification (OOS_CONT)"}],
            )
        ).collect()
        assert out[0]["job_id"] == "oos - Some Classification (OOS_CONT)"

    def test_content_squad_classification_cleaned(self, spark):
        out = build_oos_jobs_raw(
            make_oos(
                spark,
                [{"squad": "mx_content", "job_classification": "Publish Bug (OOS_CONT)"}],
            )
        ).collect()
        assert out[0]["job_id"] == "oos - publish_bug"


class TestBuildJobsUnion:
    def test_concats_shuffle_and_oos(self, spark):
        out = build_jobs_union(make_shuffle(spark, [{}]), make_oos(spark, [{}]))
        assert sorted(r["activity_type"] for r in out.collect()) == ["chat", "oos"]


# ---------------------------------------------------------------------------
# filter_dime — NTPJ filter (used for the required flag)
# ---------------------------------------------------------------------------


class TestFilterDime:
    def test_keeps_well_formed_row(self, spark):
        assert filter_dime(make_dime(spark, [{}])).count() == 1

    @pytest.mark.parametrize("bad", list(DIME_ACTIVITY_TYPE_EXCLUSIONS))
    def test_drops_excluded_activity_types(self, spark, bad):
        assert filter_dime(make_dime(spark, [{"activity_type_required": bad}])).count() == 0

    @pytest.mark.parametrize("bad", list(DIME_SQUAD_EXCLUSIONS))
    def test_drops_excluded_squads(self, spark, bad):
        assert filter_dime(make_dime(spark, [{"squad": bad}])).count() == 0

    def test_drops_null_activity_type(self, spark):
        assert filter_dime(make_dime(spark, [{"activity_type_required": None}])).count() == 0

    def test_drops_null_squad(self, spark):
        assert filter_dime(make_dime(spark, [{"squad": None}])).count() == 0

    @pytest.mark.parametrize("bad", ["pause", "training", "lunch", None])
    def test_drops_invalid_shuffle_status(self, spark, bad):
        assert filter_dime(make_dime(spark, [{"shuffle_status_required": bad}])).count() == 0

    @pytest.mark.parametrize("good", ["available", "oos"])
    def test_keeps_available_and_oos_shuffle_status(self, spark, good):
        assert filter_dime(make_dime(spark, [{"shuffle_status_required": good}])).count() == 1

    def test_keeps_meeting_dimensioned_activity(self, spark):
        # NTPJ does NOT apply the meeting/leave dimensioned_activity filter
        # (legacy dime_ntpj has no such filter); meeting slots stay.
        assert filter_dime(make_dime(spark, [{"dimensioned_activity": "Mouring"}])).count() == 1


# ---------------------------------------------------------------------------
# compute_required_activities
# ---------------------------------------------------------------------------


class TestComputeRequiredActivities:
    def test_distinct_agent_date_activity(self, spark):
        out = compute_required_activities(
            filter_dime(
                make_dime(
                    spark,
                    [
                        {"activity_type_required": "chat"},
                        {
                            "activity_type_required": "chat",
                            "slot_start_local_unix": 1800,
                            "slot_end_local_unix": 3600,
                        },
                        {
                            "activity_type_required": "email",
                            "slot_start_local_unix": 3600,
                            "slot_end_local_unix": 5400,
                        },
                    ],
                )
            )
        ).collect()
        assert sorted(r["activity_type"] for r in out) == ["chat", "email"]
        assert all(r["required_flag"] == 1 for r in out)


# ---------------------------------------------------------------------------
# compute_jobs_raw — end to end
# ---------------------------------------------------------------------------


class TestComputeJobsRaw:
    def test_basic_end_to_end_chat_path(self, spark):
        out = compute_jobs_raw(
            make_roster(spark, [{}]),
            make_dime(spark, [{}]),
            make_shuffle(spark, [{"net_time_spent_seconds": 100}]),
            empty_oos(spark),
        ).collect()
        assert len(out) == 1
        row = out[0]
        assert row["agent"] == "jane.doe"
        assert row["job_id"] == "chat - finished"
        assert row["activity_type"] == "chat"
        assert row["status"] == "finished"
        assert row["duration_seconds"] == 100
        assert row["required_activity_on_day_flag"] == 1
        assert row["district"] == "northeast"
        assert row["shift"] == "morning"
        assert row["roster_status"] == "active"

    def test_two_jobs_two_rows(self, spark):
        out = compute_jobs_raw(
            make_roster(spark, [{}]),
            make_dime(spark, [{}]),
            make_shuffle(
                spark,
                [{"net_time_spent_seconds": 100}, {"net_time_spent_seconds": 200}],
            ),
            empty_oos(spark),
        )
        assert out.count() == 2

    def test_job_without_required_activity_kept_with_flag_zero(self, spark):
        # OOS job on a day the agent only had chat scheduled → flag 0, kept.
        out = compute_jobs_raw(
            make_roster(spark, [{}]),
            make_dime(spark, [{"activity_type_required": "chat"}]),
            empty_shuffle(spark),
            make_oos(spark, [{"job_classification": "ad_hoc"}]),
        ).collect()
        assert len(out) == 1
        assert out[0]["activity_type"] == "oos"
        assert out[0]["required_activity_on_day_flag"] == 0

    @pytest.mark.parametrize("status", ["finished", "transferred", "skipped", "cancelled"])
    def test_all_shuffle_statuses_kept(self, spark, status):
        out = compute_jobs_raw(
            make_roster(spark, [{}]),
            make_dime(spark, [{}]),
            make_shuffle(spark, [{"status": status}]),
            empty_oos(spark),
        ).collect()
        assert len(out) == 1
        assert out[0]["status"] == status

    def test_inactive_agent_kept_with_roster_status(self, spark):
        # Legacy builds the benchmark from the un-roster-filtered pool, so jobs
        # raw keeps inactive-roster jobs (with roster_status carried) and the
        # metric layer scopes the contribution to active. So they are NOT dropped
        # here (unlike the old inner-join behaviour).
        out = compute_jobs_raw(
            make_roster(spark, [{"status": "inactive"}]),
            make_dime(spark, [{}]),
            make_shuffle(spark, [{}]),
            empty_oos(spark),
        ).collect()
        assert len(out) == 1
        assert out[0]["roster_status"] == "inactive"

    def test_job_without_roster_row_kept_with_null_dims(self, spark):
        # A job whose agent has no roster row that month still survives (it can
        # feed the benchmark); its roster dims/status are NULL.
        out = compute_jobs_raw(
            make_roster(spark, [{"agent": "someone.else"}]),
            make_dime(spark, [{}]),
            make_shuffle(spark, [{}]),
            empty_oos(spark),
        ).collect()
        assert len(out) == 1
        assert out[0]["agent"] == "jane.doe"
        assert out[0]["roster_status"] is None
        assert out[0]["squad"] is None

    def test_out_of_scope_squads_constant_is_empty(self):
        assert NTPJ_OUT_OF_SCOPE_SQUADS == ()

    @pytest.mark.parametrize("squad", ["social", "content"])
    def test_social_and_content_squads_kept(self, spark, squad):
        out = compute_jobs_raw(
            make_roster(spark, [{"squad": squad}]),
            make_dime(spark, [{}]),
            make_shuffle(spark, [{}]),
            empty_oos(spark),
        ).collect()
        assert len(out) == 1
        assert out[0]["squad"] == squad

    def test_output_columns_match_schema(self, spark):
        out = compute_jobs_raw(
            make_roster(spark, [{}]),
            make_dime(spark, [{}]),
            make_shuffle(spark, [{}]),
            empty_oos(spark),
        )
        assert out.columns == [c for c, _ in IO_JOBS_RAW_SCHEMA]

    def test_joins_correct_month_of_roster(self, spark):
        roster = make_roster(
            spark,
            [
                {
                    "snapshot_date": dt.date(2026, 4, 30),
                    "snapshot_month": dt.date(2026, 4, 1),
                    "squad": "credit",
                    "squad_district": "old_district",
                },
                {
                    "snapshot_date": dt.date(2026, 5, 31),
                    "snapshot_month": dt.date(2026, 5, 1),
                    "squad": "core",
                    "squad_district": "new_district",
                },
            ],
        )
        april_day = dt.date(2026, 4, 15)
        dime = make_dime(
            spark,
            [
                {
                    "date": april_day,
                    "local_timestamp_dime_slot_starts_at": dt.datetime(2026, 4, 15, 6, 0, 0),
                }
            ],
        )
        shuffle = make_shuffle(
            spark,
            [
                {
                    "date": april_day,
                    "local_start_time": dt.datetime(2026, 4, 15, 6, 0, 0),
                    "local_stop_time": dt.datetime(2026, 4, 15, 6, 10, 0),
                }
            ],
        )
        out = compute_jobs_raw(roster, dime, shuffle, empty_oos(spark)).collect()
        assert len(out) == 1
        assert out[0]["squad"] == "credit"
        assert out[0]["district"] == "old_district"

    def test_2025_roster_pinned_to_december(self, spark):
        # A 2025-06 job pins to the 2025-12-01 roster snapshot (legacy
        # ntpj_all_info_2025), not its own month.
        roster = make_roster(
            spark,
            [
                {
                    "snapshot_date": dt.date(2025, 6, 30),
                    "snapshot_month": dt.date(2025, 6, 1),
                    "squad": "june-squad",
                },
                {
                    "snapshot_date": dt.date(2025, 12, 31),
                    "snapshot_month": dt.date(2025, 12, 1),
                    "squad": "december-squad",
                },
            ],
        )
        jun_day = dt.date(2025, 6, 10)
        dime = make_dime(
            spark,
            [
                {
                    "date": jun_day,
                    "local_timestamp_dime_slot_starts_at": dt.datetime(2025, 6, 10, 6, 0, 0),
                }
            ],
        )
        shuffle = make_shuffle(
            spark,
            [
                {
                    "date": jun_day,
                    "local_start_time": dt.datetime(2025, 6, 10, 6, 0, 0),
                    "local_stop_time": dt.datetime(2025, 6, 10, 6, 10, 0),
                }
            ],
        )
        out = compute_jobs_raw(roster, dime, shuffle, empty_oos(spark)).collect()
        assert len(out) == 1
        assert out[0]["squad"] == "december-squad"

    def test_duplicate_roster_rows_do_not_fan_out_jobs(self, spark):
        # Content/enablement agents get >=2 rows per (agent, snapshot_month) from
        # agent_information's content branch (differing only in target_squad). The
        # LEFT join must NOT fan out: one job stays one row.
        roster = make_roster(
            spark,
            [
                {"agent": "omar.ramirez", "squad": "content", "team": "content"},
                {"agent": "omar.ramirez", "squad": "content", "team": "content"},
            ],
        )
        dime = make_dime(spark, [{"agent": "omar.ramirez"}])
        shuffle = make_shuffle(spark, [{"agent": "omar.ramirez"}])
        out = compute_jobs_raw(roster, dime, shuffle, empty_oos(spark)).collect()
        assert len(out) == 1

    def test_handles_tz_aware_roster_snapshot_month(self, spark):
        # snapshot_month given as a DATE; trunc keeps it month-start.
        out = compute_jobs_raw(
            make_roster(spark, [{"snapshot_month": dt.date(2026, 5, 1)}]),
            make_dime(spark, [{}]),
            make_shuffle(spark, [{}]),
            empty_oos(spark),
        )
        assert out.count() == 1

    def test_handles_empty_inputs(self, spark):
        out = compute_jobs_raw(
            make_roster(spark, [{}]),
            empty_dime(spark),
            empty_shuffle(spark),
            empty_oos(spark),
        )
        assert out.count() == 0


# ---------------------------------------------------------------------------
# Night-shift attribution (>= 2026-07-01): both jobs and the DIME required-set
# roll the early-morning tail back to the day the shift started, so the
# required-flag join stays aligned.
# ---------------------------------------------------------------------------


def _local_unix(ts: str) -> int:
    return int(dt.datetime.fromisoformat(ts).replace(tzinfo=dt.timezone.utc).timestamp())


class TestNightShiftAttribution:
    def _night_roster(self, spark, **over):
        return make_roster(
            spark,
            [
                {
                    "shift": "night",
                    "snapshot_month": dt.date(2026, 7, 1),
                    "snapshot_date": dt.date(2026, 7, 31),
                    **over,
                }
            ],
        )

    def _tail_dime(self, spark, day, ts):
        u = _local_unix(ts)
        return make_dime(
            spark,
            [
                {
                    "date": day,
                    "activity_type_required": "chat",
                    "local_timestamp_dime_slot_starts_at": dt.datetime.fromisoformat(ts),
                    "slot_start_local_unix": u,
                    "slot_end_local_unix": u + 1800,
                }
            ],
        )

    def _tail_shuffle(self, spark, day, ts):
        start = dt.datetime.fromisoformat(ts)
        return make_shuffle(
            spark,
            [
                {
                    "date": day,
                    "activity_type": "chat",
                    "local_start_time": start,
                    "local_stop_time": start + dt.timedelta(minutes=10),
                }
            ],
        )

    def test_tail_rolls_back_to_shift_start_day_and_flag_stays_one(self, spark):
        roster = self._night_roster(spark)
        dime = self._tail_dime(spark, dt.date(2026, 7, 6), "2026-07-06 03:00:00")
        shuffle = self._tail_shuffle(spark, dt.date(2026, 7, 6), "2026-07-06 03:00:00")
        out = compute_jobs_raw(roster, dime, shuffle, empty_oos(spark)).collect()
        assert len(out) == 1
        assert out[0]["date"] == dt.date(2026, 7, 5)
        assert out[0]["required_activity_on_day_flag"] == 1

    def test_morning_agent_tail_not_re_attributed(self, spark):
        roster = self._night_roster(spark, shift="morning")
        dime = self._tail_dime(spark, dt.date(2026, 7, 6), "2026-07-06 03:00:00")
        shuffle = self._tail_shuffle(spark, dt.date(2026, 7, 6), "2026-07-06 03:00:00")
        out = compute_jobs_raw(roster, dime, shuffle, empty_oos(spark)).collect()
        assert len(out) == 1
        assert out[0]["date"] == dt.date(2026, 7, 6)

    def test_before_cutover_keeps_legacy_calendar_day(self, spark):
        roster = self._night_roster(
            spark,
            snapshot_month=dt.date(2026, 6, 1),
            snapshot_date=dt.date(2026, 6, 30),
        )
        dime = self._tail_dime(spark, dt.date(2026, 6, 30), "2026-06-30 03:00:00")
        shuffle = self._tail_shuffle(spark, dt.date(2026, 6, 30), "2026-06-30 03:00:00")
        out = compute_jobs_raw(roster, dime, shuffle, empty_oos(spark)).collect()
        assert len(out) == 1
        assert out[0]["date"] == dt.date(2026, 6, 30)
