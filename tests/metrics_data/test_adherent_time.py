"""Unit tests for ``metrics_data/adherent_time.py`` (PySpark).

These tests build small, hand-crafted Spark DataFrames via the session-scoped
``spark`` fixture (see ``tests/conftest.py``). They never touch Databricks —
every check exercises the pure Spark transformation logic.

adherent_time is a RAW dataset: ``filter_dime`` keeps every slot with a non-null
``activity_type_required`` (business exclusions move to the metrics layer), and
the output is one row per DIME slot with ``adherent_minutes`` and
``required_minutes`` plus the standardized dimensions.
"""

from __future__ import annotations

import datetime as dt

import pytest
from pyspark.sql import types as T

from adherent_time import (
    CORE_OUT_OF_SCOPE_SQUADS,
    DIME_SQUAD_EXCLUSIONS,
    MEETING_LEAVE_DIMENSIONED_ACTIVITIES,
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
# A UTC-midnight-aligned unix + 6h, so the local time-of-day is exactly 06:00:00.
SLOT_06_LOCAL = (
    int(dt.datetime(2026, 5, 18, 12, 0, 0).timestamp()) // _DAY_SECONDS
) * _DAY_SECONDS + 6 * 3600
SLOT_06_UTC = SLOT_06_LOCAL + MEXICO_UTC_OFFSET_SECONDS


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

_PROD_SCHEMA = T.StructType(
    [
        T.StructField("agent", T.StringType()),
        T.StructField("actor_id", T.StringType()),
        T.StructField("timestamp", T.TimestampType()),
        T.StructField("next_event_time", T.TimestampType()),
        T.StructField("activity_start_unix", T.LongType()),
        T.StructField("activity_end_unix", T.LongType()),
        T.StructField("raw_status", T.StringType()),
        T.StructField("inferred_status", T.StringType()),
        T.StructField("channel", T.StringType()),
        T.StructField("active_jobs", T.LongType()),
        T.StructField("level_3", T.StringType()),
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
        "activity_type_required": "shuffle",
        "shuffle_status_required": "available",
        "dimensioned_activity": "Chat",
        "local_timestamp_dime_slot_starts_at": dt.datetime(2026, 5, 18, 6, 0, 0),
        "slot_start_local_unix": SLOT_06_LOCAL,
        "slot_end_local_unix": SLOT_06_LOCAL + SLOT_DURATION_SECONDS,
    }
    return _rows_to_df(spark, _DIME_SCHEMA, rows, defaults)


def make_prod(spark, rows):
    defaults = {
        "agent": "jane.doe",
        "actor_id": "actor-1",
        "timestamp": dt.datetime(2026, 5, 18, 12, 0, 0),
        "next_event_time": dt.datetime(2026, 5, 18, 12, 30, 0),
        "activity_start_unix": SLOT_06_UTC,
        "activity_end_unix": SLOT_06_UTC + 1800,
        "raw_status": "available",
        "inferred_status": "available",
        "channel": None,
        "active_jobs": 0,
        "level_3": None,
    }
    return _rows_to_df(spark, _PROD_SCHEMA, rows, defaults)


def empty_prod(spark):
    return spark.createDataFrame([], _PROD_SCHEMA)


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
# filter_dime — minimal raw slot universe
# ---------------------------------------------------------------------------


class TestFilterDime:
    def test_keeps_well_formed_row(self, spark):
        assert filter_dime(make_dime(spark, [{}])).count() == 1

    def test_drops_null_activity_type(self, spark):
        assert filter_dime(make_dime(spark, [{"activity_type_required": None}])).count() == 0

    @pytest.mark.parametrize("dim_act", list(MEETING_LEAVE_DIMENSIONED_ACTIVITIES))
    def test_drops_meeting_leave_dimensioned_activity(self, spark, dim_act):
        # Fixed DIME filter: leave/meeting slots are excluded from adherence.
        assert filter_dime(make_dime(spark, [{"dimensioned_activity": dim_act}])).count() == 0

    def test_keeps_null_dimensioned_activity(self, spark):
        assert filter_dime(make_dime(spark, [{"dimensioned_activity": None}])).count() == 1

    def test_keeps_normal_dimensioned_activity(self, spark):
        assert filter_dime(make_dime(spark, [{"dimensioned_activity": "BKO_ENG"}])).count() == 1

    @pytest.mark.parametrize("act", ["lunch_break", "time_off", "shrinkage"])
    def test_keeps_formerly_excluded_activity_types(self, spark, act):
        assert filter_dime(make_dime(spark, [{"activity_type_required": act}])).count() == 1

    @pytest.mark.parametrize("squad", list(DIME_SQUAD_EXCLUSIONS))
    def test_drops_dime_squad_exclusions(self, spark, squad):
        # Fixed DIME filter: operational/WFM squads are excluded from adherence.
        assert filter_dime(make_dime(spark, [{"squad": squad}])).count() == 0

    def test_drops_null_dime_squad(self, spark):
        # Legacy `agent_dime_squad IS NOT NULL` — a NULL DIME squad is dropped.
        assert filter_dime(make_dime(spark, [{"squad": None}])).count() == 0

    def test_adds_utc_unix_columns_with_six_hour_offset(self, spark):
        row = filter_dime(make_dime(spark, [{}])).collect()[0]
        assert row["slot_start"] == SLOT_06_LOCAL + MEXICO_UTC_OFFSET_SECONDS
        assert row["slot_end"] == row["slot_start"] + SLOT_DURATION_SECONDS


# ---------------------------------------------------------------------------
# filter_productivity
# ---------------------------------------------------------------------------


class TestFilterProductivity:
    @pytest.mark.parametrize("status", ["available", "oos", "training"])
    def test_keeps_connected_status(self, spark, status):
        assert filter_productivity(make_prod(spark, [{"inferred_status": status}])).count() == 1

    def test_keeps_paused_with_jobs(self, spark):
        out = filter_productivity(
            make_prod(spark, [{"inferred_status": "pause", "level_3": "paused_with_jobs"}])
        )
        assert out.count() == 1

    def test_drops_plain_paused(self, spark):
        out = filter_productivity(
            make_prod(spark, [{"inferred_status": "pause", "level_3": "paused"}])
        )
        assert out.count() == 0

    def test_keeps_active_jobs_positive_regardless_of_status(self, spark):
        out = filter_productivity(
            make_prod(spark, [{"inferred_status": "lunch_break", "active_jobs": 1}])
        )
        assert out.count() == 1

    def test_drops_unknown_status_with_no_active_jobs(self, spark):
        out = filter_productivity(
            make_prod(spark, [{"inferred_status": "weird_status", "active_jobs": 0}])
        )
        assert out.count() == 0

    def test_keeps_null_status_after_jan_22_2026(self, spark):
        out = filter_productivity(
            make_prod(
                spark,
                [{"inferred_status": None, "timestamp": dt.datetime(2026, 2, 15, 12, 0, 0)}],
            )
        )
        assert out.count() == 1

    def test_drops_null_status_before_jan_22_2026(self, spark):
        out = filter_productivity(
            make_prod(
                spark,
                [{"inferred_status": None, "timestamp": dt.datetime(2026, 1, 10, 12, 0, 0)}],
            )
        )
        assert out.count() == 0


# ---------------------------------------------------------------------------
# compute_slot_adherence — the overlap math + LEFT-JOIN behavior
# ---------------------------------------------------------------------------


class TestComputeSlotAdherence:
    def _one_slot(self, spark, **dime_overrides):
        return filter_dime(make_dime(spark, [dime_overrides]))

    def _final(self, slots, prod):
        rows = compute_slot_adherence(slots, prod).collect()
        assert len(rows) == 1
        return rows[0]["adherent_time_final"]

    def test_activity_fully_inside_slot(self, spark):
        prod = filter_productivity(
            make_prod(
                spark,
                [{"activity_start_unix": SLOT_06_UTC + 300, "activity_end_unix": SLOT_06_UTC + 900}],
            )
        )
        assert self._final(self._one_slot(spark), prod) == 600

    def test_activity_spans_slot(self, spark):
        prod = filter_productivity(
            make_prod(
                spark,
                [{"activity_start_unix": SLOT_06_UTC - 3600, "activity_end_unix": SLOT_06_UTC + 5400}],
            )
        )
        assert self._final(self._one_slot(spark), prod) == SLOT_DURATION_SECONDS

    def test_activity_ends_inside_slot(self, spark):
        prod = filter_productivity(
            make_prod(
                spark,
                [{"activity_start_unix": SLOT_06_UTC - 300, "activity_end_unix": SLOT_06_UTC + 600}],
            )
        )
        assert self._final(self._one_slot(spark), prod) == 600

    def test_activity_starts_inside_slot(self, spark):
        prod = filter_productivity(
            make_prod(
                spark,
                [{"activity_start_unix": SLOT_06_UTC + 1200, "activity_end_unix": SLOT_06_UTC + 2400}],
            )
        )
        assert self._final(self._one_slot(spark), prod) == 600

    def test_no_overlap_pre_cutover_phantom_full(self, spark):
        # Pre-2026-07-01 (date D is May): an unmatched slot reproduces the
        # legacy phantom-adherence bug — counted as a full slot (1800s), not 0.
        prod = filter_productivity(
            make_prod(
                spark,
                [{"activity_start_unix": SLOT_06_UTC + 9999, "activity_end_unix": SLOT_06_UTC + 20000}],
            )
        )
        assert self._final(self._one_slot(spark), prod) == SLOT_DURATION_SECONDS

    def test_no_overlap_post_cutover_returns_zero(self, spark):
        # From 2026-07-01 on, an unmatched slot correctly scores 0.
        prod = filter_productivity(
            make_prod(
                spark,
                [{"activity_start_unix": SLOT_06_UTC + 9999, "activity_end_unix": SLOT_06_UTC + 20000}],
            )
        )
        assert self._final(self._one_slot(spark, date=dt.date(2026, 7, 1)), prod) == 0

    def test_multiple_productivity_rows_sum_then_cap(self, spark):
        prod = filter_productivity(
            make_prod(
                spark,
                [
                    {"activity_start_unix": SLOT_06_UTC, "activity_end_unix": SLOT_06_UTC + 720},
                    {"activity_start_unix": SLOT_06_UTC + 720, "activity_end_unix": SLOT_06_UTC + 1440},
                    {"activity_start_unix": SLOT_06_UTC + 1440, "activity_end_unix": SLOT_06_UTC + 2160},
                ],
            )
        )
        assert self._final(self._one_slot(spark), prod) == SLOT_DURATION_SECONDS

    def test_empty_productivity_pre_cutover_phantom(self, spark):
        # Empty productivity → slot unmatched → pre-cutover phantom (full).
        assert self._final(self._one_slot(spark), empty_prod(spark)) == SLOT_DURATION_SECONDS

    def test_empty_productivity_post_cutover_zero(self, spark):
        assert self._final(self._one_slot(spark, date=dt.date(2026, 7, 1)), empty_prod(spark)) == 0


# ---------------------------------------------------------------------------
# compute_adherent_time — end-to-end small synthetic pipeline
# ---------------------------------------------------------------------------


class TestComputeAdherentTimeEndToEnd:
    def test_one_agent_one_day_one_slot_full_adherent(self, spark):
        out = compute_adherent_time(
            make_roster(spark, [{}]), make_dime(spark, [{}]), make_prod(spark, [{}])
        ).collect()
        assert len(out) == 1
        row = out[0]
        assert row["agent"] == "jane.doe"
        assert row["xforce"] == "lead.one"
        assert row["squad"] == "core"
        assert row["district"] == "northeast"
        assert row["shift"] == "morning"
        assert row["slot_time"] == "06:00:00"
        assert row["activity_type_required"] == "shuffle"
        assert row["adherent_minutes"] == SLOT_DURATION_SECONDS / 60.0
        assert row["required_minutes"] == SLOT_DURATION_SECONDS / 60.0

    def test_column_order_matches_schema(self, spark):
        out = compute_adherent_time(
            make_roster(spark, [{}]), make_dime(spark, [{}]), make_prod(spark, [{}])
        )
        assert out.columns == [
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

    def test_no_productivity_pre_cutover_phantom_full(self, spark):
        # Pre-cutover (date D is May 2026), an unmatched slot reproduces the
        # legacy phantom: full adherent minutes (= required), a fake 100%.
        out = compute_adherent_time(
            make_roster(spark, [{}]), make_dime(spark, [{}]), empty_prod(spark)
        ).collect()
        assert len(out) == 1
        assert out[0]["adherent_minutes"] == SLOT_DURATION_SECONDS / 60.0
        assert out[0]["required_minutes"] == SLOT_DURATION_SECONDS / 60.0

    def test_no_productivity_post_cutover_zero_adherent(self, spark):
        # From 2026-07-01 on, an unmatched slot scores 0 adherent minutes
        # end-to-end (the corrected behavior).
        out = compute_adherent_time(
            make_roster(
                spark,
                [{"snapshot_month": dt.date(2026, 7, 1), "snapshot_date": dt.date(2026, 7, 31)}],
            ),
            make_dime(spark, [{"date": dt.date(2026, 7, 1)}]),
            empty_prod(spark),
        ).collect()
        assert len(out) == 1
        assert out[0]["adherent_minutes"] == 0.0
        assert out[0]["required_minutes"] == SLOT_DURATION_SECONDS / 60.0

    def test_inactive_agent_dropped(self, spark):
        out = compute_adherent_time(
            make_roster(spark, [{"status": "inactive"}]),
            make_dime(spark, [{}]),
            make_prod(spark, [{}]),
        )
        assert out.count() == 0

    def test_out_of_scope_squads_constant_is_empty(self):
        assert CORE_OUT_OF_SCOPE_SQUADS == ()

    @pytest.mark.parametrize("squad", ["social", "content"])
    def test_social_and_content_squads_kept(self, spark, squad):
        out = compute_adherent_time(
            make_roster(spark, [{"squad": squad}]),
            make_dime(spark, [{}]),
            make_prod(spark, [{}]),
        ).collect()
        assert len(out) == 1
        assert out[0]["squad"] == squad

    def test_each_slot_emits_its_own_row(self, spark):
        dime = make_dime(
            spark,
            [
                {},
                {
                    "local_timestamp_dime_slot_starts_at": dt.datetime(2026, 5, 18, 6, 30, 0),
                    "slot_start_local_unix": SLOT_06_LOCAL + 1800,
                    "slot_end_local_unix": SLOT_06_LOCAL + 3600,
                },
            ],
        )
        out = compute_adherent_time(make_roster(spark, [{}]), dime, empty_prod(spark)).collect()
        assert len(out) == 2
        assert all(r["required_minutes"] == SLOT_DURATION_SECONDS / 60.0 for r in out)
        # date D is pre-cutover, so unmatched slots phantom to full adherent.
        assert all(r["adherent_minutes"] == SLOT_DURATION_SECONDS / 60.0 for r in out)
        assert sorted(r["slot_time"] for r in out) == ["06:00:00", "06:30:00"]

    def test_night_tail_attributed_to_shift_start_day(self, spark):
        # Night agent, slot at Jul 6 03:00 local -> attributed to Jul 5.
        local_unix = int(dt.datetime(2026, 7, 6, 3, 0, 0).replace(tzinfo=dt.timezone.utc).timestamp())
        dime = make_dime(
            spark,
            [
                {
                    "date": dt.date(2026, 7, 6),
                    "local_timestamp_dime_slot_starts_at": dt.datetime(2026, 7, 6, 3, 0, 0),
                    "slot_start_local_unix": local_unix,
                    "slot_end_local_unix": local_unix + SLOT_DURATION_SECONDS,
                }
            ],
        )
        roster = make_roster(
            spark,
            [{"shift": "night", "snapshot_month": dt.date(2026, 7, 1), "snapshot_date": dt.date(2026, 7, 31)}],
        )
        out = compute_adherent_time(roster, dime, empty_prod(spark)).collect()
        assert len(out) == 1
        assert out[0]["date"] == dt.date(2026, 7, 5)
        assert out[0]["slot_time"] == "03:00:00"

    def test_night_tail_before_cutover_keeps_calendar_day(self, spark):
        local_unix = int(dt.datetime(2026, 6, 30, 3, 0, 0).replace(tzinfo=dt.timezone.utc).timestamp())
        dime = make_dime(
            spark,
            [
                {
                    "date": dt.date(2026, 6, 30),
                    "local_timestamp_dime_slot_starts_at": dt.datetime(2026, 6, 30, 3, 0, 0),
                    "slot_start_local_unix": local_unix,
                    "slot_end_local_unix": local_unix + SLOT_DURATION_SECONDS,
                }
            ],
        )
        roster = make_roster(
            spark,
            [{"shift": "night", "snapshot_month": dt.date(2026, 6, 1), "snapshot_date": dt.date(2026, 6, 30)}],
        )
        out = compute_adherent_time(roster, dime, empty_prod(spark)).collect()
        assert len(out) == 1
        assert out[0]["date"] == dt.date(2026, 6, 30)

    def test_uses_month_specific_roster_snapshot(self, spark):
        roster = make_roster(
            spark,
            [
                {"snapshot_month": dt.date(2026, 4, 1), "snapshot_date": dt.date(2026, 4, 30), "squad": "wrong-april"},
                {"snapshot_month": dt.date(2026, 5, 1), "snapshot_date": dt.date(2026, 5, 31), "squad": "correct-may"},
            ],
        )
        out = compute_adherent_time(roster, make_dime(spark, [{}]), make_prod(spark, [{}])).collect()
        assert len(out) == 1
        assert out[0]["squad"] == "correct-may"


class TestSmLeaveDeckSplit:
    """Legacy SM adherence keeps Licencia/Vacacion (5-item exclusion) pre-cutover."""

    def test_sm_licencia_kept_pre_cutover(self, spark):
        out = filter_dime(make_dime(spark, [
            {"squad": "social_social", "dimensioned_activity": "Licencia"},
            {"squad": "social", "dimensioned_activity": "Vacacion"},
        ]))
        assert out.count() == 2

    def test_sm_meeting_items_still_dropped_pre_cutover(self, spark):
        out = filter_dime(make_dime(spark, [
            {"squad": "social_social", "dimensioned_activity": "Huddle"},
            {"squad": "social", "dimensioned_activity": "Weekly"},
        ]))
        assert out.count() == 0

    def test_non_sm_licencia_dropped(self, spark):
        out = filter_dime(make_dime(spark, [
            {"squad": "core", "dimensioned_activity": "Licencia"},
            {"squad": "content_content", "dimensioned_activity": "Vacacion"},
        ]))
        assert out.count() == 0

    def test_sm_licencia_dropped_post_cutover(self, spark):
        out = filter_dime(make_dime(spark, [
            {"squad": "social_social", "dimensioned_activity": "Licencia",
             "date": dt.date(2026, 7, 6)},
        ]))
        assert out.count() == 0
