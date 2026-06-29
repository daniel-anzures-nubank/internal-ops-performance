"""Unit tests for ``metrics_data/shrinkage_slots.py`` (PySpark).

These tests build small, hand-crafted Spark DataFrames via the session-scoped
``spark`` fixture (see ``tests/conftest.py``). They never touch Databricks —
every check exercises the pure Spark transformation logic.

shrinkage_slots is a RAW per-slot dataset: one row per DIME slot for active
agents, flagged with ``shrinkage_flag`` / ``controllable_shrinkage_flag`` /
``uncontrollable_shrinkage_flag``. ``filter_dime`` keeps slots with a non-null
``activity_type_required`` AND a DIME ``agent_dime_squad`` not in the
shrinkage-specific exclusion set (content / planning / quality / social / wfm /
enablement); the lunch_break drop + the required/denominator rule move to the
metrics layer.
"""

from __future__ import annotations

import datetime as dt

import pytest
from pyspark.sql import types as T

from shrinkage_slots import (
    IO_SHRINKAGE_SLOTS_SCHEMA,
    SHRINKAGE_DIME_SQUAD_EXCLUSIONS,
    SHRINKAGE_FORMULA_CUTOVER,
    SHRINKAGE_MEETING_LEAVE_DIMENSIONED_ACTIVITIES,
    SHRINKAGE_OUT_OF_SCOPE_SQUADS,
    classify_slots,
    compute_shrinkage_slots,
    filter_dime,
)

# ---------------------------------------------------------------------------
# Builders for synthetic input frames
# ---------------------------------------------------------------------------

PRE_CUTOVER_DATE = dt.date(2026, 2, 15)
POST_CUTOVER_DATE = dt.date(2026, 4, 15)

# A unix value whose wall-clock under the UTC session tz is 06:00:00.
SLOT_BASE = int(
    dt.datetime(2026, 4, 15, 6, 0, 0, tzinfo=dt.timezone.utc).timestamp()
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


def make_dime(spark, rows):
    defaults = {
        "agent": "jane.doe",
        "date": POST_CUTOVER_DATE,
        "squad": "core",  # DIME agent_dime_squad
        "affiliation": "nubank",
        "activity_type_required": "chat",
        "shuffle_status_required": "available",
        "dimensioned_activity": "Chat",
        "local_timestamp_dime_slot_starts_at": dt.datetime(2026, 4, 15, 6, 0, 0),
        "slot_start_local_unix": SLOT_BASE,
        "slot_end_local_unix": SLOT_BASE + 1800,
    }
    return _rows_to_df(spark, _DIME_SCHEMA, rows, defaults)


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
        "snapshot_date": dt.date(2026, 4, 30),
        "snapshot_month": dt.date(2026, 4, 1),
        "hire_start_date": dt.date(2025, 1, 15),
        "last_change_date": dt.date(2025, 1, 15),
    }
    return _rows_to_df(spark, _ROSTER_SCHEMA, rows, defaults)


# ---------------------------------------------------------------------------
# filter_dime — non-null activity_type_required + DIME-squad exclusion
# ---------------------------------------------------------------------------


class TestFilterDime:
    def test_keeps_a_well_formed_row(self, spark):
        assert filter_dime(make_dime(spark, [{}])).count() == 1

    def test_drops_null_activity_type_required(self, spark):
        assert (
            filter_dime(make_dime(spark, [{"activity_type_required": None}])).count()
            == 0
        )

    @pytest.mark.parametrize("act", ["dime_invalid_notation", "time_off", "shrinkage"])
    def test_keeps_all_non_null_activity_types(self, spark, act):
        # Activity-type (lunch_break/time_off/denominator) handling moves to the
        # metrics layer; the raw filter keeps every non-null activity type.
        assert (
            filter_dime(make_dime(spark, [{"activity_type_required": act}])).count()
            == 1
        )

    @pytest.mark.parametrize("squad", list(SHRINKAGE_DIME_SQUAD_EXCLUSIONS))
    def test_drops_excluded_dime_squads(self, spark, squad):
        # Fixed legacy DIME-squad filter (all dates): the org-support squads
        # (content/planning/quality/social/wfm/enablement) are dropped at the
        # slot stage — legacy shrinkage_base lines 249-250.
        assert filter_dime(make_dime(spark, [{"squad": squad}])).count() == 0

    def test_drops_null_dime_squad(self, spark):
        assert filter_dime(make_dime(spark, [{"squad": None}])).count() == 0

    @pytest.mark.parametrize("squad", ["core", "credit", "credit_evolution", "dote"])
    def test_keeps_in_scope_dime_squads(self, spark, squad):
        # The shrinkage exclusion set is BROADER on the org-support side than
        # adherence/occupancy but EXCLUDES neither credit_evolution nor dote.
        assert filter_dime(make_dime(spark, [{"squad": squad}])).count() == 1


# ---------------------------------------------------------------------------
# classify_slots — shrinkage rule + controllable/uncontrollable split
# ---------------------------------------------------------------------------


def _classify_one(spark, **overrides):
    rows = classify_slots(filter_dime(make_dime(spark, [overrides]))).collect()
    assert len(rows) == 1
    return rows[0]


class TestClassifyShrinkageFlag:
    def test_pre_cutover_shrinkage_activity_is_shrinkage(self, spark):
        row = _classify_one(
            spark, date=PRE_CUTOVER_DATE, activity_type_required="shrinkage"
        )
        assert row["shrinkage_flag"] == 1

    def test_pre_cutover_invalid_notation_meeting_not_shrinkage(self, spark):
        row = _classify_one(
            spark,
            date=PRE_CUTOVER_DATE,
            activity_type_required="dime_invalid_notation",
            dimensioned_activity="Mouring",
        )
        assert row["shrinkage_flag"] == 0

    def test_post_cutover_shrinkage_activity_is_shrinkage(self, spark):
        row = _classify_one(
            spark, date=POST_CUTOVER_DATE, activity_type_required="shrinkage"
        )
        assert row["shrinkage_flag"] == 1

    @pytest.mark.parametrize(
        "meeting_leave", list(SHRINKAGE_MEETING_LEAVE_DIMENSIONED_ACTIVITIES)
    )
    def test_post_cutover_invalid_notation_meeting_is_shrinkage(
        self, spark, meeting_leave
    ):
        row = _classify_one(
            spark,
            date=POST_CUTOVER_DATE,
            activity_type_required="dime_invalid_notation",
            dimensioned_activity=meeting_leave,
        )
        assert row["shrinkage_flag"] == 1

    def test_post_cutover_invalid_notation_non_meeting_not_shrinkage(self, spark):
        row = _classify_one(
            spark,
            date=POST_CUTOVER_DATE,
            activity_type_required="dime_invalid_notation",
            dimensioned_activity="SomethingElse",
        )
        assert row["shrinkage_flag"] == 0

    def test_normal_work_is_never_shrinkage(self, spark):
        out = classify_slots(
            filter_dime(
                make_dime(
                    spark,
                    [
                        {"date": PRE_CUTOVER_DATE, "activity_type_required": "chat"},
                        {"date": POST_CUTOVER_DATE, "activity_type_required": "email"},
                    ],
                )
            )
        )
        assert sum(r["shrinkage_flag"] for r in out.collect()) == 0

    def test_cutover_inclusive_on_post_side(self, spark):
        row = _classify_one(
            spark,
            date=SHRINKAGE_FORMULA_CUTOVER,
            activity_type_required="dime_invalid_notation",
            dimensioned_activity="Mouring",
        )
        assert row["shrinkage_flag"] == 1


class TestClassifyControlSplit:
    def test_licencia_is_uncontrollable(self, spark):
        # Licencia is in the meeting/leave set, so an invalid-notation slot
        # with Licencia is shrinkage post-cutover, and uncontrollable.
        row = _classify_one(
            spark,
            date=POST_CUTOVER_DATE,
            activity_type_required="dime_invalid_notation",
            dimensioned_activity="Licencia",
        )
        assert row["shrinkage_flag"] == 1
        assert row["uncontrollable_shrinkage_flag"] == 1
        assert row["controllable_shrinkage_flag"] == 0

    def test_lowercase_licencia_is_uncontrollable(self, spark):
        row = _classify_one(
            spark, activity_type_required="shrinkage", dimensioned_activity="licencia"
        )
        assert row["uncontrollable_shrinkage_flag"] == 1

    def test_skr_lcnc_is_uncontrollable(self, spark):
        row = _classify_one(
            spark, activity_type_required="shrinkage", dimensioned_activity="SKR_LCNC"
        )
        assert row["shrinkage_flag"] == 1
        assert row["uncontrollable_shrinkage_flag"] == 1
        assert row["controllable_shrinkage_flag"] == 0

    def test_other_shrinkage_is_controllable(self, spark):
        row = _classify_one(
            spark, activity_type_required="shrinkage", dimensioned_activity="Pausa"
        )
        assert row["shrinkage_flag"] == 1
        assert row["controllable_shrinkage_flag"] == 1
        assert row["uncontrollable_shrinkage_flag"] == 0

    def test_non_shrinkage_all_flags_zero(self, spark):
        row = _classify_one(spark, activity_type_required="chat")
        assert row["shrinkage_flag"] == 0
        assert row["controllable_shrinkage_flag"] == 0
        assert row["uncontrollable_shrinkage_flag"] == 0

    def test_control_split_sums_to_shrinkage_flag(self, spark):
        out = classify_slots(
            filter_dime(
                make_dime(
                    spark,
                    [
                        {"activity_type_required": "shrinkage", "dimensioned_activity": "Licencia"},
                        {"activity_type_required": "shrinkage", "dimensioned_activity": "Pausa"},
                        {"activity_type_required": "chat", "dimensioned_activity": "Chat"},
                    ],
                )
            )
        ).collect()
        for r in out:
            assert (
                r["controllable_shrinkage_flag"] + r["uncontrollable_shrinkage_flag"]
                == r["shrinkage_flag"]
            )


# ---------------------------------------------------------------------------
# compute_shrinkage_slots — end-to-end (one row per slot)
# ---------------------------------------------------------------------------


class TestComputeShrinkageSlots:
    def test_basic_end_to_end_one_row_per_slot(self, spark):
        roster = make_roster(spark, [{}])
        dime = make_dime(
            spark,
            [
                {"activity_type_required": "shrinkage", "dimensioned_activity": "Pausa"},
                {
                    "activity_type_required": "chat",
                    "dimensioned_activity": "Chat",
                    "slot_start_local_unix": SLOT_BASE + 1800,
                    "local_timestamp_dime_slot_starts_at": dt.datetime(2026, 4, 15, 6, 30, 0),
                },
            ],
        )
        out = compute_shrinkage_slots(roster, dime).collect()
        assert len(out) == 2
        assert sum(r["shrinkage_flag"] for r in out) == 1
        assert out[0]["district"] == "northeast"
        assert out[0]["shift"] == "morning"

    def test_output_column_order_matches_schema(self, spark):
        out = compute_shrinkage_slots(make_roster(spark, [{}]), make_dime(spark, [{}]))
        assert out.columns == [c for c, _ in IO_SHRINKAGE_SLOTS_SCHEMA]

    def test_drops_inactive_agent(self, spark):
        out = compute_shrinkage_slots(
            make_roster(spark, [{"status": "inactive"}]),
            make_dime(spark, [{"activity_type_required": "shrinkage"}]),
        )
        assert out.count() == 0

    def test_out_of_scope_squads_constant_is_empty(self):
        assert SHRINKAGE_OUT_OF_SCOPE_SQUADS == ()

    def test_excluded_dime_squad_slot_removed_end_to_end(self, spark):
        # An agent with an excluded DIME squad (e.g. wfm) on the slot is dropped
        # at the DIME stage even if the roster squad is in scope.
        roster = make_roster(spark, [{"squad": "core"}])
        dime = make_dime(
            spark, [{"squad": "wfm", "activity_type_required": "shrinkage"}]
        )
        assert compute_shrinkage_slots(roster, dime).count() == 0

    def test_roster_squad_is_emitted_not_dime_squad(self, spark):
        # Output carries the ROSTER squad; the DIME agent_dime_squad is only a
        # slot-stage filter and must not leak into the output.
        roster = make_roster(spark, [{"squad": "txn"}])
        dime = make_dime(spark, [{"squad": "core"}])
        out = compute_shrinkage_slots(roster, dime).collect()
        assert len(out) == 1
        assert out[0]["squad"] == "txn"

    def test_uncontrollable_flag_end_to_end(self, spark):
        roster = make_roster(spark, [{}])
        dime = make_dime(
            spark,
            [{"activity_type_required": "shrinkage", "dimensioned_activity": "SKR_LCNC"}],
        )
        out = compute_shrinkage_slots(roster, dime).collect()
        assert out[0]["uncontrollable_shrinkage_flag"] == 1
        assert out[0]["controllable_shrinkage_flag"] == 0

    def test_uses_month_specific_roster_snapshot(self, spark):
        roster = make_roster(
            spark,
            [
                {"snapshot_month": dt.date(2026, 3, 1), "snapshot_date": dt.date(2026, 3, 31), "squad": "wrong-march"},
                {"snapshot_month": dt.date(2026, 4, 1), "snapshot_date": dt.date(2026, 4, 30), "squad": "correct-april"},
            ],
        )
        out = compute_shrinkage_slots(roster, make_dime(spark, [{}])).collect()
        assert len(out) == 1
        assert out[0]["squad"] == "correct-april"

    def test_duplicate_roster_rows_do_not_fan_out_slots(self, spark):
        # Content/enablement agents get >=2 rows per (agent, snapshot_month) from
        # agent_information's content branch (cross-join of multiple Google-Sheet
        # rows against every month, differing only in target_squad). The roster
        # join must NOT fan out: one slot must stay one row.
        roster = make_roster(
            spark,
            [
                {"agent": "omar.ramirez", "squad": "txn", "team": "core"},
                {"agent": "omar.ramirez", "squad": "txn", "team": "core"},
            ],
        )
        dime = make_dime(spark, [{"agent": "omar.ramirez"}])
        out = compute_shrinkage_slots(roster, dime).collect()
        assert len(out) == 1

    def test_handles_empty_dime(self, spark):
        empty_dime = spark.createDataFrame([], _DIME_SCHEMA)
        out = compute_shrinkage_slots(make_roster(spark, [{}]), empty_dime)
        assert out.count() == 0


def test_schema_matches_output_columns(spark):
    out = compute_shrinkage_slots(make_roster(spark, [{}]), make_dime(spark, [{}]))
    assert out.columns == [c for c, _ in IO_SHRINKAGE_SLOTS_SCHEMA]


# ---------------------------------------------------------------------------
# Night-shift attribution (>= 2026-07-01)
# ---------------------------------------------------------------------------


class TestNightShiftAttribution:
    @staticmethod
    def _local_unix(ts: str) -> int:
        return int(
            dt.datetime.fromisoformat(ts).replace(tzinfo=dt.timezone.utc).timestamp()
        )

    def _night_slot(self, spark, day, ts):
        u = self._local_unix(ts)
        return make_dime(
            spark,
            [
                {
                    "date": day,
                    "local_timestamp_dime_slot_starts_at": dt.datetime.fromisoformat(ts),
                    "slot_start_local_unix": u,
                    "slot_end_local_unix": u + 1800,
                }
            ],
        )

    def test_night_tail_attributed_to_shift_start_day(self, spark):
        roster = make_roster(
            spark,
            [{"shift": "night", "snapshot_month": dt.date(2026, 7, 1), "snapshot_date": dt.date(2026, 7, 31)}],
        )
        dime = self._night_slot(spark, dt.date(2026, 7, 6), "2026-07-06 03:00:00")
        out = compute_shrinkage_slots(roster, dime).collect()
        assert len(out) == 1
        assert out[0]["date"] == dt.date(2026, 7, 5)
        assert out[0]["slot_time"] == "03:00:00"

    def test_morning_agent_not_re_attributed(self, spark):
        roster = make_roster(
            spark,
            [{"shift": "morning", "snapshot_month": dt.date(2026, 7, 1), "snapshot_date": dt.date(2026, 7, 31)}],
        )
        dime = self._night_slot(spark, dt.date(2026, 7, 6), "2026-07-06 03:00:00")
        out = compute_shrinkage_slots(roster, dime).collect()
        assert len(out) == 1
        assert out[0]["date"] == dt.date(2026, 7, 6)
