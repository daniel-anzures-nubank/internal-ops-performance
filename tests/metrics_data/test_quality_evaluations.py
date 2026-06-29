"""Unit tests for ``metrics_data/quality_evaluations.py`` (PySpark).

Small synthetic Spark frames, no warehouse.

quality_evaluations is a RAW dataset: one row per individual QA evaluation (no
per-day aggregation). Two sources unioned: Playvox (all teams) and Sprinklr SM
(Social Media, >= 2026-07-01). We verify the Playvox source gate, the
per-evaluation shaping, the Sprinklr cutover + ``source`` tagging, the
``scorecard_id`` / ``created_at`` carry-through, and the roster join that
attaches the standardized dimensions.
"""

from __future__ import annotations

import datetime as dt

from pyspark.sql import types as T

from quality_evaluations import (
    IO_QUALITY_EVALUATIONS_SCHEMA,
    PLAYVOX_TEAM_NAME_EXCLUSIONS,
    QUALITY_OUT_OF_SCOPE_SQUADS,
    SOURCE_PLAYVOX,
    SOURCE_SPRINKLR_SM,
    SPRINKLR_SCORECARD_ID,
    SPRINKLR_SM_CUTOVER,
    compute_quality_evaluations,
    filter_playvox,
)

_NUBANK_DOMAIN = ".".join(["nu", "com", "mx"])
_EXTERNAL_DOMAIN = "example.com"


def _mock_email(local: str, domain: str = _NUBANK_DOMAIN) -> str:
    return f"{local}@{domain}"


# ---------------------------------------------------------------------------
# Schemas + builders (mirror the extractor outputs)
# ---------------------------------------------------------------------------

_PLAYVOX_SCHEMA = T.StructType(
    [
        T.StructField("evaluation_id", T.StringType()),
        T.StructField("agent", T.StringType()),
        T.StructField("agent_email", T.StringType()),
        T.StructField("team_name", T.StringType()),
        T.StructField("scorecard_id", T.StringType()),
        T.StructField("qa_score", T.DoubleType()),
        T.StructField("created_at", T.TimestampType()),
        T.StructField("updated_at", T.TimestampType()),
    ]
)

_SPRINKLR_SCHEMA = T.StructType(
    [
        T.StructField("evaluation_id", T.StringType()),
        T.StructField("agent", T.StringType()),
        T.StructField("qa_score", T.DoubleType()),
        T.StructField("team_name", T.StringType()),
        T.StructField("scorecard_id", T.StringType()),
        T.StructField("created_at", T.TimestampType()),
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


def make_playvox(spark, rows):
    defaults = {
        "evaluation_id": "p-1",
        "agent": "jane.doe",
        "agent_email": _mock_email("jane.doe"),
        "team_name": "CREDIT",
        "scorecard_id": "sc-abc",
        "qa_score": 85.0,
        "created_at": dt.datetime(2026, 4, 15, 10, 30),
        "updated_at": dt.datetime(2026, 4, 15, 11, 0),
    }
    return _rows_to_df(spark, _PLAYVOX_SCHEMA, rows, defaults)


def make_sprinklr(spark, rows):
    defaults = {
        "evaluation_id": "sm-1",
        "agent": "jane.doe",
        "qa_score": 90.0,
        "team_name": "SM",
        "scorecard_id": SPRINKLR_SCORECARD_ID,
        "created_at": dt.datetime(2026, 7, 15, 9, 0),  # >= cutover
    }
    return _rows_to_df(spark, _SPRINKLR_SCHEMA, rows, defaults)


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


def _collect(df):
    return [r.asDict() for r in df.collect()]


# ---------------------------------------------------------------------------
# filter_playvox — source gate
# ---------------------------------------------------------------------------


class TestFilterPlayvox:
    def test_keeps_a_well_formed_row(self, spark):
        assert filter_playvox(make_playvox(spark, [{}])).count() == 1

    def test_drops_excluded_teams(self, spark):
        for team in PLAYVOX_TEAM_NAME_EXCLUSIONS:
            out = filter_playvox(make_playvox(spark, [{"team_name": team}]))
            assert out.count() == 0

    def test_drops_non_nubank_email(self, spark):
        out = filter_playvox(
            make_playvox(spark, [{"agent_email": _mock_email("jane.doe", _EXTERNAL_DOMAIN)}])
        )
        assert out.count() == 0

    def test_keeps_email_with_numeric_suffix(self, spark):
        out = filter_playvox(
            make_playvox(spark, [{"agent_email": _mock_email("maria.elena2")}])
        )
        assert out.count() == 1

    def test_does_not_apply_scorecard_blacklist(self, spark):
        # The scorecard blacklist is a metrics-layer concern, not a source gate.
        out = filter_playvox(
            make_playvox(spark, [{"scorecard_id": "68def79b3f83da8cc9cb5299"}])
        )
        assert out.count() == 1


# ---------------------------------------------------------------------------
# compute_quality_evaluations — end-to-end (one row per evaluation)
# ---------------------------------------------------------------------------


class TestComputeQualityEvaluations:
    def test_one_evaluation_one_row(self, spark):
        out = _collect(
            compute_quality_evaluations(
                make_roster(spark, [{}]), make_playvox(spark, [{"qa_score": 80.0}])
            )
        )
        assert len(out) == 1
        row = out[0]
        assert row["agent"] == "jane.doe"
        assert row["date"] == dt.date(2026, 4, 15)
        assert abs(row["qa_score"] - 80.0) < 1e-9
        assert row["evaluation_id"] == "p-1"
        assert row["squad"] == "core"
        assert row["district"] == "northeast"
        assert row["shift"] == "morning"
        assert row["source"] == SOURCE_PLAYVOX

    def test_scorecard_id_carried_through(self, spark):
        out = _collect(
            compute_quality_evaluations(
                make_roster(spark, [{}]),
                make_playvox(spark, [{"scorecard_id": "sc-xyz"}]),
            )
        )
        assert out[0]["scorecard_id"] == "sc-xyz"

    def test_created_at_carried_through(self, spark):
        ts = dt.datetime(2026, 4, 15, 13, 45)
        out = _collect(
            compute_quality_evaluations(
                make_roster(spark, [{}]),
                make_playvox(spark, [{"created_at": ts}]),
            )
        )
        assert out[0]["created_at"] == ts

    def test_date_derived_from_created_at(self, spark):
        # Mid-day timestamp: on Databricks `created_at` is UTC-naive MX-local and
        # `to_date` == legacy `DATE_TRUNC('DAY', ...)`. A 23:59 boundary value would
        # shift a day on the LOCAL test JVM (it ingests the naive datetime in the
        # machine tz, not UTC) — a known session-tz fixture quirk, not a code bug.
        out = _collect(
            compute_quality_evaluations(
                make_roster(spark, [{}]),
                make_playvox(spark, [{"created_at": dt.datetime(2026, 4, 15, 12, 0)}]),
            )
        )
        assert out[0]["date"] == dt.date(2026, 4, 15)

    def test_two_evaluations_same_day_two_rows(self, spark):
        out = _collect(
            compute_quality_evaluations(
                make_roster(spark, [{}]),
                make_playvox(
                    spark,
                    [
                        {"evaluation_id": "p-1", "qa_score": 60.0},
                        {"evaluation_id": "p-2", "qa_score": 100.0},
                    ],
                ),
            )
        )
        assert sorted(r["evaluation_id"] for r in out) == ["p-1", "p-2"]

    def test_inactive_agent_dropped(self, spark):
        out = compute_quality_evaluations(
            make_roster(spark, [{"status": "inactive"}]), make_playvox(spark, [{}])
        )
        assert out.count() == 0

    def test_out_of_scope_squads_constant_is_empty(self):
        assert QUALITY_OUT_OF_SCOPE_SQUADS == ()

    def test_social_and_content_squads_kept(self, spark):
        for squad, team in (("social", "social media"), ("content", "content")):
            out = _collect(
                compute_quality_evaluations(
                    make_roster(spark, [{"squad": squad, "team": team}]),
                    make_playvox(spark, [{}]),
                )
            )
            assert len(out) == 1
            assert out[0]["squad"] == squad

    def test_null_squad_agent_dropped(self, spark):
        out = compute_quality_evaluations(
            make_roster(spark, [{"squad": None}]), make_playvox(spark, [{}])
        )
        assert out.count() == 0

    def test_team_name_blacklist_applied(self, spark):
        out = compute_quality_evaluations(
            make_roster(spark, [{}]),
            make_playvox(spark, [{"team_name": "REGULATORY SOLUTIONS"}]),
        )
        assert out.count() == 0

    def test_non_nubank_email_dropped(self, spark):
        out = compute_quality_evaluations(
            make_roster(spark, [{}]),
            make_playvox(spark, [{"agent_email": _mock_email("jane.doe", _EXTERNAL_DOMAIN)}]),
        )
        assert out.count() == 0

    def test_no_roster_match_dropped(self, spark):
        out = compute_quality_evaluations(
            make_roster(spark, [{"agent": "someone.else"}]), make_playvox(spark, [{}])
        )
        assert out.count() == 0

    def test_uses_natural_snapshot_month(self, spark):
        out = _collect(
            compute_quality_evaluations(
                make_roster(
                    spark,
                    [
                        {"snapshot_month": dt.date(2026, 3, 1),
                         "snapshot_date": dt.date(2026, 3, 31), "squad": "core"},
                        {"snapshot_month": dt.date(2026, 4, 1),
                         "snapshot_date": dt.date(2026, 4, 30), "squad": "credit"},
                    ],
                ),
                make_playvox(
                    spark,
                    [
                        {"evaluation_id": "p-mar", "created_at": dt.datetime(2026, 3, 15)},
                        {"evaluation_id": "p-apr", "created_at": dt.datetime(2026, 4, 15)},
                    ],
                ),
            )
        )
        squads_by_date = {r["date"]: r["squad"] for r in out}
        assert squads_by_date[dt.date(2026, 3, 15)] == "core"
        assert squads_by_date[dt.date(2026, 4, 15)] == "credit"

    def test_outage_dates_not_filtered_in_raw(self, spark):
        # The raw layer is intentionally minimal; outage filtering is metrics-layer.
        out = _collect(
            compute_quality_evaluations(
                make_roster(
                    spark,
                    [
                        {"snapshot_month": dt.date(2026, 3, 1),
                         "snapshot_date": dt.date(2026, 3, 31)},
                        {"snapshot_month": dt.date(2026, 4, 1),
                         "snapshot_date": dt.date(2026, 4, 30)},
                    ],
                ),
                make_playvox(
                    spark,
                    [
                        {"evaluation_id": "p-1", "created_at": dt.datetime(2026, 3, 27)},
                        {"evaluation_id": "p-2", "created_at": dt.datetime(2026, 4, 9)},
                    ],
                ),
            )
        )
        assert sorted(r["date"] for r in out) == [dt.date(2026, 3, 27), dt.date(2026, 4, 9)]

    def test_output_schema_and_column_order(self, spark):
        out = compute_quality_evaluations(make_roster(spark, [{}]), make_playvox(spark, [{}]))
        assert out.columns == [c for c, _ in IO_QUALITY_EVALUATIONS_SCHEMA]

    def test_empty_playvox_yields_empty(self, spark):
        out = compute_quality_evaluations(
            make_roster(spark, [{}]), make_playvox(spark, [])
        )
        assert out.count() == 0
        assert out.columns == [c for c, _ in IO_QUALITY_EVALUATIONS_SCHEMA]

    def test_no_sprinklr_arg_is_playvox_only(self, spark):
        out = compute_quality_evaluations(make_roster(spark, [{}]), make_playvox(spark, [{}]))
        assert out.count() == 1
        assert {r["source"] for r in out.collect()} == {SOURCE_PLAYVOX}


# ---------------------------------------------------------------------------
# Sprinklr SM union — cutover (2026-07-01) + source provenance
# ---------------------------------------------------------------------------


class TestSprinklrSmUnion:
    def test_union_adds_rows_tagged_source(self, spark):
        out = _collect(
            compute_quality_evaluations(
                make_roster(
                    spark,
                    [{"snapshot_month": dt.date(2026, 7, 1),
                      "snapshot_date": dt.date(2026, 7, 31), "squad": "social",
                      "team": "social media"}],
                ),
                make_playvox(
                    spark,
                    [{"evaluation_id": "p-1", "created_at": dt.datetime(2026, 7, 10)}],
                ),
                make_sprinklr(
                    spark,
                    [{"evaluation_id": "sm-1", "created_at": dt.datetime(2026, 7, 15)}],
                ),
            )
        )
        by_id = {r["evaluation_id"]: r["source"] for r in out}
        assert by_id == {"p-1": SOURCE_PLAYVOX, "sm-1": SOURCE_SPRINKLR_SM}

    def test_sprinklr_qa_score_and_scorecard_preserved(self, spark):
        out = _collect(
            compute_quality_evaluations(
                make_roster(
                    spark,
                    [{"snapshot_month": dt.date(2026, 7, 1),
                      "snapshot_date": dt.date(2026, 7, 31), "squad": "social",
                      "team": "social media"}],
                ),
                make_playvox(spark, []),
                make_sprinklr(spark, [{"qa_score": 87.5}]),
            )
        )
        assert len(out) == 1
        assert abs(out[0]["qa_score"] - 87.5) < 1e-9
        assert out[0]["source"] == SOURCE_SPRINKLR_SM
        assert out[0]["scorecard_id"] == SPRINKLR_SCORECARD_ID

    def test_before_cutover_dropped(self, spark):
        # A May/June Sprinklr row is dropped: SM stays Playvox-only pre-2026-07-01.
        out = _collect(
            compute_quality_evaluations(
                make_roster(
                    spark,
                    [
                        {"snapshot_month": dt.date(2026, 6, 1),
                         "snapshot_date": dt.date(2026, 6, 30), "squad": "social",
                         "team": "social media"},
                        {"snapshot_month": dt.date(2026, 7, 1),
                         "snapshot_date": dt.date(2026, 7, 31), "squad": "social",
                         "team": "social media"},
                    ],
                ),
                make_playvox(spark, []),
                make_sprinklr(
                    spark,
                    [
                        {"evaluation_id": "sm-jun", "created_at": dt.datetime(2026, 6, 20)},
                        {"evaluation_id": "sm-jul", "created_at": dt.datetime(2026, 7, 2)},
                    ],
                ),
            )
        )
        assert [r["evaluation_id"] for r in out] == ["sm-jul"]

    def test_cutover_boundary_is_inclusive(self, spark):
        out = _collect(
            compute_quality_evaluations(
                make_roster(
                    spark,
                    [{"snapshot_month": dt.date(2026, 7, 1),
                      "snapshot_date": dt.date(2026, 7, 31), "squad": "social",
                      "team": "social media"}],
                ),
                make_playvox(spark, []),
                make_sprinklr(
                    spark,
                    [{"evaluation_id": "sm-edge", "created_at": dt.datetime(2026, 7, 1)}],
                ),
            )
        )
        assert [r["evaluation_id"] for r in out] == ["sm-edge"]
        assert SPRINKLR_SM_CUTOVER == dt.date(2026, 7, 1)

    def test_empty_sprinklr_is_noop(self, spark):
        out = compute_quality_evaluations(
            make_roster(spark, [{}]), make_playvox(spark, [{}]), make_sprinklr(spark, [])
        )
        assert out.count() == 1
        assert {r["source"] for r in out.collect()} == {SOURCE_PLAYVOX}

    def test_sprinklr_unmatched_roster_dropped(self, spark):
        out = compute_quality_evaluations(
            make_roster(
                spark,
                [{"agent": "someone.else", "snapshot_month": dt.date(2026, 7, 1),
                  "snapshot_date": dt.date(2026, 7, 31)}],
            ),
            make_playvox(spark, []),
            make_sprinklr(spark, [{"agent": "jane.doe"}]),
        )
        assert out.count() == 0
