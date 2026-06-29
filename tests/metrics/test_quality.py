"""Unit tests for ``metrics/quality.py`` (PySpark).

Small synthetic Spark frames mimicking ``io_quality_evaluations_raw``, no
warehouse. We verify the mean-score math, the latest-per-(source, evaluation_id)
dedup by ``created_at DESC``, the Content exclusion, null-score handling, the
team-scoped blacklists, the team-asymmetric outage-date exclusion, the
cross-source id-collision guard, the distinct-evaluation denominator, and the
output contract.
"""

from __future__ import annotations

import datetime as dt

from pyspark.sql import types as T

from quality import (
    BLACKLIST_EVALUATION_IDS,
    BLACKLIST_SCORECARD_IDS,
    IO_QUALITY_METRIC_SCHEMA,
    METRIC_NAME,
    SM_BLACKLIST_SCORECARD_IDS,
    compute_quality,
)

_RAW_SCHEMA = T.StructType(
    [
        T.StructField("agent", T.StringType()),
        T.StructField("xforce", T.StringType()),
        T.StructField("xplead", T.StringType()),
        T.StructField("team", T.StringType()),
        T.StructField("squad", T.StringType()),
        T.StructField("district", T.StringType()),
        T.StructField("shift", T.StringType()),
        T.StructField("date", T.DateType()),
        T.StructField("created_at", T.TimestampType()),
        T.StructField("evaluation_id", T.StringType()),
        T.StructField("team_name", T.StringType()),
        T.StructField("scorecard_id", T.StringType()),
        T.StructField("source", T.StringType()),
        T.StructField("qa_score", T.DoubleType()),
    ]
)


def make_raw(spark, rows):
    defaults = {
        "agent": "nuberto.lopez",
        "xforce": "nuliana.cruz",
        "xplead": "nuricio.diaz",
        "team": "core",
        "squad": "txn",
        "district": "csi",
        "shift": "morning",
        "date": dt.date(2026, 5, 4),  # a Monday
        "created_at": dt.datetime(2026, 5, 4, 10, 0, 0),
        "evaluation_id": "ev-1",
        "team_name": "TXN",
        "scorecard_id": "sc-ok",
        "source": "playvox",
        "qa_score": 90.0,
    }
    data = [{**defaults, **r} for r in rows]
    return spark.createDataFrame(
        [tuple(r[f.name] for f in _RAW_SCHEMA.fields) for r in data], _RAW_SCHEMA
    )


def _day_by_agent(out):
    return {
        r["agent"]: r
        for r in out.filter(out["date_granularity"] == "day").collect()
    }


def _day_one(out):
    return out.filter(out["date_granularity"] == "day").collect()[0]


class TestComputeQuality:
    def test_mean_of_scores(self, spark):
        out = compute_quality(
            make_raw(
                spark,
                [
                    {"evaluation_id": "a", "qa_score": 95.0},
                    {"evaluation_id": "b", "qa_score": 93.0},
                    {"evaluation_id": "c", "qa_score": 81.0},
                ],
            )
        )
        day = _day_one(out)
        assert day["numerator"] == 95 + 93 + 81
        assert day["denominator"] == 3
        assert abs(day["metric_value"] - (95 + 93 + 81) / 3) < 1e-9
        assert abs(day["metric_value"] - 89.6667) < 1e-3
        assert day["metric"] == METRIC_NAME

    def test_latest_per_evaluation_id_dedup_by_created_at(self, spark):
        # Same (source, evaluation_id) re-scored — keep the latest created_at.
        out = compute_quality(
            make_raw(
                spark,
                [
                    {
                        "evaluation_id": "ev-1",
                        "date": dt.date(2026, 5, 4),
                        "created_at": dt.datetime(2026, 5, 4, 8, 0),
                        "qa_score": 70.0,
                    },
                    {
                        "evaluation_id": "ev-1",
                        "date": dt.date(2026, 5, 6),
                        "created_at": dt.datetime(2026, 5, 6, 8, 0),
                        "qa_score": 100.0,
                    },
                ],
            )
        )
        month = out.filter(out["date_granularity"] == "month").collect()[0]
        assert month["denominator"] == 1
        assert month["metric_value"] == 100.0

    def test_content_excluded(self, spark):
        out = compute_quality(
            make_raw(spark, [{"agent": "c.one", "team": "content", "evaluation_id": "x"}])
        )
        assert len(out.take(1)) == 0

    def test_content_excluded_but_others_kept(self, spark):
        out = compute_quality(
            make_raw(
                spark,
                [
                    {"agent": "core.one", "team": "core", "evaluation_id": "x",
                     "qa_score": 90.0},
                    {"agent": "cont.one", "team": "content", "evaluation_id": "y",
                     "qa_score": 50.0},
                ],
            )
        )
        assert set(_day_by_agent(out)) == {"core.one"}

    def test_social_media_scored(self, spark):
        out = compute_quality(
            make_raw(
                spark,
                [
                    {"agent": "s.one", "team": "social media", "squad": "social",
                     "source": "playvox", "evaluation_id": "p1", "qa_score": 80.0},
                    {"agent": "s.one", "team": "social media", "squad": "social",
                     "source": "playvox", "evaluation_id": "p2", "qa_score": 100.0},
                ],
            )
        )
        day = _day_by_agent(out)["s.one"]
        assert day["denominator"] == 2
        assert day["metric_value"] == 90.0

    def test_null_score_dropped(self, spark):
        out = compute_quality(
            make_raw(
                spark,
                [
                    {"evaluation_id": "a", "qa_score": 90.0},
                    {"evaluation_id": "b", "qa_score": None},
                ],
            )
        )
        day = _day_one(out)
        assert day["denominator"] == 1
        assert day["metric_value"] == 90.0

    def test_per_agent_separation(self, spark):
        out = compute_quality(
            make_raw(
                spark,
                [
                    {"agent": "a.one", "evaluation_id": "1", "qa_score": 100.0},
                    {"agent": "b.two", "evaluation_id": "2", "qa_score": 50.0},
                ],
            )
        )
        day = _day_by_agent(out)
        assert day["a.one"]["metric_value"] == 100.0
        assert day["b.two"]["metric_value"] == 50.0

    def test_all_granularities_emitted(self, spark):
        out = compute_quality(make_raw(spark, [{}]))
        assert {r["date_granularity"] for r in out.collect()} == {
            "day", "week", "month", "quarter", "semester", "year"
        }

    def test_output_schema_and_column_order(self, spark):
        out = compute_quality(make_raw(spark, [{}]))
        assert out.columns == [c for c, _ in IO_QUALITY_METRIC_SCHEMA]

    def test_empty_input_yields_empty_frame_with_schema(self, spark):
        out = compute_quality(make_raw(spark, [])[0:0] if False else make_raw(spark, []))
        assert len(out.take(1)) == 0
        assert out.columns == [c for c, _ in IO_QUALITY_METRIC_SCHEMA]


class TestDistinctDenominator:
    def test_cross_source_id_collision_not_dropped(self, spark):
        # A Playvox evaluation_id and an SM case_number that happen to share the
        # same string value must BOTH survive (legacy dedups within source).
        out = compute_quality(
            make_raw(
                spark,
                [
                    {"agent": "x.one", "team": "social media", "squad": "social",
                     "source": "playvox", "evaluation_id": "12345",
                     "qa_score": 80.0,
                     "created_at": dt.datetime(2026, 5, 4, 9, 0)},
                    {"agent": "x.one", "team": "social media", "squad": "social",
                     "source": "sprinklr_sm", "evaluation_id": "12345",
                     "qa_score": 100.0,
                     "created_at": dt.datetime(2026, 5, 4, 9, 0)},
                ],
            )
        )
        day = _day_by_agent(out)["x.one"]
        assert day["denominator"] == 2
        assert day["metric_value"] == 90.0

    def test_duplicate_within_source_counts_once(self, spark):
        # Two rows, same (source, evaluation_id) -> distinct denominator = 1.
        out = compute_quality(
            make_raw(
                spark,
                [
                    {"evaluation_id": "dup", "qa_score": 60.0,
                     "created_at": dt.datetime(2026, 5, 4, 8, 0)},
                    {"evaluation_id": "dup", "qa_score": 100.0,
                     "created_at": dt.datetime(2026, 5, 4, 9, 0)},
                ],
            )
        )
        day = _day_one(out)
        assert day["denominator"] == 1
        assert day["metric_value"] == 100.0  # later created_at wins


class TestTeamScopedBlacklists:
    def test_core_fraud_drops_blacklisted_scorecard(self, spark):
        out = compute_quality(
            make_raw(
                spark,
                [
                    {"team": "core", "evaluation_id": "keep", "qa_score": 90.0},
                    {"team": "core", "evaluation_id": "drop",
                     "scorecard_id": BLACKLIST_SCORECARD_IDS[0], "qa_score": 10.0},
                ],
            )
        )
        day = _day_one(out)
        assert day["denominator"] == 1
        assert day["metric_value"] == 90.0

    def test_core_fraud_drops_blacklisted_evaluation_id(self, spark):
        out = compute_quality(
            make_raw(
                spark,
                [
                    {"team": "fraud", "squad": "idsec",
                     "evaluation_id": "keep", "qa_score": 90.0},
                    {"team": "fraud", "squad": "idsec",
                     "evaluation_id": BLACKLIST_EVALUATION_IDS[0], "qa_score": 10.0},
                ],
            )
        )
        day = _day_one(out)
        assert day["denominator"] == 1
        assert day["metric_value"] == 90.0

    def test_sm_drops_only_scorecard_keeps_blacklisted_eval_id(self, spark):
        # SM has NO evaluation_id blacklist: a Core/Fraud-blacklisted eval_id is
        # KEPT for an SM agent. Only the single SM scorecard_id is dropped.
        out = compute_quality(
            make_raw(
                spark,
                [
                    {"agent": "s.one", "team": "social media", "squad": "social",
                     "evaluation_id": BLACKLIST_EVALUATION_IDS[0], "qa_score": 88.0},
                    {"agent": "s.one", "team": "social media", "squad": "social",
                     "evaluation_id": "sm-drop",
                     "scorecard_id": SM_BLACKLIST_SCORECARD_IDS[0], "qa_score": 10.0},
                ],
            )
        )
        day = _day_by_agent(out)["s.one"]
        assert day["denominator"] == 1
        assert day["metric_value"] == 88.0

    def test_sm_keeps_core_fraud_only_scorecard(self, spark):
        # A scorecard in the Core/Fraud list but NOT the SM list is kept for SM.
        cf_only = BLACKLIST_SCORECARD_IDS[1]  # '6812b3e46abeabb0653d197e'
        assert cf_only not in SM_BLACKLIST_SCORECARD_IDS
        out = compute_quality(
            make_raw(
                spark,
                [
                    {"agent": "s.one", "team": "social media", "squad": "social",
                     "evaluation_id": "sm-keep", "scorecard_id": cf_only,
                     "qa_score": 77.0},
                ],
            )
        )
        day = _day_by_agent(out)["s.one"]
        assert day["denominator"] == 1
        assert day["metric_value"] == 77.0

    def test_blacklist_not_applied_after_cutover(self, spark):
        # On/after 2026-07-01 the blacklist is lifted (correction).
        out = compute_quality(
            make_raw(
                spark,
                [
                    {"team": "core", "evaluation_id": "now-kept",
                     "date": dt.date(2026, 7, 1),
                     "created_at": dt.datetime(2026, 7, 1, 9, 0),
                     "scorecard_id": BLACKLIST_SCORECARD_IDS[0], "qa_score": 50.0},
                ],
            )
        )
        day = _day_one(out)
        assert day["denominator"] == 1
        assert day["metric_value"] == 50.0


class TestTeamAsymmetricOutage:
    def test_core_fraud_drops_both_outage_dates(self, spark):
        out = compute_quality(
            make_raw(
                spark,
                [
                    {"team": "core", "evaluation_id": "a",
                     "date": dt.date(2026, 3, 27),
                     "created_at": dt.datetime(2026, 3, 27, 9, 0), "qa_score": 10.0},
                    {"team": "core", "evaluation_id": "b",
                     "date": dt.date(2026, 4, 9),
                     "created_at": dt.datetime(2026, 4, 9, 9, 0), "qa_score": 20.0},
                    {"team": "core", "evaluation_id": "c",
                     "date": dt.date(2026, 4, 10),
                     "created_at": dt.datetime(2026, 4, 10, 9, 0), "qa_score": 90.0},
                ],
            )
        )
        days = {r["date_reference"]: r for r in
                out.filter(out["date_granularity"] == "day").collect()}
        assert dt.date(2026, 3, 27) not in days
        assert dt.date(2026, 4, 9) not in days
        assert days[dt.date(2026, 4, 10)]["metric_value"] == 90.0

    def test_social_media_keeps_april_9_drops_march_27(self, spark):
        out = compute_quality(
            make_raw(
                spark,
                [
                    {"agent": "s.one", "team": "social media", "squad": "social",
                     "evaluation_id": "a", "date": dt.date(2026, 3, 27),
                     "created_at": dt.datetime(2026, 3, 27, 9, 0), "qa_score": 10.0},
                    {"agent": "s.one", "team": "social media", "squad": "social",
                     "evaluation_id": "b", "date": dt.date(2026, 4, 9),
                     "created_at": dt.datetime(2026, 4, 9, 9, 0), "qa_score": 88.0},
                ],
            )
        )
        days = {r["date_reference"]: r for r in
                out.filter(out["date_granularity"] == "day").collect()}
        assert dt.date(2026, 3, 27) not in days
        assert days[dt.date(2026, 4, 9)]["metric_value"] == 88.0

    def test_outage_not_applied_after_cutover(self, spark):
        # 2026-03-27 is pre-cutover so still dropped; a hypothetical future date is
        # never an outage date — covered by date-list, this just guards the gate.
        out = compute_quality(
            make_raw(
                spark,
                [
                    {"team": "core", "evaluation_id": "c",
                     "date": dt.date(2026, 7, 2),
                     "created_at": dt.datetime(2026, 7, 2, 9, 0), "qa_score": 91.0},
                ],
            )
        )
        day = _day_one(out)
        assert day["metric_value"] == 91.0
