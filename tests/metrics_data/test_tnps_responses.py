"""Unit tests for ``metrics_data/tnps_responses.py`` (PySpark).

Small synthetic Spark frames, no warehouse.

tnps_responses is a RAW dataset: one row per Social-Media tNPS survey response
(no classification, no NPS aggregation). We verify the human-agent filter, the
roster join that attaches the standardized dimensions
(agent, xforce, xplead, team, squad, district, shift) on the response's natural
snapshot_month, and the output contract.
"""

from __future__ import annotations

import datetime as dt

from pyspark.sql import types as T

from tnps_responses import (
    IO_TNPS_RESPONSES_SCHEMA,
    TNPS_OUT_OF_SCOPE_SQUADS,
    compute_tnps_responses,
)

_MEXICO_NUBANK_DOMAIN = ".".join(["nubank", "com", "mx"])


def _mock_email(local: str, domain: str = _MEXICO_NUBANK_DOMAIN) -> str:
    return f"{local}@{domain}"


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------

_TNPS_SCHEMA = T.StructType(
    [
        T.StructField("agent", T.StringType()),
        T.StructField("agent_email_id", T.StringType()),
        T.StructField("case_number", T.StringType()),
        T.StructField("date", T.DateType()),
        T.StructField("survey_response_date", T.DateType()),
        T.StructField("survey_score", T.IntegerType()),
    ]
)


def make_tnps(spark, rows):
    defaults = {
        "agent": "jane.doe",
        "agent_email_id": _mock_email("jane.doe"),
        "case_number": "1001",
        "date": dt.date(2026, 5, 15),
        "survey_response_date": dt.date(2026, 5, 15),
        "survey_score": 10,
    }
    data = [{**defaults, **r} for r in rows]
    return spark.createDataFrame(
        [tuple(r[f.name] for f in _TNPS_SCHEMA.fields) for r in data], _TNPS_SCHEMA
    )


_ROSTER_SCHEMA = T.StructType(
    [
        T.StructField("agent", T.StringType()),
        T.StructField("xforce", T.StringType()),
        T.StructField("xplead", T.StringType()),
        T.StructField("team", T.StringType()),
        T.StructField("squad", T.StringType()),
        T.StructField("squad_district", T.StringType()),
        T.StructField("status", T.StringType()),
        T.StructField("shift", T.StringType()),
        T.StructField("snapshot_date", T.DateType()),
        T.StructField("snapshot_month", T.DateType()),
    ]
)


def make_roster(spark, rows):
    defaults = {
        "agent": "jane.doe",
        "xforce": "lead.one",
        "xplead": "boss.one",
        "team": "social media",
        "squad": "social",
        "squad_district": "social",
        "status": "active",
        "shift": "morning",
        "snapshot_date": dt.date(2026, 5, 31),
        "snapshot_month": dt.date(2026, 5, 1),
    }
    data = [{**defaults, **r} for r in rows]
    return spark.createDataFrame(
        [tuple(r[f.name] for f in _ROSTER_SCHEMA.fields) for r in data], _ROSTER_SCHEMA
    )


def _collect(out):
    return out.collect()


# ---------------------------------------------------------------------------
# compute_tnps_responses — end-to-end (one row per survey response)
# ---------------------------------------------------------------------------


class TestComputeTnpsResponses:
    def test_one_response_one_row(self, spark):
        out = compute_tnps_responses(
            make_roster(spark, [{}]), make_tnps(spark, [{"survey_score": 9}])
        )
        rows = _collect(out)
        assert len(rows) == 1
        row = rows[0]
        assert row["agent"] == "jane.doe"
        assert row["date"] == dt.date(2026, 5, 15)
        assert row["case_number"] == "1001"
        assert int(row["survey_score"]) == 9
        assert row["team"] == "social media"
        assert row["squad"] == "social"
        assert row["district"] == "social"
        assert row["shift"] == "morning"

    def test_two_responses_two_rows(self, spark):
        out = compute_tnps_responses(
            make_roster(spark, [{}]),
            make_tnps(
                spark,
                [
                    {"case_number": "1001", "survey_score": 10},
                    {"case_number": "1002", "survey_score": 4},
                ],
            ),
        )
        rows = _collect(out)
        assert len(rows) == 2
        assert sorted(r["case_number"] for r in rows) == ["1001", "1002"]

    def test_same_case_two_responses_both_kept(self, spark):
        # RAW layer does NOT dedup the case — both responses survive (the metric
        # layer does classify-then-distinct).
        out = compute_tnps_responses(
            make_roster(spark, [{}]),
            make_tnps(
                spark,
                [
                    {"case_number": "1001", "survey_score": 10},
                    {"case_number": "1001", "survey_score": 3},
                ],
            ),
        )
        assert len(_collect(out)) == 2

    def test_no_classification_or_aggregation(self, spark):
        # Raw scores are preserved verbatim (no promoter/detractor mapping).
        out = compute_tnps_responses(
            make_roster(spark, [{}]),
            make_tnps(spark, [{"case_number": "c", "survey_score": 7}]),
        )
        assert int(_collect(out)[0]["survey_score"]) == 7

    def test_null_score_kept(self, spark):
        out = compute_tnps_responses(
            make_roster(spark, [{}]),
            make_tnps(spark, [{"survey_score": None}]),
        )
        rows = _collect(out)
        assert len(rows) == 1
        assert rows[0]["survey_score"] is None

    def test_unattributed_agent_dropped(self, spark):
        out = compute_tnps_responses(
            make_roster(spark, [{}]),
            make_tnps(spark, [{"agent": ""}]),
        )
        assert len(out.take(1)) == 0

    def test_null_agent_dropped(self, spark):
        out = compute_tnps_responses(
            make_roster(spark, [{}]),
            make_tnps(spark, [{"agent": None}]),
        )
        assert len(out.take(1)) == 0

    def test_inactive_agent_dropped(self, spark):
        out = compute_tnps_responses(
            make_roster(spark, [{"status": "inactive"}]),
            make_tnps(spark, [{}]),
        )
        assert len(out.take(1)) == 0

    def test_null_squad_agent_dropped(self, spark):
        out = compute_tnps_responses(
            make_roster(spark, [{"squad": None}]),
            make_tnps(spark, [{}]),
        )
        assert len(out.take(1)) == 0

    def test_no_roster_match_dropped(self, spark):
        out = compute_tnps_responses(
            make_roster(spark, [{"agent": "someone.else"}]),
            make_tnps(spark, [{}]),
        )
        assert len(out.take(1)) == 0

    def test_uses_natural_snapshot_month(self, spark):
        out = compute_tnps_responses(
            make_roster(
                spark,
                [
                    {"snapshot_month": dt.date(2026, 3, 1), "squad": "social"},
                    {"snapshot_month": dt.date(2026, 4, 1), "squad": "social_b"},
                ],
            ),
            make_tnps(
                spark,
                [
                    {"case_number": "mar", "date": dt.date(2026, 3, 10)},
                    {"case_number": "apr", "date": dt.date(2026, 4, 10)},
                ],
            ),
        )
        rows = _collect(out)
        assert len(rows) == 2
        squads_by_case = {r["case_number"]: r["squad"] for r in rows}
        assert squads_by_case["mar"] == "social"
        assert squads_by_case["apr"] == "social_b"

    def test_roster_fanout_deduped(self, spark):
        # Two identical roster rows for the same (agent, snapshot_month) must not
        # fan out the inner join into duplicate response rows.
        out = compute_tnps_responses(
            make_roster(
                spark,
                [
                    {"snapshot_date": dt.date(2026, 5, 31)},
                    {"snapshot_date": dt.date(2026, 5, 15)},
                ],
            ),
            make_tnps(spark, [{}]),
        )
        assert len(_collect(out)) == 1

    def test_outage_date_not_filtered(self, spark):
        # The legacy 2026-03-27 outage exclusion is a metrics-layer concern.
        out = compute_tnps_responses(
            make_roster(spark, [{"snapshot_month": dt.date(2026, 3, 1)}]),
            make_tnps(spark, [{"date": dt.date(2026, 3, 27)}]),
        )
        assert len(_collect(out)) == 1

    def test_validity_window_not_applied(self, spark):
        # survey_response_date far after closure is kept (deferred to metrics).
        out = compute_tnps_responses(
            make_roster(spark, [{}]),
            make_tnps(
                spark,
                [
                    {
                        "date": dt.date(2026, 5, 15),
                        "survey_response_date": dt.date(2026, 5, 30),
                    }
                ],
            ),
        )
        assert len(_collect(out)) == 1

    def test_out_of_scope_squads_constant_is_empty(self, spark):
        assert TNPS_OUT_OF_SCOPE_SQUADS == ()

    def test_output_schema_and_column_order(self, spark):
        out = compute_tnps_responses(make_roster(spark, [{}]), make_tnps(spark, [{}]))
        assert out.columns == [c for c, _ in IO_TNPS_RESPONSES_SCHEMA]

    def test_empty_input_yields_empty_frame_with_schema(self, spark):
        empty = spark.createDataFrame([], _TNPS_SCHEMA)
        out = compute_tnps_responses(make_roster(spark, [{}]), empty)
        assert len(out.take(1)) == 0
        assert out.columns == [c for c, _ in IO_TNPS_RESPONSES_SCHEMA]
