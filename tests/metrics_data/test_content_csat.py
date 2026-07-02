"""Unit tests for ``metrics_data/content_csat.py`` (PySpark).

Small synthetic Spark frames, no warehouse.

content_csat is a RAW dataset: one row per Content CSAT survey response fanned
out to each content agent serving the rated ``target_squad``. We verify the
per-response promoter count / csat_score (>= 4 promoter, NULL not a promoter, the
5-question denominator — legacy scores 5 of the survey's 8 columns), the
target_squad normalization (E.M.I./GENERAL ->
emi_general, else lower), the target_squad-based fan-out join (one response
credited to every serving agent), the roster dedup (a duplicated roster row does
NOT double-count), and the output contract.
"""

from __future__ import annotations

import datetime as dt

from pyspark.sql import types as T

from content_csat import (
    IO_CONTENT_CSAT_SCHEMA,
    NUMBER_OF_QUESTIONS,
    compute_content_csat,
)

_MEXICO_NUBANK_DOMAIN = ".".join(["nubank", "com", "mx"])


def _mock_email(local: str, domain: str = _MEXICO_NUBANK_DOMAIN) -> str:
    return f"{local}@{domain}"


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------

_CSAT_SCHEMA = T.StructType(
    [
        T.StructField("survey_timestamp", T.TimestampType()),
        T.StructField("date_reference", T.TimestampType()),
        T.StructField("requested_by", T.StringType()),
        T.StructField("email_address", T.StringType()),
        T.StructField("squad", T.StringType()),
        T.StructField("mes", T.StringType()),
        T.StructField("facilidad", T.IntegerType()),
        T.StructField("comprension", T.IntegerType()),
        T.StructField("comunicacion", T.IntegerType()),
        T.StructField("calidad", T.IntegerType()),
        T.StructField("tiempo", T.IntegerType()),
        T.StructField("manejo_de_cambios", T.IntegerType()),
        T.StructField("expectativas", T.IntegerType()),
        T.StructField("aportacion_estrategica", T.IntegerType()),
        T.StructField("nps", T.IntegerType()),
    ]
)


def make_csat(spark, rows):
    # Default: all questions = 5 (all promoters). Only the first 5 columns are
    # scored by legacy; the trailing 3 are set too but ignored by the metric.
    defaults = {
        "survey_timestamp": dt.datetime(2026, 4, 9, 15, 14, 11),
        "date_reference": dt.datetime(2026, 3, 9, 15, 14, 11),
        "requested_by": "julio.duran",
        "email_address": _mock_email("julio.duran"),
        "squad": "TXN",
        "mes": "Marzo 2026",
        "facilidad": 5,
        "comprension": 5,
        "comunicacion": 5,
        "calidad": 5,
        "tiempo": 5,
        "manejo_de_cambios": 5,
        "expectativas": 5,
        "aportacion_estrategica": 5,
        "nps": 9,
    }
    data = [{**defaults, **r} for r in rows]
    return spark.createDataFrame(
        [tuple(r[f.name] for f in _CSAT_SCHEMA.fields) for r in data], _CSAT_SCHEMA
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
        T.StructField("target_squad", T.StringType()),
    ]
)


def make_roster(spark, rows):
    defaults = {
        "agent": "alejandra.erazo",
        "xforce": "karina.gonzalez",
        "xplead": "alejandra.mota",
        "team": "content",
        "squad": "enablement",
        "squad_district": "content",
        "status": "active",
        "shift": None,
        "snapshot_date": dt.date(2026, 3, 1),
        "snapshot_month": dt.date(2026, 3, 1),
        "target_squad": "txn",
    }
    data = [{**defaults, **r} for r in rows]
    return spark.createDataFrame(
        [tuple(r[f.name] for f in _ROSTER_SCHEMA.fields) for r in data],
        _ROSTER_SCHEMA,
    )


def _collect(out):
    return out.collect()


# ---------------------------------------------------------------------------
# compute_content_csat — end-to-end (one row per response × content agent)
# ---------------------------------------------------------------------------


class TestComputeContentCsat:
    def test_all_promoters(self, spark):
        out = compute_content_csat(make_roster(spark, [{}]), make_csat(spark, [{}]))
        rows = _collect(out)
        assert len(rows) == 1
        row = rows[0]
        assert row["promoters"] == 5
        assert row["number_of_questions"] == NUMBER_OF_QUESTIONS == 5
        assert row["csat_score"] == 1.0
        assert row["agent"] == "alejandra.erazo"
        assert row["team"] == "content"
        assert row["squad"] == "enablement"
        assert row["district"] == "content"
        assert row["target_squad"] == "txn"
        assert row["date"] == dt.date(2026, 3, 9)

    def test_promoter_threshold(self, spark):
        # scores >= 4 are promoters; 3 is not.
        out = compute_content_csat(
            make_roster(spark, [{}]),
            make_csat(
                spark,
                [
                    {
                        "facilidad": 4,
                        "comprension": 3,
                        "comunicacion": 5,
                        "calidad": 1,
                        "tiempo": 4,
                        "manejo_de_cambios": 2,
                        "expectativas": 4,
                        "aportacion_estrategica": 5,
                    }
                ],
            ),
        )
        row = _collect(out)[0]
        # Scored set = questions 2-5 + expectativas (legacy qa_base ~L3859):
        # comprension=3(✗), comunicacion=5(✓), calidad=1(✗), tiempo=4(✓),
        # expectativas=4(✓) -> 3 promoters. facilidad, manejo_de_cambios and
        # aportacion_estrategica are ignored.
        assert row["promoters"] == 3
        assert abs(row["csat_score"] - 3 / 5) < 1e-9

    def test_tiempo_exclusion_drops_question_for_flagged_agent_may(self, spark):
        # jesus.morales in May 2026: the 'tiempo' question is excluded ->
        # number_of_questions 5 -> 4 and its promoter flag leaves the numerator.
        out = compute_content_csat(
            make_roster(spark, [{"agent": "jesus.morales",
                                 "snapshot_date": dt.date(2026, 5, 1),
                                 "snapshot_month": dt.date(2026, 5, 1)}]),
            make_csat(spark, [{"date_reference": dt.datetime(2026, 5, 9, 12, 0, 0)}]),
        )
        row = _collect(out)[0]
        assert row["number_of_questions"] == 4
        assert row["promoters"] == 4          # 5 promoters - tiempo(=5) promoter
        assert abs(row["csat_score"] - 1.0) < 1e-9

    def test_tiempo_exclusion_not_applied_to_other_agent(self, spark):
        out = compute_content_csat(
            make_roster(spark, [{"agent": "someone.else",
                                 "snapshot_date": dt.date(2026, 5, 1),
                                 "snapshot_month": dt.date(2026, 5, 1)}]),
            make_csat(spark, [{"date_reference": dt.datetime(2026, 5, 9, 12, 0, 0)}]),
        )
        row = _collect(out)[0]
        assert row["number_of_questions"] == 5 and row["promoters"] == 5

    def test_tiempo_exclusion_not_applied_other_month(self, spark):
        # Same flagged agent, but April 2026 -> no exclusion.
        out = compute_content_csat(
            make_roster(spark, [{"agent": "jesus.morales",
                                 "snapshot_date": dt.date(2026, 4, 1),
                                 "snapshot_month": dt.date(2026, 4, 1)}]),
            make_csat(spark, [{"date_reference": dt.datetime(2026, 4, 9, 12, 0, 0)}]),
        )
        row = _collect(out)[0]
        assert row["number_of_questions"] == 5 and row["promoters"] == 5

    def test_null_score_not_promoter(self, spark):
        out = compute_content_csat(
            make_roster(spark, [{}]),
            make_csat(spark, [{"expectativas": None}]),
        )
        # expectativas null (not a promoter); the other 4 scored questions = 5 -> 4.
        assert _collect(out)[0]["promoters"] == 4

    def test_facilidad_not_scored(self, spark):
        # facilidad is question 1 of the survey but NOT in legacy's scored set —
        # zeroing it must not move the score (the old "first 5" reading did).
        out = compute_content_csat(
            make_roster(spark, [{}]),
            make_csat(spark, [{"facilidad": 1}]),
        )
        row = _collect(out)[0]
        assert row["promoters"] == 5
        assert row["number_of_questions"] == 5

    def test_all_null_scores_zero_promoters(self, spark):
        out = compute_content_csat(
            make_roster(spark, [{}]),
            make_csat(
                spark,
                [
                    {
                        "facilidad": None,
                        "comprension": None,
                        "comunicacion": None,
                        "calidad": None,
                        "tiempo": None,
                        "manejo_de_cambios": None,
                        "expectativas": None,
                        "aportacion_estrategica": None,
                    }
                ],
            ),
        )
        row = _collect(out)[0]
        assert row["promoters"] == 0
        assert row["number_of_questions"] == 5
        assert row["csat_score"] == 0.0

    def test_emi_label_normalizes(self, spark):
        out = compute_content_csat(
            make_roster(spark, [{"target_squad": "emi_general"}]),
            make_csat(spark, [{"squad": "E.M.I."}]),
        )
        rows = _collect(out)
        assert len(rows) == 1
        assert rows[0]["target_squad"] == "emi_general"

    def test_general_label_normalizes(self, spark):
        out = compute_content_csat(
            make_roster(spark, [{"target_squad": "emi_general"}]),
            make_csat(
                spark,
                [
                    {
                        "squad": "GENERAL (CHANNEL SOLUTIONS, PLANNING, SERVICE EXCELLENCE, QA, OPS DEFENSE)"
                    }
                ],
            ),
        )
        rows = _collect(out)
        assert len(rows) == 1
        assert rows[0]["target_squad"] == "emi_general"

    def test_other_squad_lowercased(self, spark):
        # A non-special display squad is just lowercased to match the roster.
        out = compute_content_csat(
            make_roster(spark, [{"target_squad": "idsec"}]),
            make_csat(spark, [{"squad": "IDSec"}]),
        )
        rows = _collect(out)
        assert len(rows) == 1
        assert rows[0]["target_squad"] == "idsec"

    def test_fan_out_to_all_serving_agents(self, spark):
        # One response, two content agents serving TXN -> two rows.
        roster = make_roster(
            spark,
            [
                {"agent": "alejandra.erazo", "target_squad": "txn"},
                {"agent": "aura.olvera", "target_squad": "txn"},
            ],
        )
        out = compute_content_csat(roster, make_csat(spark, [{"squad": "TXN"}]))
        rows = _collect(out)
        assert len(rows) == 2
        assert {r["agent"] for r in rows} == {"alejandra.erazo", "aura.olvera"}

    def test_roster_dedup_does_not_double_count(self, spark):
        # Two identical roster rows for the same (agent, target_squad,
        # snapshot_month) must not fan out the inner join into duplicates.
        roster = make_roster(
            spark,
            [
                {"snapshot_date": dt.date(2026, 3, 1)},
                {"snapshot_date": dt.date(2026, 3, 15)},
            ],
        )
        out = compute_content_csat(roster, make_csat(spark, [{}]))
        assert len(_collect(out)) == 1

    def test_no_match_for_other_target_squad(self, spark):
        out = compute_content_csat(
            make_roster(spark, [{"target_squad": "idsec"}]),
            make_csat(spark, [{"squad": "TXN"}]),
        )
        assert len(out.take(1)) == 0

    def test_inactive_agent_dropped(self, spark):
        out = compute_content_csat(
            make_roster(spark, [{"status": "inactive"}]),
            make_csat(spark, [{}]),
        )
        assert len(out.take(1)) == 0

    def test_null_target_squad_roster_dropped(self, spark):
        # A non-content (BDX) roster row has NULL target_squad and must not match.
        out = compute_content_csat(
            make_roster(spark, [{"target_squad": None}]),
            make_csat(spark, [{}]),
        )
        assert len(out.take(1)) == 0

    def test_empty_target_squad_roster_dropped(self, spark):
        out = compute_content_csat(
            make_roster(spark, [{"target_squad": ""}]),
            make_csat(spark, [{}]),
        )
        assert len(out.take(1)) == 0

    def test_month_attribution_join(self, spark):
        # A March-rated response only matches the March roster row.
        roster = make_roster(
            spark,
            [
                {"snapshot_month": dt.date(2026, 3, 1), "agent": "march.agent"},
                {"snapshot_month": dt.date(2026, 4, 1), "agent": "april.agent"},
            ],
        )
        out = compute_content_csat(roster, make_csat(spark, [{}]))  # ref March
        assert {r["agent"] for r in _collect(out)} == {"march.agent"}

    def test_date_is_date_reference_day(self, spark):
        # `date` is the DATE of date_reference (= survey_timestamp - 1 month,
        # already applied upstream). April-filled survey rates March.
        out = compute_content_csat(
            make_roster(spark, [{}]),
            make_csat(
                spark,
                [
                    {
                        "survey_timestamp": dt.datetime(2026, 4, 9, 15, 14, 11),
                        "date_reference": dt.datetime(2026, 3, 9, 15, 14, 11),
                    }
                ],
            ),
        )
        row = _collect(out)[0]
        assert row["date"] == dt.date(2026, 3, 9)
        assert row["survey_timestamp"] == dt.datetime(2026, 4, 9, 15, 14, 11)

    def test_aggregation_deferred(self, spark):
        # Two responses for the same agent stay as two rows (no per-agent rollup).
        out = compute_content_csat(
            make_roster(spark, [{}]),
            make_csat(
                spark,
                [
                    {
                        "requested_by": "a.one",
                        "survey_timestamp": dt.datetime(2026, 4, 9, 1),
                    },
                    {
                        "requested_by": "b.two",
                        "survey_timestamp": dt.datetime(2026, 4, 9, 2),
                    },
                ],
            ),
        )
        assert len(_collect(out)) == 2

    def test_nps_ignored(self, spark):
        # The separate `nps` column is not part of CSAT and is not surfaced.
        out = compute_content_csat(
            make_roster(spark, [{}]),
            make_csat(spark, [{"nps": 0}]),
        )
        row = _collect(out)[0]
        assert "nps" not in out.columns
        assert row["promoters"] == 5  # nps does not affect the promoter count

    def test_output_schema_and_column_order(self, spark):
        out = compute_content_csat(make_roster(spark, [{}]), make_csat(spark, [{}]))
        assert out.columns == [c for c, _ in IO_CONTENT_CSAT_SCHEMA]

    def test_empty_input_yields_empty_frame_with_schema(self, spark):
        empty = spark.createDataFrame([], _CSAT_SCHEMA)
        out = compute_content_csat(make_roster(spark, [{}]), empty)
        assert len(out.take(1)) == 0
        assert out.columns == [c for c, _ in IO_CONTENT_CSAT_SCHEMA]
