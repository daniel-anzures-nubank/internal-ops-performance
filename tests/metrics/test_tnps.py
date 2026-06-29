"""Unit tests for ``metrics/tnps.py`` (PySpark).

Small synthetic Spark frames mimicking the ``io_tnps_responses_raw`` table, no
warehouse. We verify the NPS math (promoters − detractors / valid), the
classify-then-COUNT(DISTINCT case_number) semantics (esp. a case carrying BOTH a
valid promoter and a valid detractor response → +1 to both distinct counts, net
0, denom +1 — NOT the old dedup-to-one-row −1), the validity +1-day window
boundary, the pre-cutover 2026-03-27 outage drop, neutral/null handling, negative
scores, the day/week/month aggregation, team scope, and the output contract.
"""

from __future__ import annotations

import datetime as dt

from pyspark.sql import types as T

from tnps import IO_TNPS_METRIC_SCHEMA, METRIC_NAME, compute_tnps

CLOSE = dt.date(2026, 5, 4)  # a Monday

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
        T.StructField("case_number", T.StringType()),
        T.StructField("survey_response_date", T.DateType()),
        T.StructField("survey_score", T.IntegerType()),
    ]
)


def make_raw(spark, rows):
    defaults = {
        "agent": "nuberto.lopez",
        "xforce": "nuliana.cruz",
        "xplead": "nuricio.diaz",
        "team": "social media",
        "squad": "social_es",
        "district": "social",
        "shift": "morning",
        "date": CLOSE,
        "case_number": "C1",
        "survey_response_date": CLOSE,
        "survey_score": 10,
    }
    data = [{**defaults, **r} for r in rows]
    return spark.createDataFrame(
        [tuple(r[f.name] for f in _RAW_SCHEMA.fields) for r in data], _RAW_SCHEMA
    )


def _day_rows(out):
    return {
        (r["agent"]): r
        for r in out.filter(out["date_granularity"] == "day").collect()
    }


def _one_day(out):
    rows = out.filter(out["date_granularity"] == "day").collect()
    assert len(rows) == 1, f"expected one day row, got {len(rows)}"
    return rows[0]


class TestComputeTnps:
    def test_basic_nps(self, spark):
        # [10, 9, 5, 7] -> promoters=2, detractors=1, valid=4 -> (2-1)/4 = 25%.
        rows = [
            {"case_number": "C1", "survey_score": 10},
            {"case_number": "C2", "survey_score": 9},
            {"case_number": "C3", "survey_score": 5},
            {"case_number": "C4", "survey_score": 7},
        ]
        row = _one_day(compute_tnps(make_raw(spark, rows)))
        assert row["numerator"] == 1.0
        assert row["denominator"] == 4.0
        assert row["metric_value"] == 25.0
        assert row["metric"] == METRIC_NAME

    def test_negative_nps(self, spark):
        # 1 promoter, 3 detractors, valid 4 -> (1-3)/4 = -50%.
        rows = [
            {"case_number": "C1", "survey_score": 10},
            {"case_number": "C2", "survey_score": 2},
            {"case_number": "C3", "survey_score": 4},
            {"case_number": "C4", "survey_score": 6},
        ]
        row = _one_day(compute_tnps(make_raw(spark, rows)))
        assert row["numerator"] == -2.0
        assert row["denominator"] == 4.0
        assert row["metric_value"] == -50.0

    def test_neutral_counts_only_in_denominator(self, spark):
        rows = [
            {"case_number": "C1", "survey_score": 9},  # promoter
            {"case_number": "C2", "survey_score": 7},  # neutral
            {"case_number": "C3", "survey_score": 8},  # neutral
        ]
        row = _one_day(compute_tnps(make_raw(spark, rows)))
        assert row["numerator"] == 1.0
        assert row["denominator"] == 3.0

    def test_boundaries(self, spark):
        # 9 = promoter, 6 = detractor (inclusive bounds).
        rows = [
            {"case_number": "C1", "survey_score": 9},
            {"case_number": "C2", "survey_score": 6},
        ]
        row = _one_day(compute_tnps(make_raw(spark, rows)))
        assert row["numerator"] == 0.0
        assert row["denominator"] == 2.0
        assert row["metric_value"] == 0.0

    def test_null_score_excluded_from_denominator(self, spark):
        rows = [
            {"case_number": "C1", "survey_score": 10},
            {"case_number": "C2", "survey_score": None},
        ]
        row = _one_day(compute_tnps(make_raw(spark, rows)))
        assert row["numerator"] == 1.0
        assert row["denominator"] == 1.0  # null score not valid

    def test_validity_window_excludes_late_response(self, spark):
        # response 2 days after close -> outside the +1 day window.
        rows = [
            {"case_number": "C1", "survey_score": 10, "survey_response_date": CLOSE},
            {
                "case_number": "C2",
                "survey_score": 2,
                "survey_response_date": CLOSE + dt.timedelta(days=2),
            },
        ]
        row = _one_day(compute_tnps(make_raw(spark, rows)))
        # only C1 survives -> 1 promoter / 1 valid.
        assert row["numerator"] == 1.0
        assert row["denominator"] == 1.0

    def test_validity_window_allows_next_day(self, spark):
        rows = [
            {
                "case_number": "C1",
                "survey_score": 2,
                "survey_response_date": CLOSE + dt.timedelta(days=1),
            },
        ]
        row = _one_day(compute_tnps(make_raw(spark, rows)))
        assert row["denominator"] == 1.0
        assert row["numerator"] == -1.0

    def test_null_response_date_dropped(self, spark):
        # A NULL survey_response_date falls outside the validity window.
        out = compute_tnps(
            make_raw(spark, [{"case_number": "C1", "survey_response_date": None}])
        )
        assert len(out.take(1)) == 0

    # --- the core classify-then-COUNT(DISTINCT) parity tests ----------------

    def test_case_with_promoter_and_detractor_nets_zero(self, spark):
        # SAME case has TWO valid responses of conflicting class (10 and 3).
        # Legacy classifies EVERY row then COUNT(DISTINCT case) per class:
        #   promoter distinct cases = 1, detractor distinct cases = 1 -> net 0
        #   valid distinct cases    = 1 -> denominator 1.
        # (The old dedup-to-one-row code wrongly yielded numerator -1.)
        rows = [
            {"case_number": "C1", "survey_score": 10, "survey_response_date": CLOSE},
            {
                "case_number": "C1",
                "survey_score": 3,
                "survey_response_date": CLOSE + dt.timedelta(days=1),
            },
        ]
        row = _one_day(compute_tnps(make_raw(spark, rows)))
        assert row["numerator"] == 0.0
        assert row["denominator"] == 1.0
        assert row["metric_value"] == 0.0

    def test_case_with_two_promoter_responses_counts_once(self, spark):
        # One case, two promoter responses -> distinct promoter cases = 1.
        rows = [
            {"case_number": "C1", "survey_score": 10, "survey_response_date": CLOSE},
            {"case_number": "C1", "survey_score": 9, "survey_response_date": CLOSE},
        ]
        row = _one_day(compute_tnps(make_raw(spark, rows)))
        assert row["numerator"] == 1.0
        assert row["denominator"] == 1.0
        assert row["metric_value"] == 100.0

    def test_case_promoter_plus_null_counts_promoter_and_valid(self, spark):
        # A scored promoter response + a null-score response on the same case:
        # has_valid=1, has_promoter=1 -> net 1, denom 1.
        rows = [
            {"case_number": "C1", "survey_score": 10, "survey_response_date": CLOSE},
            {
                "case_number": "C1",
                "survey_score": None,
                "survey_response_date": CLOSE,
            },
        ]
        row = _one_day(compute_tnps(make_raw(spark, rows)))
        assert row["numerator"] == 1.0
        assert row["denominator"] == 1.0

    def test_mixed_case_alongside_clean_cases(self, spark):
        # C1: promoter+detractor (net 0, denom 1); C2: promoter (net +1, denom 1);
        # C3: detractor (net -1, denom 1). Totals: numerator 0, denominator 3.
        rows = [
            {"case_number": "C1", "survey_score": 10},
            {"case_number": "C1", "survey_score": 2},
            {"case_number": "C2", "survey_score": 9},
            {"case_number": "C3", "survey_score": 4},
        ]
        row = _one_day(compute_tnps(make_raw(spark, rows)))
        assert row["numerator"] == 0.0
        assert row["denominator"] == 3.0
        assert row["metric_value"] == 0.0

    # --- outage-date exclusion (pre-cutover, SM-only) -----------------------

    def test_outage_date_dropped_pre_cutover(self, spark):
        # 2026-03-27 is dropped entirely (legacy general-access-problems day).
        out = compute_tnps(
            make_raw(
                spark,
                [
                    {
                        "case_number": "C1",
                        "survey_score": 10,
                        "date": dt.date(2026, 3, 27),
                        "survey_response_date": dt.date(2026, 3, 27),
                    }
                ],
            )
        )
        assert len(out.take(1)) == 0

    def test_non_outage_date_kept(self, spark):
        # The day before the outage is unaffected.
        out = compute_tnps(
            make_raw(
                spark,
                [
                    {
                        "case_number": "C1",
                        "survey_score": 10,
                        "date": dt.date(2026, 3, 26),
                        "survey_response_date": dt.date(2026, 3, 26),
                    }
                ],
            )
        )
        assert len(out.take(1)) > 0

    def test_outage_date_kept_post_cutover(self, spark):
        # Same calendar day in a post-cutover year is NOT dropped (cutover-gated).
        out = compute_tnps(
            make_raw(
                spark,
                [
                    {
                        "case_number": "C1",
                        "survey_score": 10,
                        "date": dt.date(2027, 3, 27),
                        "survey_response_date": dt.date(2027, 3, 27),
                    }
                ],
            )
        )
        assert len(out.take(1)) > 0

    # --- scope / aggregation / contract -------------------------------------

    def test_non_social_team_excluded(self, spark):
        out = compute_tnps(make_raw(spark, [{"team": "core", "survey_score": 10}]))
        assert len(out.take(1)) == 0

    def test_all_granularities_emitted(self, spark):
        out = compute_tnps(make_raw(spark, [{}]))
        grans = {r["date_granularity"] for r in out.select("date_granularity").collect()}
        assert grans == {"day", "week", "month", "quarter", "semester", "year"}

    def test_week_bucket_is_monday(self, spark):
        out = compute_tnps(
            make_raw(
                spark,
                [
                    {
                        "date": dt.date(2026, 5, 6),
                        "survey_response_date": dt.date(2026, 5, 6),
                    }
                ],
            )
        )
        week = out.filter(out["date_granularity"] == "week").collect()[0]
        assert week["date_reference"] == dt.date(2026, 5, 4)

    def test_per_agent_separation(self, spark):
        rows = [
            {"agent": "a.one", "case_number": "C1", "survey_score": 10},
            {"agent": "b.two", "case_number": "C2", "survey_score": 2},
        ]
        by_agent = _day_rows(compute_tnps(make_raw(spark, rows)))
        assert set(by_agent) == {"a.one", "b.two"}
        assert by_agent["a.one"]["metric_value"] == 100.0
        assert by_agent["b.two"]["metric_value"] == -100.0

    def test_output_schema_and_column_order(self, spark):
        out = compute_tnps(make_raw(spark, [{}]))
        assert out.columns == [c for c, _ in IO_TNPS_METRIC_SCHEMA]

    def test_empty_input_yields_empty_frame_with_schema(self, spark):
        empty = spark.createDataFrame([], _RAW_SCHEMA)
        out = compute_tnps(empty)
        assert len(out.take(1)) == 0
        assert out.columns == [c for c, _ in IO_TNPS_METRIC_SCHEMA]

    def test_all_non_social_yields_empty_frame_with_schema(self, spark):
        # Non-SM rows are filtered out -> the empty metric path still returns the
        # contract-shaped frame.
        out = compute_tnps(make_raw(spark, [{"team": "core"}]))
        assert len(out.take(1)) == 0
        assert out.columns == [c for c, _ in IO_TNPS_METRIC_SCHEMA]
