"""Unit tests for ``metrics_data/wows.py`` (PySpark).

Small synthetic Spark frames, no warehouse.

wows is a RAW dataset: one row per Social-Media WoW experience (no count, no
target). We verify the unresolved-agent filter, the roster join that attaches
the standardized dimensions (agent, xforce, xplead, team, squad, district,
shift) on the WoW's natural snapshot_month, the roster-fanout dedup, and the
output contract.
"""

from __future__ import annotations

import datetime as dt

from pyspark.sql import types as T

from wows import (
    IO_WOWS_SCHEMA,
    WOWS_OUT_OF_SCOPE_SQUADS,
    compute_wows,
)

_BRAZIL_NUBANK_DOMAIN = ".".join(["nubank", "com", "br"])


def _mock_email(local: str, domain: str = _BRAZIL_NUBANK_DOMAIN) -> str:
    return f"{local}@{domain}"


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------

_WOWS_SCHEMA = T.StructType(
    [
        T.StructField("agent", T.StringType()),
        T.StructField("agent_email", T.StringType()),
        T.StructField("case_id", T.StringType()),
        T.StructField("date", T.DateType()),
    ]
)


def make_wows(spark, rows):
    defaults = {
        "agent": "jane.doe",
        "agent_email": _mock_email("jane.doe"),
        "case_id": "2070635",
        "date": dt.date(2026, 5, 15),
    }
    data = [{**defaults, **r} for r in rows]
    return spark.createDataFrame(
        [tuple(r[f.name] for f in _WOWS_SCHEMA.fields) for r in data], _WOWS_SCHEMA
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
# compute_wows — end-to-end (one row per WoW experience)
# ---------------------------------------------------------------------------


class TestComputeWows:
    def test_one_wow_one_row(self, spark):
        out = compute_wows(
            make_roster(spark, [{}]), make_wows(spark, [{"case_id": "777"}])
        )
        rows = _collect(out)
        assert len(rows) == 1
        row = rows[0]
        assert row["agent"] == "jane.doe"
        assert row["date"] == dt.date(2026, 5, 15)
        assert row["case_id"] == "777"
        assert row["team"] == "social media"
        assert row["squad"] == "social"
        assert row["district"] == "social"
        assert row["shift"] == "morning"

    def test_two_wows_two_rows(self, spark):
        out = compute_wows(
            make_roster(spark, [{}]),
            make_wows(spark, [{"case_id": "1001"}, {"case_id": "1002"}]),
        )
        rows = _collect(out)
        assert len(rows) == 2
        assert sorted(r["case_id"] for r in rows) == ["1001", "1002"]

    def test_no_dedup_of_repeated_case_id(self, spark):
        # Raw grain: repeated case_id rows are kept; metrics counts DISTINCT.
        out = compute_wows(
            make_roster(spark, [{}]),
            make_wows(spark, [{"case_id": "dup"}, {"case_id": "dup"}]),
        )
        assert len(_collect(out)) == 2

    def test_unresolved_agent_dropped(self, spark):
        out = compute_wows(make_roster(spark, [{}]), make_wows(spark, [{"agent": ""}]))
        assert len(out.take(1)) == 0

    def test_null_agent_dropped(self, spark):
        out = compute_wows(make_roster(spark, [{}]), make_wows(spark, [{"agent": None}]))
        assert len(out.take(1)) == 0

    def test_inactive_agent_dropped(self, spark):
        out = compute_wows(
            make_roster(spark, [{"status": "inactive"}]), make_wows(spark, [{}])
        )
        assert len(out.take(1)) == 0

    def test_null_squad_agent_dropped(self, spark):
        out = compute_wows(
            make_roster(spark, [{"squad": None}]), make_wows(spark, [{}])
        )
        assert len(out.take(1)) == 0

    def test_no_roster_match_dropped(self, spark):
        out = compute_wows(
            make_roster(spark, [{"agent": "someone.else"}]), make_wows(spark, [{}])
        )
        assert len(out.take(1)) == 0

    def test_uses_natural_snapshot_month(self, spark):
        out = compute_wows(
            make_roster(
                spark,
                [
                    {"snapshot_month": dt.date(2026, 3, 1), "squad": "social"},
                    {"snapshot_month": dt.date(2026, 4, 1), "squad": "social_b"},
                ],
            ),
            make_wows(
                spark,
                [
                    {"case_id": "mar", "date": dt.date(2026, 3, 10)},
                    {"case_id": "apr", "date": dt.date(2026, 4, 10)},
                ],
            ),
        )
        rows = _collect(out)
        assert len(rows) == 2
        squads_by_case = {r["case_id"]: r["squad"] for r in rows}
        assert squads_by_case["mar"] == "social"
        assert squads_by_case["apr"] == "social_b"

    def test_roster_fanout_deduped(self, spark):
        # Two identical roster rows for the same (agent, snapshot_month) must not
        # fan out the inner join into duplicate WoW rows.
        out = compute_wows(
            make_roster(
                spark,
                [
                    {"snapshot_date": dt.date(2026, 5, 31)},
                    {"snapshot_date": dt.date(2026, 5, 15)},
                ],
            ),
            make_wows(spark, [{}]),
        )
        assert len(_collect(out)) == 1

    def test_outage_date_not_filtered(self, spark):
        # The legacy 2026-03-27 outage exclusion is a metrics-layer concern.
        out = compute_wows(
            make_roster(spark, [{"snapshot_month": dt.date(2026, 3, 1)}]),
            make_wows(spark, [{"date": dt.date(2026, 3, 27)}]),
        )
        assert len(_collect(out)) == 1

    def test_out_of_scope_squads_constant_is_empty(self, spark):
        assert WOWS_OUT_OF_SCOPE_SQUADS == ()

    def test_output_schema_and_column_order(self, spark):
        out = compute_wows(make_roster(spark, [{}]), make_wows(spark, [{}]))
        assert out.columns == [c for c, _ in IO_WOWS_SCHEMA]

    def test_empty_input_yields_empty_frame_with_schema(self, spark):
        empty = spark.createDataFrame([], _WOWS_SCHEMA)
        out = compute_wows(make_roster(spark, [{}]), empty)
        assert len(out.take(1)) == 0
        assert out.columns == [c for c, _ in IO_WOWS_SCHEMA]
