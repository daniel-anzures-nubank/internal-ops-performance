"""Unit tests for ``metrics_data/jobs_within_sla.py`` (PySpark).

Small synthetic frames mimicking the ``oos_jobs`` extractor + roster + SLA map,
no warehouse. We verify the content_id MOS parse (comment / ticket / bare / null),
the job grain split (row-grain macros/faq/ar vs content_id grouping), the SLA
INNER-JOIN drop, the all-or-nothing on-time flag, the ``(OOS_CONT)`` normalization,
the roster dedup + Content scoping, the date scoping, and ``parse_sla_map``.
"""

from __future__ import annotations

import datetime as dt

import pytest
from pyspark.sql import types as T

from jobs_within_sla import (
    IO_JOBS_WITHIN_SLA_SCHEMA,
    compute_jobs_within_sla,
    parse_sla_map,
)

_OOS_SCHEMA = T.StructType(
    [
        T.StructField("agent", T.StringType()),
        T.StructField("date", T.DateType()),
        T.StructField("local_start_date", T.TimestampType()),
        T.StructField("job_classification", T.StringType()),
        T.StructField("net_time_spent_seconds", T.LongType()),
        T.StructField("comment", T.StringType()),
        T.StructField("ticket__id", T.StringType()),
    ]
)

_ROSTER_SCHEMA = T.StructType(
    [
        T.StructField("agent", T.StringType()),
        T.StructField("xforce", T.StringType()),
        T.StructField("xplead", T.StringType()),
        T.StructField("team", T.StringType()),
        T.StructField("squad", T.StringType()),
        T.StructField("squad_district", T.StringType()),
        T.StructField("shift", T.StringType()),
        T.StructField("status", T.StringType()),
        T.StructField("snapshot_month", T.DateType()),
    ]
)

_SLA_SCHEMA = T.StructType(
    [
        T.StructField("job_type", T.StringType()),
        T.StructField("sla_seconds", T.LongType()),
    ]
)

D = dt.date(2026, 3, 4)
TS = dt.datetime(2026, 3, 4, 9, 0, 0)
MONTH = dt.date(2026, 3, 1)


def make_oos(spark, rows):
    defaults = {
        "agent": "aura.olvera",
        "date": D,
        "local_start_date": TS,
        "job_classification": "discovery",
        "net_time_spent_seconds": 100,
        "comment": "MOS-100",
        "ticket__id": None,
    }
    data = [{**defaults, **r} for r in rows]
    return spark.createDataFrame(
        [tuple(r[f.name] for f in _OOS_SCHEMA.fields) for r in data], _OOS_SCHEMA
    )


def make_roster(spark, rows=None):
    defaults = {
        "agent": "aura.olvera",
        "xforce": "cxf",
        "xplead": "cxp",
        "team": "content",
        "squad": "enablement",
        "squad_district": "content",
        "shift": None,
        "status": "active",
        "snapshot_month": MONTH,
    }
    rows = rows if rows is not None else [{}]
    data = [{**defaults, **r} for r in rows]
    return spark.createDataFrame(
        [tuple(r[f.name] for f in _ROSTER_SCHEMA.fields) for r in data], _ROSTER_SCHEMA
    )


def make_sla_map(spark, mapping):
    return spark.createDataFrame(list(mapping.items()), _SLA_SCHEMA)


def _rows(df):
    return [r.asDict() for r in df.collect()]


DEFAULT_MAP = {"discovery": 10800, "macros": 1200, "sync": 3600}


class TestGrain:
    def test_content_id_grouping_sums_seconds(self, spark):
        # Two source rows, same content_id, non-row job type -> ONE job, summed.
        oos = make_oos(
            spark,
            [
                {"job_classification": "discovery", "comment": "MOS-100", "net_time_spent_seconds": 100},
                {"job_classification": "discovery", "comment": "MOS-100", "net_time_spent_seconds": 200},
            ],
        )
        out = _rows(compute_jobs_within_sla(oos, make_roster(spark), make_sla_map(spark, DEFAULT_MAP)))
        assert len(out) == 1
        assert out[0]["content_id"] == "MOS-100"
        assert out[0]["actual_seconds"] == 300

    def test_row_grain_types_one_row_each(self, spark):
        # macros/faq/ar: one job per source row (NOT grouped by content_id).
        oos = make_oos(
            spark,
            [
                {"job_classification": "macros", "comment": "x", "net_time_spent_seconds": 50},
                {"job_classification": "macros", "comment": "x", "net_time_spent_seconds": 60},
            ],
        )
        out = _rows(compute_jobs_within_sla(oos, make_roster(spark), make_sla_map(spark, DEFAULT_MAP)))
        assert len(out) == 2
        assert sorted(r["actual_seconds"] for r in out) == [50, 60]

    def test_non_row_null_content_id_dropped(self, spark):
        # A non-row job with no parseable content_id is dropped (legacy content_id IS NOT NULL).
        oos = make_oos(spark, [{"job_classification": "discovery", "comment": "no ticket", "ticket__id": None}])
        out = _rows(compute_jobs_within_sla(oos, make_roster(spark), make_sla_map(spark, DEFAULT_MAP)))
        assert out == []


class TestContentId:
    @pytest.mark.parametrize(
        "comment,ticket,expected",
        [
            ("MOS-123", None, "MOS-123"),
            ("mos 456", None, "MOS-456"),
            ("789", None, "MOS-789"),
            ("TICKET MOS-321", None, "MOS-321"),
            (None, "MOS-999", "MOS-999"),
            ("garbage", "555", "MOS-555"),  # falls through to ticket
        ],
    )
    def test_mos_parse(self, spark, comment, ticket, expected):
        oos = make_oos(spark, [{"job_classification": "discovery", "comment": comment, "ticket__id": ticket}])
        out = _rows(compute_jobs_within_sla(oos, make_roster(spark), make_sla_map(spark, DEFAULT_MAP)))
        assert len(out) == 1 and out[0]["content_id"] == expected


class TestSlaAndFlags:
    def test_inner_join_drops_no_sla_types(self, spark):
        # 'weduka_x' is not in the map -> dropped by the INNER JOIN.
        oos = make_oos(
            spark,
            [
                {"job_classification": "discovery", "comment": "MOS-100"},
                {"job_classification": "weduka_x", "comment": "MOS-200"},
            ],
        )
        out = _rows(compute_jobs_within_sla(oos, make_roster(spark), make_sla_map(spark, DEFAULT_MAP)))
        assert [r["job_type"] for r in out] == ["discovery"]

    def test_within_sla_all_or_nothing(self, spark):
        # within (<= sla): full sla credited; over: 0.
        oos = make_oos(
            spark,
            [
                {"job_classification": "discovery", "comment": "MOS-100", "net_time_spent_seconds": 5000},   # <= 10800
                {"job_classification": "discovery", "comment": "MOS-200", "net_time_spent_seconds": 20000},  # > 10800
            ],
        )
        out = {r["content_id"]: r for r in _rows(compute_jobs_within_sla(oos, make_roster(spark), make_sla_map(spark, DEFAULT_MAP)))}
        assert out["MOS-100"]["within_sla"] == 1 and out["MOS-100"]["sla_met_seconds"] == 10800
        assert out["MOS-200"]["within_sla"] == 0 and out["MOS-200"]["sla_met_seconds"] == 0

    def test_oos_cont_prefix_normalized(self, spark):
        # '(OOS_CONT) Sync' -> 'sync' (strip prefix, trim, lower, spaces->_).
        oos = make_oos(spark, [{"job_classification": "(OOS_CONT) Sync", "comment": "MOS-100"}])
        out = _rows(compute_jobs_within_sla(oos, make_roster(spark), make_sla_map(spark, DEFAULT_MAP)))
        assert len(out) == 1 and out[0]["job_type"] == "sync"


class TestScoping:
    def test_roster_dedup_no_fanout(self, spark):
        # Two identical roster rows (Content sheet target_squad dup) -> one job, no fan-out.
        roster = make_roster(spark, [{}, {}])
        oos = make_oos(spark, [{"job_classification": "discovery", "comment": "MOS-100"}])
        out = _rows(compute_jobs_within_sla(oos, roster, make_sla_map(spark, DEFAULT_MAP)))
        assert len(out) == 1

    def test_non_content_agent_dropped(self, spark):
        # A core agent's OOS job is dropped (roster inner join is Content-only).
        roster = make_roster(spark, [{"agent": "core.guy", "team": "core"}])
        oos = make_oos(spark, [{"agent": "core.guy", "job_classification": "discovery", "comment": "MOS-1"}])
        out = _rows(compute_jobs_within_sla(oos, roster, make_sla_map(spark, DEFAULT_MAP)))
        assert out == []

    def test_date_scoping(self, spark):
        # Drops < 2025-12-01 and the outage dates (2026-03-10); keeps others.
        oos = make_oos(
            spark,
            [
                {"comment": "MOS-111", "date": dt.date(2026, 3, 4), "local_start_date": dt.datetime(2026, 3, 4, 9)},
                {"comment": "MOS-222", "date": dt.date(2025, 11, 1), "local_start_date": dt.datetime(2025, 11, 1, 9)},
                {"comment": "MOS-333", "date": dt.date(2026, 3, 10), "local_start_date": dt.datetime(2026, 3, 10, 9)},
            ],
        )
        roster = make_roster(spark, [{"snapshot_month": dt.date(2026, 3, 1)}])
        out = _rows(compute_jobs_within_sla(oos, roster, make_sla_map(spark, DEFAULT_MAP)))
        assert {r["content_id"] for r in out} == {"MOS-111"}


class TestParseSlaMap:
    def test_none_raises(self):
        with pytest.raises(ValueError):
            parse_sla_map(None)

    def test_filters_bad_seconds_and_dedups(self, spark):
        raw = spark.createDataFrame(
            [("discovery", "10800"), ("sync", "0"), ("faq", None), ("discovery", "9999")],
            T.StructType([
                T.StructField("job_type", T.StringType()),
                T.StructField("sla_seconds", T.StringType()),
            ]),
        )
        m = {r["job_type"]: r["sla_seconds"] for r in parse_sla_map(raw).collect()}
        # sync (0) and faq (null) dropped; discovery deduped to one row.
        assert set(m) == {"discovery"}

    def test_empty_after_parse_raises(self, spark):
        raw = spark.createDataFrame(
            [("sync", "0")],
            T.StructType([
                T.StructField("job_type", T.StringType()),
                T.StructField("sla_seconds", T.StringType()),
            ]),
        )
        with pytest.raises(ValueError):
            parse_sla_map(raw)


def test_output_schema(spark):
    oos = make_oos(spark, [{"job_classification": "discovery", "comment": "MOS-100"}])
    out = compute_jobs_within_sla(oos, make_roster(spark), make_sla_map(spark, DEFAULT_MAP))
    assert out.columns == [c for c, _ in IO_JOBS_WITHIN_SLA_SCHEMA]
