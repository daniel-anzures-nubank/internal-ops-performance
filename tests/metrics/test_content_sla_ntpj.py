"""Unit tests for ``metrics/content_sla_ntpj.py`` (PySpark).

Small synthetic ``io_jobs_within_sla_raw`` frames, no warehouse. We verify the
SLA-weighted compliance ratio (SUM(sla_met)/SUM(sla)), the ≤100 bound, the
active-roster filter, the output-period restriction, and the output contract
(metric name ``ntpj``, schema).
"""

from __future__ import annotations

import datetime as dt

from pyspark.sql import types as T

from content_sla_ntpj import compute_content_sla_ntpj
from ntpj import IO_NTPJ_METRIC_SCHEMA

_SCHEMA = T.StructType(
    [
        T.StructField("agent", T.StringType()),
        T.StructField("xforce", T.StringType()),
        T.StructField("xplead", T.StringType()),
        T.StructField("team", T.StringType()),
        T.StructField("squad", T.StringType()),
        T.StructField("district", T.StringType()),
        T.StructField("shift", T.StringType()),
        T.StructField("roster_status", T.StringType()),
        T.StructField("date", T.DateType()),
        T.StructField("job_type", T.StringType()),
        T.StructField("content_id", T.StringType()),
        T.StructField("actual_seconds", T.LongType()),
        T.StructField("sla_seconds", T.LongType()),
        T.StructField("within_sla", T.IntegerType()),
        T.StructField("sla_met_seconds", T.LongType()),
    ]
)

P_START = dt.date(2026, 3, 1)
P_END = dt.date(2026, 3, 31)
D = dt.date(2026, 3, 4)


def make_jw(spark, rows):
    defaults = {
        "agent": "aura.olvera",
        "xforce": "cxf",
        "xplead": "cxp",
        "team": "content",
        "squad": "enablement",
        "district": "content",
        "shift": None,
        "roster_status": "active",
        "date": D,
        "job_type": "discovery",
        "content_id": "MOS-1",
        "actual_seconds": 100,
        "sla_seconds": 100,
        "within_sla": 1,
        "sla_met_seconds": 100,
    }
    data = [{**defaults, **r} for r in rows]
    return spark.createDataFrame(
        [tuple(r[f.name] for f in _SCHEMA.fields) for r in data], _SCHEMA
    )


def _month(out, agent="aura.olvera"):
    rows = [
        r.asDict()
        for r in out.filter(out["date_granularity"] == "month").collect()
        if r["agent"] == agent
    ]
    assert len(rows) == 1
    return rows[0]


def test_compliance_ratio(spark):
    # one on-time job (sla 300, met 300), one late (sla 100, met 0):
    # numerator=300, denominator=400 -> 75.0
    out = compute_content_sla_ntpj(
        make_jw(
            spark,
            [
                {"content_id": "MOS-1", "sla_seconds": 300, "sla_met_seconds": 300, "within_sla": 1},
                {"content_id": "MOS-2", "sla_seconds": 100, "sla_met_seconds": 0, "within_sla": 0},
            ],
        ),
        P_START,
        P_END,
    )
    r = _month(out)
    assert r["metric"] == "ntpj"
    assert abs(r["numerator"] - 300.0) < 1e-9
    assert abs(r["denominator"] - 400.0) < 1e-9
    assert abs(r["metric_value"] - 75.0) < 1e-9


def test_bounded_at_100_when_all_on_time(spark):
    out = compute_content_sla_ntpj(
        make_jw(
            spark,
            [
                {"content_id": "MOS-1", "sla_seconds": 300, "sla_met_seconds": 300},
                {"content_id": "MOS-2", "sla_seconds": 100, "sla_met_seconds": 100},
            ],
        ),
        P_START,
        P_END,
    )
    assert abs(_month(out)["metric_value"] - 100.0) < 1e-9


def test_active_roster_only(spark):
    # An inactive-roster row is dropped before aggregation.
    out = compute_content_sla_ntpj(
        make_jw(
            spark,
            [
                {"agent": "a.active", "roster_status": "active", "sla_seconds": 100, "sla_met_seconds": 100},
                {"agent": "b.inactive", "roster_status": "inactive", "sla_seconds": 100, "sla_met_seconds": 0},
            ],
        ),
        P_START,
        P_END,
    )
    agents = {r["agent"] for r in out.filter(out["date_granularity"] == "month").collect()}
    assert agents == {"a.active"}


def test_period_restriction(spark):
    out = compute_content_sla_ntpj(
        make_jw(
            spark,
            [
                {"content_id": "MOS-in", "date": dt.date(2026, 3, 4)},
                {"content_id": "MOS-out", "date": dt.date(2026, 5, 4)},
            ],
        ),
        P_START,
        P_END,
    )
    # Only the March job survives -> the March-bucket numerator reflects one job.
    r = _month(out)
    assert abs(r["denominator"] - 100.0) < 1e-9


def test_output_schema(spark):
    out = compute_content_sla_ntpj(make_jw(spark, [{}]), P_START, P_END)
    assert out.columns == [c for c, _ in IO_NTPJ_METRIC_SCHEMA]


def test_empty_input(spark):
    out = compute_content_sla_ntpj(spark.createDataFrame([], _SCHEMA), P_START, P_END)
    assert out.count() == 0
