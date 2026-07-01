"""Unit tests for ``metrics/ntpj_xforce.py`` (PySpark).

Small synthetic agent-grain ``io_ntpj_metric`` frames. We verify the per-XForce
share of on-target agents (``ntpj <= 100``), the NULL-metric_value handling
(counts in the denominator, not the numerator), the exact ``<= 100`` tie rule,
the week+month-only grain gate, that only ``metric = 'ntpj'`` rows are consumed,
distinct-agent counting, the ``xforce``/``xplead`` split, and the output contract
(agent/squad/district/shift NULL, team carried, metric name, columns).
"""

from __future__ import annotations

import datetime as dt

from pyspark.sql import types as T

from metric_utils import METRIC_COLUMNS
from ntpj_xforce import (
    IO_NTPJ_XFORCE_METRIC_SCHEMA,
    METRIC_NAME,
    compute_ntpj_xforce,
)

M = dt.date(2026, 5, 1)   # a month bucket

_SCHEMA = T.StructType(
    [
        T.StructField("agent", T.StringType()),
        T.StructField("xforce", T.StringType()),
        T.StructField("xplead", T.StringType()),
        T.StructField("team", T.StringType()),
        T.StructField("squad", T.StringType()),
        T.StructField("district", T.StringType()),
        T.StructField("shift", T.StringType()),
        T.StructField("date_reference", T.DateType()),
        T.StructField("date_granularity", T.StringType()),
        T.StructField("metric", T.StringType()),
        T.StructField("numerator", T.DoubleType()),
        T.StructField("denominator", T.DoubleType()),
        T.StructField("metric_value", T.DoubleType()),
    ]
)


def ntpj(agent, mv, *, xforce="xf", xplead="xp", team="core", dref=M, gran="month"):
    """One agent-grain ``io_ntpj_metric`` row with ntpj ``metric_value = mv``."""
    return {
        "agent": agent, "xforce": xforce, "xplead": xplead, "team": team,
        "squad": "sq", "district": "di", "shift": "morning",
        "date_reference": dref, "date_granularity": gran, "metric": "ntpj",
        "numerator": 0.0, "denominator": 0.0,
        "metric_value": None if mv is None else float(mv),
    }


def frame(spark, rows):
    return spark.createDataFrame(rows, _SCHEMA)


def _only(rows, **kw):
    """Pick the single output row matching the given field filters."""
    out = [r for r in rows if all(r[k] == v for k, v in kw.items())]
    assert len(out) == 1, f"expected 1 row for {kw}, got {len(out)}"
    return out[0]


def test_basic_share_on_target(spark):
    # 3 agents: 2 on target (<=100), 1 over → num=2, den=3.
    rows = [ntpj("a", 80), ntpj("b", 100), ntpj("c", 140)]
    out = compute_ntpj_xforce(frame(spark, rows)).collect()
    r = _only([x.asDict() for x in out], date_granularity="month")
    assert r["numerator"] == 2.0
    assert r["denominator"] == 3.0
    assert abs(r["metric_value"] - (2 / 3 * 100)) < 1e-9


def test_null_metric_value_counts_in_denominator_only(spark):
    # NULL ntpj (denominator 0 upstream) fails `<= 100` → den but not num.
    rows = [ntpj("a", 90), ntpj("b", None)]
    r = _only(
        [x.asDict() for x in compute_ntpj_xforce(frame(spark, rows)).collect()],
        date_granularity="month",
    )
    assert r["numerator"] == 1.0
    assert r["denominator"] == 2.0
    assert abs(r["metric_value"] - 50.0) < 1e-9


def test_tie_at_100_is_on_target(spark):
    # Exactly 100 is on target; 100.0001 is not.
    rows = [ntpj("a", 100), ntpj("b", 100.0001)]
    r = _only(
        [x.asDict() for x in compute_ntpj_xforce(frame(spark, rows)).collect()],
        date_granularity="month",
    )
    assert r["numerator"] == 1.0
    assert r["denominator"] == 2.0


def test_only_week_and_month_grains(spark):
    # day / quarter rows are dropped; only week + month survive.
    rows = [
        ntpj("a", 90, gran="day"),
        ntpj("a", 90, gran="week"),
        ntpj("a", 90, gran="month"),
        ntpj("a", 90, gran="quarter"),
    ]
    grans = {
        x["date_granularity"]
        for x in [r.asDict() for r in compute_ntpj_xforce(frame(spark, rows)).collect()]
    }
    assert grans == {"week", "month"}


def test_only_ntpj_metric_consumed(spark):
    # A non-ntpj row for the same xforce is ignored.
    rows = [ntpj("a", 90), ntpj("b", 200)]
    rows.append({**ntpj("z", 0), "metric": "adherence"})
    r = _only(
        [x.asDict() for x in compute_ntpj_xforce(frame(spark, rows)).collect()],
        date_granularity="month",
    )
    assert r["denominator"] == 2.0  # z (adherence) excluded


def test_distinct_agents_not_double_counted(spark):
    # Same agent appears twice (e.g. duplicate upstream row) → counted once.
    rows = [ntpj("a", 90), ntpj("a", 90), ntpj("b", 90)]
    r = _only(
        [x.asDict() for x in compute_ntpj_xforce(frame(spark, rows)).collect()],
        date_granularity="month",
    )
    assert r["denominator"] == 2.0
    assert r["numerator"] == 2.0


def test_xforce_and_xplead_split(spark):
    rows = [
        ntpj("a", 90, xforce="xf1", xplead="p1"),
        ntpj("b", 140, xforce="xf1", xplead="p1"),
        ntpj("c", 90, xforce="xf2", xplead="p2"),
    ]
    out = [x.asDict() for x in compute_ntpj_xforce(frame(spark, rows)).collect()]
    xf1 = _only(out, xforce="xf1", date_granularity="month")
    xf2 = _only(out, xforce="xf2", date_granularity="month")
    assert (xf1["numerator"], xf1["denominator"]) == (1.0, 2.0)
    assert (xf2["numerator"], xf2["denominator"]) == (1.0, 1.0)


def test_content_uses_sla_target_ge_95(spark):
    # Content NTPJ is SLA-weighted compliance (>= 95 on target), NOT the <= 100
    # duration rule. 96 and 100 are on target; 94 and 80 (which WOULD pass the
    # Core/Fraud <= 100 rule) are not.
    rows = [
        ntpj("a", 96, team="content", xforce="cxf", xplead="cxp"),
        ntpj("b", 100, team="content", xforce="cxf", xplead="cxp"),
        ntpj("c", 94, team="content", xforce="cxf", xplead="cxp"),
        ntpj("d", 80, team="content", xforce="cxf", xplead="cxp"),
    ]
    r = _only(
        [x.asDict() for x in compute_ntpj_xforce(frame(spark, rows)).collect()],
        xforce="cxf", date_granularity="month",
    )
    assert r["denominator"] == 4.0
    assert r["numerator"] == 2.0  # only 96 and 100 clear >= 95


def test_output_contract(spark):
    rows = [ntpj("a", 90)]
    out = compute_ntpj_xforce(frame(spark, rows))
    assert out.columns == list(METRIC_COLUMNS)
    r = out.collect()[0].asDict()
    assert r["metric"] == METRIC_NAME == "ntpj_xforce"
    assert r["agent"] is None
    assert r["squad"] is None
    assert r["district"] is None
    assert r["shift"] is None
    assert r["team"] == "core"
    assert r["xforce"] == "xf"
    assert r["xplead"] == "xp"


def test_empty_input(spark):
    empty = spark.createDataFrame([], _SCHEMA)
    assert compute_ntpj_xforce(empty).count() == 0


def test_schema_constant_matches_columns():
    assert [c for c, _ in IO_NTPJ_XFORCE_METRIC_SCHEMA] == list(METRIC_COLUMNS)
