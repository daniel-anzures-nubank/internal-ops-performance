"""Unit tests for ``metrics/performance_metrics.py`` (PySpark).

Small synthetic ``io_*_metric`` frames. We verify the display-team cascade —
the direct team map (incl. ``social media -> Social Media``), the squad map
(``quality -> Quality``, ``enablement -> Content``, ``planning -> NULL``), and
the modal backfills (squad / xforce / xplead / district, ties broken
alphabetically, built only from adherence rows with a non-NULL display team) —
plus the output contract: column order and row-count preservation (pure UNION
ALL, no loss/dup).
"""

from __future__ import annotations

import datetime as dt

from pyspark.sql import types as T

from metric_utils import METRIC_COLUMNS
from performance_metrics import (
    IO_PERFORMANCE_METRICS_SCHEMA,
    compute_performance_metrics,
)

D = dt.date(2026, 5, 1)
OTHER_D = dt.date(2026, 4, 1)

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
_FIELDS = [f.name for f in _SCHEMA.fields]


def m(*, agent=None, xforce=None, xplead=None, team=None, squad=None,
      district=None, shift=None, dref=D, gran="month", metric="adherence",
      mv=100.0):
    return {
        "agent": agent, "xforce": xforce, "xplead": xplead, "team": team,
        "squad": squad, "district": district, "shift": shift,
        "date_reference": dref, "date_granularity": gran, "metric": metric,
        "numerator": 1.0, "denominator": 1.0,
        "metric_value": None if mv is None else float(mv),
    }


def adh(agent, team, squad, *, xforce=None, xplead=None, district=None,
        dref=D, gran="month"):
    return m(agent=agent, xforce=xforce, xplead=xplead, team=team, squad=squad,
             district=district, dref=dref, gran=gran, metric="adherence")


def frame(spark, rows):
    data = [tuple(r[name] for name in _FIELDS) for r in rows]
    return spark.createDataFrame(data, _SCHEMA)


def compute(spark, adherence_rows, other_frames_rows=()):
    return compute_performance_metrics(
        frame(spark, adherence_rows),
        [frame(spark, rows) for rows in other_frames_rows],
    )


def only(out):
    r = out.collect()
    assert len(r) == 1, f"expected 1 row, got {len(r)}"
    return r[0]


class TestDirectTeamMap:
    def test_direct_map_all_four_teams(self, spark):
        out = compute(spark, [
            adh("a", "core", "cta"),
            adh("b", "fraud", "idsec"),
            adh("c", "social media", "social"),
            adh("d", "content", "enablement"),
        ])
        by = {r["agent"]: r["team"] for r in out.collect()}
        assert by == {
            "a": "Core", "b": "Fraud", "c": "Social Media", "d": "Content",
        }

    def test_direct_map_wins_over_squad_map(self, spark):
        # `team` is checked before `squad`: a core agent on the enablement
        # squad stays Core.
        out = compute(spark, [adh("a", "core", "enablement")])
        assert only(out)["team"] == "Core"


class TestSquadMap:
    def test_quality_squad_maps_to_quality(self, spark):
        out = compute(spark, [adh("a", None, "quality")])
        assert only(out)["team"] == "Quality"

    def test_enablement_squad_maps_to_content(self, spark):
        out = compute(spark, [adh("a", None, "enablement")])
        assert only(out)["team"] == "Content"

    def test_planning_squad_stays_null(self, spark):
        # `planning` maps to no display team AND — squad being NOT NULL — never
        # falls through to the xforce lookup, even when its xforce has a modal
        # team from other adherence rows.
        out = compute(spark, [
            adh("a", None, "planning", xforce="xf1"),
            adh("b", "core", "cta", xforce="xf1"),
        ])
        by = {r["agent"]: r["team"] for r in out.collect()}
        assert by["a"] is None
        assert by["b"] == "Core"


class TestModalBackfill:
    def test_xforce_modal_backfill_for_rollup_row(self, spark):
        # A NULL-team NULL-squad XForce roll-up row (e.g. ntpj_xforce) picks up
        # the modal display team of its xforce from the adherence rows.
        rollup = m(xforce="xf1", metric="ntpj_xforce")
        out = compute(spark, [
            adh("a", "fraud", "idsec", xforce="xf1"),
            adh("b", "fraud", "idsec", xforce="xf1"),
            adh("c", "core", "cta", xforce="xf1"),
        ], [[rollup]])
        by = {r["metric"]: r["team"] for r in out.collect()}
        assert by["ntpj_xforce"] == "Fraud"

    def test_xforce_modal_tie_broken_alphabetically(self, spark):
        # 1 Core row vs 1 Fraud row on the same xforce: Core wins the tie.
        rollup = m(xforce="xf1", metric="xforce_index")
        out = compute(spark, [
            adh("a", "core", "cta", xforce="xf1"),
            adh("b", "fraud", "idsec", xforce="xf1"),
        ], [[rollup]])
        by = {r["metric"]: r["team"] for r in out.collect()}
        assert by["xforce_index"] == "Core"

    def test_null_display_adherence_rows_excluded_from_dims(self, spark):
        # Two planning rows (display team NULL) on xf1 must NOT out-vote the
        # single core row — the dims are built only from labeled rows.
        rollup = m(xforce="xf1", metric="xforce_index")
        out = compute(spark, [
            adh("a", None, "planning", xforce="xf1"),
            adh("b", None, "planning", xforce="xf1"),
            adh("c", "core", "cta", xforce="xf1"),
        ], [[rollup]])
        by = {r["metric"]: r["team"] for r in out.collect()}
        assert by["xforce_index"] == "Core"

    def test_modal_is_bucket_scoped(self, spark):
        # The modal dim is keyed on (date_reference, date_granularity): another
        # bucket's adherence rows don't leak in.
        rollup = m(xforce="xf1", metric="xforce_index")
        out = compute(spark, [
            adh("a", "fraud", "idsec", xforce="xf1", dref=OTHER_D),
        ], [[rollup]])
        by = {r["metric"]: r["team"] for r in out.collect()}
        assert by["xforce_index"] is None

    def test_xplead_fallback_when_xforce_null(self, spark):
        # An XPLead roll-up (xforce NULL, e.g. xpeers_in_target_xplead) uses
        # the modal team of its xplead.
        rollup = m(xplead="xp1", metric="xpeers_in_target_xplead")
        out = compute(spark, [
            adh("a", "social media", "social", xforce="xf1", xplead="xp1"),
        ], [[rollup]])
        by = {r["metric"]: r["team"] for r in out.collect()}
        assert by["xpeers_in_target_xplead"] == "Social Media"

    def test_squad_backfill_for_agent_null_squad_rollup(self, spark):
        # A squad roll-up row (agent NULL, team NULL, squad not in the squad
        # map, e.g. nuvinhos_performance_squad) uses the modal team of its
        # squad.
        rollup = m(squad="cta", metric="nuvinhos_performance_squad")
        out = compute(spark, [
            adh("a", "core", "cta"),
            adh("b", "core", "cta"),
        ], [[rollup]])
        by = {r["metric"]: r["team"] for r in out.collect()}
        assert by["nuvinhos_performance_squad"] == "Core"

    def test_district_fallback_when_all_else_null(self, spark):
        # A district roll-up (squad/xforce/xplead NULL) uses the modal team of
        # its district.
        rollup = m(district="content", metric="nuvinhos_performance_district")
        out = compute(spark, [
            adh("a", "content", "enablement", district="content"),
        ], [[rollup]])
        by = {r["metric"]: r["team"] for r in out.collect()}
        assert by["nuvinhos_performance_district"] == "Content"

    def test_all_dims_null_stays_null(self, spark):
        rollup = m(metric="mystery")
        out = compute(spark, [adh("a", "core", "cta")], [[rollup]])
        by = {r["metric"]: r["team"] for r in out.collect()}
        assert by["mystery"] is None


class TestContract:
    def test_column_order(self, spark):
        out = compute(spark, [adh("a", "core", "cta")])
        assert out.columns == list(METRIC_COLUMNS)
        assert [c for c, _ in IO_PERFORMANCE_METRICS_SCHEMA] == list(
            METRIC_COLUMNS
        )

    def test_row_count_preserved(self, spark):
        # Pure UNION ALL: every input row survives exactly once, including
        # rows that hit (or miss) the modal-dim joins.
        adherence = [
            adh("a", "core", "cta", xforce="xf1", xplead="xp1"),
            adh("b", "core", "cta", xforce="xf1", xplead="xp1"),
            adh("c", "fraud", "idsec", xforce="xf1", xplead="xp1"),
            adh("d", None, "planning", xforce="xf1"),
        ]
        others = [
            [m(agent="a", team="core", squad="cta", metric="ntpj"),
             m(agent="b", team="core", squad="cta", metric="ntpj")],
            [m(xforce="xf1", metric="ntpj_xforce")],
            [m(xplead="xp1", metric="xpeers_in_target_xplead")],
            [m(district="dx", metric="nuvinhos_performance_district")],
        ]
        out = compute(spark, adherence, others)
        expected = len(adherence) + sum(len(rows) for rows in others)
        assert out.count() == expected

    def test_empty_inputs_return_empty(self, spark):
        out = compute(spark, [], [[], []])
        assert len(out.take(1)) == 0
        assert out.columns == list(METRIC_COLUMNS)
