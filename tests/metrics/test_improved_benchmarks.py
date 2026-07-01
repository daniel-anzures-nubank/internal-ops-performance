"""Unit tests for ``metrics/improved_benchmarks.py`` (PySpark).

Small synthetic Spark frames, no warehouse. The NTPJ benchmark family now
consumes the ``normalized_time_per_job`` substrate directly (one row per
``(agent, job_id, benchmark_month, xforce, xplead, team, squad, district)`` with
``exp_duration_job``), so tests specify the benchmark value directly instead of
deriving it from raw job durations. The occupancy family still consumes
``io_occupancy_time_raw``.

We verify the NTPJ (lower=better) and occupancy (higher=better) month-over-month
comparisons, the tie rule, first-month exclusion, the squad + district + xforce
roll-ups, team scope (Core/Fraud only), and the output contract — plus the parity
fixes:

* **Fix #1** — squad / district roll-ups have NO month gate and NO team cutover
  (Core April squad rows are emitted).
* **Fix #2** — the XForce roll-up is gated ``< 2026-05-01`` flat for all teams,
  plus the ``david.fernandez`` Apr-2026 carve-out (non-david Core April survives).
* **Fix #3** — the occupancy benchmark starts 2026-03 (no February comparator).
* **Fix #5** — benchmark units are gated to ``(xforce, month)`` that have an
  ``ntpj_xforce`` row (derived from the substrate presence, or an explicit
  ``ntpj_xforce`` metric). A no-op for NTPJ units; drops NTPJ-absent occ units.
* **Fix #6** — a job_id splits across squad/district rows by the agent's roster
  attribution (carried in the substrate).

Test months use 2026-03 (the previous month / LAG comparator) and 2026-04 (the
compared/emitted month). NTPJ ``benchmark_month`` values are month-start dates
(as materialized); occupancy ``date`` values stay daily.
"""

from __future__ import annotations

import datetime as dt

from pyspark.sql import types as T

from improved_benchmarks import (
    DISTRICT_METRIC,
    IO_IMPROVED_BENCHMARKS_METRIC_SCHEMA,
    SQUAD_METRIC,
    XFORCE_METRIC,
    compute_improved_benchmarks,
)

# Occupancy daily dates.
PREV = dt.date(2026, 3, 15)   # previous month
CUR = dt.date(2026, 4, 15)    # compared month (emitted)
# NTPJ benchmark_month (month-start, as materialized).
PREV_M = dt.date(2026, 3, 1)
CUR_M = dt.date(2026, 4, 1)


# --------------------------------------------------------------------------- #
# Synthetic-frame builders (Spark)
# --------------------------------------------------------------------------- #
# normalized_time_per_job — the NTPJ benchmark substrate.
_NTPJ_SCHEMA = T.StructType(
    [
        T.StructField("agent", T.StringType()),
        T.StructField("job_id", T.StringType()),
        T.StructField("benchmark_month", T.DateType()),
        T.StructField("xforce", T.StringType()),
        T.StructField("xplead", T.StringType()),
        T.StructField("team", T.StringType()),
        T.StructField("squad", T.StringType()),
        T.StructField("district", T.StringType()),
        T.StructField("exp_duration_job", T.DoubleType()),
    ]
)

_OCC_SCHEMA = T.StructType(
    [
        T.StructField("agent", T.StringType()),
        T.StructField("xforce", T.StringType()),
        T.StructField("xplead", T.StringType()),
        T.StructField("team", T.StringType()),
        T.StructField("squad", T.StringType()),
        T.StructField("district", T.StringType()),
        T.StructField("shift", T.StringType()),
        T.StructField("date", T.DateType()),
        T.StructField("activity_type_required", T.StringType()),
        T.StructField("required_minutes", T.DoubleType()),
        T.StructField("occupancy_minutes", T.DoubleType()),
    ]
)


def _ntpj_defaults() -> dict:
    return {
        "agent": "a.one", "job_id": "jobA", "benchmark_month": CUR_M,
        "xforce": "x.one", "xplead": "p.one", "team": "fraud",
        "squad": "txn", "district": "csi", "exp_duration_job": 100.0,
    }


def make_ntpj(spark, rows):
    """Build normalized_time_per_job substrate rows (NTPJ benchmark)."""
    data = [{**_ntpj_defaults(), **r} for r in rows]
    return spark.createDataFrame(
        [tuple(d[f.name] for f in _NTPJ_SCHEMA.fields) for d in data], _NTPJ_SCHEMA
    )


def empty_ntpj(spark):
    return spark.createDataFrame([], _NTPJ_SCHEMA)


def _occ_defaults() -> dict:
    return {
        "agent": "a.one", "xforce": "x.one", "xplead": "p.one",
        "team": "fraud", "squad": "txn", "district": "csi", "shift": "morning",
        "date": CUR, "activity_type_required": "oos",
        "required_minutes": 100.0, "occupancy_minutes": 20.0,
    }


def make_occs(spark, rows):
    data = [{**_occ_defaults(), **r} for r in rows]
    return spark.createDataFrame(
        [tuple(d[f.name] for f in _OCC_SCHEMA.fields) for d in data], _OCC_SCHEMA
    )


def empty_occ(spark):
    return spark.createDataFrame([], _OCC_SCHEMA)


# io_ntpj_xforce_metric rows — the explicit gate driver (optional; the gate
# otherwise derives the identical presence from the substrate).
_NTPJX_SCHEMA = T.StructType(
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


def make_ntpj_xforce(spark, pairs):
    """Build ntpj_xforce gate rows from ``(xforce, date_reference[, granularity])``."""
    rows = []
    for p in pairs:
        xforce, dref = p[0], p[1]
        gran = p[2] if len(p) > 2 else "month"
        rows.append((
            None, xforce, "p.one", "fraud", None, None, None,
            dref, gran, "ntpj_xforce", 1.0, 1.0, 100.0,
        ))
    return spark.createDataFrame(rows, _NTPJX_SCHEMA)


def empty_ntpj_xforce(spark):
    return spark.createDataFrame([], _NTPJX_SCHEMA)


def _run(spark, ntpj=None, occ=None,
         start=dt.date(2026, 4, 1), end=dt.date(2026, 4, 30)):
    return compute_improved_benchmarks(
        ntpj if ntpj is not None else empty_ntpj(spark),
        occ if occ is not None else empty_occ(spark),
        start,
        end,
    )


def _by_metric(out, metric):
    return [r for r in out.collect() if r["metric"] == metric]


def _one(out, metric):
    rows = _by_metric(out, metric)
    assert len(rows) == 1, f"expected 1 {metric} row, got {len(rows)}"
    return rows[0]


# --------------------------------------------------------------------------- #
# NTPJ benchmark (lower is better)
# --------------------------------------------------------------------------- #
class TestNtpjBenchmark:
    def test_lower_benchmark_is_improved(self, spark):
        # jobA benchmark 200 (Mar) -> 100 (Apr): dropped -> improved.
        out = _run(spark, ntpj=make_ntpj(spark, [
            {"benchmark_month": PREV_M, "exp_duration_job": 200.0},
            {"benchmark_month": CUR_M, "exp_duration_job": 100.0},
        ]))
        squad = _one(out, SQUAD_METRIC)
        assert squad["numerator"] == 1.0
        assert squad["denominator"] == 1.0
        assert squad["metric_value"] == 100.0
        assert squad["squad"] == "txn"
        assert squad["district"] is None
        assert squad["date_reference"] == CUR_M
        assert squad["date_granularity"] == "month"

    def test_higher_benchmark_is_not_improved(self, spark):
        # 100 -> 200: benchmark rose -> not improved (counted but 0).
        out = _run(spark, ntpj=make_ntpj(spark, [
            {"benchmark_month": PREV_M, "exp_duration_job": 100.0},
            {"benchmark_month": CUR_M, "exp_duration_job": 200.0},
        ]))
        squad = _one(out, SQUAD_METRIC)
        assert squad["numerator"] == 0.0
        assert squad["denominator"] == 1.0
        assert squad["metric_value"] == 0.0

    def test_tie_counts_as_improved(self, spark):
        out = _run(spark, ntpj=make_ntpj(spark, [
            {"benchmark_month": PREV_M, "exp_duration_job": 150.0},
            {"benchmark_month": CUR_M, "exp_duration_job": 150.0},
        ]))
        squad = _one(out, SQUAD_METRIC)
        assert squad["numerator"] == 1.0
        assert squad["metric_value"] == 100.0

    def test_first_month_not_counted(self, spark):
        # Only the compared month present -> no previous -> denominator 0.
        out = _run(spark, ntpj=make_ntpj(spark, [
            {"benchmark_month": CUR_M, "exp_duration_job": 100.0},
        ]))
        squad = _one(out, SQUAD_METRIC)
        assert squad["denominator"] == 0.0
        assert squad["metric_value"] is None

    def test_emits_squad_district_and_xforce_rows(self, spark):
        out = _run(spark, ntpj=make_ntpj(spark, [
            {"benchmark_month": PREV_M, "exp_duration_job": 200.0},
            {"benchmark_month": CUR_M, "exp_duration_job": 100.0},
        ]))
        assert {r["metric"] for r in out.collect()} == {
            SQUAD_METRIC, DISTRICT_METRIC, XFORCE_METRIC,
        }
        district = _one(out, DISTRICT_METRIC)
        assert district["district"] == "csi"
        assert district["squad"] is None
        assert district["metric_value"] == 100.0

    def test_xforce_rollup_sets_xforce_and_xplead(self, spark):
        out = _run(spark, ntpj=make_ntpj(spark, [
            {"benchmark_month": PREV_M, "exp_duration_job": 200.0},
            {"benchmark_month": CUR_M, "exp_duration_job": 100.0},
        ]))
        xf = _one(out, XFORCE_METRIC)
        assert xf["xforce"] == "x.one"
        assert xf["xplead"] == "p.one"
        assert xf["squad"] is None and xf["district"] is None
        assert xf["numerator"] == 1.0 and xf["metric_value"] == 100.0

    def test_benchmark_rounded_before_compare(self, spark):
        # Two benchmarks differing beyond the 5th decimal are a tie -> improved.
        out = _run(spark, ntpj=make_ntpj(spark, [
            {"benchmark_month": PREV_M, "exp_duration_job": 100.000001},
            {"benchmark_month": CUR_M, "exp_duration_job": 100.000002},
        ]))
        # Unrounded 100.000002 > 100.000001 would be 'degraded'; rounded to 5
        # decimals both are 100.0 -> tie -> improved.
        assert _one(out, SQUAD_METRIC)["numerator"] == 1.0


# --------------------------------------------------------------------------- #
# Occupancy benchmark (higher is better; starts 2026-03 — Fix #3)
# --------------------------------------------------------------------------- #
class TestOccupancyBenchmark:
    def test_higher_occupancy_is_improved(self, spark):
        # occupancy benchmark 0.1 (Mar) -> 0.2 (Apr): rose -> improved.
        # NTPJ substrate present so the (xforce, month) has an ntpj_xforce row
        # and a tie-improved NTPJ unit.
        out = _run(
            spark,
            ntpj=make_ntpj(spark, [
                {"benchmark_month": PREV_M, "exp_duration_job": 100.0},
                {"benchmark_month": CUR_M, "exp_duration_job": 100.0},
            ]),
            occ=make_occs(spark, [
                {"date": PREV, "occupancy_minutes": 10.0},
                {"date": CUR, "occupancy_minutes": 20.0},
            ]),
        )
        # Two improved units land in district 'csi': the NTPJ unit (100->100 tie
        # -> improved) and the occupancy unit (0.1->0.2 -> improved).
        district = _one(out, DISTRICT_METRIC)
        assert district["numerator"] == 2.0
        assert district["denominator"] == 2.0
        assert district["metric_value"] == 100.0

    def test_february_is_not_a_march_comparator(self, spark):
        # Fix #3: occupancy benchmark starts 2026-03, so a Feb slot cannot become
        # March's previous-month comparator. With only Feb + Mar occupancy, March
        # has NO previous month -> not counted (denominator 0 for the occ unit).
        out = compute_improved_benchmarks(
            make_ntpj(spark, [
                {"benchmark_month": dt.date(2026, 2, 1), "exp_duration_job": 100.0},
                {"benchmark_month": dt.date(2026, 3, 1), "exp_duration_job": 100.0},
            ]),
            make_occs(spark, [
                {"date": dt.date(2026, 2, 15), "occupancy_minutes": 10.0},
                {"date": dt.date(2026, 3, 15), "occupancy_minutes": 20.0},
            ]),
            dt.date(2026, 3, 1), dt.date(2026, 3, 31),
        )
        # NTPJ: Feb 100 -> Mar 100 tie -> improved & counted. Occupancy March:
        # no prev (Feb excluded) -> not counted. So district = 1/1.
        district = _one(out, DISTRICT_METRIC)
        assert district["denominator"] == 1.0
        assert district["numerator"] == 1.0


# --------------------------------------------------------------------------- #
# ntpj_xforce gating via the substrate presence (Fix #5)
# --------------------------------------------------------------------------- #
class TestNtpjXforceGating:
    def test_occupancy_unit_dropped_without_ntpj_presence(self, spark):
        # An occupancy benchmark unit for an xforce with NO NTPJ substrate row
        # that month is dropped (no ntpj_xforce presence).
        out = _run(spark, occ=make_occs(spark, [
            {"date": PREV, "occupancy_minutes": 10.0},
            {"date": CUR, "occupancy_minutes": 20.0},
        ]))
        # No NTPJ substrate at all -> no ntpj_xforce presence -> gated out.
        assert len(out.take(1)) == 0

    def test_occupancy_unit_kept_with_ntpj_presence(self, spark):
        # Same xforce has an NTPJ substrate row that month -> occ unit survives.
        out = _run(
            spark,
            ntpj=make_ntpj(spark, [
                {"benchmark_month": PREV_M, "exp_duration_job": 100.0},
                {"benchmark_month": CUR_M, "exp_duration_job": 100.0},
            ]),
            occ=make_occs(spark, [
                {"date": PREV, "occupancy_minutes": 10.0},
                {"date": CUR, "occupancy_minutes": 20.0},
            ]),
        )
        assert len(_by_metric(out, DISTRICT_METRIC)) == 1

    def test_occupancy_unit_dropped_for_unmatched_xforce(self, spark):
        # NTPJ present for x.one but the occ unit is a DIFFERENT xforce x.two ->
        # x.two has no ntpj_xforce presence -> its occ unit is dropped.
        out = _run(
            spark,
            ntpj=make_ntpj(spark, [
                {"benchmark_month": PREV_M, "exp_duration_job": 100.0},
                {"benchmark_month": CUR_M, "exp_duration_job": 100.0},
            ]),
            occ=make_occs(spark, [
                {"xforce": "x.two", "date": PREV, "occupancy_minutes": 10.0},
                {"xforce": "x.two", "date": CUR, "occupancy_minutes": 20.0},
            ]),
        )
        # Only the NTPJ (x.one) unit survives; x.two occ unit gated out.
        xfs = {r["xforce"] for r in _by_metric(out, XFORCE_METRIC)}
        assert xfs == {"x.one"}


# --------------------------------------------------------------------------- #
# ntpj_xforce gating via an explicit metric (legacy FROM ntpj_xforces)
# --------------------------------------------------------------------------- #
class TestNtpjXforceMetricGate:
    def _improving_ntpj(self, spark):
        return make_ntpj(spark, [
            {"benchmark_month": PREV_M, "exp_duration_job": 200.0},
            {"benchmark_month": CUR_M, "exp_duration_job": 100.0},
        ])

    def test_metric_present_keeps_unit(self, spark):
        out = compute_improved_benchmarks(
            self._improving_ntpj(spark), empty_occ(spark),
            dt.date(2026, 4, 1), dt.date(2026, 4, 30),
            ntpj_xforce=make_ntpj_xforce(spark, [("x.one", CUR_M)]),
        )
        assert _one(out, SQUAD_METRIC)["metric_value"] == 100.0

    def test_explicit_empty_metric_is_authoritative(self, spark):
        # When an explicit ntpj_xforce metric is passed and has NO (x.one, Apr)
        # row, it is authoritative -> even the NTPJ unit is dropped (documents the
        # legacy FROM ntpj_xforces drive; the substrate-derived gate would keep it).
        out = compute_improved_benchmarks(
            self._improving_ntpj(spark), empty_occ(spark),
            dt.date(2026, 4, 1), dt.date(2026, 4, 30),
            ntpj_xforce=empty_ntpj_xforce(spark),
        )
        assert len(out.take(1)) == 0

    def test_week_only_metric_row_does_not_gate_month_unit(self, spark):
        # Only a WEEK ntpj_xforce row exists -> the month gate finds no month row
        # -> the unit is dropped (legacy joins the month grain).
        out = compute_improved_benchmarks(
            self._improving_ntpj(spark), empty_occ(spark),
            dt.date(2026, 4, 1), dt.date(2026, 4, 30),
            ntpj_xforce=make_ntpj_xforce(spark, [("x.one", dt.date(2026, 4, 6), "week")]),
        )
        assert len(out.take(1)) == 0


# --------------------------------------------------------------------------- #
# squad/district splitting (Fix #6)
# --------------------------------------------------------------------------- #
class TestSquadSplitting:
    def test_job_splits_across_two_squads(self, spark):
        # Same job_id worked by two agents in different squads/districts of the
        # same xforce -> the unit splits into two squad rows and two district rows.
        out = _run(spark, ntpj=make_ntpj(spark, [
            {"agent": "a.one", "squad": "txn", "district": "csi",
             "benchmark_month": PREV_M, "exp_duration_job": 200.0},
            {"agent": "a.one", "squad": "txn", "district": "csi",
             "benchmark_month": CUR_M, "exp_duration_job": 100.0},
            {"agent": "b.two", "squad": "card", "district": "ops",
             "benchmark_month": PREV_M, "exp_duration_job": 200.0},
            {"agent": "b.two", "squad": "card", "district": "ops",
             "benchmark_month": CUR_M, "exp_duration_job": 100.0},
        ]))
        squads = {r["squad"]: r for r in _by_metric(out, SQUAD_METRIC)}
        assert set(squads) == {"txn", "card"}
        assert squads["txn"]["numerator"] == 1.0
        assert squads["card"]["numerator"] == 1.0
        districts = {r["district"] for r in _by_metric(out, DISTRICT_METRIC)}
        assert districts == {"csi", "ops"}
        # The xforce roll-up counts distinct (job_id, squad, district) and SUMS
        # across the split: one job_id in two (squad, district) combos is TWO units.
        xf = _one(out, XFORCE_METRIC)
        assert xf["numerator"] == 2.0 and xf["denominator"] == 2.0


# --------------------------------------------------------------------------- #
# Scope + gating (Fixes #1, #2)
# --------------------------------------------------------------------------- #
class TestScopeAndGating:
    def test_social_media_excluded(self, spark):
        out = _run(spark, ntpj=make_ntpj(spark, [
            {"team": "social media", "benchmark_month": PREV_M, "exp_duration_job": 200.0},
            {"team": "social media", "benchmark_month": CUR_M, "exp_duration_job": 100.0},
        ]))
        assert len(out.take(1)) == 0

    def test_content_excluded(self, spark):
        out = _run(spark, ntpj=make_ntpj(spark, [
            {"team": "content", "benchmark_month": PREV_M, "exp_duration_job": 200.0},
            {"team": "content", "benchmark_month": CUR_M, "exp_duration_job": 100.0},
        ]))
        assert len(out.take(1)) == 0

    def test_core_april_squad_district_kept_and_xforce_kept(self, spark):
        # Fix #1 + #2: Core April squad/district rows ARE emitted (no team cutover
        # on the S&D roll-ups); the XForce roll-up keeps non-david Core April too
        # (flat < 2026-05 gate).
        out = _run(spark, ntpj=make_ntpj(spark, [
            {"team": "core", "squad": "cuenta", "benchmark_month": PREV_M,
             "exp_duration_job": 200.0},
            {"team": "core", "squad": "cuenta", "benchmark_month": CUR_M,
             "exp_duration_job": 100.0},
        ]))
        squad = _one(out, SQUAD_METRIC)
        assert squad["squad"] == "cuenta"
        assert squad["metric_value"] == 100.0
        assert len(_by_metric(out, XFORCE_METRIC)) == 1

    def test_david_fernandez_april_dropped_from_xforce_only(self, spark):
        # Fix #2: david.fernandez April is removed from the XForce roll-up, but
        # the squad/district roll-ups still emit his benchmark units.
        out = _run(spark, ntpj=make_ntpj(spark, [
            {"team": "core", "xplead": "david.fernandez", "benchmark_month": PREV_M,
             "exp_duration_job": 200.0},
            {"team": "core", "xplead": "david.fernandez", "benchmark_month": CUR_M,
             "exp_duration_job": 100.0},
        ]))
        assert len(_by_metric(out, XFORCE_METRIC)) == 0
        assert len(_by_metric(out, SQUAD_METRIC)) == 1
        assert len(_by_metric(out, DISTRICT_METRIC)) == 1

    def test_xforce_dropped_from_may(self, spark):
        # Fix #2: the XForce roll-up gate is a flat date_reference < 2026-05-01.
        out = compute_improved_benchmarks(
            make_ntpj(spark, [
                {"team": "fraud", "benchmark_month": dt.date(2026, 4, 1),
                 "exp_duration_job": 200.0},
                {"team": "fraud", "benchmark_month": dt.date(2026, 5, 1),
                 "exp_duration_job": 100.0},
            ]),
            empty_occ(spark),
            dt.date(2026, 5, 1), dt.date(2026, 5, 31),
        )
        # May xforce roll-up is gated out; the squad/district roll-ups stay.
        assert len(_by_metric(out, XFORCE_METRIC)) == 0
        assert len(_by_metric(out, SQUAD_METRIC)) == 1


# --------------------------------------------------------------------------- #
# Output contract
# --------------------------------------------------------------------------- #
class TestOutputContract:
    def _improving(self, spark):
        return make_ntpj(spark, [
            {"benchmark_month": PREV_M, "exp_duration_job": 200.0},
            {"benchmark_month": CUR_M, "exp_duration_job": 100.0},
        ])

    def test_output_schema_and_column_order(self, spark):
        out = _run(spark, ntpj=self._improving(spark))
        assert out.columns == [c for c, _ in IO_IMPROVED_BENCHMARKS_METRIC_SCHEMA]

    def test_agent_always_null_xforce_only_on_xforce_rows(self, spark):
        out = _run(spark, ntpj=self._improving(spark)).collect()
        assert all(r["agent"] is None for r in out)
        sd = [r for r in out if r["metric"] in (SQUAD_METRIC, DISTRICT_METRIC)]
        assert all(r["xforce"] is None for r in sd)
        xf = [r for r in out if r["metric"] == XFORCE_METRIC]
        assert all(r["xforce"] is not None for r in xf)

    def test_only_month_granularity(self, spark):
        out = _run(spark, ntpj=self._improving(spark))
        assert {r["date_granularity"] for r in out.collect()} == {"month"}

    def test_empty_inputs_yield_empty_frame_with_schema(self, spark):
        out = compute_improved_benchmarks(
            empty_ntpj(spark), empty_occ(spark),
            dt.date(2026, 4, 1), dt.date(2026, 4, 30),
        )
        assert len(out.take(1)) == 0
        assert out.columns == [c for c, _ in IO_IMPROVED_BENCHMARKS_METRIC_SCHEMA]
