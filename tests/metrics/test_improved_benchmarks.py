"""Unit tests for ``metrics/improved_benchmarks.py`` (PySpark).

Small synthetic Spark frames mimicking ``io_jobs_raw`` / ``io_occupancy_time_raw``,
no warehouse. We verify the NTPJ (lower=better) and occupancy (higher=better)
month-over-month comparisons, the tie rule, first-month exclusion, the squad +
district + xforce roll-ups, team scope (Core/Fraud only), and the output
contract — plus the parity fixes:

* **Fix #1** — squad / district roll-ups have NO month gate and NO team cutover
  (Core April squad rows are emitted).
* **Fix #2** — the XForce roll-up is gated ``< 2026-05-01`` flat for all teams,
  plus the ``david.fernandez`` Apr-2026 carve-out (non-david Core April survives).
* **Fix #3** — the occupancy benchmark starts 2026-03 (no February comparator).
* **Fix #5** — benchmark units are gated to ``(xforce, month)`` that have an
  ``ntpj_xforce`` row (a finished + required + active-roster job that month).
* **Fix #6** — a job_id splits across squad/district rows by the agent's roster
  attribution.

Test months avoid the NTPJ trailing-window complication by using 2026-03 (the
look-back / previous month) and 2026-04 (the compared/emitted month).
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

PREV = dt.date(2026, 3, 15)   # previous month
CUR = dt.date(2026, 4, 15)    # compared month (emitted)


# --------------------------------------------------------------------------- #
# Synthetic-frame builders (Spark)
# --------------------------------------------------------------------------- #
_JOBS_SCHEMA = T.StructType(
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
        T.StructField("start_time", T.TimestampType()),
        T.StructField("job_type", T.StringType()),
        T.StructField("activity_type", T.StringType()),
        T.StructField("status", T.StringType()),
        T.StructField("job_id", T.StringType()),
        T.StructField("duration_seconds", T.LongType()),
        T.StructField("required_activity_on_day_flag", T.IntegerType()),
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


def _job_defaults() -> dict:
    return {
        "agent": "a.one", "xforce": "x.one", "xplead": "p.one",
        "team": "fraud", "squad": "txn", "district": "csi", "shift": "morning",
        "roster_status": "active",
        "date": CUR,
        "start_time": dt.datetime(2026, 4, 15, 9, 0, 0),
        "job_type": "queue-a",
        "activity_type": "bko",
        "status": "finished", "job_id": "jobA", "duration_seconds": 100,
        "required_activity_on_day_flag": 1,
    }


def make_jobs(spark, rows):
    data = [{**_job_defaults(), **r} for r in rows]
    return spark.createDataFrame(
        [tuple(d[f.name] for f in _JOBS_SCHEMA.fields) for d in data], _JOBS_SCHEMA
    )


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


def empty_jobs(spark):
    return spark.createDataFrame([], _JOBS_SCHEMA)


def empty_occ(spark):
    return spark.createDataFrame([], _OCC_SCHEMA)


def _run(spark, jobs=None, occ=None,
         start=dt.date(2026, 4, 1), end=dt.date(2026, 4, 30)):
    return compute_improved_benchmarks(
        jobs if jobs is not None else empty_jobs(spark),
        occ if occ is not None else empty_occ(spark),
        start,
        end,
    )


def _by_metric(out, metric):
    rows = [r for r in out.collect() if r["metric"] == metric]
    return rows


def _one(out, metric):
    rows = _by_metric(out, metric)
    assert len(rows) == 1, f"expected 1 {metric} row, got {len(rows)}"
    return rows[0]


# --------------------------------------------------------------------------- #
# NTPJ benchmark (lower is better)
# --------------------------------------------------------------------------- #
class TestNtpjBenchmark:
    def test_lower_benchmark_is_improved(self, spark):
        # jobA: 200s in Mar, 100s in Apr -> benchmark dropped -> improved.
        out = _run(spark, jobs=make_jobs(spark, [
            {"date": PREV, "duration_seconds": 200},
            {"date": CUR, "duration_seconds": 100},
        ]))
        squad = _one(out, SQUAD_METRIC)
        assert squad["numerator"] == 1.0
        assert squad["denominator"] == 1.0
        assert squad["metric_value"] == 100.0
        assert squad["squad"] == "txn"
        assert squad["district"] is None
        assert squad["date_reference"] == dt.date(2026, 4, 1)
        assert squad["date_granularity"] == "month"

    def test_higher_benchmark_is_not_improved(self, spark):
        # 100s -> 200s: benchmark rose -> not improved (counted but 0).
        out = _run(spark, jobs=make_jobs(spark, [
            {"date": PREV, "duration_seconds": 100},
            {"date": CUR, "duration_seconds": 200},
        ]))
        squad = _one(out, SQUAD_METRIC)
        assert squad["numerator"] == 0.0
        assert squad["denominator"] == 1.0
        assert squad["metric_value"] == 0.0

    def test_tie_counts_as_improved(self, spark):
        out = _run(spark, jobs=make_jobs(spark, [
            {"date": PREV, "duration_seconds": 150},
            {"date": CUR, "duration_seconds": 150},
        ]))
        squad = _one(out, SQUAD_METRIC)
        assert squad["numerator"] == 1.0
        assert squad["metric_value"] == 100.0

    def test_first_month_not_counted(self, spark):
        # Only the compared month present -> no previous -> denominator 0.
        out = _run(spark, jobs=make_jobs(spark, [
            {"date": CUR, "duration_seconds": 100},
        ]))
        squad = _one(out, SQUAD_METRIC)
        assert squad["denominator"] == 0.0
        assert squad["metric_value"] is None

    def test_emits_squad_district_and_xforce_rows(self, spark):
        out = _run(spark, jobs=make_jobs(spark, [
            {"date": PREV, "duration_seconds": 200},
            {"date": CUR, "duration_seconds": 100},
        ]))
        assert {r["metric"] for r in out.collect()} == {
            SQUAD_METRIC, DISTRICT_METRIC, XFORCE_METRIC,
        }
        district = _one(out, DISTRICT_METRIC)
        assert district["district"] == "csi"
        assert district["squad"] is None
        assert district["metric_value"] == 100.0

    def test_xforce_rollup_sets_xforce_and_xplead(self, spark):
        out = _run(spark, jobs=make_jobs(spark, [
            {"date": PREV, "duration_seconds": 200},
            {"date": CUR, "duration_seconds": 100},
        ]))
        xf = _one(out, XFORCE_METRIC)
        assert xf["xforce"] == "x.one"
        assert xf["xplead"] == "p.one"
        assert xf["squad"] is None and xf["district"] is None
        assert xf["numerator"] == 1.0 and xf["metric_value"] == 100.0


# --------------------------------------------------------------------------- #
# Occupancy benchmark (higher is better; starts 2026-03 — Fix #3)
# --------------------------------------------------------------------------- #
class TestOccupancyBenchmark:
    def test_higher_occupancy_is_improved(self, spark):
        # occupancy benchmark 0.1 (Mar) -> 0.2 (Apr): rose -> improved.
        # NTPJ jobs present so the (xforce, month) has an ntpj_xforce row.
        out = _run(
            spark,
            jobs=make_jobs(spark, [
                {"date": PREV, "duration_seconds": 100},
                {"date": CUR, "duration_seconds": 100},
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
            make_jobs(spark, [
                {"date": dt.date(2026, 2, 15), "duration_seconds": 100},
                {"date": dt.date(2026, 3, 15), "duration_seconds": 100},
            ]),
            make_occs(spark, [
                {"date": dt.date(2026, 2, 15), "occupancy_minutes": 10.0},
                {"date": dt.date(2026, 3, 15), "occupancy_minutes": 20.0},
            ]),
            dt.date(2026, 3, 1), dt.date(2026, 3, 31),
        )
        # Only the NTPJ unit (Feb->Mar) is counted; the occupancy unit is the
        # March first-month (Feb excluded), contributing 0 to the denominator.
        district = _one(out, DISTRICT_METRIC)
        # NTPJ: Feb 100 -> Mar 100 tie -> improved & counted. Occupancy March:
        # no prev -> not counted. So district numerator/denominator = 1/1.
        assert district["denominator"] == 1.0
        assert district["numerator"] == 1.0


# --------------------------------------------------------------------------- #
# ntpj_xforce gating (Fix #5)
# --------------------------------------------------------------------------- #
class TestNtpjXforceGating:
    def test_unit_dropped_without_active_roster_job(self, spark):
        # The xforce's only job is NOT active roster -> no ntpj_xforce row that
        # month -> the benchmark unit is dropped, so no metric rows at all.
        out = _run(spark, jobs=make_jobs(spark, [
            {"date": PREV, "duration_seconds": 200, "roster_status": "inactive"},
            {"date": CUR, "duration_seconds": 100, "roster_status": "inactive"},
        ]))
        assert len(out.take(1)) == 0

    def test_occupancy_unit_requires_ntpj_xforce_row(self, spark):
        # An occupancy benchmark unit for an xforce with NO ntpj_xforce row (no
        # active-roster job that month) is dropped.
        out = _run(spark, occ=make_occs(spark, [
            {"date": PREV, "occupancy_minutes": 10.0},
            {"date": CUR, "occupancy_minutes": 20.0},
        ]))
        # No jobs at all -> no ntpj_xforce rows -> everything gated out.
        assert len(out.take(1)) == 0


# --------------------------------------------------------------------------- #
# squad/district splitting (Fix #6)
# --------------------------------------------------------------------------- #
class TestSquadSplitting:
    def test_job_splits_across_two_squads(self, spark):
        # Same job_id worked by two agents in different squads/districts of the
        # same xforce -> the unit splits into two squad rows and two district rows.
        out = _run(spark, jobs=make_jobs(spark, [
            {"agent": "a.one", "squad": "txn", "district": "csi",
             "date": PREV, "duration_seconds": 200},
            {"agent": "a.one", "squad": "txn", "district": "csi",
             "date": CUR, "duration_seconds": 100},
            {"agent": "b.two", "squad": "card", "district": "ops",
             "date": PREV, "duration_seconds": 200},
            {"agent": "b.two", "squad": "card", "district": "ops",
             "date": CUR, "duration_seconds": 100},
        ]))
        squads = {r["squad"]: r for r in _by_metric(out, SQUAD_METRIC)}
        assert set(squads) == {"txn", "card"}
        assert squads["txn"]["numerator"] == 1.0
        assert squads["card"]["numerator"] == 1.0
        districts = {r["district"] for r in _by_metric(out, DISTRICT_METRIC)}
        assert districts == {"csi", "ops"}
        # The xforce roll-up counts the (job_id, xforce) ONCE despite the split.
        xf = _one(out, XFORCE_METRIC)
        assert xf["numerator"] == 1.0 and xf["denominator"] == 1.0


# --------------------------------------------------------------------------- #
# Scope + gating (Fixes #1, #2)
# --------------------------------------------------------------------------- #
class TestScopeAndGating:
    def test_social_media_excluded(self, spark):
        out = _run(spark, jobs=make_jobs(spark, [
            {"team": "social media", "date": PREV, "duration_seconds": 200},
            {"team": "social media", "date": CUR, "duration_seconds": 100},
        ]))
        assert len(out.take(1)) == 0

    def test_content_excluded(self, spark):
        out = _run(spark, jobs=make_jobs(spark, [
            {"team": "content", "date": PREV, "duration_seconds": 200},
            {"team": "content", "date": CUR, "duration_seconds": 100},
        ]))
        assert len(out.take(1)) == 0

    def test_core_april_squad_district_kept_xforce_dropped(self, spark):
        # Fix #1 + #2: Core April squad/district rows ARE emitted (no team
        # cutover on the S&D roll-ups); the XForce roll-up keeps non-david Core
        # April too (flat < 2026-05 gate).
        out = _run(spark, jobs=make_jobs(spark, [
            {"team": "core", "squad": "cuenta", "date": PREV,
             "duration_seconds": 200},
            {"team": "core", "squad": "cuenta", "date": CUR,
             "duration_seconds": 100},
        ]))
        squad = _one(out, SQUAD_METRIC)
        assert squad["squad"] == "cuenta"
        assert squad["metric_value"] == 100.0
        # Non-david Core April survives in the xforce roll-up.
        assert len(_by_metric(out, XFORCE_METRIC)) == 1

    def test_david_fernandez_april_dropped_from_xforce_only(self, spark):
        # Fix #2: david.fernandez April is removed from the XForce roll-up, but
        # the squad/district roll-ups still emit his benchmark units.
        out = _run(spark, jobs=make_jobs(spark, [
            {"team": "core", "xplead": "david.fernandez", "date": PREV,
             "duration_seconds": 200},
            {"team": "core", "xplead": "david.fernandez", "date": CUR,
             "duration_seconds": 100},
        ]))
        assert len(_by_metric(out, XFORCE_METRIC)) == 0
        assert len(_by_metric(out, SQUAD_METRIC)) == 1
        assert len(_by_metric(out, DISTRICT_METRIC)) == 1

    def test_xforce_dropped_from_may(self, spark):
        # Fix #2: the XForce roll-up gate is a flat date_reference < 2026-05-01.
        out = compute_improved_benchmarks(
            make_jobs(spark, [
                {"team": "fraud", "date": dt.date(2026, 4, 15),
                 "duration_seconds": 200},
                {"team": "fraud", "date": dt.date(2026, 5, 15),
                 "duration_seconds": 100},
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
    def test_output_schema_and_column_order(self, spark):
        out = _run(spark, jobs=make_jobs(spark, [
            {"date": PREV, "duration_seconds": 200},
            {"date": CUR, "duration_seconds": 100},
        ]))
        assert out.columns == [c for c, _ in IO_IMPROVED_BENCHMARKS_METRIC_SCHEMA]

    def test_agent_always_null_xforce_only_on_xforce_rows(self, spark):
        out = _run(spark, jobs=make_jobs(spark, [
            {"date": PREV, "duration_seconds": 200},
            {"date": CUR, "duration_seconds": 100},
        ])).collect()
        assert all(r["agent"] is None for r in out)
        # xforce populated only on the xforce roll-up rows.
        sd = [r for r in out if r["metric"] in (SQUAD_METRIC, DISTRICT_METRIC)]
        assert all(r["xforce"] is None for r in sd)
        xf = [r for r in out if r["metric"] == XFORCE_METRIC]
        assert all(r["xforce"] is not None for r in xf)

    def test_only_month_granularity(self, spark):
        out = _run(spark, jobs=make_jobs(spark, [
            {"date": PREV, "duration_seconds": 200},
            {"date": CUR, "duration_seconds": 100},
        ]))
        assert {r["date_granularity"] for r in out.collect()} == {"month"}

    def test_empty_inputs_yield_empty_frame_with_schema(self, spark):
        out = compute_improved_benchmarks(
            empty_jobs(spark), empty_occ(spark),
            dt.date(2026, 4, 1), dt.date(2026, 4, 30),
        )
        assert len(out.take(1)) == 0
        assert out.columns == [c for c, _ in IO_IMPROVED_BENCHMARKS_METRIC_SCHEMA]
