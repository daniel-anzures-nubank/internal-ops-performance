"""Unit tests for ``metrics/ntpj.py`` (PySpark).

Small synthetic Spark frames mimicking ``io_jobs_raw``, no warehouse. We verify
the finished-only filter, the required-day + active-roster contribution filter
(vs the benchmark using ALL finished jobs), the monthly benchmark window
(current-month cutover and the pre-cutover trailing window), the actual/expected
ratio, the look-back vs output-period split, the outage-date drop, and the
output contract.
"""

from __future__ import annotations

import datetime as dt

from pyspark.sql import types as T

from ntpj import (
    HARDCODED_AGENT_DATE_EXCLUSIONS,
    IO_NTPJ_METRIC_SCHEMA,
    METRIC_NAME,
    compute_ntpj,
)

_CROSS_SUPPORT_SCHEMA = T.StructType(
    [
        T.StructField("equipo", T.StringType()),
        T.StructField("agente", T.StringType()),
        T.StructField("queues_a_excluir", T.StringType()),
        T.StructField("fecha_inicio", T.StringType()),
        T.StructField("fecha_fin", T.StringType()),
    ]
)


def make_cross_support(spark, rows):
    """Build a synced ``adj_cross_support``-shaped Spark frame (snake_case cols)."""
    return spark.createDataFrame(
        [
            (
                r.get("equipo", "Core"),
                r["agente"],
                r["queues_a_excluir"],
                r.get("fecha_inicio", "2026-05-01"),
                r.get("fecha_fin", "9000-01-01"),
            )
            for r in rows
        ],
        _CROSS_SUPPORT_SCHEMA,
    )

_RAW_SCHEMA = T.StructType(
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
        T.StructField("end_time", T.TimestampType()),
        T.StructField("job_type", T.StringType()),
        T.StructField("activity_type", T.StringType()),
        T.StructField("status", T.StringType()),
        T.StructField("job_id", T.StringType()),
        T.StructField("duration_seconds", T.LongType()),
        T.StructField("required_activity_on_day_flag", T.IntegerType()),
    ]
)


def make_jobs(spark, rows):
    defaults = {
        "agent": "nuberto.lopez",
        "xforce": "nuliana.cruz",
        "xplead": "nuricio.diaz",
        "team": "core",
        "squad": "txn",
        "district": "csi",
        "shift": "morning",
        "roster_status": "active",
        "date": dt.date(2026, 5, 4),  # a Monday, >= cutover (current-month bench)
        "start_time": dt.datetime(2026, 5, 4, 9, 0, 0),
        "end_time": dt.datetime(2026, 5, 4, 9, 5, 0),
        "job_type": "queue-a",
        "activity_type": "backoffice",
        "status": "finished",
        "job_id": "bko - queue-a - finished",
        "duration_seconds": 300,
        "required_activity_on_day_flag": 1,
    }
    data = [{**defaults, **r} for r in rows]
    return spark.createDataFrame(
        [tuple(r[f.name] for f in _RAW_SCHEMA.fields) for r in data], _RAW_SCHEMA
    )


MAY_START = dt.date(2026, 5, 1)
MAY_END = dt.date(2026, 5, 31)


def _day_by_agent(out):
    return {
        r["agent"]: r
        for r in out.filter(out["date_granularity"] == "day").collect()
    }


class TestComputeNtpj:
    def test_basic_ratio_current_month(self, spark):
        # Two agents, same job_id, May (current-month benchmark).
        # durations 200 & 100 -> benchmark = 300/2 = 150.
        out = compute_ntpj(
            make_jobs(
                spark,
                [
                    {"agent": "a.one", "duration_seconds": 200},
                    {"agent": "b.two", "duration_seconds": 100},
                ],
            ),
            MAY_START,
            MAY_END,
        )
        day = _day_by_agent(out)
        assert abs(day["a.one"]["numerator"] - 200) < 1e-9
        assert abs(day["a.one"]["denominator"] - 150) < 1e-9
        assert abs(day["a.one"]["metric_value"] - 200 / 150 * 100) < 1e-6
        assert abs(day["b.two"]["metric_value"] - 100 / 150 * 100) < 1e-6
        assert day["a.one"]["metric"] == METRIC_NAME

    def test_unfinished_jobs_excluded_everywhere(self, spark):
        out = compute_ntpj(
            make_jobs(
                spark,
                [
                    {"agent": "a.one", "duration_seconds": 200, "status": "finished"},
                    {"agent": "a.one", "duration_seconds": 999, "status": "transferred"},
                ],
            ),
            MAY_START,
            MAY_END,
        )
        day = _day_by_agent(out)["a.one"]
        assert day["numerator"] == 200
        assert day["denominator"] == 200
        assert day["metric_value"] == 100.0

    def test_required_flag_filters_contribution_not_benchmark(self, spark):
        # a.one has a required job (flag 1); b.two's job (flag 0) only feeds the
        # benchmark. benchmark = (100 + 300) / 2 = 200.
        out = compute_ntpj(
            make_jobs(
                spark,
                [
                    {"agent": "a.one", "duration_seconds": 100,
                     "required_activity_on_day_flag": 1},
                    {"agent": "b.two", "duration_seconds": 300,
                     "required_activity_on_day_flag": 0},
                ],
            ),
            MAY_START,
            MAY_END,
        )
        day = _day_by_agent(out)
        assert set(day) == {"a.one"}
        assert day["a.one"]["denominator"] == 200
        assert day["a.one"]["metric_value"] == 50.0

    def test_inactive_roster_feeds_benchmark_not_contribution(self, spark):
        # a.one (active) contributes; b.two (inactive roster) only feeds the
        # benchmark. benchmark = (100 + 300)/2 = 200 -> a.one is 100/200 = 50%.
        out = compute_ntpj(
            make_jobs(
                spark,
                [
                    {"agent": "a.one", "duration_seconds": 100, "roster_status": "active"},
                    {"agent": "b.two", "duration_seconds": 300, "roster_status": "inactive"},
                ],
            ),
            MAY_START,
            MAY_END,
        )
        day = _day_by_agent(out)
        assert set(day) == {"a.one"}
        assert day["a.one"]["denominator"] == 200
        assert day["a.one"]["metric_value"] == 50.0

    def test_multiple_job_types_sum(self, spark):
        out = compute_ntpj(
            make_jobs(
                spark,
                [
                    {"job_id": "A", "duration_seconds": 100},
                    {"job_id": "B", "duration_seconds": 400},
                ],
            ),
            MAY_START,
            MAY_END,
        )
        day = _day_by_agent(out)["nuberto.lopez"]
        assert day["numerator"] == 500
        assert day["denominator"] == 500
        assert day["metric_value"] == 100.0

    def test_trailing_window_and_lookback(self, spark):
        # Target Feb 2026 (<= 2026-03) uses a trailing window incl. Jan.
        # Jan dur 100 + Feb dur 300 -> benchmark(Feb) = 400/2 = 200.
        # Output period = Feb only; Jan row is look-back (not emitted).
        out = compute_ntpj(
            make_jobs(
                spark,
                [
                    {"agent": "a.one", "date": dt.date(2026, 1, 15),
                     "start_time": dt.datetime(2026, 1, 15, 9, 0, 0),
                     "duration_seconds": 100},
                    {"agent": "a.one", "date": dt.date(2026, 2, 16),
                     "start_time": dt.datetime(2026, 2, 16, 9, 0, 0),
                     "duration_seconds": 300},
                ],
            ),
            dt.date(2026, 2, 1),
            dt.date(2026, 2, 28),
        )
        day = out.filter(out["date_granularity"] == "day").collect()
        assert len(day) == 1
        row = day[0]
        assert row["date_reference"] == dt.date(2026, 2, 16)
        assert row["denominator"] == 200
        assert abs(row["metric_value"] - 300 / 200 * 100) < 1e-6

    def test_current_month_ignores_prior_months(self, spark):
        out = compute_ntpj(
            make_jobs(
                spark,
                [
                    {"agent": "a.one", "date": dt.date(2026, 4, 15),
                     "start_time": dt.datetime(2026, 4, 15, 9, 0, 0),
                     "duration_seconds": 1000},
                    {"agent": "a.one", "date": dt.date(2026, 5, 4),
                     "start_time": dt.datetime(2026, 5, 4, 9, 0, 0),
                     "duration_seconds": 200},
                ],
            ),
            MAY_START,
            MAY_END,
        )
        day = out.filter(out["date_granularity"] == "day").collect()
        assert len(day) == 1
        row = day[0]
        assert row["denominator"] == 200
        assert row["metric_value"] == 100.0

    def test_outage_dates_dropped_from_contribution_but_kept_in_benchmark(self, spark):
        # 2026-04-09 is a legacy outage date. It is dropped from the CONTRIBUTION
        # (no 04-09 output row) but KEPT in the benchmark pool — legacy's
        # expected_duration_per_job_ntpj filters the outage only on the self-join
        # target side, so the 04-09 job still feeds exp_duration_job. Two jobs of
        # the same job_id: 04-09 dur=900, 04-10 dur=300.
        out = compute_ntpj(
            make_jobs(
                spark,
                [
                    {"agent": "a.one", "date": dt.date(2026, 4, 9),
                     "start_time": dt.datetime(2026, 4, 9, 9, 0, 0),
                     "duration_seconds": 900},
                    {"agent": "a.one", "date": dt.date(2026, 4, 10),
                     "start_time": dt.datetime(2026, 4, 10, 9, 0, 0),
                     "duration_seconds": 300},
                ],
            ),
            dt.date(2026, 4, 1),
            dt.date(2026, 4, 30),
        )
        day = out.filter(out["date_granularity"] == "day").collect()
        # Only the 04-10 contribution row survives (04-09 dropped from output).
        assert len(day) == 1
        assert day[0]["date_reference"] == dt.date(2026, 4, 10)
        # Benchmark INCLUDES the 04-09 job: exp = (900+300)/2 = 600.
        # metric = actual 300 / (600 * 1) * 100 = 50.0  (NOT 100.0, which is what
        # dropping 04-09 from the benchmark would have given).
        assert day[0]["metric_value"] == 50.0

    def test_contribution_job_always_builds_its_own_benchmark(self, spark):
        # A finished contribution job is itself in the benchmark pool, so its
        # (job_id, month) always has an exp_duration_job — even when its only
        # sibling for that job_id is in a different (out-of-window) month. With
        # the current-month window, the May contribution's own job builds the
        # May benchmark (200/1=200), and the April sibling is ignored.
        out = compute_ntpj(
            make_jobs(
                spark,
                [
                    # April sibling builds an April benchmark only (ignored for May).
                    {"agent": "a.one", "date": dt.date(2026, 4, 15),
                     "start_time": dt.datetime(2026, 4, 15, 9, 0, 0),
                     "job_id": "lonely", "duration_seconds": 500,
                     "required_activity_on_day_flag": 0},
                    {"agent": "a.one", "date": dt.date(2026, 5, 4),
                     "start_time": dt.datetime(2026, 5, 4, 9, 0, 0),
                     "job_id": "lonely", "duration_seconds": 200,
                     "required_activity_on_day_flag": 1},
                ],
            ),
            MAY_START,
            MAY_END,
        )
        day = out.filter(out["date_granularity"] == "day").collect()
        assert len(day) == 1
        row = day[0]
        assert row["numerator"] == 200
        assert row["denominator"] == 200  # May benchmark from the May job itself
        assert row["metric_value"] == 100.0

    def test_all_granularities_emitted(self, spark):
        out = compute_ntpj(make_jobs(spark, [{}]), MAY_START, MAY_END)
        assert set(r["date_granularity"] for r in out.collect()) == {
            "day", "week", "month", "quarter", "semester", "year"
        }

    def test_weekly_aggregates_across_days(self, spark):
        out = compute_ntpj(
            make_jobs(
                spark,
                [
                    {"date": dt.date(2026, 5, 4),
                     "start_time": dt.datetime(2026, 5, 4, 9, 0, 0),
                     "duration_seconds": 300},
                    {"date": dt.date(2026, 5, 5),
                     "start_time": dt.datetime(2026, 5, 5, 9, 0, 0),
                     "duration_seconds": 300},
                ],
            ),
            MAY_START,
            MAY_END,
        )
        week = out.filter(out["date_granularity"] == "week").collect()[0]
        assert week["date_reference"] == dt.date(2026, 5, 4)  # Monday
        assert week["numerator"] == 600

    def test_dimensions_take_latest_value_in_bucket(self, spark):
        out = compute_ntpj(
            make_jobs(
                spark,
                [
                    {"date": dt.date(2026, 5, 4),
                     "start_time": dt.datetime(2026, 5, 4, 9, 0, 0), "squad": "txn"},
                    {"date": dt.date(2026, 5, 20),
                     "start_time": dt.datetime(2026, 5, 20, 9, 0, 0), "squad": "cuenta"},
                ],
            ),
            MAY_START,
            MAY_END,
        )
        month = out.filter(out["date_granularity"] == "month").collect()[0]
        assert month["squad"] == "cuenta"

    def test_empty_input_yields_empty_frame_with_schema(self, spark):
        empty = make_jobs(spark, [{}]).limit(0)
        out = compute_ntpj(empty, MAY_START, MAY_END)
        assert out.count() == 0
        assert out.columns == [c for c, _ in IO_NTPJ_METRIC_SCHEMA]

    def test_no_finished_jobs_yields_empty(self, spark):
        out = compute_ntpj(
            make_jobs(spark, [{"status": "transferred"}]), MAY_START, MAY_END
        )
        assert out.count() == 0

    def test_output_schema_and_column_order(self, spark):
        out = compute_ntpj(make_jobs(spark, [{}]), MAY_START, MAY_END)
        assert out.columns == [c for c, _ in IO_NTPJ_METRIC_SCHEMA]


class TestCrossSupportQueueNormalization:
    """Fix 1: the sheet's hyphenated queue must match the prefixed/underscored
    shuffle ``job_type`` (``incredible_machine__<queue_underscored>``)."""

    def test_prefixed_underscored_job_type_is_dropped(self, spark):
        # The sheet lists the hyphenated queue ``backoffice-payment-srf``; the raw
        # shuffle job_type is ``incredible_machine__backoffice_payment_srf``.
        # Before Fix 1 the loose lowercase ``contains`` never matched, so the
        # cross-support job leaked into BOTH the contribution and the benchmark.
        jobs = make_jobs(
            spark,
            [
                {
                    "agent": "daniel.cano",
                    "team": "core",
                    "job_type": "incredible_machine__backoffice_payment_srf",
                    "job_id": "bko - incredible_machine__backoffice_payment_srf - finished",
                    "duration_seconds": 500,
                },
            ],
        )
        cross_support = make_cross_support(
            spark,
            [
                {
                    "agente": "daniel.cano",
                    "queues_a_excluir": "backoffice-payment-srf",
                    "fecha_inicio": "2026-04-09",
                    "fecha_fin": "9000-01-01",
                }
            ],
        )
        out = compute_ntpj(jobs, MAY_START, MAY_END, cross_support=cross_support)
        # The cross-support job is the only job: after the drop nothing remains.
        assert out.count() == 0

    def test_non_matching_queue_is_kept(self, spark):
        # A queue NOT in the sheet must survive (normalized equality, not a loose
        # substring — ``backoffice-payment`` must NOT drop ``...payment-srf``).
        jobs = make_jobs(
            spark,
            [
                {
                    "agent": "daniel.cano",
                    "team": "core",
                    "job_type": "incredible_machine__backoffice_payment_srf",
                    "job_id": "bko - incredible_machine__backoffice_payment_srf - finished",
                    "duration_seconds": 500,
                },
            ],
        )
        cross_support = make_cross_support(
            spark,
            [
                {
                    "agente": "daniel.cano",
                    "queues_a_excluir": "backoffice-payment",  # partial token
                    "fecha_inicio": "2026-04-09",
                    "fecha_fin": "9000-01-01",
                }
            ],
        )
        out = compute_ntpj(jobs, MAY_START, MAY_END, cross_support=cross_support)
        day = _day_by_agent(out)["daniel.cano"]
        assert day["numerator"] == 500


class TestHardcodedAgentExclusions:
    """Fix 2: un-ported legacy per-agent date exclusions remove the agent's jobs."""

    def test_excluded_agent_day_is_dropped(self, spark):
        # adriana.lopez 2026-05-14 is a hardcoded vacation exclusion. A benchmark
        # buddy (b.two, not excluded) keeps the benchmark pool non-empty so we can
        # assert adriana drops out of the CONTRIBUTION specifically.
        out = compute_ntpj(
            make_jobs(
                spark,
                [
                    {"agent": "adriana.lopez", "date": dt.date(2026, 5, 14),
                     "start_time": dt.datetime(2026, 5, 14, 9, 0, 0),
                     "duration_seconds": 300},
                    {"agent": "b.two", "date": dt.date(2026, 5, 14),
                     "start_time": dt.datetime(2026, 5, 14, 9, 0, 0),
                     "duration_seconds": 100},
                ],
            ),
            MAY_START,
            MAY_END,
        )
        day = _day_by_agent(out)
        assert "adriana.lopez" not in day
        assert "b.two" in day

    def test_excluded_agent_kept_outside_window(self, spark):
        # adriana.lopez on a DIFFERENT day (2026-05-15) is unaffected.
        out = compute_ntpj(
            make_jobs(
                spark,
                [
                    {"agent": "adriana.lopez", "date": dt.date(2026, 5, 15),
                     "start_time": dt.datetime(2026, 5, 15, 9, 0, 0),
                     "duration_seconds": 300},
                ],
            ),
            MAY_START,
            MAY_END,
        )
        day = _day_by_agent(out)
        assert "adriana.lopez" in day

    def test_open_ended_exclusion(self, spark):
        # evelyn.macedo is excluded from 2026-04-27 onwards (open-ended).
        out = compute_ntpj(
            make_jobs(
                spark,
                [
                    {"agent": "evelyn.macedo", "date": dt.date(2026, 5, 20),
                     "start_time": dt.datetime(2026, 5, 20, 9, 0, 0),
                     "duration_seconds": 300},
                    {"agent": "b.two", "date": dt.date(2026, 5, 20),
                     "start_time": dt.datetime(2026, 5, 20, 9, 0, 0),
                     "duration_seconds": 100},
                ],
            ),
            MAY_START,
            MAY_END,
        )
        day = _day_by_agent(out)
        assert "evelyn.macedo" not in day

    def test_exclusion_set_covers_surfaced_named_agents(self):
        # Sanity: the named-date agents surfaced in the Apr-May parity diff are
        # present. (ivette.melendez / daniel.cano are resolved by Fix 1's
        # cross-support queue normalization, NOT this named-date list.)
        names = {n for n, _, _ in HARDCODED_AGENT_DATE_EXCLUSIONS}
        for surfaced in ("evelyn.macedo", "tania.enciso", "adriana.lopez"):
            assert surfaced in names
