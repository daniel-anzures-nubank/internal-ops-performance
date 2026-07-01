# normalized_time_per_job (NTPJ benchmark substrate)

**Not a tidy metric** — `io_normalized_time_per_job` materializes legacy
`usr.mx__cx.normalized_time_per_job`: the NTPJ **benchmark substrate** that
`improved_benchmarks` consumes for its NTPJ benchmark family. One row per
`(agent, job_id, benchmark_month, xforce, xplead, team, squad, district)`
carrying the **cohort-wide** `exp_duration_job` (the expected seconds per job
type, with the NTPJ trailing-window rule — months ≤ 2026-03 use `[M-4 … M]`,
months ≥ 2026-04 the current month).

Because it shares `_ntpj_base` with the NTPJ metric (same manual adjustments —
`exclusiones_generales`, `cross_support`, `exclusiones_jobs` — applied **before**
the benchmark groupby, same finished/required/active-roster scoping), the
benchmark values and the `(xforce, xplead, squad, district)` attribution are
identical to the shipped NTPJ metric, not re-derived from raw jobs.

- Module: `metrics/ntpj.py` (`compute_normalized_time_per_job`, sharing
  `_ntpj_base` with `compute_ntpj`)
- Build script: `scripts/metrics_scripts/build_normalized_time_per_job.py`
- Input: `usr.danielanzures.io_jobs_raw` (read with the NTPJ benchmark
  look-back)
- Default target table: `usr.danielanzures.io_normalized_time_per_job`
  (no `_metric` suffix — it is a substrate, not a metric)

## Derivation

1. Build the row-level NTPJ base (`_ntpj_base`): manual adjustments → finished
   jobs only → cohort-wide monthly `exp_duration_job` per `job_id` (computed
   over ALL finished jobs of that job type, every team).
2. Collapse days within a `(job_id, month)` — `exp_duration_job` is a
   per-`(job_id, month)` constant — grouping by
   `(agent, job_id, month, xforce, xplead, team, squad, district)`.
3. Restrict to `benchmark_month` within `[period_start month, period_end month]`.

## Window note

`improved_benchmarks` needs **one previous month** before its output start for
the month-over-month LAG, so this build runs with `--period-start` =
(improved_benchmarks output start − 1 month). The NTPJ trailing benchmark needs
~4 more months of `io_jobs_raw` before that — read automatically via the
look-back.

## Output schema (one row per agent × job_id × month × dims)

| column | type | notes |
|--------|------|-------|
| `agent` | STRING | |
| `job_id` | STRING | the job type |
| `benchmark_month` | DATE | month start |
| `xforce`, `xplead`, `team`, `squad`, `district` | STRING | attribution (matches the NTPJ metric) |
| `exp_duration_job` | DOUBLE | cohort-wide expected seconds for `(job_id, benchmark_month)` |
