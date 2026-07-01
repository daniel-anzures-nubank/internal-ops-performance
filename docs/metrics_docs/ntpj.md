# ntpj

The **NTPJ** (Normalized Time Per Job) performance metric. One row per agent per
**day / week / month / quarter / semester / year**.

NTPJ compares the time an agent spends on jobs against a monthly expected-time
benchmark:

> `ntpj = SUM(actual job duration) / SUM(exp_duration_job * job_count)`.
> **Target ≤ 100%** (lower = faster than benchmark).

Applies to **Core, Fraud** — **not** Social Media (social jobs aren't in the
shuffle/OOS sources, so social agents have no input rows). See
`docs/metrics_definitions.md`.

> **Content is a different metric under the same name.** Content's jobs still
> feed the cohort-wide benchmark here, but the build script **drops the Content
> output rows and unions** the SLA-weighted Content NTPJ in their place — see
> [content_sla_ntpj.md](content_sla_ntpj.md). `io_ntpj_metric` stays one
> `metric = 'ntpj'` table.

- Module: `metrics/ntpj.py`
- Build script: `scripts/metrics_scripts/build_ntpj.py`
- Input: `usr.danielanzures.io_jobs_raw`
  (+ `usr.danielanzures.io_jobs_within_sla_raw` for the Content union)
- Default target table: `usr.danielanzures.io_ntpj_metric`

## Input

The `io_jobs_raw` table (`metrics_data/jobs_raw.py`), one row per finished/-
unfinished job, carrying `job_id`, `activity_type`, `status`,
`duration_seconds`, and the precomputed `required_activity_on_day_flag`.

> **Benchmark look-back.** The build script reads **4 extra months** before
> `period_start` because a month's benchmark can use a trailing window (below).
> Look-back rows feed benchmarks only — they are never emitted.

## The benchmark (`exp_duration_job`)

`exp_duration_job(job_id, month)` = `SUM(duration) / SUM(count)` across **all
finished jobs** of that `job_id` (every agent), over a month window:

- target month **≤ 2026-03** → trailing window `[M-4 … M]` (5 calendar months
  inclusive — the legacy "4-month window");
- target month **≥ 2026-04** → the current month only.

The benchmark is computed from **all finished jobs** (no required-day filter).
Job content (Content team) is all OOS — those jobs live in
`taskmaster_consolidated_registry` and carry content-specific `job_id`s, so
their benchmarks are naturally content-scoped.

## Filters applied here

- **Finished only**: `status == 'finished'` (OOS jobs are synthesized as
  `finished` in the raw table).
- **Agent contribution rows**: `required_activity_on_day_flag == 1` — the agent
  was scheduled for that job's `activity_type` that day (the legacy
  "required_hours IS NOT NULL" filter, precomputed into the raw flag). The
  **benchmark** is NOT restricted this way.

## Derivation

1. Keep finished jobs.
2. Benchmark: monthly `(job_id, month)` totals over all finished jobs →
   windowed `exp_duration_job` per the cutover rule above.
3. Contribution: keep `required_activity_on_day_flag == 1`; aggregate to
   `(agent, date, job_id)` with `count` and `actual_seconds`.
4. `expected_seconds = exp_duration_job * count`; drop rows with no benchmark.
5. Restrict to the output period (look-back rows drop out).
6. Bucket `date` to day / week / month / quarter / semester / year; per
   `(agent, date_reference)` `numerator = SUM(actual_seconds)`,
   `denominator = SUM(expected_seconds)` (each job keeps its own month's
   benchmark when rolled up to broader buckets); dimensions take their
   most-recent value in the bucket.
7. `metric_value = numerator / denominator * 100` (NULL when denominator 0).

## Adjustments & carve-outs (applied here)

- **Manual adjustments** (when the synced `adj_*` tables are present):
  `exclusiones_generales` (slot/date windows), `cross_support` (queue
  exclusions), and `exclusiones_jobs` (job exclusions) are applied **before**
  the benchmark groupby, so an excluded job leaves both the benchmark and the
  contribution — matching legacy.
- **Outage dates** (2026-03-27, 2026-04-09) are dropped from the
  **contribution only** — they stay in the benchmark pool (legacy filters only
  the self-join target side; reproducing the asymmetry is what makes April's
  benchmark match).
- **Hardcoded per-agent date exclusions** (`HARDCODED_AGENT_DATE_EXCLUSIONS` in
  `metrics/ntpj.py`) — un-ported legacy vacation / leave / holiday / day-off
  carve-outs, contribution-only, pending migration to the adjustments sheet.

## Deferred (NOT applied here)

- **Content "always 4-month window"**: this module applies the unified legacy
  cutover (≤2026-03 trailing, ≥2026-04 current month) to all teams. Moot for
  the shipped output — Content's duration rows are replaced by the SLA metric —
  but the Content jobs feeding the cohort benchmark use the unified rule.

## Output schema (one row per agent per period)

| column | type | notes |
|--------|------|-------|
| `agent` | STRING | |
| `xforce` | STRING | most-recent in bucket |
| `xplead` | STRING | most-recent in bucket |
| `team` | STRING | `core` / `fraud` / `content` |
| `squad` | STRING | most-recent in bucket |
| `district` | STRING | most-recent in bucket |
| `shift` | STRING | most-recent in bucket |
| `date_reference` | DATE | bucket start (day / Monday / first-of-month/quarter/year / Jan 1 or Jul 1) |
| `date_granularity` | STRING | `day` / `week` / `month` / `quarter` / `semester` / `year` |
| `metric` | STRING | always `ntpj` |
| `numerator` | DOUBLE | actual job seconds |
| `denominator` | DOUBLE | expected job seconds (`exp_duration_job * count`) |
| `metric_value` | DOUBLE | `numerator / denominator * 100` (percentage; NULL if denominator 0) |
