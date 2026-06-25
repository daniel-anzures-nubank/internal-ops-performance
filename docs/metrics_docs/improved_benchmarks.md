# improved_benchmarks

The **Improved Benchmarks** metric. Unlike the other metrics (agent grain, six
granularities), this one is **squad / district / xforce level**, **month only**,
and **Core / Fraud only**.

> `improved_benchmarks = COUNT(improved benchmarks) / COUNT(comparable benchmarks) * 100`.
> **Target ≥ 60%.** Ties ("stayed the same") count as **improved**.

- Module: `metrics/improved_benchmarks.py`
- Build script: `scripts/metrics_scripts/build_improved_benchmarks.py`
- Inputs: `usr.danielanzures.io_jobs_raw` + `usr.danielanzures.io_occupancy_time_raw`
- Default target table: `usr.danielanzures.io_improved_benchmarks_metric`

It measures, per squad / district, what share of that group's **monthly
benchmarks improved vs. the previous month**.

## The two benchmark families

Matches legacy `[IO] Performance 2026 - S&D.sql` (Improved Benchmarks section):

| family | benchmark | "improved" rule | source |
|--------|-----------|-----------------|--------|
| **NTPJ (time-per-job)** | monthly `exp_duration_job` per `job_id` (cohort-wide expected seconds, with the NTPJ trailing-window rule) | benchmark **≤** previous month (faster is better) | `io_jobs_raw` |
| **Occupancy** | monthly NO benchmark per `district + shift` (mean of squad occupancy ratios) | benchmark **≥** previous month (higher occupancy is better) | `io_occupancy_time_raw` (from `2026-02`) |

A benchmark's **first month** (no previous month to compare) is **not counted**
in the numerator or denominator.

## Grain & roll-up

Benchmark units are formed per `(benchmark_key, xforce, month)` — the set of job
types / district-shifts an XForce's agents actually worked — flagged
improved/not, then summed:

- **`improved_benchmark_squad`** — over the XForces in each squad (`squad` set,
  `district` / `xforce` NULL).
- **`improved_benchmark_district`** — over the XForces in each district
  (`district` set, `squad` / `xforce` NULL).
- **`improved_benchmark_xforce`** — over the benchmark units of each XForce
  (`xforce` + `xplead` set, `squad` / `district` NULL). This is legacy
  `improved_benchmark` and is what the composite **`xforce_index`** metric
  consumes.

`agent` / `shift` are always NULL; `date_granularity` is always `month`.

## Scope & removal

- **Core / Fraud only.** Social Media and Content never had Improved Benchmarks,
  so their rows are filtered out (`team in ('core', 'fraud')`).
- **Removed from each team after its cutover** — months **≥** the cutover are not
  emitted:
  - **Core:** removed from **2026-04**.
  - **Fraud:** removed from **2026-05**.

  (The metric was dropped as an XForce-**Index** component on these dates; here we
  simply stop emitting it.)

## Benchmark look-back

The build script reads extra months before the output period so the
previous-month comparison (and the NTPJ trailing window) have data:

- `io_jobs_raw`: **6-month** look-back.
- `io_occupancy_time_raw`: **2-month** look-back.

Look-back rows are used only to build benchmarks / the previous month; only the
requested months are emitted.

## NOT applied here (future Adjustments layer)

Improved Benchmarks inherits whatever the NTPJ / NO benchmarks are, so everything
those raw/metric layers defer (cross-support queue exclusions, per-agent
vacation/maternity/day-control carve-outs, outage-date exclusions, DIME-squad
business exclusions) is **not** applied here either.

## Output schema

| column | type | notes |
|--------|------|-------|
| `agent` | STRING | always NULL |
| `xforce` | STRING | set for `improved_benchmark_xforce`, else NULL |
| `xplead` | STRING | set for `improved_benchmark_xforce`, else NULL |
| `team` | STRING | `core` / `fraud` |
| `squad` | STRING | set for `improved_benchmark_squad`, else NULL |
| `district` | STRING | set for `improved_benchmark_district`, else NULL |
| `shift` | STRING | always NULL |
| `date_reference` | DATE | first of month |
| `date_granularity` | STRING | always `month` |
| `metric` | STRING | `improved_benchmark_squad` / `improved_benchmark_district` / `improved_benchmark_xforce` |
| `numerator` | DOUBLE | improved benchmarks |
| `denominator` | DOUBLE | comparable benchmarks (had a previous month) |
| `metric_value` | DOUBLE | `numerator / denominator * 100` (NULL if denominator 0) |
