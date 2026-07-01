# improved_benchmarks

The **Improved Benchmarks** metric. Unlike the other metrics (agent grain, six
granularities), this one is **XForce level**, **month only**, and
**Core / Fraud only** — it emits the **`improved_benchmark_xforce` metric
only** (legacy main-deck `improved_benchmark`), the component the composite
`xforce_index` consumes.

> `improved_benchmarks = COUNT(improved benchmarks) / COUNT(comparable benchmarks) * 100`.
> **Target ≥ 60%.** Ties ("stayed the same") count as **improved**.

- Module: `metrics/improved_benchmarks.py`
- Build script: `scripts/metrics_scripts/build_improved_benchmarks.py`
- Inputs: `usr.danielanzures.io_normalized_time_per_job` +
  `usr.danielanzures.io_occupancy_time_raw` +
  `usr.danielanzures.io_ntpj_xforce_metric`
- Default target table: `usr.danielanzures.io_improved_benchmarks_metric`

It measures, per XForce, what share of that group's **monthly benchmarks
improved vs. the previous month**.

## Scope note — the S&D squad/district roll-ups are OUT of scope

Legacy also emits `improved_benchmark_squad` / `improved_benchmark_district`
from the **S&D deck** (`[IO] Performance 2026 - S&D.sql`), which attributes each
benchmark via an `agent_information` *snapshot* join. That deck is not a
documented pipeline component (see `legacy/CLAUDE.md`), so those roll-ups are
intentionally **not** built here.

## The two benchmark families

Matches legacy `[IO] Performance 2026.sql` (main deck):

| family | benchmark | "improved" rule | source |
|--------|-----------|-----------------|--------|
| **NTPJ (time-per-job)** | monthly `exp_duration_job` per `job_id` (cohort-wide expected seconds, with the NTPJ trailing-window rule) | benchmark **≤** previous month (faster is better) | `io_normalized_time_per_job` (see [normalized_time_per_job](normalized_time_per_job.md)) |
| **Occupancy** | monthly NO benchmark per `district + shift` (mean of squad occupancy ratios) | benchmark **≥** previous month (higher occupancy is better) | `io_occupancy_time_raw` (earliest benchmark month is **2026-03** — the legacy occupancy source is filtered `date >= '2026-03-01'`) |

Both benchmarks are rounded to 5 decimals before the month-over-month
LAG/compare, and the LAG **partitions by `(key, xforce)`** — a `(key, xforce)`
new this month is a first month (not counted) even if the `key` appeared last
month under a different xforce. A benchmark's **first month** (no previous
month to compare) is **not counted** in the numerator or denominator.

## Grain & gating

Benchmark units are formed per `(benchmark_key, xforce, month)` — the set of
job types / district-shifts an XForce's agents actually worked — flagged
improved/not, then summed per `(xforce, xplead, month)`:

- **`ntpj_xforce` LEFT-JOIN gate** — legacy `improved_benchmark_final` is
  driven by `ntpj_xforces`, so units for an `(xforce, month)` with **no**
  [ntpj_xforce](ntpj_xforce.md) output row that month are dropped (a no-op for
  NTPJ units; it only drops occupancy units for xforces with no NTPJ row).
- **Month gate** — `date_reference < 2026-05-01` (flat, for **all** teams; the
  metric was dropped as an XForce-Index component from May 2026), **plus** the
  `david.fernandez` carve-out (his xforces drop from **2026-04**). There is no
  per-team Core/Fraud removal cutover.

## Benchmark look-back

The build script reads extra months before the output period so the
previous-month comparison has data:

- `io_normalized_time_per_job`: **one previous month** (the LAG comparator; the
  NTPJ trailing-window look-back is baked into that table's own build).
- `io_occupancy_time_raw`: **2-month** look-back.
- `io_ntpj_xforce_metric`: output period only (the gate keys on the emitted month).

Look-back rows only build the previous-month comparators; only the requested
months are emitted.

## Manual adjustments

The NTPJ-side adjustments (`exclusiones_generales`, `cross_support`,
`exclusiones_jobs`) are applied **upstream** in `build_normalized_time_per_job`.
The build applies only the occupancy-side adjustments here:
`exclusiones_generales` (slot/date windows) and `inconsistencias_dime`
(DIME reclassification), read from their synced `adj_*` Delta tables if present.
Everything else the NTPJ / NO layers defer is inherited, not applied.

## Output schema

| column | type | notes |
|--------|------|-------|
| `agent` | STRING | always NULL |
| `xforce` | STRING | the XForce |
| `xplead` | STRING | the XForce's XPLead |
| `team` | STRING | always NULL |
| `squad` | STRING | always NULL |
| `district` | STRING | always NULL |
| `shift` | STRING | always NULL |
| `date_reference` | DATE | first of month |
| `date_granularity` | STRING | always `month` |
| `metric` | STRING | always `improved_benchmark_xforce` |
| `numerator` | DOUBLE | improved benchmarks |
| `denominator` | DOUBLE | comparable benchmarks (had a previous month) |
| `metric_value` | DOUBLE | `numerator / denominator * 100` (NULL if denominator 0) |
