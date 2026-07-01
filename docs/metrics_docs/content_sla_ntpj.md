# content_sla_ntpj (Content NTPJ)

**Content's NTPJ** — a **different metric under the same `ntpj` name**. Unlike
the Core/Fraud duration ratio (`actual / expected`, lower-is-better — see
[ntpj.md](ntpj.md)), Content NTPJ is **SLA-weighted compliance**
(higher-is-better, bounded ≤ 100). One row per agent per **day / week / month /
quarter / semester / year**.

Legacy calls it `ntpj_sla_old` (`[IO] Performance 2026 - Content Temp Fix.sql`)
but ships it as `metric = 'ntpj_agent'` for standardization; we emit it as
`metric = 'ntpj'` so `io_ntpj_metric` stays one standardized table.

> Per Content OOS job, the job earns its **full** `sla_seconds` if delivered
> within its OLD-SLA threshold, else **0** (all-or-nothing credit):
>
> `ntpj (Content) = SUM(sla_met_seconds) / SUM(sla_seconds) * 100`
>
> **Higher is better; target ≥ 95%** (the on-target floor `ntpj_xforce` and
> `xpeer_index` consumers use).

- Module: `metrics/content_sla_ntpj.py` (`compute_content_sla_ntpj`)
- Build script: `scripts/metrics_scripts/build_ntpj.py` — the **same script as
  Core/Fraud NTPJ**: it computes the duration NTPJ from `io_jobs_raw` (Content
  jobs still feed the cohort-wide benchmark), then **drops the Content output
  rows and unions** these SLA-based rows in their place.
- Input: `usr.danielanzures.io_jobs_within_sla_raw` (override `--sla-source`)
- Target table: `usr.danielanzures.io_ntpj_metric` (shared with Core/Fraud NTPJ)

## Team coverage

**Content only.** Core/Fraud keep the duration NTPJ; Social Media has no NTPJ.

## Input

The `io_jobs_within_sla_raw` table (`metrics_data/jobs_within_sla.py`): one row
per Content OOS job — a distinct `content_id` for most job types, one source
row for `macros` / `faq` / `ar` — carrying `actual_seconds`, its `sla_seconds`
threshold (from the synced `adj_content_slas` table, the **`Content - SLAs`**
sheet tab; the map is mandatory, job types without an SLA are dropped by an
inner join), `within_sla`, `sla_met_seconds` (= `sla_seconds` if on time else
0), and `roster_status`.

The raw layer already scoped to Content agents and applied the date scoping
(`date >= 2025-12-01`, dropping the outage dates `2026-03-10` / `2026-03-27` /
`2026-04-09`) **before** the `content_id` grouping, so jobs straddling the
`2026-03-10` boundary are truncated exactly like legacy's source-level drop.

## Filters applied here (deferred by the raw layer)

- `roster_status == 'active'` (carried by the raw table, applied here —
  matching the `jobs_raw` → `ntpj` split).
- Restrict to `[period_start, period_end]`.

## Derivation

1. Apply the two filters above.
2. Bucket each job's `date` to the granularity (day / Monday-week /
   first-of-month/quarter/year / Jan 1 or Jul 1 semester) via the shared
   `aggregate_long`.
3. Per `(agent, date_reference)`: `numerator = SUM(sla_met_seconds)`,
   `denominator = SUM(sla_seconds)`; dimension fields take their most-recent
   value within the bucket.
4. `metric_value = numerator / denominator * 100` (NULL when denominator is 0).
   Bounded ≤ 100 by construction (`sla_met_seconds <= sla_seconds` per job).

## Consumers are Content-aware

- `xpeer_index` adds Content NTPJ **raw** (Core/Fraud NTPJ is folded around
  100 — Content's is already a higher-is-better 0–100 score).
- `ntpj_xforce` uses the floor `>= 95` for Content (vs `<= 100` for Core/Fraud).

## Deferred to the future Adjustments layer (NOT applied here)

Per-agent carve-outs beyond the roster/date scoping above (the Core/Fraud NTPJ
manual adjustments — `exclusiones_generales`, `cross_support`,
`exclusiones_jobs` — are applied on the duration side only).

## Output schema (one row per agent per period)

| column | type | notes |
|--------|------|-------|
| `agent` | STRING | Content agent |
| `xforce` | STRING | most-recent in bucket |
| `xplead` | STRING | most-recent in bucket |
| `team` | STRING | always `content` |
| `squad` | STRING | most-recent in bucket (`enablement`) |
| `district` | STRING | most-recent in bucket (`content`) |
| `shift` | STRING | most-recent in bucket |
| `date_reference` | DATE | bucket start |
| `date_granularity` | STRING | `day` / `week` / `month` / `quarter` / `semester` / `year` |
| `metric` | STRING | always `ntpj` (standardized with Core/Fraud) |
| `numerator` | DOUBLE | Σ `sla_met_seconds` (SLA seconds of on-time jobs) |
| `denominator` | DOUBLE | Σ `sla_seconds` |
| `metric_value` | DOUBLE | `numerator / denominator * 100` (≤ 100; NULL if denominator 0) |
