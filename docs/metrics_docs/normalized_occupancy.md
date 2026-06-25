# normalized_occupancy

The **Normalized Occupancy (NO)** performance metric. One row per agent per
**day / week / month / quarter / semester / year**.

NO compares an agent's occupancy against the average occupancy of their
**district + shift** cohort that month:

> `occupancy = SUM(occupancy_minutes) / SUM(required_minutes)`
> `NO = occupancy / occupancy_benchmark`. **Target ≥ 100%.**

Applies to **all four teams** (Core, Fraud, Social Media, Content). Social Media
occupancy comes from Sprinklr and is already folded into the raw table via
`sm_jobs`. See `docs/metrics_definitions.md`.

- Module: `metrics/normalized_occupancy.py`
- Build script: `scripts/metrics_scripts/build_normalized_occupancy.py`
- Input: `usr.danielanzures.io_occupancy_time_raw`
- Default target table: `usr.danielanzures.io_normalized_occupancy_metric`

## Input

The `io_occupancy_time_raw` table (`metrics_data/occupancy_time.py`), one row per
agent per DIME slot, carrying `activity_type_required`, `required_minutes`
(always 30), and `occupancy_minutes` (matching-activity job overlap in the slot,
≤ 30).

## The benchmark (matches legacy `[IO] Normalized Occupancy Dataset.sql`)

Two-step, per **month**:

1. Per `(month, district, shift, squad)`: `SUM(occupancy_minutes) /
   SUM(required_minutes)` — that squad-cohort's occupancy ratio.
2. Per `(month, district, shift)`: the **mean of the squad ratios** from step 1
   (equal-weight across squads — the legacy `AVG(occupancy_monthly)` over the
   squads sharing a district + shift).

Each slot carries its `(month, district, shift)` benchmark; rolled up to a
multi-month bucket the benchmark is averaged weighted by required minutes (a
single-month bucket therefore just keeps that month's benchmark). The benchmark
is computed from the slots in the read window, so prefer whole-month builds.

## Filter applied here (deferred by the raw layer)

- **Drop non-productive slots**: `activity_type_required` in
  `{lunch_break, time_off, shrinkage}` (case-insensitive). The remaining slots
  feed both the agent occupancy and the benchmark.

## Output convention

To keep the shared `metric_value = numerator / denominator * 100` contract:

- `numerator` = the agent's **occupancy %** (`SUM(occupancy_minutes) /
  SUM(required_minutes) * 100`),
- `denominator` = the **benchmark %**,
- `metric_value` = NO % = `numerator / denominator * 100`.

## Deferred to the future Adjustments layer (NOT applied here)

- Legacy `dimensioned_activity` meeting/leave carve-outs and the per-agent
  `time_off` reclassifications (the raw table doesn't carry
  `dimensioned_activity`).
- DIME-squad exclusions (`wfm` / `credit_evolution` / `dote`) — so they
  currently still feed the benchmark.
- Per-agent vacation / outage-date exclusions (e.g. 2026-03-27, 2026-04-09).

## Output schema (one row per agent per period)

| column | type | notes |
|--------|------|-------|
| `agent` | STRING | |
| `xforce` | STRING | most-recent in bucket |
| `xplead` | STRING | most-recent in bucket |
| `team` | STRING | `core` / `fraud` / `social media` / `content` |
| `squad` | STRING | most-recent in bucket |
| `district` | STRING | most-recent in bucket |
| `shift` | STRING | most-recent in bucket (NULL for content) |
| `date_reference` | DATE | bucket start (day / Monday / first-of-month/quarter/year / Jan 1 or Jul 1) |
| `date_granularity` | STRING | `day` / `week` / `month` / `quarter` / `semester` / `year` |
| `metric` | STRING | always `normalized_occupancy` |
| `numerator` | DOUBLE | agent occupancy % |
| `denominator` | DOUBLE | district+shift benchmark occupancy % |
| `metric_value` | DOUBLE | NO % = `numerator / denominator * 100` (NULL if denominator 0) |
