# wows

The **WoWs** performance metric (Social Media only). One row per agent per
**day / week / month / quarter / semester / year**.

> `wows = COUNT(DISTINCT case_id)` — the number of WoW experiences an agent
> delivered. **Monthly target ≥ 5.**

WoWs is a **count**, not a ratio. Only **Social Media** has WoWs (the source
sheet only contains social agents' WoWs — see `docs/metrics_definitions.md`).

- Module: `metrics/wows_metric.py` (named `wows_metric` to avoid colliding with
  the raw module `metrics_data/wows.py`; the metric itself is `wows`)
- Build script: `scripts/metrics_scripts/build_wows.py`
- Input: `usr.danielanzures.io_wows_raw`
- Default target table: `usr.danielanzures.io_wows_metric`

## Output convention (differs from the ratio metrics)

Because WoWs is a raw count, `metric_value` is the **count itself** — it is *not*
`numerator / denominator * 100`:

| field | meaning |
|-------|---------|
| `numerator` | the WoW count (same as `metric_value`) |
| `denominator` | the monthly target (`5`), carried for reference only (legacy `MAX(monthly_target)`) |
| `metric_value` | the WoW count |

## Input

The `io_wows_raw` table (`metrics_data/wows.py`), one row per WoW experience,
carrying `case_id` + the dimensions. The count + target are deferred to this
metric layer.

## Filters / rules applied here (deferred by the raw layer)

- **Team scope** — keep `team = 'social media'` (defensive; source is social-only).
- **Count** — `COUNT(DISTINCT case_id)` per `(agent, period)`.

## Derivation

1. Apply the team scope.
2. Bucket each `date` to the granularity (`day` → date, `week` → Monday,
   `month`/`quarter`/`year` → first of period, `semester` → Jan 1 or Jul 1).
3. Per `(agent, date_reference)`: `numerator = nunique(case_id)`.
4. `denominator = 5` (monthly target), `metric_value = numerator`.
5. Dimension/hierarchy fields take their most-recent value within the bucket.

## Deferred to the future Adjustments layer (NOT applied here)

- The outage-date exclusion `date = 2026-03-27` (legacy drops it for "general
  access problems") — deferred to match the other metric modules.

## Output schema (one row per agent per period)

| column | type | notes |
|--------|------|-------|
| `agent` | STRING | |
| `xforce` | STRING | most-recent in bucket |
| `xplead` | STRING | most-recent in bucket |
| `team` | STRING | always `social media` |
| `squad` | STRING | most-recent in bucket |
| `district` | STRING | most-recent in bucket |
| `shift` | STRING | most-recent in bucket |
| `date_reference` | DATE | bucket start (day / Monday / first-of-month/quarter/year / Jan 1 or Jul 1) |
| `date_granularity` | STRING | `day` / `week` / `month` / `quarter` / `semester` / `year` |
| `metric` | STRING | always `wows` |
| `numerator` | DOUBLE | WoW count (= `metric_value`) |
| `denominator` | DOUBLE | monthly target (`5`), reference only |
| `metric_value` | DOUBLE | the WoW count (NOT a ratio) |
