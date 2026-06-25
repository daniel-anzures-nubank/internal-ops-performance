# nuvinhos_performance

The **Nuvinhos Performance** metric (legacy `nuvinhos_performance*`). An
**index-level** metric (no agent grain) that compares how new agents perform
versus tenured ones:

> `Nuvinhos Performance = avg Xpeer Index (Nuvinhos) / avg Xpeer Index (Old) * 100`

Applies to **all four teams**. See `docs/metrics_definitions.md`.

- Module: `metrics/nuvinhos_performance.py`
- Build script: `scripts/metrics_scripts/build_nuvinhos_performance.py`
- Inputs: `usr.danielanzures.io_xpeer_index_metric` + the `agent_information`
  extractor (tenure)
- Default target table: `usr.danielanzures.io_nuvinhos_performance_metric`

## Inputs (a finished metric + roster tenure, not a raw table)

- **`io_xpeer_index_metric`** — the agent-level Xpeer Index (`metric =
  'xpeer_index'`), all granularities. This is what gets averaged.
- **`agent_information`** — one row per `(agent, snapshot_month)`, providing
  `last_change_date` (hire date or last squad-change date).

## Who is a Nuvinho?

An agent is a **Nuvinho** for a bucket when the bucket's **month** lies in
`[month(last_change_date), month(last_change_date) + 2 months]` — the hire /
change month plus the next two. Everyone else is **old**, including agents with
a NULL `last_change_date` (e.g. the temp Content roster, which therefore yields
no Nuvinhos).

## Computation (documented flat average)

For each roll-up key, take a **flat average of the Xpeer Index over agents**,
split by Nuvinho vs old, and divide. Three roll-ups are emitted (one table,
three `metric` names):

| metric | grouped by | dims kept |
|--------|-----------|-----------|
| `nuvinhos_performance` | `(team, xforce, xplead)` | xforce, xplead |
| `nuvinhos_performance_squad` | `(team, squad)` | squad |
| `nuvinhos_performance_district` | `(team, district)` | district |

`agent` and `shift` are always NULL; dimensions outside a roll-up's key are
NULL too.

> **Deviation from the legacy SQL (intentional).** The legacy notebook computes a
> **two-level** mean (average per `(xforce, xplead, squad, district, nuvinho)`
> cohort, then average those means with opposite-flag zeros included). That
> biases the ratio by the *number* of Nuvinho vs old cohorts — e.g. a single
> Nuvinho squad against five old squads dilutes the result toward 0 — and only
> matches the documented `avg/avg` when an XForce has one cohort. Per
> `docs/metrics_definitions.md` (the source of truth) we average agents directly
> instead. Legacy also built only the XForce roll-up for Core/Fraud; we extend
> squad + district to all teams for a consistent table.

## Output convention

- `numerator` = mean Index of Nuvinhos, `denominator` = mean Index of old agents
  (both flat averages over the agents in the roll-up key),
- `metric_value` = `numerator / denominator * 100` (NULL when there are no old
  agents). When a roll-up key has no Nuvinhos, `numerator` is 0 and
  `metric_value` is 0.

## Deferred to the future Adjustments layer (NOT applied here)

- Per-agent carve-outs inherited from the Xpeer Index.
- Content yields a degenerate result (no Nuvinhos) until the real Content roster
  with hire dates replaces the temp BDX source.

## Output schema (one row per roll-up cohort per period)

| column | type | notes |
|--------|------|-------|
| `agent` | STRING | always NULL |
| `xforce` | STRING | set on the XForce roll-up, else NULL |
| `xplead` | STRING | set on the XForce roll-up, else NULL |
| `team` | STRING | `core` / `fraud` / `social media` / `content` |
| `squad` | STRING | set on the squad roll-up, else NULL |
| `district` | STRING | set on the district roll-up, else NULL |
| `shift` | STRING | always NULL |
| `date_reference` | DATE | bucket start |
| `date_granularity` | STRING | `day` / `week` / `month` / `quarter` / `semester` / `year` |
| `metric` | STRING | `nuvinhos_performance` / `_squad` / `_district` |
| `numerator` | DOUBLE | mean Index of Nuvinhos (0 when none) |
| `denominator` | DOUBLE | mean Index of old agents |
| `metric_value` | DOUBLE | Nuvinho/old comparison % = `numerator / denominator * 100` |
