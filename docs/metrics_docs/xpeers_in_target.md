# xpeers_in_target

The **Xpeers In Target** metric (legacy `xpeers_in_target_xforce` /
`xpeers_in_target_xplead`), at two roll-up grains in one table, each at
**day / week / month / quarter / semester / year**:

- `xpeers_in_target` — **XForce** grain, one row per `(team, xforce, xplead)`.
- `xpeers_in_target_xplead` — **XPLead** grain, one row per `(team, xplead)`
  (`xforce` NULL); the same in-target/total counts aggregated to the XPLead.

> `Xpeers In Target = Σ targets achieved / Σ targets * 100`. **Target ≥ 70%.**

A *target* is one agent-metric. For each era-active component, every Xpeer with
that metric contributes one target (denominator) and an achieved target
(numerator) when their value clears the threshold.

Applies to **Core / Fraud / Social Media** (not Content — the legacy Content
notebook doesn't build it). See `docs/metrics_definitions.md`.

- Module: `metrics/xpeers_in_target.py`
  (`compute_xpeers_in_target` + `compute_xpeers_in_target_xplead`)
- Build script: `scripts/metrics_scripts/build_xpeers_in_target.py` (writes both grains)
- Inputs: the agent-level `io_*_metric` tables (adherence is the driver)
- Default target table: `usr.danielanzures.io_xpeers_in_target_metric`

## Inputs (finished agent-level metric tables, not a raw table)

`io_adherence_metric` (driver — defines the XForce universe and `xplead`),
`io_ntpj_metric`, `io_normalized_occupancy_metric`, `io_quality_metric`,
`io_tnps_metric`, `io_wows_metric`.

## Component targets

| component | threshold | teams |
|-----------|-----------|-------|
| adherence | `>= 95` | Core / Fraud / Social Media |
| ntpj | `<= 100` | Core / Fraud |
| normalized_occupancy | `>= 100` | Core / Fraud / Social Media |
| quality | `>= 95` | Core / Fraud / Social Media |
| tnps | `>= 88` | Social Media |
| wows | `>= 5` (count) | Social Media |

An agent counts toward a component's **denominator** when they have a row for
that metric in the bucket; toward the **numerator** only when `metric_value`
clears the threshold (NULL `metric_value` fails the target but still counts in
the denominator).

## Era windows (anchored on the bucket's month)

- **Core / Fraud**: adherence + ntpj always; `+ quality` from **Feb 2026**;
  `+ normalized_occupancy` from **March 2026**.
- **Social Media**: adherence + tnps + wows always; `+ quality` from **Feb
  2026**; `+ normalized_occupancy` from **March 2026**.

`day` / `week` / `month` buckets use their own month; `quarter` / `semester` /
`year` anchor on the period's **end** month. Buckets ending before 2026 are
dropped.

## Output convention

- `numerator` = targets achieved (sum of in-target agent counts across active
  components),
- `denominator` = total targets (sum of agent counts across active components),
- `metric_value` = `numerator / denominator * 100` (NULL when denominator 0).

## Deferred to the future Adjustments layer (NOT applied here)

Per-agent carve-outs inherited from the underlying component metrics.

## Output schema (one row per XForce/XPLead per period)

| column | type | notes |
|--------|------|-------|
| `agent` | STRING | always NULL |
| `xforce` | STRING | the XForce (NULL on `xpeers_in_target_xplead` rows) |
| `xplead` | STRING | the XPLead (from the Adherence driver) |
| `team` | STRING | `core` / `fraud` / `social media` |
| `squad` | STRING | always NULL |
| `district` | STRING | always NULL |
| `shift` | STRING | always NULL |
| `date_reference` | DATE | bucket start |
| `date_granularity` | STRING | `day` / `week` / `month` / `quarter` / `semester` / `year` |
| `metric` | STRING | `xpeers_in_target` (XForce) or `xpeers_in_target_xplead` (XPLead) |
| `numerator` | DOUBLE | targets achieved |
| `denominator` | DOUBLE | total targets |
| `metric_value` | DOUBLE | `numerator / denominator * 100` |
