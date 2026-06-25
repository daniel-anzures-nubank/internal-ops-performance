# average_xpeer_index

The XForce-level **Average Xpeer Index** (legacy `average_index_agent`). One row
per `(team, xforce, xplead)` per **day / week / month / quarter / semester /
year**.

> `Average Xpeer Index = AVG(agent Xpeer Index)` per XForce.

A pure mean of the **agent-level Xpeer Index** rolled up to the XForce. Applies
to **all four teams** (Core / Fraud / Social Media / Content) — the agent index
already bakes in each team's era-specific composition, so this layer just
averages.

- Module: `metrics/average_xpeer_index.py`
- Build script: `scripts/metrics_scripts/build_average_xpeer_index.py`
- Input: `usr.danielanzures.io_xpeer_index_metric` (agent-level)
- Default target table: `usr.danielanzures.io_average_xpeer_index_metric`

## Input

`io_xpeer_index_metric` — agent-level index, where `metric_value` is each
agent's Xpeer Index. Agents with a NULL `metric_value` are excluded from the
average (matching SQL `AVG`, which ignores NULLs).

## Output convention

Legacy left `numerator` / `denominator` NULL and used `AVG()`. We instead fill
them so the row is self-describing:

- `numerator` = Σ agent index,
- `denominator` = agent count,
- `metric_value` = `numerator / denominator` (the identical mean).

The legacy notebook only materialized `week` + `month`; the metric layer emits
whatever granularities the agent index provides (all six).

## Output schema (one row per XForce per period)

| column | type | notes |
|--------|------|-------|
| `agent` | STRING | always NULL |
| `xforce` | STRING | the XForce |
| `xplead` | STRING | the XForce's XPLead |
| `team` | STRING | `core` / `fraud` / `social media` / `content` |
| `squad` | STRING | always NULL |
| `district` | STRING | always NULL |
| `shift` | STRING | always NULL |
| `date_reference` | DATE | bucket start |
| `date_granularity` | STRING | `day` / `week` / `month` / `quarter` / `semester` / `year` |
| `metric` | STRING | always `average_xpeer_index` |
| `numerator` | DOUBLE | Σ agent index |
| `denominator` | DOUBLE | agent count |
| `metric_value` | DOUBLE | mean agent index |
