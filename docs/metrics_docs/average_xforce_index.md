# average_xforce_index

The XPLead-level **Average XForce Index** (legacy `average_index_xforce`). One
row per `(team, xplead)` per **day / week / month / quarter / semester / year**,
for **all four teams**.

> `Average XForce Index = AVG(xforce_index)` per XPLead.

A pure mean of the **XForce-level `xforce_index`** rolled up to the XPLead.

- Module: `metrics/average_xforce_index.py`
- Build script: `scripts/metrics_scripts/build_average_xforce_index.py`
- Input: `usr.danielanzures.io_xforce_index_metric` (XForce grain)
- Default target table: `usr.danielanzures.io_average_xforce_index_metric`

## Input

`io_xforce_index_metric` — XForce-grain composite index, where `metric_value` is
each XForce's `xforce_index`. Only `metric = 'xforce_index'` rows are averaged;
XForces with a NULL `metric_value` are excluded (matching SQL `AVG`).

## Output convention

Legacy left `numerator` / `denominator` NULL and used `AVG()`. We instead fill
them so the row is self-describing:

- `numerator` = Σ XForce index,
- `denominator` = XForce count,
- `metric_value` = `numerator / denominator` (the identical mean).

The legacy notebook only materialized `week` + `month`; the metric layer emits
whatever granularities the XForce Index provides (all six).

## Output schema (one row per XPLead per period)

| column | type | notes |
|--------|------|-------|
| `agent` | STRING | always NULL |
| `xforce` | STRING | always NULL |
| `xplead` | STRING | the XPLead |
| `team` | STRING | `core` / `fraud` / `social media` / `content` |
| `squad` | STRING | always NULL |
| `district` | STRING | always NULL |
| `shift` | STRING | always NULL |
| `date_reference` | DATE | bucket start |
| `date_granularity` | STRING | `day` / `week` / `month` / `quarter` / `semester` / `year` |
| `metric` | STRING | always `average_xforce_index` |
| `numerator` | DOUBLE | Σ XForce index |
| `denominator` | DOUBLE | XForce count |
| `metric_value` | DOUBLE | mean XForce index |
