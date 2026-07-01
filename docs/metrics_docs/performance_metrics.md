# performance_metrics

The **consolidated performance-metrics table** — the one-stop reporting view of
the whole metric layer. It is the **UNION ALL of all 16 finished
`io_*_metric` tables** with the same 13 tidy columns, except `team` is
replaced by a **display team** (`Core` / `Fraud` / `Social Media` / `Content`
/ `Quality`). No rows are filtered, aggregated, or recomputed — one input row
in, one output row out.

- Module: `metrics/performance_metrics.py`
- Build script: `scripts/metrics_scripts/build_performance_metrics.py`
- Inputs: the 16 `usr.danielanzures.io_*_metric` tables (below)
- Default target table: `usr.danielanzures.io_performance_metrics`

## Inputs

All 16 finished metric tables, unioned as-is:

`io_adherence_metric` (also the modal-dim driver), `io_ntpj_metric`,
`io_normalized_occupancy_metric`, `io_quality_metric`, `io_shrinkage_metric`,
`io_tnps_metric`, `io_wows_metric`, `io_content_csat_metric`,
`io_ntpj_xforce_metric`, `io_improved_benchmarks_metric`,
`io_xpeer_index_metric`, `io_nuvinhos_performance_metric`,
`io_xpeers_in_target_metric`, `io_average_xpeer_index_metric`,
`io_xforce_index_metric`, `io_average_xforce_index_metric`.

## Display-team cascade

The `team` column of every row is replaced by the display team, resolved in
this exact order:

1. **Direct team map** of the source `team` column: `core -> Core`,
   `fraud -> Fraud`, `social media -> Social Media`, `content -> Content`.
2. **Squad map** (the support squads legacy keeps with `team = NULL`):
   `quality -> Quality`, `enablement -> Content`.
3. else, rows with `squad` NOT NULL: the **modal display team of that squad**
   at the same `(date_reference, date_granularity)`.
4. else, rows with `squad` NULL and `xforce` NOT NULL: the modal display team
   of that **xforce**.
5. else, `xforce` NULL and `xplead` NOT NULL: the modal display team of that
   **xplead**.
6. else, all of those NULL and `district` NOT NULL: the modal display team of
   that **district**.
7. else NULL.

The cascade **branches on which dimension is populated**, not on lookup
success: a `squad` NOT NULL row whose squad has no modal team stays NULL — it
never falls through to the xforce/xplead/district lookups. That is why the
`planning` support squad deliberately ends up NULL (`planning` maps to no
display team, and its own adherence rows never enter the modal dims).

### Modal dims (steps 3-6)

Built **only from `io_adherence_metric` rows** — adherence is the driver
metric (every agent has adherence rows carrying the full roster dimensions).
Each adherence row is labeled with its own steps-1-2 display team; rows whose
display team is NULL (e.g. `planning`) are dropped. Per
`(dimension value, date_reference, date_granularity)` the **modal** display
team is the one with the highest row count, **ties broken alphabetically**.

This backfills the roll-up metrics that legacy emits without a usable `team`:
the XForce roll-ups (`ntpj_xforce`, `improved_benchmark_xforce`,
`xpeers_in_target`, `average_xpeer_index`, `xforce_index`,
`shrinkage_xforce`), the XPLead roll-ups (`xpeers_in_target_xplead`,
`shrinkage_xplead`, `average_xforce_index`), and the squad/district
`nuvinhos_performance` roll-ups.

## Filters applied / deferred

- **Applied**: none — the table is a pure UNION ALL (row-for-row, no
  loss/dup). Only the `team` column is rewritten.
- **Deferred**: everything metric-specific already happened upstream in the
  16 input tables (exclusions, benchmarks, era gates, manual adjustments).

## Output schema (same grain as each input row)

| column | type | notes |
|--------|------|-------|
| `agent` | STRING | as in the source metric row (NULL on roll-ups) |
| `xforce` | STRING | as in the source metric row |
| `xplead` | STRING | as in the source metric row |
| `team` | STRING | **display team**: `Core` / `Fraud` / `Social Media` / `Content` / `Quality`, or NULL (`planning`, unmatched) |
| `squad` | STRING | as in the source metric row |
| `district` | STRING | as in the source metric row |
| `shift` | STRING | as in the source metric row |
| `date_reference` | DATE | bucket start |
| `date_granularity` | STRING | `day` / `week` / `month` / `quarter` / `semester` / `year` |
| `metric` | STRING | the source metric name (e.g. `adherence`, `xforce_index`) |
| `numerator` | DOUBLE | as in the source metric row |
| `denominator` | DOUBLE | as in the source metric row |
| `metric_value` | DOUBLE | as in the source metric row |
