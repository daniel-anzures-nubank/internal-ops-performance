# shrinkage

The **Shrinkage** performance metric. One row per agent per **day / week /
month / quarter / semester / year**.

Shrinkage = the share of an agent's *dimensioned* (required) time spent on
non-productive activities:

> `shrinkage = SUM(shrinkage_flag) / SUM(required_slot)` over the agent's
> required DIME slots. **Target ≤ 20%.**

Same definition for all teams (Core, Fraud, Social Media, Content) — see
`docs/metrics_definitions.md`.

The build also publishes two **slot-weighted roll-ups** of the agent metric into
the same table (legacy `shrinkage_xforce` / `shrinkage_xplead`):

- `shrinkage_xforce` — one row per `(team, xforce, xplead)`.
- `shrinkage_xplead` — one row per `(team, xplead)` (`xforce` NULL).

Both sum the agent `numerator` / `denominator` and then divide (NOT a flat
average of per-agent percentages) — identical to the shrinkage component inside
`xforce_index`.

- Module: `metrics/shrinkage.py`
  (`compute_shrinkage` + `compute_shrinkage_rollups`)
- Build script: `scripts/metrics_scripts/build_shrinkage.py` (writes all grains)
- Input: `usr.danielanzures.io_shrinkage_slots_raw`
- Default target table: `usr.danielanzures.io_shrinkage_metric`

## Input

The `io_shrinkage_slots_raw` table (`metrics_data/shrinkage_slots.py`), one row
per DIME slot, already carrying `shrinkage_flag` (the pre/post-2026-03-01 slot-
level shrinkage rule), `activity_type_required`, and the dimensions. The
numerator is built by the raw layer; this metric only applies the **denominator
(`required_slot`)** rule the raw layer deferred.

## Filters / rules applied here (deferred by the raw layer)

- **Drop `lunch_break`** — never counts on either side (legacy `shrinkage_base`
  drops it before any counting).
- **Required-slot rule** (the denominator), by era:
  - **pre-cutover** (`date < 2026-03-01`): a slot is required unless
    `activity_type_required = 'dime_invalid_notation'`.
  - **post-cutover** (`date >= 2026-03-01`): a slot is required unless
    `activity_type_required = 'time_off'`.

  Every shrinkage-flagged slot is also a required slot, so `metric_value` stays
  in `[0, 100]`.

## Derivation

1. Drop `lunch_break` slots.
2. Per slot, compute `required_slot_flag` from the era rule above.
3. Bucket each slot's `date` to the granularity (`day` → date, `week` → Monday,
   `month`/`quarter`/`year` → first of period, `semester` → Jan 1 or Jul 1).
4. Per `(agent, date_reference)`: `numerator = SUM(shrinkage_flag)`,
   `denominator = SUM(required_slot_flag)`.
5. Dimension/hierarchy fields (`xforce, xplead, team, squad, district, shift`)
   take their most-recent value within the bucket.
6. `metric_value = numerator / denominator * 100` (NULL when denominator is 0).

## Deferred to the future Adjustments layer (NOT applied here)

- Per-agent maternity / vacation reclassifications that legacy folds straight
  into `shrinkage_slot` (e.g. `maria.reyes` maternity leave; the hardcoded
  per-agent vacation dates).
- Training / shadowing slot exclusions and outage-date carve-outs
  (e.g. the 2026-03-24..28 block for specific agents).
- Legacy DIME-squad business exclusions (`wfm` / `enablement` / …). Note
  `quality` and `planning` now flow through the extractor (kept for legacy
  parity, `team = NULL`) rather than being excluded upstream.

## Controllable vs. uncontrollable

The raw table also carries `controllable_shrinkage_flag` /
`uncontrollable_shrinkage_flag` (uncontrollable = `Licencia` / `SKR_LCNC`). This
metric reports the **headline** shrinkage only; a controllable/uncontrollable
breakdown can be added later from the same raw table.

## Output schema (agent rows + XForce/XPLead roll-up rows)

| column | type | notes |
|--------|------|-------|
| `agent` | STRING | the agent (NULL on roll-up rows) |
| `xforce` | STRING | most-recent in bucket (NULL on `shrinkage_xplead` rows) |
| `xplead` | STRING | most-recent in bucket |
| `team` | STRING | `core` / `fraud` / `social media` / `content` |
| `squad` | STRING | most-recent in bucket (NULL on roll-up rows) |
| `district` | STRING | most-recent in bucket (NULL on roll-up rows) |
| `shift` | STRING | most-recent in bucket (NULL on roll-up rows) |
| `date_reference` | DATE | bucket start (day / Monday / first-of-month/quarter/year / Jan 1 or Jul 1) |
| `date_granularity` | STRING | `day` / `week` / `month` / `quarter` / `semester` / `year` |
| `metric` | STRING | `shrinkage` (agent), `shrinkage_xforce`, or `shrinkage_xplead` |
| `numerator` | DOUBLE | shrinkage slots (summed on roll-ups) |
| `denominator` | DOUBLE | required slots (summed on roll-ups) |
| `metric_value` | DOUBLE | `numerator / denominator * 100` (percentage; NULL if denominator 0) |
