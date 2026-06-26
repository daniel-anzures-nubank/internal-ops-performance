# adherence

The **Adherence** performance metric. One row per agent per **day / week /
month / quarter / semester / year**.

Adherence = the share of an agent's dimensioned (scheduled) time that overlapped
a "connected" productivity status:

> `adherence = SUM(adherent_minutes) / SUM(required_minutes)` over the agent's
> **productive** DIME slots. **Target ≥ 95%.**

Same definition for all teams (Core, Fraud, Social Media, Content) — see
`docs/metrics_definitions.md`.

- Module: `metrics/adherence.py`
- Build script: `scripts/metrics_scripts/build_adherence.py`
- Input: `usr.danielanzures.io_adherent_time_raw`
- Default target table: `usr.danielanzures.io_adherence_metric`

## Team coverage

All four teams. Verified live (2026-05-05): rows for `core`, `fraud`,
`social media`, and `content`; 282 agents, day-grain `metric_value` averaging
~94.5% — the expected adherence shape.

## Input

The `io_adherent_time_raw` table (`metrics_data/adherent_time.py`), one row per
agent per DIME slot, carrying `activity_type_required`, `required_minutes`
(always 30), and `adherent_minutes` (connected overlap in the slot).

## Filter applied here (deferred by the raw layer)

- **Drop non-productive slots**: `activity_type_required` in
  `{lunch_break, time_off, shrinkage}` (case-insensitive), per the metric
  definition. The remaining slots form both the numerator and the denominator.

The meeting/leave `dimensioned_activity` slots (Mouring / Weekly / Permiso
Medico / Huddle / Licencia / Vacacion) are **already excluded upstream** by the
raw layer's fixed DIME filter (`metrics_data/adherent_time.py`), so they never
reach this metric — see `MEETING_LEAVE_DIMENSIONED_ACTIVITIES`.

## Derivation

1. Drop the excluded activity types.
2. Bucket each slot's `date` to the granularity (`day` → date, `week` → Monday,
   `month`/`quarter`/`year` → first of period, `semester` → Jan 1 or Jul 1).
3. Per `(agent, date_reference)`: `numerator = SUM(adherent_minutes)`,
   `denominator = SUM(required_minutes)`.
4. Dimension/hierarchy fields (`xforce, xplead, team, squad, district, shift`)
   take their most-recent value within the bucket.
5. `metric_value = numerator / denominator * 100` (NULL when denominator is 0).

## Deferred to the future Adjustments layer (NOT applied here)

- Legacy DIME-squad exclusions (`wfm` / `credit_evolution` / `dote`).
- Per-agent manual time-off adjustments and outage-date exclusions
  (e.g. 2026-03-27, 2026-04-09).

## Output schema (one row per agent per period)

| column | type | notes |
|--------|------|-------|
| `agent` | STRING | |
| `xforce` | STRING | most-recent in bucket |
| `xplead` | STRING | most-recent in bucket |
| `team` | STRING | `core` / `fraud` / `social media` / `content` |
| `squad` | STRING | most-recent in bucket |
| `district` | STRING | most-recent in bucket |
| `shift` | STRING | most-recent in bucket |
| `date_reference` | DATE | bucket start (day / Monday / first-of-month/quarter/year / Jan 1 or Jul 1) |
| `date_granularity` | STRING | `day` / `week` / `month` / `quarter` / `semester` / `year` |
| `metric` | STRING | always `adherence` |
| `numerator` | DOUBLE | delivered (adherent) minutes |
| `denominator` | DOUBLE | required (dimensioned) minutes |
| `metric_value` | DOUBLE | `numerator / denominator * 100` (percentage; NULL if denominator 0) |
