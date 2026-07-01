# ntpj_xforce

The **NTPJ XForce** metric (legacy `ntpj_xforces`): the XForce-grain roll-up of
the agent-level NTPJ metric, at **week / month only** (legacy only rolls up the
`ntpj_agents_weekly` / `ntpj_agents_monthly` views — there is no
day/quarter/semester/year `ntpj_xforce`).

> `ntpj_xforce = COUNT(DISTINCT on-target agents) / COUNT(DISTINCT agents) * 100`
> per `(team, xforce, xplead, period)`.

**"On target" is team-aware** — Core/Fraud NTPJ and Content NTPJ are different
metrics under the same `ntpj` name (see [ntpj.md](ntpj.md) /
[content_sla_ntpj.md](content_sla_ntpj.md)):

| team | agent NTPJ | on-target rule |
|------|------------|----------------|
| Core / Fraud | duration `actual / expected` (lower-is-better) | `metric_value <= 100` |
| Content | SLA-weighted compliance (higher-is-better) | `metric_value >= 95` |

The Content `>= 95` reproduces legacy `ntpj_sla_old_xforces` (legacy's own
xplead roll-up `ntpj_sla_old_xpleads` inconsistently uses `>= 100`; the
XForce-level 95 is what we match). A NULL `metric_value` fails both tests, so
the agent counts in the denominator but not the numerator (legacy
`CASE WHEN metric_value <= 100` — `NULL <= 100` is not true).

- Module: `metrics/ntpj_xforce.py` (`compute_ntpj_xforce`)
- Build script: `scripts/metrics_scripts/build_ntpj_xforce.py`
- Input: `usr.danielanzures.io_ntpj_metric`
- Default target table: `usr.danielanzures.io_ntpj_xforce_metric`

## Why this metric exists

1. **Output-table parity** — legacy emits standalone `metric = 'ntpj_xforce'`
   rows the new pipeline previously produced nowhere.
2. **It gates `improved_benchmarks`** — legacy `improved_benchmark_final` is
   driven by `ntpj_xforces`, so a benchmark unit for an `(xforce, month)` with
   no `ntpj_xforce` row that month is dropped. See
   [improved_benchmarks.md](improved_benchmarks.md).

## Team coverage

Every team present in `io_ntpj_metric` — **Core / Fraud / Content**. Social
Media has no NTPJ rows (no shuffle/OOS jobs), so no SM `ntpj_xforce` is
produced. `team` is part of the group key (an XForce maps to a single team, so
this never splits a row).

## Input

The finished agent-grain `io_ntpj_metric` (**not** a raw table). Only
`metric == 'ntpj'` rows at `week` / `month` granularity are consumed; anything
else is ignored.

## Derivation

1. Filter the input to `metric = 'ntpj'` and `date_granularity` in
   `{week, month}`.
2. Group by `(team, xforce, xplead, date_reference, date_granularity)`.
3. `numerator = COUNT(DISTINCT agent WHERE on-target)` (team-aware rule above);
   `denominator = COUNT(DISTINCT agent)`.
4. `metric_value = numerator / denominator * 100` (NULL when denominator is 0 —
   kept for safety, cannot happen for a group that exists).

An agent contributes to the denominator iff it has an `io_ntpj_metric` row for
that `(xforce, period)` — i.e. a finished, required-activity, active-roster job.

## Deferred / inherited (NOT applied here)

Nothing new is applied at this grain — the agent-level NTPJ already applied
every outage / hardcode / manual-adjustment exclusion, and this roll-up inherits
them.

## Output schema (one row per XForce per week/month)

| column | type | notes |
|--------|------|-------|
| `agent` | STRING | always NULL |
| `xforce` | STRING | the XForce |
| `xplead` | STRING | the XForce's XPLead |
| `team` | STRING | `core` / `fraud` / `content` |
| `squad` | STRING | always NULL |
| `district` | STRING | always NULL |
| `shift` | STRING | always NULL |
| `date_reference` | DATE | Monday of the week / first of the month |
| `date_granularity` | STRING | `week` / `month` only |
| `metric` | STRING | always `ntpj_xforce` |
| `numerator` | DOUBLE | distinct on-target agents |
| `denominator` | DOUBLE | distinct agents |
| `metric_value` | DOUBLE | `numerator / denominator * 100` (NULL if denominator 0) |
