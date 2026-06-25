# xpeer_index

The agent-level **Xpeer Index** performance metric (legacy `index_agent`). One
row per agent per **day / week / month / quarter / semester / year**.

The Index folds an agent's other metrics into a single comparable score:

> `Xpeer Index = mean(included component %s)`. **Target ≥ 95%.**

Applies to **all four teams** (Core, Fraud, Social Media, Content), each with its
own component roster. See `docs/metrics_definitions.md`.

- Module: `metrics/xpeer_index.py`
- Build script: `scripts/metrics_scripts/build_xpeer_index.py`
- Default target table: `usr.danielanzures.io_xpeer_index_metric`

## Inputs — the per-agent metric tables (not a raw table)

Unlike every other metric module, the Index reads the **already-aggregated**
`io_*_metric` tables and combines their `metric_value`s per
`(agent, date_reference, date_granularity)`:

| input table | component | used by |
|-------------|-----------|---------|
| `io_adherence_metric` | Adherence (**driver**) | all teams |
| `io_ntpj_metric` | NTPJ | Core / Fraud / Content |
| `io_normalized_occupancy_metric` | NO | all teams (from March 2026) |
| `io_quality_metric` | Quality | Core / Fraud / Social Media |
| `io_tnps_metric` | Human tNPS | Social Media |
| `io_wows_metric` | WoWs | Social Media |
| `io_content_csat_metric` | CSAT (the Content "quality" term) | Content |

Adherence is the **driver**: an agent appears in the Index for a bucket iff it
has an Adherence row there. Dimensions and `team` are taken from Adherence.

## Component transforms (legacy `index_agents_final`)

- **Adherence**: `COALESCE(0)`, as-is.
- **NTPJ** (lower-is-better, folded around 100): `≤100 → 100`;
  `100 < x ≤ 200 → 200 − x`; `>200` or missing `→ 0`.
- **NO** (truncated): `≥100 → 100`; `<100 → x`; missing `→ 0`.
- **WoWs** (count → 0-100, target 5/month): `≥5 → 100`; `<5 → x/5*100`;
  missing `→ 0`.
- **tNPS / Quality / CSAT**: used **raw** (tNPS may be negative).

## Composition — which terms enter the mean, by team and era

The component roster grew over the 2026 rollout, so it is **anchored on the
bucket's month**:

| team | always | `+ Quality / CSAT` | `+ NO` | other |
|------|--------|--------------------|--------|-------|
| Core / Fraud | Adherence, NTPJ | from **Feb 2026** (if present) | from **March 2026** | — |
| Content | Adherence, NTPJ | CSAT from **March 2026** (if present) | from **March 2026** | — |
| Social Media | Adherence, WoWs | from **Feb 2026** (if present) | from **March 2026** | `+ tNPS` whenever present |

Quality / CSAT / tNPS terms drop out of **both** the sum and the divisor when
the agent has no value for the bucket. Once NO's era starts it is always counted
in the divisor (a missing NO contributes 0). An unrecognized `team` falls back
to the Core / Fraud composition.

### Era anchoring across granularities

`day` / `week` / `month` buckets sit inside one calendar month, so the era is
that month. `quarter` / `semester` / `year` buckets straddle the cutovers, so
they anchor on the bucket's **last month** (its end) — a longer aggregation
therefore includes every component active by the end of the period. Buckets
ending before 2026 are dropped (the Index is a 2026 construct).

## Output convention

To keep the shared `metric_value = numerator / denominator * 100` contract:

- `numerator` = the **sum of the included component %s**,
- `denominator` = `n_components * 100`,
- `metric_value` = the Index % = `numerator / denominator * 100` (their mean).

## Deferred to the future Adjustments layer (NOT applied here)

- Per-agent legacy carve-outs (e.g. the `nitza.zarza` 2026-04 / 2026-05 NO
  suppression). The Index is otherwise team-generic.

## Output schema (one row per agent per period)

| column | type | notes |
|--------|------|-------|
| `agent` | STRING | |
| `xforce` | STRING | from the Adherence row |
| `xplead` | STRING | from the Adherence row |
| `team` | STRING | `core` / `fraud` / `social media` / `content` |
| `squad` | STRING | from the Adherence row |
| `district` | STRING | from the Adherence row |
| `shift` | STRING | from the Adherence row (NULL for content) |
| `date_reference` | DATE | bucket start (day / Monday / first-of-month/quarter/year / Jan 1 or Jul 1) |
| `date_granularity` | STRING | `day` / `week` / `month` / `quarter` / `semester` / `year` |
| `metric` | STRING | always `xpeer_index` |
| `numerator` | DOUBLE | sum of included component %s |
| `denominator` | DOUBLE | `n_components * 100` |
| `metric_value` | DOUBLE | Index % = `numerator / denominator * 100` |
