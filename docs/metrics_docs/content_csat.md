# content_csat

The **Content Quality (CSAT)** performance metric (Content only). One row per
agent per **day / week / month / quarter / semester / year**.

> `content_csat = SUM(promoters) / SUM(number_of_questions) * 100`, where a
> *promoter* is any of the 8 survey questions answered `>= 4` (1–5 scale).
> **Target ≥ 95%.**

This is **Content's** quality component — Core / Fraud / Social Media use the
Playvox [`quality`](quality.md) metric instead (see
`docs/metrics_definitions.md`).

- Module: `metrics/content_csat_metric.py` (named `content_csat_metric` to avoid
  colliding with the raw module `metrics_data/content_csat.py`; the metric itself
  is `content_csat`)
- Build script: `scripts/metrics_scripts/build_content_csat.py`
- Input: `usr.danielanzures.io_content_csat_raw`
- Default target table: `usr.danielanzures.io_content_csat_metric`

## Input

The `io_content_csat_raw` table (`metrics_data/content_csat.py`), one row per CSAT
survey response × content agent, already carrying `promoters` (the count of the 8
questions scored `>= 4`) and `number_of_questions` (8) plus the dimensions.

Each survey response rates how a `target_squad` was supported that month and is
credited to **every** active content agent serving that squad — that fan-out
happens in the raw layer, so this metric just sums per agent.

## Filters / rules applied here (deferred by the raw layer)

- **Team scope** — keep `team = 'content'` (defensive; the raw table is
  content-only).
- **Aggregation** — `SUM(promoters) / SUM(number_of_questions)` per
  `(agent, period)`.

## Derivation

1. Apply the team scope.
2. Bucket each `date` (the month rated) to the granularity (`day` → date,
   `week` → Monday, `month`/`quarter`/`year` → first of period, `semester` → Jan
   1 or Jul 1).
3. Per `(agent, date_reference)`: `numerator = SUM(promoters)`,
   `denominator = SUM(number_of_questions)`.
4. Dimension/hierarchy fields take their most-recent value within the bucket.
5. `metric_value = numerator / denominator * 100` (NULL when denominator is 0).

## Deferred to the future Adjustments layer (NOT applied here)

- Any per-agent manual adjustments / outage-date carve-outs.

## Output schema (one row per agent per period)

| column | type | notes |
|--------|------|-------|
| `agent` | STRING | |
| `xforce` | STRING | most-recent in bucket |
| `xplead` | STRING | most-recent in bucket |
| `team` | STRING | always `content` |
| `squad` | STRING | content roster squad (e.g. `enablement`) |
| `district` | STRING | content roster district (`content`) |
| `shift` | STRING | NULL for content |
| `date_reference` | DATE | bucket start (day / Monday / first-of-month/quarter/year / Jan 1 or Jul 1) |
| `date_granularity` | STRING | `day` / `week` / `month` / `quarter` / `semester` / `year` |
| `metric` | STRING | always `content_csat` |
| `numerator` | DOUBLE | promoter answers |
| `denominator` | DOUBLE | total questions (multiple of 8) |
| `metric_value` | DOUBLE | `numerator / denominator * 100` (percentage; NULL if denominator 0) |
