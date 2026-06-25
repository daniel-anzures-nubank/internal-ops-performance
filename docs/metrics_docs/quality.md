# quality

The **Quality (QA)** performance metric. One row per agent per
**day / week / month / quarter / semester / year**.

Quality is the simple average of an agent's QA evaluation scores for the period:

> `quality = SUM(qa_score) / COUNT(evaluations)` (mean of the scores).
> **Target ≥ 95%.**

Scores are already 0-100, so `metric_value` is the mean directly (`scale = 1`):
`numerator = SUM(qa_score)`, `denominator = # of evaluations`.

Applies to **Core, Fraud, Social Media**. Core/Fraud score from **Playvox** QA;
**Social Media** scores from **Playvox + Sprinklr SM** (the Sprinklr feed is
added from `2026-05-01`). **Content is excluded** — its quality of record is the
separate [Quality (CSAT)](content_csat) metric. See `docs/metrics_definitions.md`.

- Module: `metrics/quality.py`
- Build script: `scripts/metrics_scripts/build_quality.py`
- Input: `usr.danielanzures.io_quality_evaluations_raw`
- Default target table: `usr.danielanzures.io_quality_metric`

## Input

The `io_quality_evaluations_raw` table (`metrics_data/quality_evaluations.py`),
one row per evaluation (Playvox + Sprinklr SM), carrying `evaluation_id`,
`source` and `qa_score`.

## Steps applied here (deferred by the raw layer)

1. **Latest record per `evaluation_id`** — legacy `ROW_NUMBER() OVER (PARTITION
   BY evaluation_id ORDER BY created_at DESC)`, keep rn=1. The raw table only
   carries day-grain `date` (not the original `created_at`), so we keep the row
   with the latest `date` per `evaluation_id`.
   > **Caveat:** same-day re-scores can't be ordered finer than the day. If exact
   > parity is needed, thread `created_at`/`updated_at` through the raw table.
2. **Drop Content** (`team == 'content'`).
3. Drop rows with a null `qa_score`.

## Sources (Playvox + Sprinklr SM)

Quality unions two feeds in the raw layer, tagged by a `source` column:

- **Playvox** (`qmo_playvox_consolidated`) — Core / Fraud / Content, and
  historically Social Media.
- **Sprinklr SM** (`social_media_case_summary_information`) — Social-Media case
  QA, from `2026-05-01` onward.

Legacy carried the Sprinklr `UNION ALL` only in the Core/Fraud dataset, where it
was **dead code** — the active-roster join excluded `social`, so none ever
reached `usr.mx__cx.quality_io`. The new pipeline keeps social agents in the
roster, so the Sprinklr SM rows now actually score SM Quality. Both feeds are on
the same 0-100 scale and are averaged together here regardless of `source`
(additive — a social agent with both Playvox and Sprinklr evaluations
contributes both).

## Deferred to the future Adjustments layer (NOT applied here)

- The `scorecard_id` / `evaluation_id` blacklists.
- Outage-date exclusions (2026-03-27, 2026-04-09).

## Output schema (one row per agent per period)

| column | type | notes |
|--------|------|-------|
| `agent` | STRING | |
| `xforce` | STRING | most-recent in bucket |
| `xplead` | STRING | most-recent in bucket |
| `team` | STRING | `core` / `fraud` / `social media` |
| `squad` | STRING | most-recent in bucket |
| `district` | STRING | most-recent in bucket |
| `shift` | STRING | most-recent in bucket |
| `date_reference` | DATE | bucket start (day / Monday / first-of-month/quarter/year / Jan 1 or Jul 1) |
| `date_granularity` | STRING | `day` / `week` / `month` / `quarter` / `semester` / `year` |
| `metric` | STRING | always `quality` |
| `numerator` | DOUBLE | sum of `qa_score` |
| `denominator` | DOUBLE | number of evaluations |
| `metric_value` | DOUBLE | mean `qa_score` (0-100; NULL if no evaluations) |
