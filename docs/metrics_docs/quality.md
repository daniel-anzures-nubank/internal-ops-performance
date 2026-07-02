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

1. **Team-scoped blacklists** (date < 2026-07-01): Core/Fraud drop
   `scorecard_id` / `evaluation_id` in the legacy blacklists; Social Media
   drops only its single blacklisted `scorecard_id`. Cutover-gated — lifted
   from 2026-07-01 onward.
2. **Latest record per `(source, evaluation_id)`** by `created_at DESC` —
   legacy `ROW_NUMBER() OVER (PARTITION BY evaluation_id ORDER BY
   local_mx_evaluation__created_at DESC)`, keep rn=1. Deduped within each
   source (legacy dedups inside each single-source notebook); the Playvox and
   Sprinklr id spaces are disjoint, so cross-source dedup is a no-op.
3. **Drop Content** (`team == 'content'`).
4. Drop rows with a null `qa_score`.

**No date drops.** No outage-date exclusion is applied: the 2026-06-30 legacy
re-export re-included the 2026-03-27 / 2026-04-09 quality rows (the published
`usr.mx__cx.quality_io` and `usr.danielanzures.sm_temp_quality` both carry
those dates), so current legacy drops no quality dates and neither do we. An
earlier revision dropped them (ported from a pre-re-export legacy snapshot);
reverted for parity.

## Sources (Playvox + Sprinklr SM — a union, like legacy)

Quality unions two feeds in the raw layer, tagged by a `source` column,
matching legacy's SM deck `qa_base` (`[IO] Performance 2026 - Social Media
Temp Fix.sql`, lines 2988-3028):

- **Playvox** (`qmo_playvox_consolidated`) — Core / Fraud / Content / Social
  Media. **No upper date bound** (legacy's Playvox branch has no SM/May cutoff;
  SM Playvox evaluations keep flowing until they naturally end after
  2026-05-15).
- **Sprinklr SM** (`social_media_case_summary_information`) — Social-Media case
  QA, **floored at `2026-05-01`** (legacy `sm.report_date >= "2026-05-01"`,
  line 3025).

Both feeds are on the same 0-100 scale and are averaged together here
regardless of `source` (additive — in early May a social agent with both
Playvox and Sprinklr evaluations contributes both, exactly like legacy). The
Core/Fraud Quality dataset also carries the Sprinklr `UNION ALL`, but there it
is **dead code** — its active-roster join excludes `social`, so none ever
reached `usr.mx__cx.quality_io`; the SM deck's union is the live one.

## Applied blacklists / no date drops

- The team-scoped `scorecard_id` / `evaluation_id` blacklists ARE applied here
  (hardcoded, cutover-gated to date < 2026-07-01) — see step 1 above.
- **No outage-date exclusions.** The 2026-06-30 legacy re-export re-included
  the 2026-03-27 / 2026-04-09 quality rows, so no quality dates are dropped.

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
