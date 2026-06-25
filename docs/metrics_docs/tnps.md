# tnps

The **Human tNPS** performance metric (Social Media only). One row per agent per
**day / week / month / quarter / semester / year**.

> `tnps = (promoters − detractors) / valid_responses * 100`. **Target ≥ 88%.**

`metric_value` is on the NPS scale and can be **negative** (more detractors than
promoters). Only **Social Media** has tNPS — the source only contains surveys for
cases handled by a human social agent (see `docs/metrics_definitions.md`).

- Module: `metrics/tnps.py`
- Build script: `scripts/metrics_scripts/build_tnps.py`
- Input: `usr.danielanzures.io_tnps_responses_raw`
- Default target table: `usr.danielanzures.io_tnps_metric`

## Input

The `io_tnps_responses_raw` table (`metrics_data/tnps_responses.py`), one row per
survey response, carrying the raw 0–10 `survey_score`, the `case_number`, the
`survey_response_date`, and the closure `date` + dimensions. Classification and
the validity rules are deferred to this metric layer.

## Filters / rules applied here (deferred by the raw layer)

- **Team scope** — keep `team = 'social media'` (defensive; the source is already
  social-only).
- **Validity window** — keep responses where
  `survey_response_date <= date + 1 day` (legacy
  `survey_response_date <= case_closure_time + INTERVAL 1 DAY`). A NULL
  `survey_response_date` falls outside the window.
- **One response per case** — dedup to a single row per `(agent, case_number)`
  (legacy `COUNT(DISTINCT case_number)`), preferring a scored row and the latest
  `survey_response_date`.
- **Classification** — promoter `>= 9` (+1), detractor `<= 6` (−1), neutral 7–8
  (0). A response is *valid* (counts in the denominator) when `survey_score` is
  not null.

## Derivation

1. Apply the team scope + validity window.
2. Dedup to one response per `(agent, case_number)`.
3. Per response: `net_flag = promoter − detractor` (∈ {−1, 0, +1}),
   `valid_flag = 1` if scored else 0.
4. Bucket each `date` to the granularity (`day` → date, `week` → Monday,
   `month`/`quarter`/`year` → first of period, `semester` → Jan 1 or Jul 1).
5. Per `(agent, date_reference)`: `numerator = SUM(net_flag)`,
   `denominator = SUM(valid_flag)`. (A case closes on a single day, so summing
   the per-response flags equals the legacy distinct-case counts.)
6. Dimension/hierarchy fields take their most-recent value within the bucket.
7. `metric_value = numerator / denominator * 100` (NULL when denominator is 0).

## Deferred to the future Adjustments layer (NOT applied here)

- The outage-date exclusion `date = 2026-03-27` (legacy drops it for "general
  access problems") — deferred to match the other metric modules.

## Output schema (one row per agent per period)

| column | type | notes |
|--------|------|-------|
| `agent` | STRING | |
| `xforce` | STRING | most-recent in bucket |
| `xplead` | STRING | most-recent in bucket |
| `team` | STRING | always `social media` |
| `squad` | STRING | most-recent in bucket |
| `district` | STRING | most-recent in bucket |
| `shift` | STRING | most-recent in bucket |
| `date_reference` | DATE | bucket start (day / Monday / first-of-month/quarter/year / Jan 1 or Jul 1) |
| `date_granularity` | STRING | `day` / `week` / `month` / `quarter` / `semester` / `year` |
| `metric` | STRING | always `tnps` |
| `numerator` | DOUBLE | promoters − detractors (can be negative) |
| `denominator` | DOUBLE | valid responses |
| `metric_value` | DOUBLE | `numerator / denominator * 100` (NPS %; NULL if denominator 0) |
