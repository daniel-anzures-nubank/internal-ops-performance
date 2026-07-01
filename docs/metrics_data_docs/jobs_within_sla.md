# `jobs_within_sla` — Content OOS jobs vs their SLA (raw)

One row per **Content OOS "job"** with its SLA threshold and an on-time flag. This
is the raw substrate for **Content NTPJ**, which — unlike Core/Fraud NTPJ (duration
`actual/expected`, lower-is-better) — is a **jobs-within-SLA compliance** metric
(higher-is-better, bounded ≤100). Legacy calls it `ntpj_sla_old`
(`[IO] Performance 2026 - Content Temp Fix.sql` L2006-2200) but ships it under
`metric='ntpj_agent'` for standardization.

- **Module:** `metrics_data/jobs_within_sla.py` (`compute_jobs_within_sla`, `parse_sla_map`)
- **Build script:** `scripts/metrics_data_scripts/build_jobs_within_sla.py`
- **Default target:** `usr.danielanzures.io_jobs_within_sla_raw`
- **Metric built from it:** `metrics/content_sla_ntpj.py` → unioned into `io_ntpj_metric`

## Source tables

| extractor | underlying table | role |
| --- | --- | --- |
| `oos_jobs` | `etl.mx__dataset.taskmaster_consolidated_registry` | one row per OOS job; provides `job_classification`, `net_time_spent_seconds`, `comment`, `ticket__id` |
| `agent_information` | roster (Content = the Google-Sheet roster) | scope to Content agents + attach the 7 shared dims + `roster_status` |
| `adj_content_slas` | the **"Content - SLAs"** sheet tab | the OLD-SLA seconds per job type (see `docs/adjustments_docs/content_slas.md`) |

## Derivation

1. **Normalize** `job_classification` unconditionally (legacy `oos_jobs_ntpj` L958):
   `LOWER(REPLACE(TRIM(REPLACE(job_classification, '(OOS_CONT)', '')), ' ', '_'))`.
2. **Parse `content_id`** via the MOS-ticket regex over `comment` **and** `ticket__id`
   (COALESCE; legacy L965-984). Requires 3+ digits (`\d{3,}`).
3. **Job grain:** `macros` / `faq` / `ar` → one job = one source row; every other type
   → one job = one distinct `content_id`, `actual_seconds = SUM(net_time_spent_seconds)`.
4. **SLA INNER JOIN** to `adj_content_slas` on the normalized `job_type` → `sla_seconds`;
   **drops job types with no Content SLA** (legacy `mastery_cx`, `sop`, generic `projects`,
   stray Core/Fraud OOS types).
5. **On-time (all-or-nothing):** `within_sla = actual_seconds <= sla_seconds`;
   `sla_met_seconds = sla_seconds if within_sla else 0`.
6. **Roster join** (Content only, `team='content'`) on `(agent, snapshot_month)`, with the
   `jobs_raw` dedup to prevent content-agent fan-out.

## Filters applied here (intrinsic to the legacy source view)

- Scoped to **Content agents** (roster `team='content'` — legacy `content_agents`).
- **Date scoping** `date >= 2025-12-01`, dropping `2026-03-10 / 2026-03-27 / 2026-04-09`,
  is applied **before the `content_id` grouping** so a `content_id` straddling the
  `2026-03-10` boundary is truncated exactly as legacy's source-level drop (L986), not
  kept whole.

## Deferred to the metric layer (`content_sla_ntpj.py`)

- `roster_status == 'active'` scoping (carried here, applied there — matching `jobs_raw`).
- Aggregation to the SLA-weighted compliance % and the tidy long roll-ups.

## Output schema

| column | type | notes |
| --- | --- | --- |
| `agent, xforce, xplead, team, squad, district, shift` | STRING | roster dims (`team='content'`) |
| `roster_status` | STRING | `'active'`/… — applied in the metric layer |
| `date` | DATE | job day (content jobs: `DATE(MIN(local_start_date))`) |
| `job_type` | STRING | normalized `job_classification` |
| `content_id` | STRING | MOS ticket (NULL for `macros`/`faq`/`ar`) |
| `actual_seconds` | BIGINT | seconds worked on the job |
| `sla_seconds` | BIGINT | OLD-SLA threshold (from the sheet map) |
| `within_sla` | INT | 1 if `actual_seconds <= sla_seconds` else 0 |
| `sla_met_seconds` | BIGINT | `sla_seconds` if on-time else 0 (all-or-nothing credit) |
