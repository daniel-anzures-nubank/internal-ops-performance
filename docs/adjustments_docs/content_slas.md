# `Content - SLAs` — Content OOS SLA map

**Sheet tab:** `Content - SLAs` → synced to `usr.danielanzures.adj_content_slas`
(slug `content_slas`). **Config tab** (a static map), not date/agent windows like
the other adjustment tabs.

## What it is

A map of Content OOS `job_type` → **OLD-SLA threshold in seconds**. It is the sole
source of the SLA thresholds for **Content NTPJ**, which is an SLA-weighted
compliance metric ("jobs within SLA"), not the duration ratio Core/Fraud use. There
is **no hardcoded fallback** — `metrics_data/jobs_within_sla.py::parse_sla_map`
raises if the table is missing/empty, so a mis-synced tab fails the build loudly
rather than silently under-crediting.

## Columns

| column (sheet) | slug | type | notes |
| --- | --- | --- | --- |
| `Job Type` | `job_type` | string | normalized OOS job classification (lower, `_`-joined) |
| `SLA Seconds` | `sla_seconds` | int (stored as string) | OLD-SLA threshold in seconds |

## How it is used

`build_jobs_within_sla` reads it via `read_adjustment_table(spark, "content_slas")`
→ `parse_sla_map` (lowercases `job_type`, casts `sla_seconds` to long, drops
null/≤0, dedups). `compute_jobs_within_sla` **INNER JOIN**s each Content OOS job to
this map — so a job type absent from the map is **dropped** (legacy's no-SLA types:
`mastery_cx`, `sop`, generic `projects`, stray Core/Fraud OOS). A job then earns its
full `sla_seconds` if delivered on time (`actual <= sla`), else 0.

## Affects

- **NTPJ (Content)** — `io_ntpj_metric` Content rows (`metric='ntpj'`).
- **Xpeer Index (Content)** — folds Content NTPJ **raw** (higher-is-better).
- **ntpj_xforce (Content)** — on-target = `ntpj >= 95` for Content.

## Validation note

The `sync_adjustments` validator (`download_adjustments.check_tab`) is built for
window-based tabs (Fecha/Hora/Estatus/Equipo/Agente). A config tab with none of
those columns passes untouched — so a typo'd `job_type` or non-numeric seconds
syncs silently and is caught downstream (INNER-JOIN drop / `parse_sla_map` filter).
Keep the tab clean; the build asserts the parsed map is non-empty.
