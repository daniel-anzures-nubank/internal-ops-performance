# adjustments — manual adjustments layer

The **Adjustments layer** applies the manual, per-agent / per-date carve-outs
that the raw (`metrics_data/`) and metric (`metrics/`) layers intentionally
**defer**. Raw tables stay faithful to the source systems and the metric layer
applies only systematic business rules; anything that depends on a human
decision (this agent was on maternity leave, this squad gave cross-support that
month, these dates were an outage) lives here instead, so the rest of the
pipeline stays deterministic and reproducible.

> **Status: implemented for the current approved rows.** Scalable sheet-backed
> adjustments are handled by `adjustments/manual.py` and passed into the affected
> build scripts. `Ajustes Index` and `Correcciones Generales Datos` remain
> intentionally explicit hardcoded exceptions documented separately.

## Source of truth

Most manual adjustments are sourced from a single Google Sheet — this is the
**source of truth for sheet-backed adjustments**:

<https://docs.google.com/spreadsheets/d/1Y5P6LijLxT6hFTd69DiSPBTUPKHO-m_6zzrs-PmOjfU/edit?gid=720896495#gid=720896495>

- Spreadsheet id: `1Y5P6LijLxT6hFTd69DiSPBTUPKHO-m_6zzrs-PmOjfU`
- One tab per adjustment type (see the catalog below). The `Guía` tab is
  documentation (tab → description → affected metrics), not data.
- The sheet must be **shared (Viewer) with the `nu-mx-internal-ops` service
  account** (its `client_email`; address form
  `<name>@<project>.iam.gserviceaccount.com`); read it programmatically with
  `gsheets.py` (repo root). On Databricks the SA key comes from the
  `nu-mx-internal-ops-sa-secret` secret scope (one secret key per JSON field,
  wired to `GOOGLE_SA_*` env vars on the job cluster); locally, from a `.env`
  (see `gsheets.py`).
- Missing DIME slots are the exception: they live in the repo at
  `adjustments/slots_faltantes_dime.csv` because they are append-style source
  rows, not reviewer-maintained adjustment windows.

## Downloading the sheet locally

`scripts/adjustments_scripts/download_adjustments.py` downloads every tab
(except `Guía`) to one CSV each under `adjustments/data/` (gitignored). For tabs
with `Estatus`, it exports only rows where `Estatus = Aprobado`; tabs without
`Estatus` are exported unchanged. It then runs per-tab sanity checks on the
exported rows — `Fecha *` columns must be valid `YYYY-MM-DD` dates, open-ended
`Fecha Fin` values use the sentinel `9000-01-01`, `Hora *` columns must be
`HH:MM` values, and full-day windows use `00:00` to `23:59`. Date/hour ranges
must be ordered, `Estatus` / `Equipo` values must be known, duplicates are
reported. Format violations are **errors** (exit code 1); the rest are warnings.

```bash
uv run --group sheets python scripts/adjustments_scripts/download_adjustments.py
```

## Sheet tabs

| tab | contents | grain |
|-----|----------|-------|
| `Guía` | documentation: description + affected metrics per tab | one row per tab |
| `Exclusiones Generales` | manual exclusions per team/agent and date-time range (general outages, audits, special permissions) | one row per exclusion window |
| `Inconsistencias DIME` | DIME slots with a wrong label, to be reclassified to the `Etiqueta Correcta` (e.g. `shrinkage` for vacations/licenses); requires evidence + approval | one row per incident |
| `Cross Support` | agents temporarily supporting another squad; the listed queues are excluded from their NTPJ / benchmarks (`Fecha Fin = 9000-01-01` for open-ended windows) | one row per agent × support window |
| `Training` | per-agent training windows (date + `HH:MM`–`HH:MM`; full-day windows use `00:00`–`23:59`) | one row per agent × window |
| `Shadowing` | per-agent shadowing windows, same shape as `Training` | one row per agent × window |
| `Exclusiones Jobs` | individual Shuffle/Taskmaster jobs to exclude (outliers, anomalous records) by agent (or squad), job classification, date range and optionally customer id | one row per exclusion |
| `Ajustes Index` | index components excluded for an agent or XPLead over a period; the index is recomputed with the remaining components | one row per carve-out |
| `Correcciones Generales Datos` | raw-data corrections (not exclusions), e.g. timestamps shifted because an agent's laptop clock was behind; the data is fixed before metrics are computed | one row per correction window |
| `Content - SLAs` | **config map** (not date/agent windows): Content OOS `job_type` → OLD-SLA seconds. Drives Content NTPJ (jobs-within-SLA compliance). See [content_slas.md](content_slas.md). | one row per job type |

## File-backed adjustments

| file | contents | grain |
|------|----------|-------|
| `adjustments/slots_faltantes_dime.csv` | missing DIME slots appended to the DIME slot universe; combines `usr.danielanzures.h1_missing_dime_slots` (real scheduled rows only) and the Content temp-fix table `usr.danielanzures.missing_agents_dime_slots_content_h1` | one row per missing DIME slot |

Shared columns across the data tabs: `Equipo` (comma-separated teams or
`Todos`), `Agente`, `Fecha Inicio` / `Fecha Fin`, `Hora Inicio` / `Hora Fin`
(where applicable), `Estatus` (`Aprobado` / `Pendiente` / `Denegado`), and
`Persona que Aprueba/Denega`.

## Where adjustments apply

Adjustments are applied in the **metrics layer** whenever possible (after the
raw tables, before / within the finished metric). Two source-universe exceptions
are applied while building raw inputs: file-backed missing DIME slots are
appended to the DIME slot input for Adherence / NOCC / NTPJ, and DIME
reclassifications are passed into `jobs_raw` so NTPJ's
`required_activity_on_day_flag` uses the corrected labels.

## Catalog of planned adjustments

Compiled from the "deferred to the Adjustments layer" notes across the existing
modules (`metrics/adherence.py`, `ntpj.py`, `shrinkage.py`, `improved_benchmarks.py`,
`xpeer_index.py`). Each will get its own `docs/adjustments_docs/<name>.md` and a
compute module when implemented.

| adjustment | sheet tab | what it does | metrics affected | status |
|------------|-----------|--------------|------------------|--------|
| cross-support exclusions | `Cross Support` | Exclude listed queues an agent worked **for another squad** so those jobs don't count toward that agent's — or their XForce's — NTPJ / Improved Benchmark universe. | `ntpj`, `improved_benchmarks` | implemented in `adjustments/manual.py` |
| DIME reclassifications | `Inconsistencias DIME` | Reclassify mislabeled DIME slots to their correct activity type. Vacations and licenses (incl. maternity) dimensioned as work are reclassified to `shrinkage`, matching legacy (they count as shrinkage and stay in the required-slot base). | `adherence`, `shrinkage`, `normalized_occupancy`, `ntpj` | implemented in `adjustments/manual.py` |
| training & shadowing exclusions | `Training`, `Shadowing` | Drop slots during **training / shadowing** windows from Shrinkage. | `shrinkage` | implemented in `adjustments/manual.py` |
| general exclusions (outages etc.) | `Exclusiones Generales` | Carve out specific **date/time windows** (per agent / team / global, e.g. the 2026-03-27 general outage) from the calculations. | `adherence`, `ntpj`, `normalized_occupancy`, `shrinkage`, `improved_benchmarks` | implemented in `adjustments/manual.py` |
| job-level exclusions | `Exclusiones Jobs` | Drop individual **Shuffle/Taskmaster jobs** (outliers like tasks left open for hours, anomalous carrier-report records) from the job universe. | `ntpj`, `improved_benchmarks` | implemented in `adjustments/manual.py` |
| index component carve-outs | [`Ajustes Index`](ajustes_index.md) | Exclude an **index component** for an agent (e.g. `nitza.zarza` NO, Apr–May 2026) or an XPLead (e.g. `david.fernandez` Improved Benchmarks, Apr 2026); the index is recomputed over the remaining components. | `xpeer_index`, `xforce_index`, `average_xforce_index`, `normalized_occupancy` | implemented for current approved rows |
| Content SLA map | [`Content - SLAs`](content_slas.md) | Per-job-type OLD-SLA thresholds that define **Content NTPJ** (SLA-weighted compliance, not duration). INNER-JOINed in `jobs_within_sla`; no-SLA job types are dropped. Mandatory (no hardcoded fallback). | `ntpj` (Content), `xpeer_index` (Content), `ntpj_xforce` (Content) | implemented in `metrics_data/jobs_within_sla.py` |
| DIME-squad business exclusions | — (code-side) | Exclude certain **DIME squads** (`wfm` / `enablement` / …) from the productivity-based metrics. | `adherence`, `shrinkage` | planned |
| missing DIME slots | `adjustments/slots_faltantes_dime.csv` | Append manually recovered DIME slots that are absent from the ETL DIME table. | `adherence`, `ntpj`, `normalized_occupancy` | implemented in raw build scripts |
| data corrections | [`Correcciones Generales Datos`](correcciones_generales_datos.md) | Fix raw data before computing metrics (e.g. `luis.contreras` Taskmaster job timestamps shifted +2 h / +1 h because his laptop clock was behind, Jan–May 2026). | `normalized_occupancy` (Content) | implemented for current approved rows |

## Legacy mapping

All manual adjustments hardcoded in the legacy notebooks (`legacy/*.sql`) were
inventoried and mapped into the sheet (2026-06-10, re-checked 2026-06-12 after
the legacy update): training/shadowing windows and cross-support queue
exclusions (including the May-2026 credit-cancellation wave and the "Group F"
leftovers — `jonathan.pineda`, `daniel.cano`, `fernanda.ibanez`, `jose.velez`,
`ivette.melendez`, `tania.llamas`), vacation/maternity DIME reclassifications,
the general-outage and CNBV-audit windows, the `david.fernandez`-XPLead DIME
mismatch on 2026-03-10, job-level outlier exclusions (`Exclusiones Jobs`:
`alan.elizalde` carrier reports + the lifecycle-squad Estafeta jobs of April
2026), the per-agent / per-XPLead index carve-outs (`Ajustes Index`), and the
`luis.contreras` laptop-clock timestamp correction (`Correcciones Generales Datos`). The
improved-benchmarks **global** removal from May 2026 is *not* in the sheet — it
is a systematic era rule handled by the metrics layer.

## Conventions (to follow when implementing)

Mirror the rest of the repo:

- Compute modules live in `adjustments/` (pure pandas, one module per adjustment).
- Build scripts live in `scripts/adjustments_scripts/`.
- Unit tests live in `tests/adjustments/`.
- One `docs/adjustments_docs/<name>.md` per adjustment, documenting the source
  tab, the columns it reads, and exactly which metric rows it changes.
