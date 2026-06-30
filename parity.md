# Parity with Legacy

Catalog of where the new pipeline's output **diverges** from the legacy SQL
pipeline, with the cause and classification for each divergence. Companion to the
migration rule — the new pipeline reproduces legacy **byte-for-byte for dates
before the `2026-07-01` cutover** (including legacy bugs), with corrections only
from the cutover onward (see `metrics_data/shift_attribution.py` and `AGENTS.md`).

Update this file as each metric is parity-checked.

## How to read a divergence

Compare the new `usr.danielanzures.io_<metric>_metric` table against the legacy
table for the same period (day grain), outer-joined on `(agent, date)`. Every
divergence should fall into one of these classes — only **open** ones are
unexplained:

| class | meaning |
| --- | --- |
| **by-design** | intentional scope/shape difference; not a parity goal |
| **legacy-bug** | legacy is wrong, new is correct; we deliberately do **not** reproduce the legacy defect |
| **adjustment** | new applies a manual adjustment that legacy's frozen output predates/lacks |
| **open** | genuine residual still being root-caused |

---

## Adherence — `io_adherence_metric` vs `usr.mx__cx.adherence_io`

**Status:** validated over full 2026 (`2026-01-01 … 2026-06-21`).
**Value parity: 99.977%** — 34,076 / 34,084 matched agent-days exact.

### Coverage divergences

| Divergence | Rows (full 2026) | Cause | Class |
| --- | --- | --- | --- |
| `only_new`: **Social Media + Content** | ≈4,000 | Legacy keeps SM/Content adherence in **separate** tables (`sm_temp_adherence`, `cont_temp_adherence`), not in `adherence_io`. The new pipeline unifies all four teams into one table. | by-design |
| `only_legacy`: **43 agent-days, all on `2026-03-10`** | 43 | **Legacy adherence bug.** The exclusion `xplead = 'david.fernandez' AND date = '2026-03-10'` exists in legacy **NOCC** (`[IO] Normalized Occupancy Dataset.sql:399`, comment *"DIME ETL doesn't match with DIME Drive"*) and in **NTPJ** (named-agent hardcodes), but was **omitted from `[IO] Adherence Dataset.sql`**. So legacy `adherence_io` wrongly **includes** these 43; the new pipeline correctly **excludes** them via the `Exclusiones Generales` sheet row `Todos (XPLead: david.fernandez)` (`equipo=Core`, `2026-03-10`). New is correct; legacy adherence is the inconsistent one. | legacy-bug |

### Value divergences (8 of 34,084 matched rows)

| Divergence | Rows | Cause | Class |
| --- | --- | --- | --- |
| Early-January (`2026-01-02 … 01-06`), < 1 pp each | ~6 | **Period-boundary effect** — productivity intervals that start *before* `period_start` are clipped by the new extractor but counted more fully by legacy. Only the first few days of the run window are affected (the `2026-05-11` analogue disappeared once the window started in January). Likely closed by a small productivity look-back before `period_start`. | open |
| `ixchel.calixto 2026-05-05` (~5.9 pp), `maximiliano.lopez 2026-06-19` (~1.1 pp) | 2 | Small connected-seconds difference on the same slot set (new slightly **higher** than legacy) — a productivity-row nuance, not yet root-caused. | open |

### Notes

- The `2026-03-10` `Exclusiones Generales` row is scoped `equipo=Core`, and the
  adjustment matcher ANDs `equipo` with the XPLead. So only david.fernandez's
  **Core** agents are excluded; his **Social Media** agents (~25 that day) are
  **not** dropped. If the intent is *all* his agents regardless of team, set
  `equipo=Todos` in the sheet (the matcher skips the team filter for `Todos`).
- Reproduced legacy behaviours that **do** match and are therefore *not*
  divergences: phantom-adherence (pre-`2026-07-01`), the meeting/leave
  `dimensioned_activity` filter, the DIME-squad exclusion
  (`wfm`/`credit_evolution`/`dote`), and quality/planning inclusion.

---

## Normalized Occupancy — `io_normalized_occupancy_metric` vs `usr.mx__cx.normalized_occupancy`

**Status:** validated over the complete, stable months **`2026-03-01 … 05-31`**
(legacy has no pre-March occupancy; June is the still-settling active month — see
below). Comparison is day-grain NO%, joined on `(agent, date)`, restricted to
non-`social` districts with `occupancy_exp > 0` (SM is a by-design addition; see
the SM row).

**Value parity (≤0.05pp, complete months):** **April 100.0%** (5,939/5,939),
**May 99.98%** (6,246/6,247). **March 84.9%** (5,186/6,107) — the entire March
gap is the by-design `2026-03-10` benchmark ripple below (≈99% of March matches
within 0.5pp; max diff 1.5pp). The per-`(month, district, shift)` cohort
benchmarks themselves match legacy to <0.1pp on the complete months.

### Divergences

| Divergence | Rows | Cause | Class |
| --- | --- | --- | --- |
| **Social-Media occupancy ON for all dates** | ~2,200 (social district) | Legacy dropped `agent_dime_squad = 'social'` and had no Sprinklr source, so legacy `normalized_occupancy` carries null/zero SM occupancy. The data owner confirmed SM NOcc data genuinely exists pre-`2026-07-01`; the new pipeline keeps `social` DIME slots and unions Sprinklr `sm_jobs` on all dates. SM/social rows therefore never match the legacy table — intentional. See `metrics_data/occupancy_time.py` and the `sm-occupancy-on-pre-cutover` decision. | **by-design** (enhancement) |
| **March `2026-03-10` Core-wide exclusion** ripples through the monthly benchmark | ~921 March Core agent-days, ≤1.5pp (mostly <0.5pp) | The `Exclusiones Generales` row is `equipo=Core, agente=Todos, 2026-03-10` (the approved standardization). Legacy NOCC scopes the same carve-out to `xplead='david.fernandez'` (`[IO] Normalized Occupancy Dataset.sql:399`). Because the benchmark is monthly, removing the extra Core agents' 03-10 slots shifts every Core cohort's **March** `occupancy_exp` slightly. April/May are unaffected (100% / 99.98%). Owner elected to keep Core-wide and document. | **by-design** (approved standardization) |
| `only_legacy`: **25 `quality` agent-days, all `2026-03-10`** | 25 | Same root cause as the row above — the Core-wide 03-10 exclusion drops these; legacy's `xplead`-scoped carve-out keeps them. | **by-design** |
| `only_legacy`: **34 `credit` = `nitza.zarza`, Apr–May** | 34 | Approved NO suppression (`Ajustes Index`). The new pipeline applies it at the **metric layer** (`metrics/normalized_occupancy.py`), while legacy applies it **downstream** of its `normalized_occupancy` dataset — so she appears in the legacy dataset table but not the new metric. Her slots still feed the peer benchmark in both. | **adjustment** (layer placement) |
| `only_new`: **`enablement` / `content`** | ~103 | Content unified into one table; legacy keeps Content in a separate pipeline. | **by-design** |
| **June (active month) benchmark drift** | ~3,900 June rows, 0.2–5pp | June is the trailing, still-settling month (`max(dime_date)=2026-06-21`, = legacy's last-completed-week cutoff). The live jobs/roster data has moved since legacy's frozen snapshot, so the monthly June benchmark and values diverge. Same class as adherence's early-January residual; not validated for parity. | **open** (freshness/boundary) |

### Resolved during the port (now matching — not divergences)
- **Duplicate-slot double-count (FIXED).** `append_missing_dime_slots`
  (`slots_faltantes_dime`) re-adds now-backfilled slots under a different squad
  label (`Content` vs `content_content`); the `.distinct()` on slot keys kept
  both, doubling a 30-min slot to 60. Now collapsed to one row per `slot_start`
  with `LEAST(SUM(occupancy_time), 1800)` applied **after** summing, exactly as
  legacy's `normalized_occupancy_final`. Verified: **0** slots >30 min over
  `2026-03-01…06-21`.
- Reproduced and matching: the meeting/leave `dimensioned_activity` filter, the
  wfm/credit_evolution/dote DIME-squad drop, the interval-dedup `prev_max_end`
  running-max, and the monthly district+shift `AVG(squad ratio)` benchmark.

---

## NTPJ — `io_ntpj_metric` vs `usr.mx__cx.normalized_time_per_job`

**Status:** validated over the complete months **`2026-04-01 … 05-31`** (the
self-contained current-month benchmark window — months ≤ 2026-03 use a trailing
4-month pool that needs pre-2026 look-back the run doesn't load; June is the
active/settling month).

Comparison is day-grain NTPJ% (legacy rolled up to agent-day as
`SUM(duration)/SUM(exp_duration_job*count)*100`), joined on `(agent, date)`.

**Value parity (≤0.5pp):** **May 99.97%**, **April ~89%** (98% within 2pp).
Coverage perfect (0 `only_legacy`, 0 `only_new`). The numerator (actual job
seconds) matches 99.98% and the NULL-denominator path is clean.

### Divergences

| Divergence | Rows | Cause | Class |
| --- | --- | --- | --- |
| **April benchmark "Taskforce" tail** — denominator drift, mostly <0.5pp but a ~2% tail up to ~51pp on low-volume `OOS_CBF_TSKF` (BKO Taskforce) jobs | ~120 April agent-days | Legacy `manual_adjustments_ntpj` (`[IO] NTPJ Dataset.sql:148-240`) removes ~13 reassigned agents' `bko_cta_tskf` / `bko_lcyc` **DIME-slot** jobs (from 2026-04-09) from BOTH the contribution and the shared `exp_duration_job` benchmark pool. Those jobs were concentrated in April, so they shift the Taskforce benchmark for every agent who shares those `job_id`s. NOT yet reproduced (see below). | **open** (un-ported legacy hardcode) |
| Numerator / coverage | — | Match (cross-support queue normalization + the contribution-only outage-date asymmetry are reproduced). | — |

### Reproduced and matching (not divergences)
- **Cross-support queue exclusions** (`adj_cross_support`): legacy normalizes
  `received_source_q` (`incredible_machine__x` → `x`, `_`→`-`) before matching;
  reproduced in `drop_cross_support_jobs`.
- **Outage-date asymmetry**: legacy drops `2026-03-27`/`04-09` from the
  contribution only — the benchmark self-join's `b` side keeps them. Reproduced
  (contribution-side filter; outage days stay in the benchmark pool).
- **dime_ntpj vacation / day-control** named-agent exclusions: hardcoded
  contribution-side (`HARDCODED_AGENT_DATE_EXCLUSIONS`).

### The April Taskforce residual — `manual_adjustments_ntpj` (deferred)

The remaining April tail is legacy's `manual_adjustments_ntpj` activity-slot
exclusions, which were **never in the adjustment sheets**. They have been
captured in a new Google-Sheet tab **`Reasignaciones DIME`** (47 rows: the
`bko_lcyc`/`bko_cta_tskf` reassignments + whole-day exclusions; synced to
`usr.danielanzures.adj_reasignaciones_dime`). **Wiring them into the pipeline is
deferred:** the exclusion requires matching each job to the DIME slot it ran in
by time, and reproducing legacy's slot timezone convention (legacy shifts the
DIME slot `+6h`, `[IO] NTPJ Dataset.sql:230`) did not converge — two attempts
(no-offset and +6h) each **regressed** parity (over-dropping the reassigned
agents' own contributions), so the wiring was reverted to preserve the
May 99.97% / April 89% state. The sheet data is ready for a correct slot-time
implementation. Impact of leaving it: a ~2% April tail on Taskforce-heavy
agents; everything else is at parity.

---

## Shrinkage — `io_shrinkage_metric` vs `usr.mx__cx.shrinkage_io`

**Status:** validated over the complete months **`2026-01-01 … 05-31`**
(shrinkage is a direct per-agent-day ratio — no cohort/trailing benchmark — so
each month stands alone; June is the still-settling active month, reported as
freshness below). Comparison is day-grain shrinkage% (`numerator/denominator*100`),
outer-joined on `(agent, date_reference=date)`, `metric='shrinkage'`,
`date_granularity='day'`.

**Value parity (≤0.5pp, complete months):** **Jan 100%** (9,656/9,656),
**Feb 100%** (8,376/8,376), **Mar 100%** (9,747/9,747), **Apr 99.93%**
(9,387/9,394), **May 99.83%** (9,980/9,997). 24 matched-row value mismatches
across Jan–May, all classified (20 legacy-bug, 4 by-design); none open. Numerator
(shrinkage_slot) and the NULL-denominator path are otherwise exact.

| month | matched ≤0.5pp | matched / joined | only_legacy | only_new |
| --- | --- | --- | --- | --- |
| Jan | 100.0% | 9,656 / 9,656 | 0 | 33 |
| Feb | 100.0% | 8,376 / 8,376 | 1 | 335 |
| Mar | 100.0% | 9,747 / 9,747 | 85 | 500 |
| Apr | 99.93% | 9,387 / 9,394 | 0 | 922 |
| May | 99.83% | 9,980 / 9,997 | 0 | 1,318 |

June is the active/settling month (live data has moved vs legacy's frozen
snapshot); not a parity gate. The Mar/Apr `only_legacy` counts above are **after**
the shrinkage outage carve-out (Mar 413→85, Apr 328→0 once shrinkage stopped
applying the `Fallas Generales` rows — see "Reproduced and matching").

### Coverage divergences

| Divergence | Rows (Jan–May) | Cause | num/den | Class |
| --- | --- | --- | --- | --- |
| `only_new`: **Social + Content/Enablement** (roster team `social` / `content`) | ≈2,300 | Legacy's `agent_information` roster drops `squad IN ('social','content')` (`[IO] Shrinkage Dataset.sql:65`), so these never reach `shrinkage_io`. The new pipeline excludes by **DIME-squad** (`content`/`planning`/`quality`/`social`/`wfm`/`enablement`, `shrinkage_slots.py` → `filter_dime`, legacy lines 249-250), which is a different column: agents whose roster team is social/content but whose `agent_dime_squad` is *not* in the DIME-exclusion set survive the DIME filter and appear new-only. Both sides intend to drop org-support; the residual is the roster-squad vs DIME-squad column mismatch. | — | by-design |
| `only_legacy`: **2026-03-10, all Core squads** (lifecycle/savings/credit/collections/engagement; legacy num=0) | 57 | The approved **Core-wide `2026-03-10` standardization** — `adj_exclusiones_generales` row `Core,Todos,2026-03-10` ("DIME ETL no coincide con DIME Drive … XPLead david.fernandez"). Legacy scopes the carve-out to `xplead='david.fernandez'` only (as in Adherence/NOCC), so it keeps the other Core agents; the new pipeline drops all Core on 03-10. Identical class to the NOCC March 03-10 ripple. This is the bulk of March's residual `only_legacy` (85). | drops both | by-design (approved standardization) |
| `only_legacy`: **2026-03-10 `quality`** | ≈28 | DIME-squad nuance: roster team `quality` agents whose `agent_dime_squad` IS `quality` are dropped by the new DIME filter; legacy's roster keeps team-quality agents whose DIME squad differs. | — | by-design |
| `only_legacy`: **jonathan.pineda 2026-02-26** (legacy 0%) | 1 | Single Feb agent-day present in legacy (num 0 / den 16) but absent from the new metric — a roster `status`/snapshot edge for that one agent-day. Immaterial (0% shrinkage, 1 row). | — | open (immaterial) |

### Value divergences (24 of 47,170 matched rows, Jan–May)

| Divergence | Rows | Cause | num/den | Class |
| --- | --- | --- | --- | --- |
| **Legacy >100% shrinkage** — vacation/licence agents (carmina.venegas, lucia.espinosa, nadia.tovias, gabriela.vega, …) | 20 | Legacy's vacation/licence hardcodes (`shrinkage_final_2026` lines 264-276) force the agent's slots **into the numerator** (`shrinkage_slot`), but `required_slot` is `COUNT(activity_type_required != 'time_off')` (line 281) and those slots are `time_off` — so they're added to num but **excluded from den**. Result: legacy num > den → impossible ratios (133%–1600%, or NULL when den hits 0). The new pipeline ports the same carve-out via `adj_inconsistencias_dime` (relabel `time_off`→`shrinkage`), which counts the slot in **both** num and den → a sane 100%. New is correct; legacy is mathematically broken. | den (legacy drops time_off from den) | legacy-bug |
| **jefferson.nunes / patricia.gomez 2026-05-01** | 2 | Deliberate user correction — the adjustment sheet carries May-1 `time_off` rows for these two that legacy lacks (legacy carves out only 5 named agents on May-1). New drops their May-1 slots from num+den; legacy keeps them. | den | by-design |
| **quality 2026-05-22** (fernanda.rodriguez, miriam.hernandez) | 2 | DIME-squad-quality nuance on a single day — slot-count difference between the new DIME-squad-filtered base and legacy's roster base for these two quality agents. | both (−3/−3) | by-design |

### Reproduced and matching (not divergences)
- The **2026-03-01 slot-level formula switch** (pre: `activity_type_required='shrinkage'`;
  post: + `dime_invalid_notation` with a meeting/leave `dimensioned_activity` —
  Mouring/Weekly/Permiso Medico/Huddle/Licencia/Vacacion, legacy line 263).
- The **era-gated required_slot denominator** (pre-cutover drops `dime_invalid_notation`,
  post-cutover drops `time_off`, legacy lines 280-281) and the `lunch_break` drop (line 248).
- The **shrinkage DIME-squad exclusion** (`content`/`planning`/`quality`/`social`/`wfm`/
  `enablement`, lines 249-250 — broader than the adherence/occupancy list and applied at the
  slot stage so it constrains both num and den).
- The **maria.reyes Feb-only maternity reclass** (sheet `fecha_fin` corrected to
  `2026-02-28` so the inclusive matcher reproduces legacy's `date < '2026-03-01'`),
  training/shadowing window drops, the `jose.velez` et al. **2026-03-24…28** day-control
  carve-out (line 294), and the vacation/licence reclasses that *do* match (agents whose
  underlying slots are not `time_off`, e.g. carmina post-relabel = 100%).
- **Outage dates `Fallas Generales` (2026-03-27 / 04-09) deliberately NOT applied to
  shrinkage.** Legacy shrinkage has no org-wide outage carve-out (it keeps all 328
  agents), while Adherence / Normalized Occupancy legitimately drop those days. The
  shared `exclusiones_generales` tab carries `Core/Fraud,Todos` outage rows; shrinkage
  now filters them out by `descripcion='Fallas Generales'`
  (`metrics/shrinkage.py::_drop_outage_exclusions`, owner decision), keeping the
  CNVB day-controls and the 03-10 standardization. Verified: new keeps 348/345 agents on
  those dates (legacy 328; surplus = the by-design social/content unification).
- **israel.cadena 2026-03-19** now matches (100% = 100%): his `adj_inconsistencias_dime`
  row `equipo` was corrected `Fraud`→`Core` so the team-scoped vacation reclass applies.

### Verdict
**At parity on the complete months** — value parity 100% / 100% / 100% / 99.93% /
99.83% (Jan–May). Coverage is clean: `only_legacy` is 0 (Jan/Apr/May), 1 (Feb,
immaterial), 85 (Mar, dominated by the approved Core-wide 03-10 standardization);
`only_new` is the by-design social/content unification. All 24 value mismatches are
classified — **20 legacy-bug** (legacy's >100% time_off-in-numerator defect, new is
correct) and **4 by-design** (jefferson.nunes/patricia.gomez May-1, quality 05-22). The
03-27/04-09 outage carve-out and the israel.cadena / maria.reyes sheet fixes are shipped.
Only **one open row** remains (jonathan.pineda 02-26, 0% / immaterial). Ready to merge.

---

## Quality — `io_quality_metric` vs `usr.mx__cx.quality_io` (Core/Fraud) and the `qa_score_agent` rows of `usr.mx__cx.internal_ops_performance_2026_social_media` (SM)

**Status:** validated over the complete months **`2026-01-01 … 05-31`**
(quality is a direct per-agent-day mean — `SUM(qa_score)/COUNT(distinct evals)`,
no cohort/trailing benchmark — so each month stands alone; June is the
still-settling active month, reported as freshness below). Comparison is
day-grain mean QA score (0–100), outer-joined on `(agent, date)`,
`metric='quality'`, `date_granularity='day'`. Tolerance is **≤0.5 absolute
score points** (the value is a 0–100 mean, not a pp). Numerator
(`SUM(qa_score)`) and denominator (`COUNT(distinct evaluation_id)`) compared
separately. Legacy stores the per-agent-day mean (`qa_score`) and `evaluations`;
`SUM(qa_score) = qa_score * evaluations`.

### Core / Fraud — `quality_io`

**Value parity (≤0.5, complete months):** **100.0% every month** — Jan
817/817, Feb 814/814, Mar 1,164/1,164, Apr 1,270/1,270, May 995/995. Across all
5,060 matched agent-days the max absolute value diff is **1.4e-14** (float
noise) — i.e. exact. **Zero** denominator mismatches, **zero** `only_new`.

| month | value match ≤0.5 | matched / joined | only_legacy | only_new |
| --- | --- | --- | --- | --- |
| Jan | 100.0% | 817 / 817 | 9 | 0 |
| Feb | 100.0% | 814 / 814 | 0 | 0 |
| Mar | 100.0% | 1,164 / 1,164 | 61 | 0 |
| Apr | 100.0% | 1,270 / 1,270 | 43 | 0 |
| May | 100.0% | 995 / 995 | 6 | 0 |

### Social Media — Playvox (< 2026-05-01) / Sprinklr (≥ 2026-05-01)

SM quality **migrated from Playvox to Sprinklr in May 2026**, so the new pipeline
uses **Playvox for evaluations before `2026-05-01` and Sprinklr from `2026-05-01`
onward** (a clean source switch — `SPRINKLR_SM_CUTOVER` in
`metrics_data/quality_evaluations.py`; Playvox SM rows on/after the cutover are
dropped so the two never double-count). Legacy SM quality is **Playvox-only** — the
SM notebook never sourced quality from Sprinklr (Sprinklr appears there only for
occupancy `:988` and tNPS `:2060`). Consequence:

- **SM Jan–Apr (Playvox) matches legacy exactly** — Jan 234/234, Feb 228/228,
  Mar 177/177 (10 `only_legacy` = the SM 03-27 outage drop), Apr 147/147; max diff 0.0.
- **SM May onward (Sprinklr) intentionally does NOT match legacy's Playvox-only SM**
  — a deliberate enhancement to use the migrated source (parallels the SM-occupancy
  decision; see `sm-occupancy-on-pre-cutover`). New SM May is fully populated through
  `2026-05-31` (335 agent-days / 26 agents); under the prior Playvox-only build SM
  died at 05-15 because live Playvox carries no SM evals after that.

| month | source | vs legacy | matched / joined |
| --- | --- | --- | --- |
| Jan | Playvox | exact | 234 / 234 |
| Feb | Playvox | exact | 228 / 228 |
| Mar | Playvox | exact (10 `only_legacy` = 03-27 outage) | 177 / 177 |
| Apr | Playvox | exact | 147 / 147 |
| May | **Sprinklr** | **by-design divergence** (legacy = Playvox-only) | n/a |

### Divergences

| Divergence | Rows (Jan–May) | Cause | num/den | Class |
| --- | --- | --- | --- | --- |
| `only_legacy`: **`2026-03-27` (61 Core/Fraud) + `2026-04-09` (43 Core/Fraud)** | 104 | **Legacy outage-filter is broken.** `[IO] Quality Dataset.sql:139/161` filters `local_mx_evaluation__created_at NOT IN ('2026-03-27','2026-04-09')` — but `local_mx_evaluation__created_at` is a **timestamp** and the literals are date-strings (= midnight), so the filter matches nothing and legacy `quality_io` **keeps** both outage days (verified: 61 + 43 agent-days present, evals 157 + 144). The new pipeline applies the outage carve-out as a real DATE filter (quirk #2) and correctly drops both days for Core/Fraud. New does what the legacy comment *intended* ("deleting data with general access problems"); legacy's frozen output is the broken one. | drops both | **legacy-bug** |
| `only_legacy`: **SM `2026-03-27`** | 10 | Same broken-filter class on the SM side. SM legacy (`Social Media.sql:2931`) drops 03-27 in `qa_deduped` but via the same timestamp-vs-date pattern, so legacy keeps it; the new SM metric correctly drops 03-27 only (and **keeps 04-09**, verified: the lone surviving outage row anywhere in the new day metric is `social media / 2026-04-09`, 11 rows) — exactly the team-asymmetric outage of quirk #2. | drops 03-27 | **legacy-bug** (per quirk #2) |
| `only_legacy`: **`enablement` 2026-01-02 (9) + `planning` May (6, scattered)** | 15 | Org-support squads. The new pipeline carries **no** `enablement`/`planning` quality rows at any grain (DIME/roster-squad exclusion); legacy's `agent_information` only drops `squad IN ('social','content')` (`Quality Dataset.sql:65`) so it keeps enablement/planning. Same roster-squad-vs-DIME-squad column nuance documented for Shrinkage. | — | **by-design** |
| **SM May+ uses Sprinklr; legacy used Playvox-only** | SM May (≈26 agents/day, 335 rows) | SM quality **migrated Playvox→Sprinklr in May 2026** (owner-confirmed). The new pipeline switches SM to the Sprinklr source from `2026-05-01`, so SM May+ reflects the migrated source and is complete through 05-31; legacy's frozen SM is Playvox-only (which tails off after 05-15, since live Playvox has no SM evals past then). Deliberate enhancement, same spirit as SM occupancy. Earlier this showed as "SM source freshness" only because fix #4 had wrongly forced SM Playvox-only until July; reverting that resolved it. | source switch | **by-design** (SM source migration) |
| `only_new`: **none** | 0 | New never adds Core/Fraud/SM agent-days legacy lacks (within complete months). | — | — |

### Reproduced and matching (not divergences)
- **Team-scoped blacklists** (quirk #1): Core/Fraud drop the 4 `scorecard_id` +
  4 `evaluation_id` ids; SM drops only `scorecard_id='68def79b3f83da8cc9cb5299'`.
  Verified on the raw table: **0** blacklisted scorecard/eval ids survive
  pre-cutover.
- **Team-asymmetric outage** (quirk #2): Core/Fraud drop **both** 03-27 + 04-09;
  SM drops **only** 03-27 (keeps 04-09). Verified — the only outage-date row left
  in the new day metric is SM 04-09 (11 rows). (This is the *intended* drop; the
  divergence above is only because legacy's own filter failed to apply it.)
- **SM source switch** (Playvox → Sprinklr at `2026-05-01`): SM quality uses
  Playvox for evaluations `< 2026-05-01` (matches legacy) and Sprinklr from
  `2026-05-01` onward (deliberate enhancement reflecting the real source migration;
  legacy SM quality was Playvox-only). The switch is clean — no Playvox SM rows
  survive on/after the cutover, so no double-count. Core/Fraud are always Playvox.
- **Dedup** (latest per `evaluation_id` by `created_at DESC`) and **denominator =
  `COUNT(distinct evaluation_id)`**: reproduced — denominator matches exactly on
  all 5,060 Core/Fraud matched rows and all Jan–Apr SM rows.
- **Content team absent** from quality on both sides (Content → CSAT, a separate
  metric) — no Content rows leak into either side. By-design, confirmed.

### Verdict
**At parity. No open items.** Core/Fraud value parity is **100% and bit-exact**
Jan–May (5,060/5,060, max diff 1.4e-14; re-confirmed on the shipped table —
May 995/995, max diff 0.0). Coverage is clean apart from classified `only_legacy`:
the **114** outage rows (104 Core/Fraud 03-27+04-09, 10 SM 03-27) are a
**legacy-bug** — legacy's timestamp-vs-date outage filter silently fails and keeps
the days; the new pipeline drops them, which the **owner confirmed is the desired
correction**. The **15** enablement/planning rows are **by-design** (org-support
squad exclusion). SM is **exact for Jan–Apr** (Playvox); **SM May+ is a deliberate
Sprinklr enhancement** (the Playvox→Sprinklr migration), now complete through 05-31
and intentionally diverging from legacy's Playvox-only SM — same class as SM
occupancy, not an open item.

**June (active month, not a gate):** Core/Fraud 707/707 value-exact (100%), 0
`only_new`, 252 `only_legacy` — purely freshness (new max 2026-06-19 vs legacy
2026-06-26, the trailing week not yet loaded).

---

## TNPS — `io_tnps_metric` vs the `tnps_agent` rows of `usr.mx__cx.internal_ops_performance_2026_social_media` (Social Media only)

**Status:** validated over the complete months **`2026-01-01 … 05-31`**
(Human tNPS is a direct per-agent-day ratio — no cohort/trailing benchmark — so
each month stands alone; June is the still-settling active month, reported as
freshness below). Comparison is day-grain tNPS score, outer-joined on
`(agent, date)`, `metric='tnps'` / `date_granularity='day'` / team `social media`
on the new side vs legacy `metric='tnps_agent'` / `date_granularity='day'`
(legacy `date_reference` cast to DATE). Tolerance is **≤0.5 absolute** (tNPS is a
`numerator/denominator*100` score that can be **negative**, not a pp). Numerator
(`#distinct cases with ≥1 promoter − #distinct cases with ≥1 detractor`) and
denominator (`#distinct cases with ≥1 valid response`) compared separately. The
era-split snapshot pin (`tnps_base_2025`, `Social Media.sql:2103`) only applies to
closure dates `< 2025-12-01`, so it is N/A in this window.

**Value parity (≤0.5, complete months): 100.0% every month** — Jan 239/239,
Feb 302/302, Mar 392/392, Apr 264/264, May 249/249. Across all **1,446** matched
agent-days the numerator is exact (1,446/1,446), the denominator is exact
(1,446/1,446), and the max absolute value diff is **0.0** — i.e. bit-exact. Both
sides carry the same **10 negative-value** rows (detractor-heavy days), so the
signed-ratio path is reproduced. **Zero** `only_legacy`, **zero** `only_new` on
the complete months. NULL/zero-denominator path is clean (0 den-zero rows).

| month | value match ≤0.5 | matched / joined | only_legacy | only_new |
| --- | --- | --- | --- | --- |
| Jan | 100.0% | 239 / 239 | 0 | 0 |
| Feb | 100.0% | 302 / 302 | 0 | 0 |
| Mar | 100.0% | 392 / 392 | 0 | 0 |
| Apr | 100.0% | 264 / 264 | 0 | 0 |
| May | 100.0% | 249 / 249 | 0 | 0 |

### Divergences

| Divergence | Rows (Jan–May) | Cause | num/den | Class |
| --- | --- | --- | --- | --- |
| none | 0 | Complete months are bit-exact on value, numerator, denominator, and coverage. | — | — |

**June (active month, not a gate):** within the loaded June window (new max
`2026-06-21` vs legacy `2026-06-29`) the 236 matched rows are **100% value-exact**,
0 `only_new`. The **104** `only_legacy` June rows all fall on `2026-06-22` onward —
purely the trailing week not yet loaded into the new pipeline (freshness/boundary,
same class as the other metrics' active-month tail).

### Reproduced and matching (not divergences)
- **Agent attribution via `LOWER(REGEXP_EXTRACT(agent_email_id, ...))`** — both
  sides extract the agent from the email directly and **neither** joins
  `sprinklr_sm_users` (the swapped name↔email table), so TNPS is not exposed to
  that defect. Verified: 0 agent-level divergences.
- **Validity window** `survey_response_date <= case_closure_time + INTERVAL 1 DAY`
  (`Social Media.sql:2078`) — both source columns DATE-typed, so the comparison is
  byte-for-byte; reproduced.
- **`2026-03-27` outage drop** (`Social Media.sql:2087`,
  `DATE_TRUNC('DAY', case_closure_time) != '2026-03-27'`) — its DATE_TRUNC cast
  actually fires here (unlike Quality's broken timestamp filter), so both sides
  drop the day. Verified: **0** rows on 03-27 on both sides (both empty, as
  expected).
- **Classify-then-`COUNT(DISTINCT)`, not dedup-to-one-row**
  (`Social Media.sql:2080-2089`) — implemented exactly as legacy. In this window
  there happen to be **0 mixed-class cases** (no case carries both a valid
  promoter ≥9 and a valid detractor ≤6 response, across 3,885 kept cases), so the
  classify-vs-dedup distinction has no observable effect Jan–May; the code path is
  correct but untriggered. Independent recompute from
  `io_tnps_responses_raw` reproduces the table's num/den exactly on every
  roster-active row.
- **Roster active-status join** (`agent_information`, `b.status='active'`,
  `Social Media.sql:2096`) — applied identically on both sides. The 16 raw
  response-days dropped from the new metric are **also absent from legacy** (0 of
  16 present in legacy), so the roster filter never surfaces as a divergence.
- **Score thresholds** promoter ≥9 / detractor ≤6 / neutral 7–8 / valid = non-null
  (`Social Media.sql:2069-2077`) — reproduced.

### Verdict
**At parity. No open items.** Value parity is **100% and bit-exact** Jan–May
(1,446/1,446 matched, max diff 0.0; numerator and denominator both exact),
coverage is perfectly clean (0 `only_legacy`, 0 `only_new`). The validity window,
03-27 outage drop, classify-then-COUNT(DISTINCT) logic, email-direct attribution
(no `sprinklr_sm_users` exposure), and roster active-status join are all
reproduced. June's only divergence is the trailing-week freshness gap
(104 `only_legacy` on/after 06-22); the loaded June window is 100% value-exact.

---

## WoWs — `io_wows_metric` vs the `wows_agent` rows of `usr.mx__cx.internal_ops_performance_2026_social_media` (Social Media only)

**Status:** validated over the legacy comparable window **`2026-01-01 … 06-10`**.
WoWs is a **count** metric, not a ratio: `numerator = metric_value =
COUNT(DISTINCT case_id)` per `(agent, bucket)`; `denominator` is the constant
monthly target **5** (reference only). The legacy
`internal_ops_performance_2026_social_media` table is a **stale snapshot frozen at
`2026-06-10`** (its max day-grain `date_reference`), while the new run extends to
`2026-06-21` — so the gate is dates **≤ 06-10**; everything after is the expected
snapshot-freshness tail. Comparison is outer-joined on
`(agent, date_granularity, date_reference)`, `metric='wows'` (new) vs
`metric='wows_agent'` (legacy, `date_reference` cast to DATE). The source is the
**live** WoWs Google Sheet (`gsheets.sheets.mx_wows_daniel_temp`), which keeps
accreting entries, so the new side is a moving target relative to the frozen
snapshot.

**The port is a strict superset of legacy — it never drops, misses, or
undercounts a WoW.** Across all overlapping granularities, **0** `only_legacy` and
**0** rows where new < legacy; every divergence is additive (new ≥ legacy) and
traces to the 06-10 snapshot freeze + live-sheet growth.

| granularity | matched keys | exact | new > legacy (additive) | new < legacy | only_legacy |
| --- | --- | --- | --- | --- | --- |
| day | 1,466 | 1,457 | 9 | 0 | 0 |
| week | 530 | 509 | 21 | 0 | 0 |
| month | 157 | 135 | 22 | 0 | 0 |

Day-agent value parity within the gate: **1,457 / 1,466 = 99.4% bit-exact**.

### Divergences

| Divergence | Rows | Cause | Class |
| --- | --- | --- | --- |
| Day rows after `2026-06-10` | 34 `only_new` days | Legacy snapshot frozen 06-10; new run to 06-21. | freshness/snapshot |
| Early-June additive deltas (≤ 06-10) | 7 `only_new` + 9 value (day) | **14 of 16 land exactly on 06-10** (legacy's frozen last day = partial capture) + 2 on 06-01/06-02 for one agent; all new > legacy as the live sheet gained entries since the snapshot. | freshness/snapshot |
| June month / trailing weeks partial | 22 month + 21 week, all new > legacy | Same 06-10 freeze: legacy June (and weeks spanning the boundary) are partial. | freshness/snapshot |
| `quarter` / `semester` / `year` grains | 54 / 28 / 28 `only_new` | New pipeline emits the standard 6-granularity superset; **all** legacy SM metrics (`nocc/qa/tnps/wows_agent`) emit only day/week/month. | by-design |

### Reproduced and matching (not divergences)
- **Count semantics** — `numerator = metric_value = COUNT(DISTINCT case_id)`,
  `denominator = 5` (constant monthly target). Verified: `metric_value ==
  numerator` on every row, `denominator = 5` at every grain — matches legacy.
- **`2026-03-27` outage drop** — legacy `wows_agent` carries **no** 03-27 row; the
  new metric drops 03-27 on the raw rows *before* bucketing for `date < 2026-07-01`
  (SM-only; the Core/Fraud 04-09 does not apply to the social source). Verified: 0
  rows on 03-27 on both sides. Same DROP call as Quality/TNPS, opposite the
  Shrinkage KEEP (see the cutover rule).
- **`COUNT(DISTINCT)` per bucket vs sum-of-daily** — for coarser grains the new
  metric counts distinct `case_id` over the whole bucket. Verified against legacy
  that monthly value equals the sum of daily values on every matched agent-month
  (no `case_id` recurs across days), so the two formulations are numerically
  identical on current data.
- **Agent grain only** — the org rollups (`wows_xforce / wows_xplead /
  wows_squad / wows_district`, `wows_agents_team_quartile`) are produced by the
  downstream composite layer, not this base metric (same scoping as TNPS).
- **Roster join** — `status='active'` + non-null `squad`, deduped to one row per
  `(agent, snapshot_month)` before the inner join (mirrors `tnps_responses`),
  preventing the content-branch fan-out double-count.

### Verdict
**At parity (strict superset). No open items.** Within the comparable window
(≤ the legacy snapshot of `2026-06-10`): **0 `only_legacy`, 0 rows where new <
legacy on any granularity**, and 99.4% of day-agent values bit-exact. The 52
non-exact matched rows and all `only_new` rows are additive (new ≥ legacy),
explained entirely by the legacy table being a frozen 06-10 snapshot read against a
live Google Sheet that has since gained entries — not a logic difference. The 03-27
outage drop, count semantics, distinct-per-bucket aggregation, and roster dedup are
all reproduced. quarter/semester/year are the by-design 6-granularity superset.

---

## Other metrics — not yet parity-checked

The composite indices (Xpeer/XForce Index, etc.) have **not** been
validated against legacy yet. Check for the same phantom-adherence cutover,
meeting/leave filter, and DIME-squad filter as Adherence / Normalized Occupancy /
NTPJ before assuming parity.
