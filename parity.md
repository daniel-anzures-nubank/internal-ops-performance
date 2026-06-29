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

## Other metrics — not yet parity-checked

Quality and the composite indices (Xpeer/XForce Index, etc.) have **not** been
validated against legacy yet. Check for the same phantom-adherence cutover,
meeting/leave filter, and DIME-squad filter as Adherence / Normalized Occupancy /
NTPJ before assuming parity.
