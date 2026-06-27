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

## Other metrics — not yet parity-checked

NTPJ, Quality, Shrinkage, and the composite indices (Xpeer/XForce Index, etc.)
have **not** been validated against legacy yet. Check for the same
phantom-adherence cutover, meeting/leave filter, and DIME-squad filter as
Adherence / Normalized Occupancy before assuming parity.
