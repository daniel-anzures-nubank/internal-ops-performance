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

## Other metrics — not yet parity-checked

NTPJ, Normalized Occupancy, Quality, Shrinkage, and the composite indices
(Xpeer/XForce Index, etc.) have **not** been validated against legacy yet.
Normalized Occupancy in particular almost certainly needs the same
phantom-adherence cutover, meeting/leave filter, and DIME-squad filter as
Adherence — check before assuming parity.
