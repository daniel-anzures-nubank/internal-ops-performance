# adjustments/

Compute modules for the **manual adjustments layer** — the per-agent / per-date
carve-outs the raw and metric layers defer (cross-support exclusions, leave
reclassifications, training/shadowing windows, outage dates, DIME-squad
exclusions, per-agent Index carve-outs).

The **source of truth** for sheet-backed adjustments is this Google Sheet:
<https://docs.google.com/spreadsheets/d/1Y5P6LijLxT6hFTd69DiSPBTUPKHO-m_6zzrs-PmOjfU/edit?gid=720896495#gid=720896495>

Missing DIME slots are file-backed instead of sheet-backed:
`adjustments/slots_faltantes_dime.csv` contains manually recovered DIME slots
from `usr.danielanzures.h1_missing_dime_slots` plus the Content temp-fix table
`usr.danielanzures.missing_agents_dime_slots_content_h1`.

Status: **implemented for current approved rows** — scalable adjustment tabs are
handled by `manual.py` and wired into the affected build scripts. `Ajustes
Index` and `Correcciones Generales Datos` are implemented as explicit hardcoded
exceptions. See
[`docs/adjustments_docs/README.md`](../docs/adjustments_docs/README.md) for the
catalog, source-of-truth details, and the conventions to follow (build scripts
in `scripts/adjustments_scripts/`, tests in `tests/adjustments/`, one
`docs/adjustments_docs/<name>.md` per adjustment).
