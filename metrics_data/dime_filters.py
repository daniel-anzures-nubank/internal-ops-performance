"""dime_filters — the single home of the fixed DIME-universe filter constants.

These are the FIXED legacy DIME filters (NOT manual adjustments) shared by the
slot-based raw tables (``adherent_time`` / ``occupancy_time`` /
``shrinkage_slots`` / ``jobs_raw``). Each module previously carried its own
copy of these tuples; they are consolidated here so the values cannot drift.
This module is constants-only — no imports — so it can be shared without
pulling in any Spark dependency.
"""

# Meeting/leave dimensioned_activity tokens excluded from the productive DIME
# universe. This is a fixed DIME data filter (NOT a manual adjustment): these
# slots are leave (Licencia, Vacacion, Permiso Medico) or meetings (Mouring,
# Weekly, Huddle), so they are not eligible scheduled work. Legacy excludes
# them with the same `dimensioned_activity NOT IN (...)` filter at the DIME
# stage; exact legacy list, incl. the 'Permiso Medico'/'Permiso medico' case
# variants.
#
# WARNING: the dual casing ('Permiso Medico' AND 'Permiso medico') is
# INTENTIONAL — the legacy source data contains both variants, and
# deduplicating (or normalizing case) breaks pre-2026-07-01 byte-for-byte
# parity. Do not "clean up" this list.
MEETING_LEAVE_DIMENSIONED_ACTIVITIES: tuple[str, ...] = (
    "Mouring",
    "Weekly",
    "Permiso Medico",
    "Permiso medico",
    "Huddle",
    "Licencia",
    "Vacacion",
)

# The leave subset of the list above. The legacy SM deck's ADHERENCE filter
# ([IO] Performance 2026 - Social Media Temp Fix.sql:231) excludes only the
# five meeting items -- it KEEPS `Licencia`/`Vacacion` slots in the SM
# adherence universe (where the deck's LEAST/GREATEST NULL-skip quirk then
# scores them ~100% adherent). adherent_time.filter_dime reproduces that deck
# split for SM DIME squads pre-cutover; every other consumer keeps the unified
# 7-item exclusion.
LEAVE_DIMENSIONED_ACTIVITIES: tuple[str, ...] = ("Licencia", "Vacacion")

# The SM DIME squads (legacy SM deck scope: `agent_dime_squad IN
# ('social', 'social_social')`). Shared by the SM-specific legacy-parity
# carve-outs in adherent_time (leave exclusion) and occupancy_time (legacy SM
# scoring quirks).
SM_DIME_SQUADS: tuple[str, ...] = ("social", "social_social")

# DIME squads excluded from adherence / occupancy / NTPJ — a fixed legacy
# filter on the DIME `agent_dime_squad` (operational / workforce-management
# squads, not part of the productive universe). Legacy applies
# `agent_dime_squad IS NOT NULL AND NOT IN (...)` at the DIME stage. Note this
# is the DIME squad, not the roster squad. (Occupancy note: legacy's NOcc
# dataset also excluded 'social' here, but the new pipeline intentionally
# KEEPS social DIME slots — Social-Media occupancy is Sprinklr-sourced and ON
# for the whole history; see metrics_data/occupancy_time.py.)
DIME_SQUAD_EXCLUSIONS: tuple[str, ...] = ("wfm", "credit_evolution", "dote")

# DIME squads excluded from shrinkage — a fixed legacy filter on the DIME
# ``agent_dime_squad``. Legacy ``shrinkage_base`` ([IO] Shrinkage Dataset.sql
# lines 249-250) keeps only ``agent_dime_squad IS NOT NULL AND
# agent_dime_squad NOT IN (...)``. The exclusion changes BOTH numerator and
# denominator (legacy counts shrinkage_slot / required_slot FROM this
# already-filtered base), so it must be applied at the slot stage, before the
# roster merge drops the DIME squad column.
#
# NOTE this set is shrinkage-specific. It is DELIBERATELY BROADER on the
# org-support side than adherence/occupancy's ``DIME_SQUAD_EXCLUSIONS``
# (wfm / credit_evolution / dote): shrinkage excludes content / planning /
# quality / social / wfm / enablement and excludes neither credit_evolution
# nor dote. Do NOT reuse one list for the other.
SHRINKAGE_DIME_SQUAD_EXCLUSIONS: tuple[str, ...] = (
    "content",
    "planning",
    "quality",
    "social",
    "wfm",
    "enablement",
)
