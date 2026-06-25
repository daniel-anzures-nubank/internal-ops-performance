"""adherence — the Adherence performance metric (Core / Fraud / Social Media / Content).

This is the first module of the **metrics layer**. Unlike `metrics_data/`
(which produces granular raw tables), this layer consumes a raw table and
produces a finished, agent-level metric at day / week / month granularity.

Adherence = the share of an agent's dimensioned (scheduled) time that overlapped
a "connected" productivity status:

    adherence = SUM(adherent_minutes) / SUM(required_minutes)

computed over the agent's **productive** DIME slots for the period. Target ≥ 95%.
Same definition for all teams (see `docs/metrics_definitions.md`).

Input
-----
The ``io_adherent_time_raw`` table (one row per agent per DIME slot), via
``metrics_data/adherent_time.py`` / ``read_table``. Required columns:
``agent, xforce, xplead, team, squad, district, shift, date,
activity_type_required, required_minutes, adherent_minutes``.

Business filter applied here (deferred by the raw layer)
--------------------------------------------------------
* Drop non-productive slots: ``activity_type_required`` in
  ``{lunch_break, time_off, shrinkage}`` (per the metric definition). The
  remaining slots form the adherence denominator.

NOT applied here (future Adjustments layer — keeps this a clean baseline)
------------------------------------------------------------------------
* Legacy ``dimensioned_activity`` meeting/leave carve-outs (Mouring / Weekly /
  Permiso Medico / Huddle / Licencia / Vacacion). These ride on
  ``activity_type_required = 'dime_invalid_notation'`` (NOT 'shrinkage'), and
  the raw table does not carry ``dimensioned_activity`` — so they are not
  removed here. They can be applied later by joining ``io_shrinkage_slots_raw``.
* Legacy DIME-squad exclusions (``wfm`` / ``credit_evolution`` / ``dote``).
* Per-agent manual time-off adjustments and outage-date exclusions
  (e.g. 2026-03-27, 2026-04-09).

Output — one row per (agent, date_reference, granularity)
---------------------------------------------------------
Tidy "long" metric shape shared across the metrics layer:
``agent, xforce, xplead, team, squad, district, shift, date_reference,
date_granularity, metric, numerator, denominator, metric_value``.
``metric_value`` is a **percentage** (``numerator / denominator * 100``) to
match the legacy metric tables and the Xpeer-Index math; it is NULL when the
denominator is 0.
"""

from __future__ import annotations

import pandas as pd

from metric_utils import aggregate_long, empty_metric_frame
from adjustments.manual import drop_slot_windows, reclassify_dime_slots

METRIC_NAME = "adherence"

# Per the metric definition, these dimensioned-activity types do not count
# toward adherence (numerator or denominator).
ADHERENCE_EXCLUDED_ACTIVITY_TYPES: tuple[str, ...] = (
    "lunch_break",
    "time_off",
    "shrinkage",
)


def compute_adherence(
    adherent_time: pd.DataFrame,
    *,
    general_exclusions: pd.DataFrame | None = None,
    dime_inconsistencies: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Compute the Adherence metric at day/week/month grain.

    Args:
        adherent_time: the ``io_adherent_time_raw`` table (one row per slot).

    Returns:
        Tidy long-format metric rows (see module docstring / schema).
    """
    if adherent_time.empty:
        return empty_metric_frame()

    work = reclassify_dime_slots(adherent_time, dime_inconsistencies)
    work = drop_slot_windows(work, general_exclusions)

    productive = work.loc[
        ~work["activity_type_required"]
        .astype("string")
        .str.lower()
        .isin(ADHERENCE_EXCLUDED_ACTIVITY_TYPES)
    ].copy()

    if productive.empty:
        return empty_metric_frame()

    return aggregate_long(
        productive,
        numerator_col="adherent_minutes",
        denominator_col="required_minutes",
        metric_name=METRIC_NAME,
    )


IO_ADHERENCE_METRIC_SCHEMA: tuple[tuple[str, str], ...] = (
    ("agent", "STRING"),
    ("xforce", "STRING"),
    ("xplead", "STRING"),
    ("team", "STRING"),
    ("squad", "STRING"),
    ("district", "STRING"),
    ("shift", "STRING"),
    ("date_reference", "DATE"),
    ("date_granularity", "STRING"),
    ("metric", "STRING"),
    ("numerator", "DOUBLE"),
    ("denominator", "DOUBLE"),
    ("metric_value", "DOUBLE"),
)
