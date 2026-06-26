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
* Legacy ``dimensioned_activity`` meeting/leave carve-outs and the DIME-squad
  exclusion (``agent_dime_squad`` not in wfm / credit_evolution / dote) are
  **applied upstream** as fixed DIME filters in the raw layer
  (``metrics_data/adherent_time.py`` → ``filter_dime``), so they never reach
  this metric.
* Per-agent manual time-off adjustments and outage-date exclusions
  (e.g. 2026-03-27, 2026-04-09) — deferred to the Adjustments layer.

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

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from metric_utils import aggregate_long
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
    adherent_time: DataFrame,
    *,
    general_exclusions: DataFrame | None = None,
    dime_inconsistencies: DataFrame | None = None,
) -> DataFrame:
    """Compute the Adherence metric at day/week/month grain.

    Args:
        adherent_time: the ``io_adherent_time_raw`` table (one row per slot).
        general_exclusions: ``adj_exclusiones_generales`` slot windows to drop
            (``None`` to skip).
        dime_inconsistencies: ``adj_inconsistencias_dime`` slot relabels
            (``None`` to skip).

    Returns:
        Tidy long-format metric rows (see module docstring / schema). Empty
        input naturally yields an empty frame with the metric schema.
    """
    work = reclassify_dime_slots(adherent_time, dime_inconsistencies)
    work = drop_slot_windows(work, general_exclusions)

    productive = work.filter(
        ~F.lower(F.col("activity_type_required")).isin(
            list(ADHERENCE_EXCLUDED_ACTIVITY_TYPES)
        )
    )

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
