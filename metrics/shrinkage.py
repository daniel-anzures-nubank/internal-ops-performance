"""shrinkage — the Shrinkage performance metric (Core / Fraud, PySpark).

Part of the **metrics layer**: consumes the raw ``io_shrinkage_slots_raw`` table
(one row per DIME slot, already flagged for shrinkage) and produces a finished,
agent-level metric at day / week / month / quarter / semester / year grain.

Shrinkage = the share of an agent's *dimensioned* (required) time spent on
non-productive activities:

    shrinkage = SUM(shrinkage_flag) / SUM(required_slot)

over the agent's required slots for the period. **Target <= 20%.** Same
definition for all teams (see ``docs/metrics_definitions.md``).

The numerator (``shrinkage_flag``) is computed by the raw layer (the pre/post-
2026-03-01 slot-level rule). This module only applies the **denominator**
("required slot") rule that the raw layer deferred, then the manual adjustments.

Adjustment order (matches legacy intent)
----------------------------------------
1. ``reclassify_dime_slots`` (``Inconsistencias DIME``): relabel a slot's
   ``activity_type_required`` and (re)derive ``shrinkage_flag`` from the new
   label BEFORE the required/numerator counting.
2. ``drop_slot_windows`` (``Exclusiones Generales`` + ``Training`` +
   ``Shadowing``): remove matched (agent, date, time-window) slots from BOTH the
   numerator and denominator — legacy's ``manual_adjustments_shrinkage.exclude``
   and the ``jose.velez`` et al. day carve-out.
3. ``apply_no_shrinkage`` (``No Shrinkage``): keep the slot in the required base
   but clear the shrinkage numerator flags — legacy's ``not_shrinkage`` /
   vacation-keeps-required carve-out.
4. Drop ``lunch_break`` (never counted on either side — legacy ``shrinkage_base``
   line 248).
5. Apply the era-gated required-slot denominator rule.

Denominator rule (legacy ``required_slot``, applied here)
---------------------------------------------------------
* ``lunch_break`` slots never count (numerator or denominator).
* A slot is a *required* slot unless, by era, its ``activity_type_required`` is:
    - **pre-cutover** (``date < 2026-03-01``): ``dime_invalid_notation``
    - **post-cutover** (``date >= 2026-03-01``): ``time_off``
  (The 2025 path uses the same ``dime_invalid_notation`` rule as pre-cutover.)

Every shrinkage slot is, by construction, also a required slot, so the ratio is
always in ``[0, 1]``.

Input
-----
The ``io_shrinkage_slots_raw`` table (one row per DIME slot), via
``metrics_data/shrinkage_slots.py`` / ``read_table``. Required columns:
``agent, xforce, xplead, team, squad, district, shift, date, slot_time,
activity_type_required, shrinkage_flag``.

Note on the fixed DIME filters
------------------------------
Legacy's DIME-squad business exclusion (``content`` / ``planning`` / ``quality``
/ ``social`` / ``wfm`` / ``enablement``) is applied **upstream** as a fixed DIME
filter in the raw layer (``metrics_data/shrinkage_slots.py`` → ``filter_dime``),
so it already constrains BOTH the shrinkage numerator and the required
denominator here — there is no separate roster-squad filter at this layer.

NOT applied here (deferred — none currently)
--------------------------------------------
All the per-agent maternity / vacation / training / shadowing / outage carve-outs
are wired through the adjustment helpers above; for pre-2026-07-01 byte-for-byte
parity those ``adj_*`` tables must be populated to match legacy exactly.

Output — one row per (agent, date_reference, granularity)
---------------------------------------------------------
Tidy "long" metric shape shared across the metrics layer:
``agent, xforce, xplead, team, squad, district, shift, date_reference,
date_granularity, metric, numerator, denominator, metric_value``.
``metric_value`` is a **percentage** (``numerator / denominator * 100``); it is
NULL when the denominator is 0.

Roll-ups (``compute_shrinkage_rollups``)
----------------------------------------
``compute_shrinkage`` produces the agent grain (``metric = 'shrinkage'``). The
SOT also reports Shrinkage at the **XForce** and **XPLead** levels, so the build
script additionally emits ``shrinkage_xforce`` (per ``team, xforce, xplead``) and
``shrinkage_xplead`` (per ``team, xplead``; ``xforce`` NULL) into the same table.
Both are **slot-weighted** (sum the agent numerator/denominator, then divide).
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from metric_utils import METRIC_COLUMNS, aggregate_long, empty_metric_frame
from adjustments.manual import (
    apply_no_shrinkage,
    drop_slot_windows,
    reclassify_dime_slots,
)

METRIC_NAME = "shrinkage"  # agent grain
XFORCE_METRIC_NAME = "shrinkage_xforce"  # XForce roll-up
XPLEAD_METRIC_NAME = "shrinkage_xplead"  # XPLead roll-up

# Cutover for the denominator (required_slot) rule — same date as the raw
# layer's shrinkage_flag formula switch.
SHRINKAGE_FORMULA_CUTOVER: date = date(2026, 3, 1)

# Slots that never count toward the metric (numerator or denominator): legacy
# ``shrinkage_base`` drops lunch_break before any counting.
SHRINKAGE_EXCLUDED_ACTIVITY_TYPES: tuple[str, ...] = ("lunch_break",)

# activity_type_required that is NOT a required slot, by era.
PRE_CUTOVER_NON_REQUIRED_ACTIVITY = "dime_invalid_notation"
POST_CUTOVER_NON_REQUIRED_ACTIVITY = "time_off"

# Org-wide outage rows (``descripcion = 'Fallas Generales'``) live in the shared
# ``exclusiones_generales`` tab and legitimately drop whole-team slots for
# Adherence / Normalized Occupancy (legacy excluded those outage days for both).
# Legacy *shrinkage* has NO outage carve-out (``[IO] Shrinkage Dataset.sql`` has
# no 2026-03-27 / 04-09 org-wide drop), so to stay byte-for-byte we must NOT apply
# these rows to shrinkage. All other general-exclusion rows — the CNVB
# day-controls (legacy hardcode, lines 294-295) and the 2026-03-10 Core-wide
# standardization — are retained. Matched case-insensitively on ``descripcion``.
SHRINKAGE_SKIP_GENERAL_EXCLUSION_DESCRIPTIONS: tuple[str, ...] = ("fallas generales",)


def _drop_outage_exclusions(
    general_exclusions: DataFrame | None,
) -> DataFrame | None:
    """Remove org-wide outage rows from the general-exclusion windows.

    Shrinkage must not apply the shared ``exclusiones_generales`` outage rows
    (``descripcion = 'Fallas Generales'``): legacy shrinkage had no outage
    carve-out, while Adherence / Normalized Occupancy do drop those days. See
    ``parity.md`` (Shrinkage) and ``SHRINKAGE_SKIP_GENERAL_EXCLUSION_DESCRIPTIONS``.
    """
    if general_exclusions is None or "descripcion" not in general_exclusions.columns:
        return general_exclusions
    skip = [s.lower() for s in SHRINKAGE_SKIP_GENERAL_EXCLUSION_DESCRIPTIONS]
    desc = F.lower(F.trim(F.col("descripcion")))
    return general_exclusions.filter(desc.isNull() | ~desc.isin(skip))


def compute_shrinkage(
    shrinkage_slots: DataFrame,
    *,
    general_exclusions: DataFrame | None = None,
    dime_inconsistencies: DataFrame | None = None,
    training: DataFrame | None = None,
    shadowing: DataFrame | None = None,
    no_shrinkage: DataFrame | None = None,
) -> DataFrame:
    """Compute the Shrinkage metric at all granularities.

    Args:
        shrinkage_slots: the ``io_shrinkage_slots_raw`` table (one row per slot).
        general_exclusions / training / shadowing: ``adj_*`` slot windows to drop
            from both numerator and denominator (``None`` to skip).
        dime_inconsistencies: ``adj_inconsistencias_dime`` slot relabels
            (``None`` to skip).
        no_shrinkage: ``adj_no_shrinkage`` windows that keep the required slot but
            clear the shrinkage flags (``None`` to skip).

    Returns:
        Tidy long-format metric rows (see module docstring / schema).
    """
    spark = shrinkage_slots.sparkSession

    work = reclassify_dime_slots(shrinkage_slots, dime_inconsistencies)
    general_exclusions = _drop_outage_exclusions(general_exclusions)
    work = drop_slot_windows(work, general_exclusions, training, shadowing)
    work = apply_no_shrinkage(work, no_shrinkage)

    act = F.lower(F.col("activity_type_required"))

    # Drop lunch_break (never counted on either side).
    work = work.filter(~act.isin(list(SHRINKAGE_EXCLUDED_ACTIVITY_TYPES)))

    cal = F.to_date(F.col("date"))
    is_post_cutover = cal >= F.lit(SHRINKAGE_FORMULA_CUTOVER)
    non_required = F.when(
        is_post_cutover, F.lit(POST_CUTOVER_NON_REQUIRED_ACTIVITY)
    ).otherwise(F.lit(PRE_CUTOVER_NON_REQUIRED_ACTIVITY))

    work = work.withColumn(
        "required_slot_flag", (act != non_required).cast("int")
    ).withColumn("shrinkage_flag", F.col("shrinkage_flag").cast("int"))

    return aggregate_long(
        work,
        numerator_col="shrinkage_flag",
        denominator_col="required_slot_flag",
        metric_name=METRIC_NAME,
    )


def _rollup(agent_metric: DataFrame, *, level: str) -> DataFrame:
    """Slot-weighted shrinkage roll-up of the agent metric to ``level``.

    The roll-up is **slot-weighted** (sum the agent numerator/denominator, then
    divide) — identical to legacy ``shrinkage_xforce`` / ``shrinkage_xplead`` and
    to the shrinkage component inside ``xforce_index``. It is NOT a flat average
    of the per-agent percentages.
    """
    if level == "xforce":
        keys = ["team", "xforce", "xplead", "date_reference", "date_granularity"]
        metric_name = XFORCE_METRIC_NAME
    elif level == "xplead":
        keys = ["team", "xplead", "date_reference", "date_granularity"]
        metric_name = XPLEAD_METRIC_NAME
    else:  # pragma: no cover - guarded by caller
        raise ValueError(f"unknown level: {level!r}")

    agg = agent_metric.groupBy(*keys).agg(
        F.sum("numerator").alias("numerator"),
        F.sum("denominator").alias("denominator"),
    )

    out = (
        agg.withColumn("agent", F.lit(None).cast("string"))
        .withColumn(
            "xforce",
            F.col("xforce") if level == "xforce" else F.lit(None).cast("string"),
        )
        .withColumn("squad", F.lit(None).cast("string"))
        .withColumn("district", F.lit(None).cast("string"))
        .withColumn("shift", F.lit(None).cast("string"))
        .withColumn("metric", F.lit(metric_name))
        .withColumn(
            "metric_value",
            F.when(
                F.col("denominator") > 0,
                F.col("numerator") / F.col("denominator") * F.lit(100.0),
            ).otherwise(F.lit(None).cast("double")),
        )
        .select(*METRIC_COLUMNS)
    )
    return out


def compute_shrinkage_rollups(agent_metric: DataFrame) -> DataFrame:
    """XForce + XPLead slot-weighted roll-ups of the agent shrinkage metric.

    Args:
        agent_metric: the agent-level shrinkage metric (output of
            ``compute_shrinkage``, ``metric == 'shrinkage'``).

    Returns:
        ``shrinkage_xforce`` and ``shrinkage_xplead`` rows (``agent`` NULL;
        ``xforce`` NULL on the XPLead rows), all granularities.
    """
    spark = agent_metric.sparkSession
    work = agent_metric.filter(F.col("metric") == F.lit(METRIC_NAME))
    rolled = _rollup(work, level="xforce").unionByName(
        _rollup(work, level="xplead")
    )
    if len(rolled.take(1)) == 0:
        return empty_metric_frame(spark)
    return rolled.orderBy(
        "date_granularity",
        "date_reference",
        "team",
        "metric",
        F.col("xplead").asc_nulls_last(),
    )


IO_SHRINKAGE_METRIC_SCHEMA: tuple[tuple[str, str], ...] = (
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
