"""xforce_index — the composite XForce Index (all teams), PySpark.

The headline XForce score: the **mean of up to four normalized 0-100
components**, per XForce per period. (Legacy ``index_xforce``; renamed
``xforce_index`` to match the ``xpeer_index`` naming convention.)

    xforce_index = (shrinkage + xpeers_in_target + average_xpeer_index
                    [+ improved_benchmark]) / N

where ``N`` is the number of active components (3 or 4) and each component is
mapped to a 0-100 scale:

================  ================================================  ============================
component         transform (legacy ``index_xforces_final``)        source
================  ================================================  ============================
shrinkage         ``<= target → 100``; ``> target → (100+t) - shr``;  ``io_shrinkage_metric``
                  target 23 for May/June-2026 month else 20; NULL→0  (agent rows, summed → XForce)
xpeers_in_target  ``>= 70 → 90 + (x-70)/3``; ``< 70 → raw``; NULL→0   ``io_xpeers_in_target_metric``
average_xpeer_    raw value, NULL → 0                                ``io_average_xpeer_index_metric``
index
improved_         ``>= 60 → 100``; ``< 60 → improved / 0.6``;         ``io_improved_benchmarks_metric``
benchmark         NULL → 0                                          (``improved_benchmark_xforce``)
================  ================================================  ============================

``shrinkage_xforce`` is the slot-weighted XForce shrinkage — we **sum** the
agent ``io_shrinkage_metric`` numerator/denominator per XForce (identical to
legacy's ``SUM(shrinkage_slot)/SUM(required_slot)``), not an average of agent
percentages.

Component count — legacy's explicit DATE rule (NOT a presence test)
-------------------------------------------------------------------
The pandas port gated the 4th component on *whether* an
``improved_benchmark_xforce`` row was present. That diverges from legacy in
several ways (build-plan fixes #1-#3), so this PySpark port instead reproduces
legacy ``index_xforces_monthly`` / ``index_xforces_weekly`` directly: a bucket is
**4-component** (``denominator = 400``) iff::

    date_reference < 2026-05-01
        AND NOT (xplead == 'david.fernandez' AND date_reference >= 2026-04-01)

and **3-component** (``denominator = 300``) otherwise. A *missing*
``improved_benchmark`` value in a 4-component bucket folds to 0 (legacy
``index_xforces_final`` ``ELSE 0``) — it is still counted. This single rule
subsumes the old presence test and the build-plan fixes:

* **Fix #1 (presence → date):** the 4th component is added by the date rule,
  not by the join hitting a row. A pre-May Core/Fraud XForce with no improved
  row is still 4-component (the improved term folds to 0).
* **Fix #2 (weekly 4th component):** the date rule is applied to BOTH the
  ``week`` and ``month`` grains, so weekly pre-May buckets are 4-component even
  though the upstream ``improved_benchmarks`` table is currently month-only.
* **Fix #3 (Core-April over-removal):** a single flat ``2026-05`` cutoff for all
  teams plus only the ``david.fernandez >= 2026-04`` carve-out — non-david Core
  April buckets stay 4-component (legacy keeps them).

Fix #4 (granularity scope) — week + month only
----------------------------------------------
Legacy materialized ``index_xforces`` for **week + month only**. For
``date_reference < 2026-07-01`` (the legacy-parity cutover) we therefore emit
only those two grains; the broader set is allowed from the cutover onward.

Fix #5 (Content / Social Media scope) — KNOWN GAP
-------------------------------------------------
Legacy builds a per-deck ``index_xforce``: the main deck (Core + Fraud) and the
Content "Temp Fix" notebook each have one; the Social-Media main notebook does
**not** (only its Temp Fix file does). The component count differs by deck:

* **Core / Fraud (main deck):** the date rule above (4-component Jan-Apr 2026
  except david-April, 3-component from May).
* **Content:** 3-component through ``2026-03-31``, 4-component from
  ``2026-04-01`` (Content improved benchmarks start April).

This port consumes the **all-teams** shrinkage driver but the upstream
``improved_benchmarks`` metric is Core/Fraud-only and month-only. Until
Content's April-onward improved component and the weekly improved rows land, the
date rule here applies uniformly to every deck, so:

* **Content** Apr-Jun 2026 buckets are emitted **4-component** (the date rule
  fires) but with the improved term folded to 0 — legacy Content is also
  4-component from April, so the COUNT matches; the residual is only the
  improved numerator. Content Jan-Mar are 4-component here vs legacy's 3 — a
  divergence that resolves when Content's deck-specific 2026-04 cutover is wired.
* **Social Media** has no legacy ``index_xforce`` in the main notebook, so any
  SM XForce emitted here is extra relative to the main-deck legacy table.

Validation (2026-07-01, vs legacy ``index_xforce`` in
``internal_ops_performance_2026``, month grain)
--------------------------------------------------------------------------
Now that ``improved_benchmarks`` has landed, this metric is cluster-validated.
The ``xforce_index``-specific logic is byte-exact: **denominators match legacy
in every matched row** (the 3-vs-4 component date rule and the david-April
carve-out reproduce exactly), and every legacy main-deck XForce is reproduced
(``legacy_only = 0``). The 3-component clean baseline (May) is **0.53** avg
abs diff, and the April 4-component unblock is **1.44** (tracking improved
benchmark's own Apr ~0.96 residual). The remaining value gaps are inherited
**entirely from the upstream component numerators** — the shrinkage /
xpeers_in_target / average_xpeer_index tables carry their own documented
early-month (NTPJ) and just-closed-month bounds — not from anything this module
computes. The ``new_only`` rows each month are the SM/Content deck XForces, which
the main-deck legacy table does not carry.

Deck grouping, NOT team (hard-won lesson)
-----------------------------------------
Legacy ``index_xforce`` is **per deck**, and the main deck merges Core + Fraud
with no team column. We roll the agent-grain shrinkage up by a synthetic
**deck** — ``core`` / ``fraud`` / NULL-team → ``main``; ``social media`` →
``sm``; ``content`` → ``content`` — NOT by ``team``, so a cross-team XForce
(agents in both Core and Fraud) merges into one ``main`` row instead of
splitting. ``team`` is emitted NULL (consistent with the other composites; the
downstream ``average_xforce_index`` joins on ``xforce`` / ``xplead`` alone).

Output grain
------------
One row per ``(xforce, xplead)`` per period (per deck), driven by the
XForce-rolled shrinkage. ``agent``, ``team``, ``squad``, ``district``,
``shift`` are NULL. ``numerator`` = Σ active components, ``denominator`` =
``100 * N`` (300 or 400), ``metric_value`` = ``numerator / denominator * 100``
(the component mean).
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from metric_utils import METRIC_COLUMNS, empty_metric_frame

METRIC_NAME = "xforce_index"
IMPROVED_BENCHMARK_XFORCE_METRIC = "improved_benchmark_xforce"

# Legacy-parity cutover: before it, emit only the week + month grain legacy
# materialized (index_xforces unions only *_monthly + *_weekly) and apply the
# byte-for-byte component-count date rule.
LEGACY_CUTOVER: date = date(2026, 7, 1)

# Component-count date rule (legacy index_xforces_monthly/weekly).
# 4-component (denominator 400) when date_reference < FOUR_COMPONENT_CUTOFF and
# NOT the david.fernandez April carve-out; 3-component (300) otherwise.
FOUR_COMPONENT_CUTOFF: date = date(2026, 5, 1)
DAVID_IMPROVED_SUPPRESSION_XPLEAD = "david.fernandez"
DAVID_IMPROVED_SUPPRESSION_FROM: date = date(2026, 4, 1)

CORE = "core"
FRAUD = "fraud"
SOCIAL_MEDIA = "social media"
CONTENT = "content"

# Synthetic decks (legacy runs a notebook per deck; the main deck merges
# Core + Fraud + NULL-team support squads). We group by deck, NOT team.
DECK_MAIN = "main"
DECK_SM = "sm"
DECK_CONTENT = "content"

# XForce-grain join keys for the component metric tables.
_JOIN_KEYS = ["xforce", "date_reference", "date_granularity"]


def _deck_col() -> Column:
    """Map ``team`` to its deck (``main`` = core/fraud/NULL; ``sm``; ``content``)."""
    team = F.lower(F.col("team"))
    return (
        F.when(team.isin(CORE, FRAUD) | team.isNull(), F.lit(DECK_MAIN))
        .when(team == F.lit(SOCIAL_MEDIA), F.lit(DECK_SM))
        .when(team == F.lit(CONTENT), F.lit(DECK_CONTENT))
        .otherwise(F.lit("other"))
    )


# Shrinkage target (legacy index_xforces_final): 23% for May/June-2026 MONTH
# buckets, 20% otherwise. Legacy gates on `date_reference IN (2026-05-01,
# 2026-06-01)`, which matches month-first dates only — weekly May/June keep 20%.
SHRINKAGE_RELAXED_MONTHS: tuple[date, ...] = (date(2026, 5, 1), date(2026, 6, 1))
SHRINKAGE_TARGET_DEFAULT = 20.0
SHRINKAGE_TARGET_RELAXED = 23.0


def _shrinkage_component(col: Column, date_ref: Column) -> Column:
    """``<= target → 100``; ``> target → (100 + target) - shrinkage``; NULL → 0.

    ``target`` is 23 for May/June-2026 month buckets (``date_reference`` in
    :data:`SHRINKAGE_RELAXED_MONTHS`), else 20.
    """
    relaxed = date_ref.isin(*SHRINKAGE_RELAXED_MONTHS)
    return (
        F.when(relaxed & (col <= F.lit(SHRINKAGE_TARGET_RELAXED)), F.lit(100.0))
        .when(
            relaxed & (col > F.lit(SHRINKAGE_TARGET_RELAXED)),
            F.lit(100.0 + SHRINKAGE_TARGET_RELAXED) - col,
        )
        .when(col <= F.lit(SHRINKAGE_TARGET_DEFAULT), F.lit(100.0))
        .when(
            col > F.lit(SHRINKAGE_TARGET_DEFAULT),
            F.lit(100.0 + SHRINKAGE_TARGET_DEFAULT) - col,
        )
        .otherwise(F.lit(0.0))
    )


def _xpeers_in_target_component(col: Column) -> Column:
    """On-target (``>= 70``) rescaled into the 90-100 band; below-target raw.

    ``90 + (xit - 70) * (10 / 30)`` — so 70 → 90 and 100 → 100. ``col`` must
    already be ``COALESCE(.., 0)`` (legacy index_xforces_final).
    """
    return F.when(
        col >= F.lit(70),
        F.lit(90.0) + (col - F.lit(70.0)) * F.lit((100.0 - 90.0) / (100.0 - 70.0)),
    ).otherwise(col)


def _improved_component(col: Column) -> Column:
    """``>= 60 → 100``; ``< 60 → improved / 0.6``; NULL → 0."""
    return (
        F.when(col >= F.lit(60), F.lit(100.0))
        .when(col < F.lit(60), col / F.lit(0.6))
        .otherwise(F.lit(0.0))
    )


def _xforce_value(
    df: DataFrame | None, col: str, *, metric: str | None = None
) -> DataFrame | None:
    """Project an XForce-grain metric table to ``_JOIN_KEYS`` + ``col``.

    Returns ``None`` when the table is absent/empty (the caller then stages the
    column as a typed NULL). The base ``io_*_metric`` tables are unique per
    ``(xforce, date_reference, date_granularity, metric)``; we still dedup on
    ``_JOIN_KEYS`` to guard against an accidental cross-team duplicate sneaking
    through the metric filter.
    """
    if df is None or len(df.take(1)) == 0:
        return None
    work = df
    if metric is not None:
        work = work.filter(F.col("metric") == F.lit(metric))
    if len(work.take(1)) == 0:
        return None
    return (
        work.select(*_JOIN_KEYS, F.col("metric_value").cast("double").alias(col))
        .dropDuplicates(_JOIN_KEYS)
    )


def compute_xforce_index(
    shrinkage: DataFrame,
    xpeers_in_target: DataFrame | None = None,
    average_xpeer_index: DataFrame | None = None,
    improved_benchmarks: DataFrame | None = None,
) -> DataFrame:
    """Compute the composite XForce Index at all granularities.

    Args:
        shrinkage: ``io_shrinkage_metric`` (agent grain) — the driver; summed to
            the XForce (per deck) for the slot-weighted shrinkage.
        xpeers_in_target: ``io_xpeers_in_target_metric`` (XForce grain). May be
            ``None``/empty.
        average_xpeer_index: ``io_average_xpeer_index_metric`` (XForce grain).
            May be ``None``/empty.
        improved_benchmarks: ``io_improved_benchmarks_metric`` (we use only the
            ``improved_benchmark_xforce`` rows). May be ``None``/empty —
            currently DEFERRED, so the improved term folds to 0 and only the
            component COUNT (the date rule) is exercised.

    Returns:
        Tidy long-format metric rows (XForce grain, ``team`` NULL); week + month
        only before the 2026-07-01 cutover.
    """
    spark = shrinkage.sparkSession
    if len(shrinkage.take(1)) == 0:
        return empty_metric_frame(spark)

    # The shrinkage table also carries shrinkage_xforce / shrinkage_xplead
    # roll-up rows; roll up from the **agent** rows only to avoid double counting.
    agent = shrinkage.filter(F.col("metric") == F.lit("shrinkage"))
    if len(agent.take(1)) == 0:
        return empty_metric_frame(spark)

    # --- roll shrinkage up to the XForce, grouping by DECK (not team) ---------
    # Group with xplead so it rides along on the XForce row. A cross-team XForce
    # (core + fraud agents) lands in one 'main' deck row; a cross-deck XForce
    # (core + SM agents) stays split into a 'main' and an 'sm' row.
    agent = agent.withColumn("_deck", _deck_col())
    sh = agent.groupBy(
        "_deck", "xforce", "xplead", "date_reference", "date_granularity"
    ).agg(
        F.sum(F.col("numerator")).alias("_num"),
        F.sum(F.col("denominator")).alias("_den"),
    )
    base = sh.withColumn(
        "shrinkage_xforce",
        F.when(F.col("_den") > F.lit(0), F.col("_num") / F.col("_den") * F.lit(100.0))
        .otherwise(F.lit(None).cast("double")),
    ).drop("_num", "_den")

    # --- attach the XForce-grain component values on (xforce, period) ---------
    xit = _xforce_value(xpeers_in_target, "xit", metric="xpeers_in_target")
    avg = _xforce_value(average_xpeer_index, "avg_idx")
    imp = _xforce_value(
        improved_benchmarks, "improved", metric=IMPROVED_BENCHMARK_XFORCE_METRIC
    )
    for comp, col in ((xit, "xit"), (avg, "avg_idx"), (imp, "improved")):
        if comp is not None:
            base = base.join(comp, on=_JOIN_KEYS, how="left")
        else:
            base = base.withColumn(col, F.lit(None).cast("double"))

    # --- Fix #4: week + month only pre-cutover -------------------------------
    gran = F.col("date_granularity")
    date_ref = F.col("date_reference")
    pre_cutover = date_ref < F.lit(LEGACY_CUTOVER)
    base = base.filter((~pre_cutover) | gran.isin("week", "month"))
    if len(base.take(1)) == 0:
        return empty_metric_frame(spark)

    # --- component transforms (legacy index_xforces_final) -------------------
    s = _shrinkage_component(
        F.col("shrinkage_xforce").cast("double"), F.col("date_reference")
    )
    x = _xpeers_in_target_component(
        F.coalesce(F.col("xit").cast("double"), F.lit(0.0))
    )
    a = F.coalesce(F.col("avg_idx").cast("double"), F.lit(0.0))
    i = _improved_component(F.col("improved").cast("double"))

    # --- Fixes #1-#3: legacy's DATE-based component count (3 vs 4) ------------
    # 4-component iff date_reference < 2026-05-01 AND NOT the david.fernandez
    # >= 2026-04-01 carve-out. Applied to BOTH week and month grains. A missing
    # improved value folds to 0 via _improved_component (legacy ELSE 0).
    david_carveout = (
        F.coalesce(
            F.col("xplead") == F.lit(DAVID_IMPROVED_SUPPRESSION_XPLEAD), F.lit(False)
        )
        & (date_ref >= F.lit(DAVID_IMPROVED_SUPPRESSION_FROM))
    )
    has_improved = (date_ref < F.lit(FOUR_COMPONENT_CUTOFF)) & ~david_carveout

    num = s + x + a + F.when(has_improved, i).otherwise(F.lit(0.0))
    den = (F.lit(3) + has_improved.cast("int")) * F.lit(100)

    out = base.select(
        F.lit(None).cast("string").alias("agent"),
        F.col("xforce"),
        F.col("xplead"),
        F.lit(None).cast("string").alias("team"),
        F.lit(None).cast("string").alias("squad"),
        F.lit(None).cast("string").alias("district"),
        F.lit(None).cast("string").alias("shift"),
        F.col("date_reference"),
        F.col("date_granularity"),
        F.lit(METRIC_NAME).alias("metric"),
        num.cast("double").alias("numerator"),
        den.cast("double").alias("denominator"),
        F.when(
            den > F.lit(0), num / den * F.lit(100.0)
        ).otherwise(F.lit(None).cast("double")).alias("metric_value"),
    )
    return out.select(*METRIC_COLUMNS)


IO_XFORCE_INDEX_METRIC_SCHEMA: tuple[tuple[str, str], ...] = (
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
