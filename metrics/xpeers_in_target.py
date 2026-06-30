"""xpeers_in_target — the XForce-level Xpeers In Target metric (Core/Fraud + SM), PySpark.

Measures, per XForce (and per XPLead), the share of its Xpeers' **metric
targets** that were met:

    Xpeers In Target = Σ targets achieved / Σ targets * 100

A "target" is one agent-metric: for every active component metric, each Xpeer
contributes one target (the denominator) and counts as achieved (the numerator)
when their value clears that metric's threshold. **Target ≥ 70%.**

Like the Xpeer Index, this reads the finished **agent-level metric tables**
(``io_*_metric``) rather than a raw table, then aggregates to the XForce.

Component targets (legacy ``*_xforce`` in-target counts)
-------------------------------------------------------
======================  ==========  =============================
component               threshold   teams
======================  ==========  =============================
adherence               ``>= 95``   Core / Fraud / Social Media
ntpj                    ``<= 100``  Core / Fraud
normalized_occupancy    ``>= 100``  Core / Fraud / Social Media
quality                 ``>= 95``   Core / Fraud / Social Media
tnps                    ``>= 88``   Social Media
wows                    ``>= 5``    Social Media
======================  ==========  =============================

An agent counts toward a component's **denominator** when they have a row for
that metric in the bucket (``COUNT(DISTINCT agent)``), and toward the
**numerator** only when their ``metric_value`` clears the threshold (NULLs fail).

Deck grouping (parity with legacy — Fix #5)
-------------------------------------------
Legacy runs a **separate notebook per deck**, and the main deck
(``internal_ops_performance_2026``) merges Core + Fraud with **no team column**,
grouping the XForce roll-up by ``(xforce, xplead)`` across both teams. So we group
by a synthetic **deck** — ``core`` / ``fraud`` / NULL-team → ``main``;
``social media`` → ``sm`` — NOT by ``team``. This merges a cross-team XForce
(agents in both Core and Fraud) into one ``main`` row (legacy emits one), while a
cross-*deck* XForce (Core + Social-Media agents) stays split into a ``main`` and
an ``sm`` row. ``team`` is left NULL on output (legacy carries none; downstream
``xforce_index`` joins on ``xforce`` alone).

Era windows (parity with legacy — Fix #2)
-----------------------------------------
The component roster grew over the 2026 rollout. Legacy gates Quality / NO on the
**raw** ``date_reference`` (the bucket's Monday for weeks), and the boundary
differs by grain/deck — so a January *weekly* bucket can already carry Quality
for some paths:

* **main deck, XForce grain**: ``+ Quality`` from ``date_reference >=
  2026-02-01``; ``+ NO`` from ``>= 2026-03-01``. (Month-anchored cutover.)
* **main deck, XPLead grain** and **Social Media (both grains)**:
  ``+ Quality`` from ``date_reference > 2026-01-01`` (so **every** January weekly
  bucket includes it); ``+ NO`` from ``date_reference > 2026-02-01``.

Adherence + NTPJ (main) and Adherence + tNPS + WoWs (Social Media) are always in
scope.

Coalescing (parity with legacy — Fix #3)
----------------------------------------
* **main deck (Core / Fraud)**: Adherence and NTPJ are **not** coalesced — a
  missing NTPJ match yields NULL, so the whole XForce/XPLead row carries NULL
  numerator/denominator/metric_value (the row is still emitted; legacy keeps it).
  Quality / NO are ``COALESCE(..., 0)``.
* **Social Media**: every component is ``COALESCE(..., 0)`` — a missing component
  contributes 0, never NULL.

Granularity & floor (Fix #1)
----------------------------
Legacy materialized only **week + month** (the ``*_monthly`` + ``*_weekly`` views
are the only ones built/unioned). For ``date_reference < 2026-07-01`` we emit
only those two grains. Buckets whose month is before 2026 are dropped (the first
weekly bucket is the Monday ``2026-01-05``; the ``2025-12-29`` partial week is
excluded, matching legacy ``MIN(date_reference) = 2026-01``).

**Content has no Xpeers In Target** (the legacy Content notebook doesn't build
it), so Content XForces are excluded.

Output grain (Fix #4 — SM squad/district roll-ups)
--------------------------------------------------
Six metric names land in the same table:

* ``xpeers_in_target``                 — XForce grain (legacy ``*_xforce``).
* ``xpeers_in_target_xplead``          — XPLead roll-up (``xforce`` NULL).
* ``xpeers_in_target_squad``           — **SM only**, the XForce rows summed by
  ``squad`` (legacy ``*_xforce_squad``). SM carries a NULL squad, so this
  collapses to one total-SM row per (date, granularity).
* ``xpeers_in_target_district``        — **SM only**, the XForce rows summed by
  ``district`` (legacy ``*_xforce_district``); also degenerate (NULL key).
* ``xpeers_in_target_xplead_squad``    — **SM only**, the XPLead rows summed by
  ``squad`` (legacy ``*_xplead_squad``).
* ``xpeers_in_target_xplead_district`` — **SM only**, the XPLead rows summed by
  ``district`` (legacy ``*_xplead_district``).

For every row ``agent`` / ``squad`` / ``district`` / ``shift`` are NULL (except
the degenerate key, which is also NULL for SM), ``numerator`` = targets achieved,
``denominator`` = total targets, ``metric_value`` = ``numerator / denominator *
100``.

Known parity bounds (pre-cutover)
---------------------------------
This module computes the metric **correctly** (per ``(xforce, xplead)``); two
legacy divergences are intentionally NOT reproduced and bound byte-for-byte
parity to the early months (both peak in January):

1. **Legacy xplead fan-out bug** — legacy ``xpeers_in_target_base`` LEFT JOINs
   each component to adherence on ``xforce`` *only* (not ``xplead``), so an
   XForce that spans multiple XPLeads gets a cross-product (its adherence is
   multiplied and its components summed across XPLeads). We aggregate once per
   ``(xforce, xplead)`` instead. Affects only multi-XPLead XForces (~36 in Jan,
   2–5/month after — XPLead reassignments concentrate at onboarding).
2. **NTPJ base-metric early-month gap** — the in-target counts inherit the NTPJ
   metric, which is not yet at parity in the early months (the deferred NTPJ
   early-month benchmark / improved_benchmarks work). This bounds every month
   (Jan ~11 avg abs diff → Apr/May ~0.5). Apr–Jun (where NTPJ is at parity) match
   legacy; Jan–Mar cannot until that base lands. Social Media is at parity
   throughout (~2 avg abs diff, bounded only by the SM base metrics).
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from metric_utils import METRIC_COLUMNS, empty_metric_frame

METRIC_NAME = "xpeers_in_target"  # XForce grain (legacy xpeers_in_target_xforce)
XPLEAD_METRIC_NAME = "xpeers_in_target_xplead"  # XPLead roll-up grain
# SM-only degenerate roll-ups (legacy xpeers_in_target_xforce_{squad,district} /
# xpeers_in_target_xplead_{squad,district}).
SQUAD_METRIC_NAME = "xpeers_in_target_squad"
DISTRICT_METRIC_NAME = "xpeers_in_target_district"
XPLEAD_SQUAD_METRIC_NAME = "xpeers_in_target_xplead_squad"
XPLEAD_DISTRICT_METRIC_NAME = "xpeers_in_target_xplead_district"

# Legacy-parity cutover. Before it: week + month only (Fix #1).
LEGACY_CUTOVER: date = date(2026, 7, 1)

# Era floor — the metric is a 2026 construct; the month of the bucket must be
# >= 2026-01 (drops the 2025-12-29 partial week, matching legacy).
ERA_FLOOR: date = date(2026, 1, 1)

# Era boundaries (compared against the RAW date_reference; see module docstring).
JAN_1: date = date(2026, 1, 1)
FEB_1: date = date(2026, 2, 1)
MAR_1: date = date(2026, 3, 1)

CORE_FRAUD = ("core", "fraud")
SOCIAL_MEDIA = "social media"

# Synthetic decks (legacy runs a notebook per deck; the main deck merges
# Core + Fraud + NULL-team support squads). We group by deck, not team.
DECK_MAIN = "main"
DECK_SM = "sm"

# component column (in the metric tables) -> ("ge"|"le", threshold)
_TARGETS: dict[str, tuple[str, float]] = {
    "adherence": ("ge", 95.0),
    "ntpj": ("le", 100.0),
    "normalized_occupancy": ("ge", 100.0),
    "quality": ("ge", 95.0),
    "tnps": ("ge", 88.0),
    "wows": ("ge", 5.0),
}

# Source tables, in the order the public functions accept them.
_SOURCE_ORDER: tuple[str, ...] = (
    "ntpj",
    "normalized_occupancy",
    "quality",
    "tnps",
    "wows",
)


def _deck_col() -> Column:
    """Map ``team`` to its deck (``main`` = core/fraud/NULL; ``sm`` = social media)."""
    team = F.lower(F.col("team"))
    return (
        F.when(team.isin(list(CORE_FRAUD)) | team.isNull(), F.lit(DECK_MAIN))
        .when(team == F.lit(SOCIAL_MEDIA), F.lit(DECK_SM))
        .otherwise(F.lit("other"))
    )


def _component_counts(df: DataFrame, name: str, *, keys: list[str]) -> DataFrame:
    """Per-group in-target / total agent counts for one component metric table.

    Adds the synthetic ``_deck`` and keeps only the main / SM decks. ``{name}_in``
    = agents whose ``metric_value`` clears the threshold (NULL fails);
    ``{name}_tot`` = distinct agents present for the metric in the bucket.
    """
    comparator, threshold = _TARGETS[name]
    mv = F.col("metric_value").cast("double")
    passed = mv >= F.lit(threshold) if comparator == "ge" else mv <= F.lit(threshold)
    work = (
        df.withColumn("_deck", _deck_col())
        .filter(F.col("_deck").isin(DECK_MAIN, DECK_SM))
        .withColumn(
            "_pass",
            F.when(F.coalesce(passed, F.lit(False)), F.lit(1)).otherwise(F.lit(0)),
        )
    )
    return work.groupBy(*keys).agg(
        F.sum("_pass").cast("double").alias(f"{name}_in"),
        F.countDistinct("agent").cast("double").alias(f"{name}_tot"),
    )


def _compute_grain(
    adherence: DataFrame,
    sources: dict[str, DataFrame | None],
    *,
    grain: str,
    squad_metric: str,
    district_metric: str,
) -> DataFrame:
    """Targets-achieved / total-targets, aggregated to ``grain`` (xforce|xplead).

    Groups by the synthetic **deck** (not team) so a cross-team XForce merges into
    one ``main`` row. Returns the XForce/XPLead rows (``team`` NULL) plus the
    SM-only squad/district roll-ups (Fix #4).
    """
    spark = adherence.sparkSession
    if len(adherence.take(1)) == 0:
        return empty_metric_frame(spark)

    if grain == "xforce":
        # Group with xplead so it rides along; join components on (deck, xforce).
        base_keys = ["_deck", "xforce", "xplead", "date_reference", "date_granularity"]
        join_keys = ["_deck", "xforce", "date_reference", "date_granularity"]
        metric_name = METRIC_NAME
    elif grain == "xplead":
        base_keys = ["_deck", "xplead", "date_reference", "date_granularity"]
        join_keys = base_keys
        metric_name = XPLEAD_METRIC_NAME
    else:  # pragma: no cover - guarded by callers
        raise ValueError(f"unknown grain: {grain!r}")

    base = _component_counts(adherence, "adherence", keys=base_keys)
    for name in _SOURCE_ORDER:
        df = sources.get(name)
        if df is not None and len(df.take(1)) != 0:
            base = base.join(_component_counts(df, name, keys=join_keys),
                             on=join_keys, how="left")
        else:
            base = base.withColumn(f"{name}_in", F.lit(None).cast("double")).withColumn(
                f"{name}_tot", F.lit(None).cast("double")
            )

    is_main = F.col("_deck") == F.lit(DECK_MAIN)
    is_sm = F.col("_deck") == F.lit(DECK_SM)
    date_ref = F.col("date_reference")
    gran = F.col("date_granularity")

    # Fix #1: week + month only pre-cutover; month-of-bucket >= 2026-01 (raw
    # date_reference floor — exact for week+month). (Deck filter already applied
    # in _component_counts.)
    pre_cutover = date_ref < F.lit(LEGACY_CUTOVER)
    base = base.filter(
        ((~pre_cutover) | gran.isin("week", "month")) & (date_ref >= F.lit(ERA_FLOOR))
    )
    if len(base.take(1)) == 0:
        return empty_metric_frame(spark)

    # Fix #2: per-grain/deck era boundaries on the RAW date_reference.
    if grain == "xforce":
        qa_ok = (is_main & (date_ref >= F.lit(FEB_1))) | (is_sm & (date_ref > F.lit(JAN_1)))
        no_ok = (is_main & (date_ref >= F.lit(MAR_1))) | (is_sm & (date_ref > F.lit(FEB_1)))
    else:  # xplead — main and SM share the > Jan-1 / > Feb-1 boundary
        qa_ok = date_ref > F.lit(JAN_1)
        no_ok = date_ref > F.lit(FEB_1)

    def opt(prefix: str, suffix: str, ok: Column) -> Column:
        """A COALESCE(.., 0) component that is only added inside its era."""
        return F.when(ok, F.coalesce(F.col(f"{prefix}_{suffix}"), F.lit(0.0))).otherwise(
            F.lit(0.0)
        )

    # Fix #3: the main deck does NOT coalesce adherence/ntpj (NULL propagates and
    # the row carries NULL value); Social Media coalesces everything to 0.
    main_num = F.col("adherence_in") + F.col("ntpj_in") + opt(
        "quality", "in", qa_ok
    ) + opt("normalized_occupancy", "in", no_ok)
    main_den = F.col("adherence_tot") + F.col("ntpj_tot") + opt(
        "quality", "tot", qa_ok
    ) + opt("normalized_occupancy", "tot", no_ok)

    def c(col: str) -> Column:
        return F.coalesce(F.col(col), F.lit(0.0))

    sm_num = c("adherence_in") + c("tnps_in") + c("wows_in") + opt(
        "quality", "in", qa_ok
    ) + opt("normalized_occupancy", "in", no_ok)
    sm_den = c("adherence_tot") + c("tnps_tot") + c("wows_tot") + opt(
        "quality", "tot", qa_ok
    ) + opt("normalized_occupancy", "tot", no_ok)

    num = F.when(is_main, main_num).when(is_sm, sm_num).otherwise(F.lit(None).cast("double"))
    den = F.when(is_main, main_den).when(is_sm, sm_den).otherwise(F.lit(None).cast("double"))
    mv = F.when(den > F.lit(0), num / den * F.lit(100.0)).otherwise(
        F.lit(None).cast("double")
    )

    # Score frame retains _deck so the SM roll-ups can be derived before we null
    # team on the emitted rows.
    xforce_col = F.col("xforce") if grain == "xforce" else F.lit(None).cast("string")
    scored = base.select(
        F.col("_deck"),
        xforce_col.alias("xforce"),
        F.col("xplead"),
        F.col("date_reference"),
        F.col("date_granularity"),
        num.cast("double").alias("numerator"),
        den.cast("double").alias("denominator"),
        mv.alias("metric_value"),
    )

    grain_rows = scored.select(
        F.lit(None).cast("string").alias("agent"),
        F.col("xforce"),
        F.col("xplead"),
        F.lit(None).cast("string").alias("team"),
        F.lit(None).cast("string").alias("squad"),
        F.lit(None).cast("string").alias("district"),
        F.lit(None).cast("string").alias("shift"),
        F.col("date_reference"),
        F.col("date_granularity"),
        F.lit(metric_name).alias("metric"),
        F.col("numerator"),
        F.col("denominator"),
        F.col("metric_value"),
    ).select(*METRIC_COLUMNS)

    rollups = _sm_rollups(scored, squad_metric=squad_metric, district_metric=district_metric)
    return grain_rows.unionByName(rollups)


def _sm_rollups(scored: DataFrame, *, squad_metric: str, district_metric: str) -> DataFrame:
    """Fix #4: SM-only degenerate squad + district roll-ups of a grain's rows.

    Sums numerator/denominator across the Social-Media (``_deck == sm``) rows of
    the scored grain per (date_reference, date_granularity). SM carries a NULL
    squad/district, so legacy ``GROUP BY ALL`` collapses to one row per (date,
    granularity); the squad and district variants share the same totals (the only
    difference is the metric name and which — already NULL — key column nominally
    holds the group).
    """
    sm = scored.filter(F.col("_deck") == F.lit(DECK_SM))
    agg = sm.groupBy("date_reference", "date_granularity").agg(
        F.sum("numerator").cast("double").alias("numerator"),
        F.sum("denominator").cast("double").alias("denominator"),
    )
    base = agg.select(
        F.lit(None).cast("string").alias("agent"),
        F.lit(None).cast("string").alias("xforce"),
        F.lit(None).cast("string").alias("xplead"),
        F.lit(None).cast("string").alias("team"),
        F.lit(None).cast("string").alias("squad"),
        F.lit(None).cast("string").alias("district"),
        F.lit(None).cast("string").alias("shift"),
        F.col("date_reference"),
        F.col("date_granularity"),
        F.col("numerator"),
        F.col("denominator"),
        F.when(
            F.col("denominator") > F.lit(0),
            F.col("numerator") / F.col("denominator") * F.lit(100.0),
        )
        .otherwise(F.lit(None).cast("double"))
        .alias("metric_value"),
    )
    squad = base.withColumn("metric", F.lit(squad_metric)).select(*METRIC_COLUMNS)
    district = base.withColumn("metric", F.lit(district_metric)).select(*METRIC_COLUMNS)
    return squad.unionByName(district)


def compute_xpeers_in_target(
    adherence: DataFrame,
    ntpj: DataFrame | None = None,
    normalized_occupancy: DataFrame | None = None,
    quality: DataFrame | None = None,
    tnps: DataFrame | None = None,
    wows: DataFrame | None = None,
) -> DataFrame:
    """Compute XForce Xpeers In Target (``xpeers_in_target`` + SM squad/district).

    Args:
        adherence: ``io_adherence_metric`` — the driver (defines the XForce
            universe; ``xplead`` rides along).
        ntpj / normalized_occupancy / quality / tnps / wows: the corresponding
            ``io_*_metric`` tables. Any may be ``None``/empty.

    Returns:
        Tidy long-format metric rows (XForce grain, ``team`` NULL) plus the
        SM-only ``xpeers_in_target_squad`` / ``xpeers_in_target_district``.
    """
    sources = {
        "ntpj": ntpj,
        "normalized_occupancy": normalized_occupancy,
        "quality": quality,
        "tnps": tnps,
        "wows": wows,
    }
    return _compute_grain(
        adherence, sources, grain="xforce",
        squad_metric=SQUAD_METRIC_NAME, district_metric=DISTRICT_METRIC_NAME,
    )


def compute_xpeers_in_target_xplead(
    adherence: DataFrame,
    ntpj: DataFrame | None = None,
    normalized_occupancy: DataFrame | None = None,
    quality: DataFrame | None = None,
    tnps: DataFrame | None = None,
    wows: DataFrame | None = None,
) -> DataFrame:
    """Compute XPLead Xpeers In Target (``xpeers_in_target_xplead`` + SM roll-ups).

    Identical target/era logic to the XForce version, but the in-target and total
    agent counts are aggregated per ``(deck, xplead)`` instead of per XForce
    (``xforce`` NULL). Appends the SM-only ``xpeers_in_target_xplead_squad`` /
    ``xpeers_in_target_xplead_district`` roll-ups.
    """
    sources = {
        "ntpj": ntpj,
        "normalized_occupancy": normalized_occupancy,
        "quality": quality,
        "tnps": tnps,
        "wows": wows,
    }
    return _compute_grain(
        adherence, sources, grain="xplead",
        squad_metric=XPLEAD_SQUAD_METRIC_NAME, district_metric=XPLEAD_DISTRICT_METRIC_NAME,
    )


IO_XPEERS_IN_TARGET_METRIC_SCHEMA: tuple[tuple[str, str], ...] = (
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
