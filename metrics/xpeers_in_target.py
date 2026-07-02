"""xpeers_in_target — the XForce-level Xpeers In Target metric, PySpark.

Covers Core/Fraud (the "main" deck), Social Media, and — since the 2026-06-30
legacy re-export added it to the Content Temp Fix notebook — Content.

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
adherence               ``>= 95``   all teams
ntpj                    ``<= 100``  Core / Fraud
ntpj (SLA)              ``>= 95``   Content (higher-is-better)
normalized_occupancy    ``>= 100``  Core / Fraud / SM / Content
quality                 ``>= 95``   Core / Fraud / Social Media
content_csat            ``>= 95``   Content (its Quality)
tnps                    ``>= 88``   Social Media
wows                    ``>= 5``    Social Media
======================  ==========  =============================

Content NTPJ is the SLA-weighted compliance metric (bounded ≤ 100,
higher-is-better), so its in-target rule is ``>= 95`` — with one legacy quirk:
the Content **XPLead** roll-up flags ``>= 100`` instead (Content Temp Fix
``ntpj_sla_old_xpleads_monthly``, L2341 — almost certainly a copy-paste of the
NOcc threshold). Reproduced pre-cutover; fixed to ``>= 95`` from 2026-07-01.

An agent counts toward a component's **denominator** when they have a row for
that metric in the bucket (``COUNT(DISTINCT agent)``), and toward the
**numerator** only when their ``metric_value`` clears the threshold (NULLs fail).

Deck grouping (parity with legacy — Fix #5)
-------------------------------------------
Legacy runs a **separate notebook per deck**, and the main deck
(``internal_ops_performance_2026``) merges Core + Fraud with **no team column**,
grouping the XForce roll-up by ``(xforce, xplead)`` across both teams. So we group
by a synthetic **deck** — ``core`` / ``fraud`` / NULL-team → ``main``;
``social media`` → ``sm``; ``content`` → ``content`` — NOT by ``team``. This
merges a cross-team XForce (agents in both Core and Fraud) into one ``main`` row
(legacy emits one), while a cross-*deck* XForce (Core + Social-Media agents)
stays split into a ``main`` and an ``sm`` row. ``team`` is left NULL on output
(legacy carries none; downstream ``xforce_index`` joins on ``xforce`` alone).

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
* **Content (both grains)**: rows exist only from ``date_reference >=
  2026-02-01`` (the legacy Content deck's save filter permanently drops
  January 2026); ``+ NTPJ`` and ``+ NO`` from ``>= 2026-03-01`` (in legacy this
  is data-driven — the Content SLA-NTPJ and NOcc agent rows start in March — we
  gate it explicitly because our base tables carry earlier rows, e.g. Feb NOcc).
  CSAT joins wherever ``io_content_csat_metric`` has a row (surveys arrive in
  monthly batches, so weekly CSAT buckets exist only on a handful of Mondays —
  matching legacy's sparse ``qa_xforce`` weekly rows).

Adherence + NTPJ (main), Adherence + tNPS + WoWs (Social Media), and Adherence +
CSAT (Content) are always in scope.

Coalescing (parity with legacy — Fix #3)
----------------------------------------
* **main deck (Core / Fraud)**: Adherence and NTPJ are **not** coalesced — a
  missing NTPJ match yields NULL, so the whole XForce/XPLead row carries NULL
  numerator/denominator/metric_value (the row is still emitted; legacy keeps it).
  Quality / NO are ``COALESCE(..., 0)``.
* **Social Media / Content**: every component is ``COALESCE(..., 0)`` — a
  missing component contributes 0, never NULL.

Granularity & floor (Fix #1)
----------------------------
Legacy materialized only **week + month** (the ``*_monthly`` + ``*_weekly`` views
are the only ones built/unioned). For ``date_reference < 2026-07-01`` we emit
only those two grains. Buckets whose month is before 2026 are dropped (the first
weekly bucket is the Monday ``2026-01-05``; the ``2025-12-29`` partial week is
excluded, matching legacy ``MIN(date_reference) = 2026-01``).

**Content XPLead is month-only pre-cutover**: the legacy XPLead base excludes
``week`` from its granularity list (Content Temp Fix L6906), so its weekly view
is empty — we mirror that with a month-only filter on the Content XPLead grain.

The legacy Content table also carries six stray ``2025-12-01`` rows (one
adherence-driver agent bucketed into Dec-2025 by an upstream bad date, kept by
the save filter's ``date_reference < '2026-01-01'`` branch). That is an upstream
data artifact — our adherence table has no Dec-2025 Content rows, and the
``ERA_FLOOR`` would drop them anyway — deliberately NOT reproduced.

Output grain (Fix #4 — SM/Content squad/district roll-ups)
----------------------------------------------------------
Six metric names land in the same table:

* ``xpeers_in_target``                 — XForce grain (legacy ``*_xforce``).
* ``xpeers_in_target_xplead``          — XPLead roll-up (``xforce`` NULL).
* ``xpeers_in_target_squad``           — **SM + Content**, the XForce rows rolled
  up by ``squad`` (legacy ``*_xforce_squad``). Both carry a NULL squad, so this
  collapses to one total-deck row per (date, granularity).
* ``xpeers_in_target_district``        — **SM + Content**, by ``district``
  (legacy ``*_xforce_district``); also degenerate (NULL key).
* ``xpeers_in_target_xplead_squad``    — **SM + Content**, the XPLead rows rolled
  up by ``squad`` (legacy ``*_xplead_squad``).
* ``xpeers_in_target_xplead_district`` — **SM + Content**, by ``district``
  (legacy ``*_xplead_district``).

The two decks roll up **differently** (each matching its own legacy notebook):
SM sums the in-target/total counts (``numerator``/``denominator``) and divides;
Content averages the grain rows' ``metric_value`` (legacy ``SUM(metric_value) AS
numerator, COUNT(DISTINCT agent) AS denominator, AVG(metric_value)`` — and since
``agent`` is NULL on every grain row, the Content denominator is the constant
0 while ``metric_value`` is still the non-NULL average; Content Temp Fix
L5641-5730 / L7029-7098).

For every row ``agent`` / ``shift`` are NULL, and ``squad`` / ``district`` are
NULL except on the squad/district roll-up variants, which carry the deck's
group label (SM: ``social``; Content: ``enablement``/``content``) so the two
decks' same-named rows stay distinguishable in the single metric table (the
degenerate roll-up key is also NULL), ``numerator`` = targets achieved,
``denominator`` = total targets, ``metric_value`` = ``numerator / denominator *
100`` (except the Content roll-ups above).

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
   2–5/month after — XPLead reassignments concentrate at onboarding). The
   Content deck has the same on-xforce join but a single XPLead in practice,
   so no fan-out there.
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
# SM + Content degenerate roll-ups (legacy xpeers_in_target_xforce_{squad,district}
# / xpeers_in_target_xplead_{squad,district}).
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
CONTENT = "content"

# Synthetic decks (legacy runs a notebook per deck; the main deck merges
# Core + Fraud + NULL-team support squads). We group by deck, not team.
DECK_MAIN = "main"
DECK_SM = "sm"
DECK_CONTENT = "content"

# Content rows exist only from Feb 2026 — the legacy Content deck's save filter
# (`date_reference < '2026-01-01' OR date_reference >= '2026-02-01'`) drops
# January 2026 permanently (Content Temp Fix, "[Temp Fix] Joins and Save").
CONTENT_FLOOR: date = date(2026, 2, 1)

# component column (in the metric tables) -> ("ge"|"le", threshold)
_TARGETS: dict[str, tuple[str, float]] = {
    "adherence": ("ge", 95.0),
    "ntpj": ("le", 100.0),  # Core/Fraud duration NTPJ; Content overrides in _passed
    "normalized_occupancy": ("ge", 100.0),
    "quality": ("ge", 95.0),
    "content_csat": ("ge", 95.0),
    "tnps": ("ge", 88.0),
    "wows": ("ge", 5.0),
}

# Content SLA-NTPJ is higher-is-better: in-target is >= 95 (Content Temp Fix
# ntpj_sla_old_xforces, L2301). Legacy quirk: the Content XPLead roll-up flags
# >= 100 instead (ntpj_sla_old_xpleads_monthly, L2341) — reproduced pre-cutover,
# fixed to >= 95 from the 2026-07-01 cutover.
CONTENT_NTPJ_THRESHOLD = 95.0
CONTENT_NTPJ_XPLEAD_LEGACY_THRESHOLD = 100.0

# Source tables, in the order the public functions accept them.
_SOURCE_ORDER: tuple[str, ...] = (
    "ntpj",
    "normalized_occupancy",
    "quality",
    "content_csat",
    "tnps",
    "wows",
)


def _deck_col() -> Column:
    """Map ``team`` to its deck (``main`` = core/fraud/NULL; ``sm``; ``content``)."""
    team = F.lower(F.col("team"))
    return (
        F.when(team.isin(list(CORE_FRAUD)) | team.isNull(), F.lit(DECK_MAIN))
        .when(team == F.lit(SOCIAL_MEDIA), F.lit(DECK_SM))
        .when(team == F.lit(CONTENT), F.lit(DECK_CONTENT))
        .otherwise(F.lit("other"))
    )


def _passed(name: str, grain: str) -> Column:
    """The per-row in-target predicate for one component (deck-aware for NTPJ).

    Content NTPJ is the SLA-weighted compliance metric — higher-is-better, so its
    rule is ``>= 95`` rather than the main deck's ``<= 100``. On the XPLead grain
    legacy flags ``>= 100`` (the quirk documented on the module constants);
    reproduced for ``date_reference < 2026-07-01``.
    """
    comparator, threshold = _TARGETS[name]
    mv = F.col("metric_value").cast("double")
    default = mv >= F.lit(threshold) if comparator == "ge" else mv <= F.lit(threshold)
    if name != "ntpj":
        return default
    if grain == "xforce":
        content = mv >= F.lit(CONTENT_NTPJ_THRESHOLD)
    else:  # xplead — legacy >= 100 quirk pre-cutover
        content = F.when(
            F.col("date_reference") < F.lit(LEGACY_CUTOVER),
            mv >= F.lit(CONTENT_NTPJ_XPLEAD_LEGACY_THRESHOLD),
        ).otherwise(mv >= F.lit(CONTENT_NTPJ_THRESHOLD))
    return F.when(F.col("_deck") == F.lit(DECK_CONTENT), content).otherwise(default)


def _component_counts(
    df: DataFrame, name: str, *, keys: list[str], grain: str
) -> DataFrame:
    """Per-group in-target / total agent counts for one component metric table.

    Adds the synthetic ``_deck`` and keeps only the main / SM / Content decks.
    ``{name}_in`` = agents whose ``metric_value`` clears the threshold (NULL
    fails); ``{name}_tot`` = distinct agents present for the metric in the bucket.
    """
    work = (
        df.withColumn("_deck", _deck_col())
        .filter(F.col("_deck").isin(DECK_MAIN, DECK_SM, DECK_CONTENT))
        .withColumn(
            "_pass",
            F.when(
                F.coalesce(_passed(name, grain), F.lit(False)), F.lit(1)
            ).otherwise(F.lit(0)),
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
    SM + Content squad/district roll-ups (Fix #4).
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

    base = _component_counts(adherence, "adherence", keys=base_keys, grain=grain)
    for name in _SOURCE_ORDER:
        df = sources.get(name)
        if df is not None and len(df.take(1)) != 0:
            base = base.join(
                _component_counts(df, name, keys=join_keys, grain=grain),
                on=join_keys, how="left",
            )
        else:
            base = base.withColumn(f"{name}_in", F.lit(None).cast("double")).withColumn(
                f"{name}_tot", F.lit(None).cast("double")
            )

    is_main = F.col("_deck") == F.lit(DECK_MAIN)
    is_sm = F.col("_deck") == F.lit(DECK_SM)
    is_content = F.col("_deck") == F.lit(DECK_CONTENT)
    date_ref = F.col("date_reference")
    gran = F.col("date_granularity")

    # Fix #1: week + month only pre-cutover; month-of-bucket >= 2026-01 (raw
    # date_reference floor — exact for week+month). (Deck filter already applied
    # in _component_counts.) Content adds its own floor (no Jan-2026 rows — the
    # legacy save filter) and, on the XPLead grain, month-only pre-cutover (the
    # legacy XPLead base excludes 'week', so its weekly view is empty).
    pre_cutover = date_ref < F.lit(LEGACY_CUTOVER)
    base = base.filter(
        ((~pre_cutover) | gran.isin("week", "month")) & (date_ref >= F.lit(ERA_FLOOR))
    )
    base = base.filter((~is_content) | (date_ref >= F.lit(CONTENT_FLOOR)))
    if grain == "xplead":
        base = base.filter(
            (~is_content) | (~pre_cutover) | (gran == F.lit("month"))
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
    # Content: SLA-NTPJ + NOcc join from March (data-driven in legacy — its base
    # rows start then; explicit here because our tables carry earlier rows).
    content_ntpj_ok = date_ref >= F.lit(MAR_1)
    content_no_ok = date_ref >= F.lit(MAR_1)

    def opt(prefix: str, suffix: str, ok: Column) -> Column:
        """A COALESCE(.., 0) component that is only added inside its era."""
        return F.when(ok, F.coalesce(F.col(f"{prefix}_{suffix}"), F.lit(0.0))).otherwise(
            F.lit(0.0)
        )

    # Fix #3: the main deck does NOT coalesce adherence/ntpj (NULL propagates and
    # the row carries NULL value); Social Media and Content coalesce everything
    # to 0 (the Content base is a plain LEFT JOIN + COALESCE, L5523-5529).
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

    # Content: adherence + CSAT always (CSAT presence-driven), NTPJ + NOcc from
    # March. CSAT plays the qa role (legacy Content 'quality' IS the CSAT).
    content_num = c("adherence_in") + opt("ntpj", "in", content_ntpj_ok) + opt(
        "normalized_occupancy", "in", content_no_ok
    ) + c("content_csat_in")
    content_den = c("adherence_tot") + opt("ntpj", "tot", content_ntpj_ok) + opt(
        "normalized_occupancy", "tot", content_no_ok
    ) + c("content_csat_tot")

    num = (
        F.when(is_main, main_num)
        .when(is_sm, sm_num)
        .when(is_content, content_num)
        .otherwise(F.lit(None).cast("double"))
    )
    den = (
        F.when(is_main, main_den)
        .when(is_sm, sm_den)
        .when(is_content, content_den)
        .otherwise(F.lit(None).cast("double"))
    )
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
    content_rollups = _content_rollups(
        scored, squad_metric=squad_metric, district_metric=district_metric
    )
    return grain_rows.unionByName(rollups).unionByName(content_rollups)


def _rollup_rows(
    agg: DataFrame,
    *,
    squad_metric: str,
    district_metric: str,
    group_squad: str,
    group_district: str,
) -> DataFrame:
    """Emit one squad + one district row per aggregated bucket.

    The ``squad`` / ``district`` columns carry the deck's group label
    (SM: ``social``/``social``; Content: ``enablement``/``content`` — the same
    convention nuvinhos_performance uses). Legacy leaves these NULL, but its
    decks live in separate tables; in our single metric table both decks emit
    the SAME metric names, so without the label the SM and Content rows for a
    (metric, grain, date) are indistinguishable (a real collision found on the
    2026-07-02 verification).
    """
    base = agg.select(
        F.lit(None).cast("string").alias("agent"),
        F.lit(None).cast("string").alias("xforce"),
        F.lit(None).cast("string").alias("xplead"),
        F.lit(None).cast("string").alias("team"),
        F.lit(group_squad).alias("squad"),
        F.lit(group_district).alias("district"),
        F.lit(None).cast("string").alias("shift"),
        F.col("date_reference"),
        F.col("date_granularity"),
        F.col("numerator"),
        F.col("denominator"),
        F.col("metric_value"),
    )
    squad = base.withColumn("metric", F.lit(squad_metric)).select(*METRIC_COLUMNS)
    district = base.withColumn("metric", F.lit(district_metric)).select(*METRIC_COLUMNS)
    return squad.unionByName(district)


def _sm_rollups(scored: DataFrame, *, squad_metric: str, district_metric: str) -> DataFrame:
    """Fix #4: SM degenerate squad + district roll-ups of a grain's rows.

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
    agg = agg.withColumn(
        "metric_value",
        F.when(
            F.col("denominator") > F.lit(0),
            F.col("numerator") / F.col("denominator") * F.lit(100.0),
        ).otherwise(F.lit(None).cast("double")),
    )
    return _rollup_rows(
        agg,
        squad_metric=squad_metric,
        district_metric=district_metric,
        group_squad="social",
        group_district="social",
    )


def _content_rollups(
    scored: DataFrame, *, squad_metric: str, district_metric: str
) -> DataFrame:
    """Fix #4 (Content flavor): degenerate roll-ups that AVERAGE the grain rows.

    The Content deck rolls up differently from SM: ``numerator =
    SUM(metric_value)``, ``denominator = COUNT(DISTINCT agent)`` — the constant 0,
    since ``agent`` is NULL on every XForce/XPLead row — and ``metric_value =
    AVG(metric_value)`` (Content Temp Fix L5641-5730 / L7029-7098). Note the
    denominator-0 rows still carry a non-NULL average — a reproduced legacy quirk.
    """
    ct = scored.filter(F.col("_deck") == F.lit(DECK_CONTENT))
    agg = ct.groupBy("date_reference", "date_granularity").agg(
        F.sum("metric_value").cast("double").alias("numerator"),
        F.avg("metric_value").cast("double").alias("metric_value"),
    )
    agg = agg.withColumn("denominator", F.lit(0.0))
    return _rollup_rows(
        agg,
        squad_metric=squad_metric,
        district_metric=district_metric,
        group_squad="enablement",
        group_district="content",
    )


def compute_xpeers_in_target(
    adherence: DataFrame,
    ntpj: DataFrame | None = None,
    normalized_occupancy: DataFrame | None = None,
    quality: DataFrame | None = None,
    tnps: DataFrame | None = None,
    wows: DataFrame | None = None,
    content_csat: DataFrame | None = None,
) -> DataFrame:
    """Compute XForce Xpeers In Target (``xpeers_in_target`` + squad/district).

    Args:
        adherence: ``io_adherence_metric`` — the driver (defines the XForce
            universe; ``xplead`` rides along).
        ntpj / normalized_occupancy / quality / tnps / wows / content_csat: the
            corresponding ``io_*_metric`` tables. Any may be ``None``/empty.
            ``content_csat`` plays the Quality role for the Content deck.

    Returns:
        Tidy long-format metric rows (XForce grain, ``team`` NULL) plus the
        SM + Content ``xpeers_in_target_squad`` / ``xpeers_in_target_district``.
    """
    sources = {
        "ntpj": ntpj,
        "normalized_occupancy": normalized_occupancy,
        "quality": quality,
        "content_csat": content_csat,
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
    content_csat: DataFrame | None = None,
) -> DataFrame:
    """Compute XPLead Xpeers In Target (``xpeers_in_target_xplead`` + roll-ups).

    Identical target/era logic to the XForce version, but the in-target and total
    agent counts are aggregated per ``(deck, xplead)`` instead of per XForce
    (``xforce`` NULL). Appends the SM + Content ``xpeers_in_target_xplead_squad``
    / ``xpeers_in_target_xplead_district`` roll-ups. Content is month-only
    pre-cutover and flags NTPJ ``>= 100`` (the legacy quirk).
    """
    sources = {
        "ntpj": ntpj,
        "normalized_occupancy": normalized_occupancy,
        "quality": quality,
        "content_csat": content_csat,
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
