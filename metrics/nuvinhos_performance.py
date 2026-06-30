"""nuvinhos_performance — the Nuvinhos Performance composite (all decks), PySpark.

Compares the average **Xpeer Index** of *Nuvinhos* (recently-hired / recently-
moved agents) against the average Xpeer Index of tenured ("old") agents, so
managers can see whether new agents are ramping:

    Nuvinhos Performance = avg Index(Nuvinhos) / avg Index(Old) * 100

It is an **index-level** composite (no agent grain). Like Average Xpeer Index it
reads a finished metric (here ``io_xpeer_index_metric`` — legacy ``index_agents``
``metric = 'index_agent'``) plus the roster tenure from ``agent_information``.

Who is a Nuvinho? (legacy ``nuvinhos_performance_base``)
-------------------------------------------------------
An agent is a *Nuvinho* for a bucket when the bucket's **month** falls in
``[month(change_date), month(change_date) + 2 months]`` — the hire/squad-change
month plus the next two. Everyone else is *old*. The change date is the BDX
``last_change_date`` for the main / Social-Media / S&D decks, and the temp
roster's ``valid_from`` for Content (which, being a constant 2024-01-01, makes
every 2026 Content agent *old* — Content has no Nuvinhos pre-real-roster).

Legacy parity (the hard part — reproduced byte-for-byte before 2026-07-01)
--------------------------------------------------------------------------
Legacy runs a **separate notebook per deck**, and the same metric is built
differently per deck. We reproduce each exactly, era-gated to
``date_reference < 2026-07-01`` (corrections only from the cutover onward).

* **Deck grouping, NOT team.** The main deck merges Core + Fraud with no team
  column (``internal_ops_performance_2026``). The S&D notebook reads that merged
  table back to build the main-deck squad / district roll-ups. So we group by a
  synthetic **deck** (core / fraud / NULL-team → ``main``; social media → ``sm``;
  content → ``content``), never by ``team`` — grouping by team would split a
  cross-team XForce and break parity (mirror ``average_xpeer_index``).

* **Two-level aggregation** (main, sm). Inner: ``AVG(metric_value)`` per
  ``(deck, xforce, xplead, squad, district, date_reference, date_granularity,
  nuvinho)`` cohort — legacy ``GROUP BY ALL`` *including the nuvinho flag*, so a
  cohort with both Nuvinhos and old agents yields TWO inner rows (a 'nuvinho'
  row and an 'old' row). Each row carries ``nuvinhos_average`` (the real AVG on
  the nuvinho row, the ELSE value otherwise) and ``old_average`` (vice-versa).
  Outer: ``AVG(nuvinhos_average)`` / ``AVG(old_average)`` per roll-up key.

* **Per-deck ELSE clause** (the parity-target choice). On the opposite-flag row
  the inner ``nuvinhos_average`` / ``old_average`` is ``ELSE NULL`` for the main
  deck (``internal_ops_performance_2026``) and Content, vs ``ELSE 0`` for Social
  Media and the S&D-derived main-deck squad/district. NULL is ignored by the
  outer ``AVG`` (so the outer mean equals the present cohort's mean); a real 0
  is averaged in, deflating both numerator and denominator. We picked the main
  ``internal_ops_performance_2026`` table (ELSE NULL) over the 'Old' file's
  ``internal_ops_performance_only_2026`` (ELSE 0) for the main-deck XForce roll-
  up, matching what every other composite parities against.

* **Per-deck roll-up gating.** main: XForce (ELSE NULL) + squad + district (ELSE
  0, from S&D). sm: XForce + squad + district (ELSE 0). content: XForce single-
  level (ELSE NULL) + squad + district degenerate (below).

* **Content special path.** (a) XForce roll-up is SINGLE-LEVEL — the inner final
  groups by ``(xforce, xplead, date_reference, date_granularity, nuvinho)`` (no
  squad/district), then the monthly/weekly view passes ``nuvinhos_average`` /
  ``old_average`` straight through (``TRY_DIVIDE`` with NO outer AVG). The
  tenure window uses ``valid_from`` (not ``last_change_date``). (b) Content
  squad/district are DEGENERATE: they read FROM the XForce roll-up output (where
  squad & district are already NULL), ``GROUP BY ALL`` collapsing to a single
  NULL-key bucket per (date, granularity): ``numerator = SUM(metric_value)``,
  ``denominator = COUNT(DISTINCT agent)`` (agent is NULL → 0), ``metric_value =
  AVG(metric_value)``. These NULL-keyed rows are KEPT (not dropped).

* **Scope.** week + month only; floor ``date_reference >= 2025-12-01``; the
  tenure join is monthly (``month(date_reference) = snapshot_month``) and is
  applied to weekly rows too (legacy joins monthly even for weekly grain).

From 2026-07-01 onward the documented corrected formula (a flat agent-level
``mean(Index | Nuvinho) / mean(Index | old)`` per roll-up key, all six
granularities) is emitted instead — era-gated, never switched unconditionally.

Output — tidy long format, one row per roll-up key + (date_reference, granularity)
----------------------------------------------------------------------------------
``agent, xforce, xplead, team, squad, district, shift, date_reference,
date_granularity, metric, numerator, denominator, metric_value``. ``agent``,
``team`` and ``shift`` are always NULL; the dimensions outside a roll-up's key
are NULL too (e.g. squad/district NULL on the XForce roll-up).
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from metric_utils import METRIC_COLUMNS, empty_metric_frame

METRIC_XFORCE = "nuvinhos_performance"
METRIC_SQUAD = "nuvinhos_performance_squad"
METRIC_DISTRICT = "nuvinhos_performance_district"

# The Xpeer Index rows this metric is derived from (legacy `index_agent`).
XPEER_INDEX_METRIC = "xpeer_index"

# Nuvinho window: the change month plus the next N months (legacy INTERVAL 2 MONTH).
NUVINHO_WINDOW_MONTHS = 2

# Legacy-parity cutover. Before it, reproduce legacy byte-for-byte (per-deck two-
# level agg / ELSE-clause / roll-up gating / Content path / week+month / floor).
# From this date onward the documented flat-mean formula is the corrected output.
LEGACY_CUTOVER: date = date(2026, 7, 1)

# Legacy floor: nuvinhos_performance_base filters date_reference >= '2025-12-01'.
ERA_FLOOR: date = date(2025, 12, 1)

# Deck labels.
CORE = "core"
FRAUD = "fraud"
SOCIAL_MEDIA = "social media"
CONTENT = "content"
DECK_MAIN = "main"
DECK_SM = "sm"
DECK_CONTENT = "content"

_PRE_GRAINS = ("week", "month")


def _deck_col() -> Column:
    """Synthetic deck from ``team`` (legacy = one notebook per deck)."""
    team = F.lower(F.col("team"))
    return (
        F.when(team.isin(CORE, FRAUD) | team.isNull(), F.lit(DECK_MAIN))
        .when(team == F.lit(SOCIAL_MEDIA), F.lit(DECK_SM))
        .when(team == F.lit(CONTENT), F.lit(DECK_CONTENT))
        .otherwise(F.lit("other"))
    )


def _empty_like(df: DataFrame) -> DataFrame:
    return empty_metric_frame(df.sparkSession)


def _out_columns(
    grp: DataFrame,
    *,
    metric_name: str,
    keep_xforce: bool,
    keep_xplead: bool,
    keep_squad: bool,
    keep_district: bool,
) -> DataFrame:
    """Project an aggregated frame to the tidy METRIC_COLUMNS contract.

    ``numerator`` / ``denominator`` / ``metric_value`` must already exist on
    ``grp``; key dimensions outside the roll-up are NULLed.
    """
    null_str = F.lit(None).cast("string")
    return grp.select(
        null_str.alias("agent"),
        (F.col("xforce") if keep_xforce else null_str).alias("xforce"),
        (F.col("xplead") if keep_xplead else null_str).alias("xplead"),
        null_str.alias("team"),
        (F.col("squad") if keep_squad else null_str).alias("squad"),
        (F.col("district") if keep_district else null_str).alias("district"),
        null_str.alias("shift"),
        F.col("date_reference"),
        F.col("date_granularity"),
        F.lit(metric_name).alias("metric"),
        F.col("numerator").cast("double").alias("numerator"),
        F.col("denominator").cast("double").alias("denominator"),
        F.col("metric_value").cast("double").alias("metric_value"),
    ).select(*METRIC_COLUMNS)


def _inner_cohorts(idx: DataFrame, *, inner_keys: list[str], else_zero: bool) -> DataFrame:
    """Legacy ``nuvinhos_performance_final``: AVG(metric_value) per full cohort.

    Grouped by ``(*inner_keys, date_reference, date_granularity, nuvinho)`` (the
    nuvinho flag included), producing one row per cohort. ``nuvinhos_average`` is
    the cohort mean on the 'nuvinho' row and the ELSE value (NULL or 0)
    elsewhere; ``old_average`` mirrors it for 'old'.
    """
    else_val = F.lit(0.0) if else_zero else F.lit(None).cast("double")
    full_keys = [*inner_keys, "date_reference", "date_granularity"]
    grouped = idx.groupBy(*full_keys, "nuvinho").agg(
        F.avg(F.col("_mv")).alias("_cohort_avg")
    )
    return grouped.select(
        *full_keys,
        "nuvinho",
        F.when(F.col("nuvinho") == F.lit("nuvinho"), F.col("_cohort_avg"))
        .otherwise(else_val)
        .alias("nuvinhos_average"),
        F.when(F.col("nuvinho") == F.lit("old"), F.col("_cohort_avg"))
        .otherwise(else_val)
        .alias("old_average"),
    )


def _outer_rollup(
    inner: DataFrame, *, by: list[str], metric_name: str
) -> DataFrame:
    """Legacy outer view: AVG(nuvinhos_average)/AVG(old_average) per roll-up key.

    ``by`` is the subset of ``(xforce, xplead, squad, district)`` that keys this
    roll-up; the rest are NULLed. ``metric_value = num/den*100`` (TRY_DIVIDE →
    NULL when the denominator AVG is NULL/0). Rows whose key dimension is NULL
    are dropped (legacy GROUP BY ALL keeps a NULL key as its own bucket, but the
    roll-up dimensions here are never NULL for a real cohort; an all-NULL key
    would be a noise group we do not emit).
    """
    grp_keys = ["_deck", *by, "date_reference", "date_granularity"]
    grp = inner.groupBy(*grp_keys).agg(
        F.avg("nuvinhos_average").alias("numerator"),
        F.avg("old_average").alias("denominator"),
    )
    for col in by:
        grp = grp.filter(F.col(col).isNotNull())
    grp = grp.withColumn(
        "metric_value",
        F.when(
            F.col("denominator") > 0,
            F.col("numerator") / F.col("denominator") * F.lit(100.0),
        ).otherwise(F.lit(None).cast("double")),
    )
    return _out_columns(
        grp,
        metric_name=metric_name,
        keep_xforce="xforce" in by,
        keep_xplead="xplead" in by,
        keep_squad="squad" in by,
        keep_district="district" in by,
    )


# --------------------------------------------------------------------------- #
# Per-deck builders (pre-cutover, byte-for-byte legacy)                        #
# --------------------------------------------------------------------------- #
def _build_main(idx: DataFrame) -> list[DataFrame]:
    """Main deck (core/fraud/NULL-team).

    XForce roll-up: two-level, ELSE NULL (legacy main file). squad + district:
    two-level, ELSE 0 (legacy S&D file reads the merged main deck back).
    """
    inner_full = ["_deck", "xforce", "xplead", "squad", "district"]
    inner_null = _inner_cohorts(idx, inner_keys=inner_full, else_zero=False)
    inner_zero = _inner_cohorts(idx, inner_keys=inner_full, else_zero=True)
    return [
        _outer_rollup(inner_null, by=["xforce", "xplead"], metric_name=METRIC_XFORCE),
        _outer_rollup(inner_zero, by=["squad"], metric_name=METRIC_SQUAD),
        _outer_rollup(inner_zero, by=["district"], metric_name=METRIC_DISTRICT),
    ]


def _build_sm(idx: DataFrame) -> list[DataFrame]:
    """Social Media: XForce + squad + district, all two-level, all ELSE 0."""
    inner_full = ["_deck", "xforce", "xplead", "squad", "district"]
    inner = _inner_cohorts(idx, inner_keys=inner_full, else_zero=True)
    return [
        _outer_rollup(inner, by=["xforce", "xplead"], metric_name=METRIC_XFORCE),
        _outer_rollup(inner, by=["squad"], metric_name=METRIC_SQUAD),
        _outer_rollup(inner, by=["district"], metric_name=METRIC_DISTRICT),
    ]


def _build_content(idx: DataFrame) -> list[DataFrame]:
    """Content special path.

    XForce: SINGLE-LEVEL — inner final groups by (xforce, xplead, date, gran,
    nuvinho) with NO squad/district, ELSE NULL, then passes nuvinhos_average /
    old_average straight through (no outer AVG). squad/district: degenerate
    NULL-keyed rows from the XForce output (SUM / COUNT DISTINCT agent / AVG).
    """
    inner_keys = ["_deck", "xforce", "xplead"]
    inner = _inner_cohorts(idx, inner_keys=inner_keys, else_zero=False)
    # Single-level: pass the cohort means straight through (legacy GROUP BY ALL
    # over already-distinct (xforce, xplead, date, gran, nuvinho) is a no-op).
    xforce = inner.withColumn(
        "metric_value",
        F.when(
            F.col("old_average") > 0,
            F.col("nuvinhos_average") / F.col("old_average") * F.lit(100.0),
        ).otherwise(F.lit(None).cast("double")),
    ).withColumnRenamed("nuvinhos_average", "numerator").withColumnRenamed(
        "old_average", "denominator"
    )
    # Drop NULL-xforce noise (mirrors the other XForce roll-ups).
    xforce = xforce.filter(F.col("xforce").isNotNull())
    xforce_out = _out_columns(
        xforce,
        metric_name=METRIC_XFORCE,
        keep_xforce=True,
        keep_xplead=True,
        keep_squad=False,
        keep_district=False,
    )

    # Degenerate squad/district: read FROM the XForce output. squad & district
    # are NULL there, so GROUP BY ALL collapses to one NULL-key row per
    # (date, granularity). agent is NULL so COUNT(DISTINCT agent) = 0.
    deg_base = xforce_out.groupBy("date_reference", "date_granularity").agg(
        F.sum("metric_value").alias("numerator"),
        F.countDistinct("agent").cast("double").alias("denominator"),
        F.avg("metric_value").alias("metric_value"),
    )

    def _degenerate(metric_name: str) -> DataFrame:
        null_str = F.lit(None).cast("string")
        return deg_base.select(
            null_str.alias("agent"),
            null_str.alias("xforce"),
            null_str.alias("xplead"),
            null_str.alias("team"),
            null_str.alias("squad"),
            null_str.alias("district"),
            null_str.alias("shift"),
            F.col("date_reference"),
            F.col("date_granularity"),
            F.lit(metric_name).alias("metric"),
            F.col("numerator").cast("double").alias("numerator"),
            F.col("denominator").cast("double").alias("denominator"),
            F.col("metric_value").cast("double").alias("metric_value"),
        ).select(*METRIC_COLUMNS)

    return [xforce_out, _degenerate(METRIC_SQUAD), _degenerate(METRIC_DISTRICT)]


# --------------------------------------------------------------------------- #
# Post-cutover builder (documented corrected formula)                         #
# --------------------------------------------------------------------------- #
def _build_corrected(idx: DataFrame) -> list[DataFrame]:
    """From 2026-07-01: flat agent-level mean(Index|Nuvinho)/mean(Index|old).

    A single-level mean over agents per roll-up key (no inner cohort weighting,
    no ELSE-0 deflation, all granularities, all decks the same). This is the
    corrected behavior the documented formula intends.
    """
    nuv_mv = F.when(F.col("nuvinho") == F.lit("nuvinho"), F.col("_mv"))
    old_mv = F.when(F.col("nuvinho") == F.lit("old"), F.col("_mv"))
    work = idx.withColumn("_nuv", nuv_mv).withColumn("_old", old_mv)

    def _flat(by: list[str], metric_name: str) -> DataFrame:
        grp_keys = ["_deck", *by, "date_reference", "date_granularity"]
        grp = work.groupBy(*grp_keys).agg(
            F.coalesce(F.avg("_nuv"), F.lit(0.0)).alias("numerator"),
            F.avg("_old").alias("denominator"),
        )
        for col in by:
            grp = grp.filter(F.col(col).isNotNull())
        grp = grp.withColumn(
            "metric_value",
            F.when(
                F.col("denominator") > 0,
                F.col("numerator") / F.col("denominator") * F.lit(100.0),
            ).otherwise(F.lit(None).cast("double")),
        )
        return _out_columns(
            grp,
            metric_name=metric_name,
            keep_xforce="xforce" in by,
            keep_xplead="xplead" in by,
            keep_squad="squad" in by,
            keep_district="district" in by,
        )

    return [
        _flat(["xforce", "xplead"], METRIC_XFORCE),
        _flat(["squad"], METRIC_SQUAD),
        _flat(["district"], METRIC_DISTRICT),
    ]


def compute_nuvinhos_performance(
    xpeer_index: DataFrame,
    agent_tenure: DataFrame,
) -> DataFrame:
    """Compute Nuvinhos Performance at XForce / squad / district roll-ups.

    Args:
        xpeer_index: ``io_xpeer_index_metric`` (agent-level Xpeer Index, all
            granularities). Only ``metric == 'xpeer_index'`` rows are used.
        agent_tenure: the ``agent_information`` extractor (one row per
            ``(agent, snapshot_month)``), providing ``last_change_date`` (and,
            where present, the Content ``valid_from``). Joined monthly
            (``month(date_reference) = snapshot_month``) for every grain.

    Returns:
        Tidy long-format metric rows. Pre-cutover: per-deck legacy roll-ups,
        week + month only. Post-cutover: the documented flat-mean formula.
    """
    spark = xpeer_index.sparkSession
    if len(xpeer_index.take(1)) == 0:
        return empty_metric_frame(spark)

    idx = xpeer_index.filter(F.col("metric") == F.lit(XPEER_INDEX_METRIC))
    if len(idx.take(1)) == 0:
        return empty_metric_frame(spark)

    idx = idx.withColumn("_mv", F.col("metric_value").cast("double"))
    idx = idx.withColumn("_deck", _deck_col())
    idx = idx.withColumn("_snap", F.to_date(F.date_trunc("month", F.col("date_reference"))))

    # --- tenure join (monthly key, applied to weekly rows too) ---------------
    # agent_information is unique per (agent, snapshot_month); dedup defensively
    # so a duplicate snapshot can't fan the index rows out.
    ten = agent_tenure.select(
        F.col("agent"),
        F.to_date(F.date_trunc("month", F.col("snapshot_month"))).alias("_snap"),
        F.col("last_change_date"),
        # valid_from may not exist on the BDX tenure table; default NULL.
        (F.col("valid_from") if "valid_from" in agent_tenure.columns
         else F.lit(None).cast("date")).alias("valid_from"),
    ).dropDuplicates(["agent", "_snap"])

    idx = idx.join(ten, on=["agent", "_snap"], how="left")

    # --- nuvinho window ------------------------------------------------------
    # Content uses valid_from as the change date; everyone else last_change_date.
    change_date = F.when(
        F.col("_deck") == F.lit(DECK_CONTENT), F.col("valid_from")
    ).otherwise(F.col("last_change_date"))
    lc_month = F.to_date(F.date_trunc("month", change_date))
    window_end = F.add_months(lc_month, NUVINHO_WINDOW_MONTHS)
    is_nuv = (F.col("_snap") >= lc_month) & (F.col("_snap") <= window_end)
    idx = idx.withColumn(
        "nuvinho",
        F.when(F.coalesce(is_nuv, F.lit(False)), F.lit("nuvinho")).otherwise(F.lit("old")),
    )

    # --- era split: pre-cutover legacy vs post-cutover corrected -------------
    pre = idx.filter(F.col("date_reference") < F.lit(LEGACY_CUTOVER))
    post = idx.filter(F.col("date_reference") >= F.lit(LEGACY_CUTOVER))

    parts: list[DataFrame] = []

    # Pre-cutover: floor + week/month only, then per-deck builders.
    pre = pre.filter(F.col("date_reference") >= F.lit(ERA_FLOOR))
    pre = pre.filter(F.col("date_granularity").isin(*_PRE_GRAINS))
    if len(pre.take(1)) > 0:
        main = pre.filter(F.col("_deck") == F.lit(DECK_MAIN))
        sm = pre.filter(F.col("_deck") == F.lit(DECK_SM))
        content = pre.filter(F.col("_deck") == F.lit(DECK_CONTENT))
        if len(main.take(1)) > 0:
            parts.extend(_build_main(main))
        if len(sm.take(1)) > 0:
            parts.extend(_build_sm(sm))
        if len(content.take(1)) > 0:
            parts.extend(_build_content(content))

    # Post-cutover: documented corrected formula, all grains, all decks.
    if len(post.take(1)) > 0:
        parts.extend(_build_corrected(post))

    if not parts:
        return empty_metric_frame(spark)

    result = parts[0]
    for extra in parts[1:]:
        result = result.unionByName(extra)
    return result.select(*METRIC_COLUMNS)


IO_NUVINHOS_PERFORMANCE_METRIC_SCHEMA: tuple[tuple[str, str], ...] = (
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
