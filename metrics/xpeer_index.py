"""xpeer_index — the agent-level Xpeer Index (all teams), PySpark.

The Xpeer Index folds an agent's other performance metrics into a single
comparable score (legacy ``index_agent``). Unlike every other module in this
folder it does **not** read an ``io_*_raw`` table — it consumes the already
aggregated **per-agent metric tables** and combines their ``metric_value``s per
``(agent, date_reference, date_granularity)``.

Inputs (each a tidy long ``io_*_metric`` table, agent grain)
-----------------------------------------------------------
* ``io_adherence_metric``              → Adherence (the **driver**; an agent is
  in the index iff it has an Adherence row for that bucket)
* ``io_ntpj_metric``                   → NTPJ (Core / Fraud / Content)
* ``io_normalized_occupancy_metric``   → NO (all teams, from March 2026)
* ``io_quality_metric``                → Quality (Core / Fraud / Social Media)
* ``io_tnps_metric``                   → Human tNPS (Social Media)
* ``io_wows_metric``                   → WoWs (Social Media)
* ``io_content_csat_metric``           → CSAT, the Content "quality" term

Component transforms (legacy ``index_agents_final``)
----------------------------------------------------
* **Adherence**: ``COALESCE(0)``, taken as-is.
* **NTPJ** (lower-is-better, folded around 100): ``<=100 -> 100``;
  ``100 < x <= 200 -> 200 - x``; ``>200 or NULL -> 0``.
* **NO** (truncated): ``>=100 -> 100``; ``<100 -> x``; ``NULL -> 0``.
* **WoWs** (Social Media): ``>=5 -> 100``; ``<5 -> x/5*100``; ``NULL -> 0``.
* **tNPS / Quality / CSAT**: used raw (tNPS may be negative).

Composition — which terms enter the average, by team and **era**
----------------------------------------------------------------
The index is a simple mean of the included components. The roster of components
grew over the 2026 rollout, so it is **anchored on the bucket's month**:

* **Core / Fraud**: Adherence + NTPJ always (NTPJ is a **fixed** divisor term —
  a missing NTPJ row folds to 0 but still counts; legacy den 200/300/400 never
  100); ``+ Quality`` from **Feb 2026** (when present); ``+ NO`` from **March
  2026**, except the approved ``nitza.zarza`` Apr-May 2026 carve-out. SM-only
  WoWs/tNPS never apply. The main-deck **support squads** (``quality`` /
  ``planning`` / ``enablement`` / ``idsec``) that legacy keeps with ``team =
  NULL`` get this same Core/Fraud roster.
* **Content**:      Adherence always; ``+ NTPJ`` **present-only** (drops from
  sum AND divisor when the agent has no NTPJ row); ``+ NO`` and ``+ CSAT`` (when
  present) from **March 2026**. Jan & Feb are therefore Adherence-only (Content
  has no NTPJ rows before March → legacy den 100, not 200).
* **Social Media**: Adherence + WoWs always; ``+ tNPS`` whenever present;
  ``+ Quality`` from **Feb 2026** (when present); ``+ NO`` from **March 2026**.
  SM **excludes NTPJ**.

Quality / CSAT / tNPS — and NTPJ for **Content** — drop out of both the sum
**and** the divisor when the agent has no value for the bucket. NTPJ for
**Core/Fraud** and NO (once its era starts) are always counted in the divisor (a
missing value contributes 0).

Era classification (parity with legacy ``index_agents_*``)
----------------------------------------------------------
Legacy unions ``index_agents_monthly`` + ``index_agents_weekly`` (day / quarter /
semester / year are never built pre-cutover). For ``date_reference <
XPEER_CUTOVER`` (2026-07-01) we therefore emit ONLY ``week`` + ``month`` grain.

The **era month** that decides the component roster is:
* **month** grain — the month of ``date_reference`` (a 2026-01 month always
  truncs to 2026-01-01).
* **week** grain — classified by the RAW ``date_reference`` (the bucket's Monday),
  NOT the month of the Monday. Legacy keeps the first ISO bucket
  (``2025-12-29``) and classifies by ``date_reference`` boundaries:
  ``<= 2026-01-31 -> Jan era``; ``<= 2026-02-28 -> Feb era``;
  ``>= 2026-03-01 -> Mar+ era``. The weekly floor is therefore ``2025-12-01``
  (so the ``2025-12-29`` Monday survives), with its era mapped to Jan.
* **quarter / semester / year** — only emitted from the cutover onward; there the
  era anchors on the bucket's last month (longer aggregations include every
  component active by the period end). This branch has no pre-cutover effect.

Output convention
------------------
To keep the shared ``metric_value = numerator / denominator * 100`` contract,
``numerator`` is the **sum of the included component %s** and ``denominator`` is
``n_components * 100``; ``metric_value`` is then their mean (the Index %).

Output — tidy long format, one row per (agent, date_reference, granularity)
---------------------------------------------------------------------------
``agent, xforce, xplead, team, squad, district, shift, date_reference,
date_granularity, metric, numerator, denominator, metric_value``.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from metric_utils import DIM_COLS, METRIC_COLUMNS, empty_metric_frame

METRIC_NAME = "xpeer_index"

# Rollout cutovers (anchored on the bucket's era-month, see module docstring).
NO_CUTOVER: date = date(2026, 3, 1)  # NO joins the Index
QUALITY_CUTOVER: date = date(2026, 2, 1)  # Quality joins (Core / Fraud / SM)
QUALITY_CUTOVER_CONTENT: date = date(2026, 3, 1)  # CSAT joins (Content)

# Legacy-parity cutover. Before it, byte-for-byte legacy behaviour: emit only the
# week + month grain (legacy unions index_agents_weekly + index_agents_monthly).
# From this date onward the broader granularity set / end-of-period anchoring is
# allowed.
XPEER_CUTOVER: date = date(2026, 7, 1)

# Weekly era floor. Legacy filters date_reference >= '2025-12-01' so the first
# ISO weekly bucket (Monday 2025-12-29) survives; its era is classified as Jan.
WEEKLY_ERA_FLOOR: date = date(2025, 12, 1)
# Month-grain floor — the Index is a 2026 construct; earlier months have no era.
ERA_FLOOR: date = date(2026, 1, 1)

# Weekly era boundaries (classified on the RAW date_reference / bucket Monday).
JAN_WEEK_MAX: date = date(2026, 1, 31)
FEB_WEEK_MAX: date = date(2026, 2, 28)

SOCIAL_MEDIA = "social media"
CONTENT = "content"
CORE = "core"
FRAUD = "fraud"
# The teams that get the Core/Fraud (NTPJ + NO + Quality) composition. A NULL
# team (main-deck support squad) is folded in at the predicate (`is_cf`), not here.
CORE_FRAUD_TEAMS = (CORE, FRAUD)

NITZA_NO_SUPPRESSION_AGENT = "nitza.zarza"
NITZA_NO_SUPPRESSION_MONTHS = (date(2026, 4, 1), date(2026, 5, 1))

_KEYS = ["agent", "date_reference", "date_granularity"]
# Component metric tables -> the column name we stage them under.
_COMPONENTS: tuple[tuple[str, str], ...] = (
    ("ntpj", "ntpj"),
    ("normalized_occupancy", "nocc"),
    ("quality", "quality"),
    ("tnps", "tnps"),
    ("wows", "wows"),
    ("content_csat", "csat"),
)


def _fold_ntpj(col: Column) -> Column:
    """Fold NTPJ around 100 (lower-is-better); NULL/>200 -> 0."""
    return (
        F.when(col <= F.lit(100), F.lit(100.0))
        .when((col > F.lit(100)) & (col <= F.lit(200)), F.lit(200.0) - col)
        .otherwise(F.lit(0.0))
    )


def _truncate_nocc(col: Column) -> Column:
    """Truncate NO at 100; NULL -> 0."""
    return (
        F.when(col >= F.lit(100), F.lit(100.0))
        .when(col < F.lit(100), col)
        .otherwise(F.lit(0.0))
    )


def _fold_wows(col: Column) -> Column:
    """WoWs count -> 0-100 (target 5/month); NULL -> 0."""
    return (
        F.when(col >= F.lit(5), F.lit(100.0))
        .when(col < F.lit(5), col / F.lit(5.0) * F.lit(100.0))
        .otherwise(F.lit(0.0))
    )


def _era_month(date_ref: Column, granularity: Column) -> Column:
    """The month that decides a bucket's component roster (era).

    * ``week`` — classified on the RAW ``date_reference`` (the bucket Monday):
      ``<= 2026-01-31`` -> Jan era; ``<= 2026-02-28`` -> Feb era; else Mar+.
      Returned as the synthetic era-month start (2026-01/02/03-01).
    * ``quarter`` / ``semester`` / ``year`` — anchor on the bucket's last month
      (post-cutover only; no pre-cutover rows reach here).
    * everything else (``day`` / ``month``) — the month of ``date_reference``.
    """
    month_of = F.trunc(date_ref, "month")
    week_era = (
        F.when(date_ref <= F.lit(JAN_WEEK_MAX), F.lit(ERA_FLOOR))
        .when(date_ref <= F.lit(FEB_WEEK_MAX), F.lit(date(2026, 2, 1)))
        .otherwise(F.lit(date(2026, 3, 1)))
    )
    return (
        F.when(granularity == F.lit("week"), week_era)
        .when(granularity == F.lit("quarter"), F.trunc(F.add_months(date_ref, 2), "month"))
        .when(granularity == F.lit("semester"), F.trunc(F.add_months(date_ref, 5), "month"))
        .when(granularity == F.lit("year"), F.trunc(F.add_months(date_ref, 11), "month"))
        .otherwise(month_of)
    )


def _component(df: DataFrame | None, name: str) -> DataFrame | None:
    """Project a metric table to its keys + a renamed ``metric_value`` column.

    The base ``io_*_metric`` tables are unique per
    ``(agent, date_reference, date_granularity, metric)`` and already filtered to
    their own metric, so a straight left join on ``_KEYS`` is a no-op-dedup join
    (no order-dependent ``keep='last'`` needed).
    """
    if df is None or len(df.take(1)) == 0:
        return None
    return df.select(
        *_KEYS, F.col("metric_value").cast("double").alias(name)
    )


def compute_xpeer_index(
    adherence: DataFrame,
    ntpj: DataFrame | None = None,
    normalized_occupancy: DataFrame | None = None,
    quality: DataFrame | None = None,
    tnps: DataFrame | None = None,
    wows: DataFrame | None = None,
    content_csat: DataFrame | None = None,
) -> DataFrame:
    """Compute the agent-level Xpeer Index at all granularities.

    Args:
        adherence: ``io_adherence_metric`` — the driver. An agent appears in the
            Index iff it has an Adherence row for the bucket; dimensions and
            ``team`` are taken from this table.
        ntpj / normalized_occupancy / quality / tnps / wows / content_csat: the
            corresponding ``io_*_metric`` tables. Any may be ``None``/empty (the
            term is then simply absent for the affected teams).

    Returns:
        Tidy long-format metric rows (see module docstring / schema).
    """
    spark = adherence.sparkSession
    if len(adherence.take(1)) == 0:
        return empty_metric_frame(spark)

    base = adherence.select(
        "agent",
        *DIM_COLS,
        "date_reference",
        "date_granularity",
        F.col("metric_value").cast("double").alias("adherence"),
    )

    sources = {
        "ntpj": ntpj,
        "normalized_occupancy": normalized_occupancy,
        "quality": quality,
        "tnps": tnps,
        "wows": wows,
        "content_csat": content_csat,
    }
    for metric_table, col in _COMPONENTS:
        comp = _component(sources[metric_table], col)
        if comp is not None:
            base = base.join(comp, on=_KEYS, how="left")
        else:
            base = base.withColumn(col, F.lit(None).cast("double"))

    gran = F.col("date_granularity")
    date_ref = F.col("date_reference")

    # --- Fix #1: pre-cutover emits ONLY week + month grain ------------------
    # Legacy unions index_agents_weekly + index_agents_monthly. Day / quarter /
    # semester / year are never built pre-cutover; allow the broader set only
    # from XPEER_CUTOVER onward.
    pre_cutover = date_ref < F.lit(XPEER_CUTOVER)
    base = base.filter(
        (~pre_cutover) | gran.isin(["week", "month"])
    )

    # --- era window floor ----------------------------------------------------
    # Month grain: drop pre-2026 months. Week grain: keep from 2025-12-01 so the
    # first ISO weekly bucket (Monday 2025-12-29, classified as Jan) survives
    # (Fix #2). Other grains only exist post-cutover and aren't era-floored here.
    era = _era_month(date_ref, gran)
    on_floor = (
        F.when(gran == F.lit("week"), date_ref >= F.lit(WEEKLY_ERA_FLOOR))
        .when(gran == F.lit("month"), date_ref >= F.lit(ERA_FLOOR))
        .otherwise(F.lit(True))
    )
    base = base.filter(on_floor)
    if len(base.take(1)) == 0:
        return empty_metric_frame(spark)

    team = F.lower(F.col("team"))
    # NULL-safe team predicates: a NULL team must read as False everywhere (not
    # NULL), so it poisons neither the term count nor the numerator.
    nz = lambda cond: F.coalesce(cond, F.lit(False))  # noqa: E731
    is_sm = nz(team == F.lit(SOCIAL_MEDIA))
    is_content = nz(team == F.lit(CONTENT))
    # is_cf covers explicit core/fraud AND a NULL team. Legacy keeps the main-deck
    # support squads (quality / planning / enablement / idsec) with team = NULL
    # (extractors/agent_information.sql, matching legacy adherence_io), and the
    # legacy CF index still gives them the full Core/Fraud roster (denominators
    # 200/300/400, never 100). In the unified adherence driver a NULL team is, by
    # construction, a main-deck agent: verified that every NULL-team adherence
    # agent is in the legacy CF deck (40/40), none in SM/Content. A genuinely
    # unexpected NON-NULL team still falls through to Adherence-only.
    is_cf = nz(team.isin(list(CORE_FRAUD_TEAMS))) | team.isNull()
    is_known = is_sm | is_content | is_cf

    mar_plus = era >= F.lit(NO_CUTOVER)
    qual_cutover = F.when(is_content, F.lit(QUALITY_CUTOVER_CONTENT)).otherwise(
        F.lit(QUALITY_CUTOVER)
    )
    qual_era_ok = era >= qual_cutover

    adh = F.coalesce(F.col("adherence").cast("double"), F.lit(0.0))
    ntpj_t = _fold_ntpj(F.col("ntpj"))
    nocc_t = _truncate_nocc(F.col("nocc"))
    wows_t = _fold_wows(F.col("wows"))
    tnps_v = F.col("tnps").cast("double")
    # Content's quality term is CSAT; everyone else's is Playvox Quality.
    qual_v = F.when(is_content, F.col("csat").cast("double")).otherwise(
        F.col("quality").cast("double")
    )

    # Accumulate the numerator (sum of included %s) and the term count.
    num = adh
    cnt = F.lit(1)  # Adherence is always in.

    # Core/Fraud carry NTPJ as a FIXED denominator term (counted even when the
    # agent has no NTPJ row — it then folds to 0; verified: Jan CF agents with no
    # ntpj row still have den=200). Content carries NTPJ present-only: it drops
    # from BOTH numerator and denominator when absent (verified: Feb Content has
    # no ntpj rows -> den=100, Adherence-only; Mar+ Content has ntpj for all).
    use_ntpj = is_cf | (is_content & F.col("ntpj").isNotNull())
    num = num + F.when(use_ntpj, ntpj_t).otherwise(F.lit(0.0))
    cnt = cnt + use_ntpj.cast("int")

    num = num + F.when(is_sm, wows_t).otherwise(F.lit(0.0))
    cnt = cnt + is_sm.cast("int")

    use_tnps = is_sm & tnps_v.isNotNull()
    num = num + F.when(use_tnps, tnps_v).otherwise(F.lit(0.0))
    cnt = cnt + use_tnps.cast("int")

    # Approved manual adjustment from `Ajustes Index`: for Apr-May 2026, NO is
    # removed from nitza.zarza's Xpeer Index and the index is recomputed with
    # the remaining active components (legacy index_agent behavior).
    suppress_nitza_no = (F.col("agent") == F.lit(NITZA_NO_SUPPRESSION_AGENT)) & era.isin(
        list(NITZA_NO_SUPPRESSION_MONTHS)
    )
    use_nocc = is_known & mar_plus & ~suppress_nitza_no
    num = num + F.when(use_nocc, nocc_t).otherwise(F.lit(0.0))
    cnt = cnt + use_nocc.cast("int")

    use_qual = is_known & qual_v.isNotNull() & qual_era_ok
    num = num + F.when(use_qual, qual_v).otherwise(F.lit(0.0))
    cnt = cnt + use_qual.cast("int")

    out = (
        base.withColumn("metric", F.lit(METRIC_NAME))
        .withColumn("numerator", num.cast("double"))
        .withColumn("denominator", (cnt * F.lit(100)).cast("double"))
        .withColumn(
            "metric_value",
            F.when(
                F.col("denominator") > 0,
                F.col("numerator") / F.col("denominator") * F.lit(100.0),
            ).otherwise(F.lit(None).cast("double")),
        )
    )

    return out.select(*METRIC_COLUMNS)


IO_XPEER_INDEX_METRIC_SCHEMA: tuple[tuple[str, str], ...] = (
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
