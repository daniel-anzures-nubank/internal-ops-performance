"""shift_attribution — re-attribute night-shift activity to the day the shift started (PySpark).

Night-shift agents work across midnight, so plain calendar-day attribution
splits one shift across two days: the evening head on day *N* and the
early-morning tail on day *N+1*. This helper rolls that activity back onto the
day the shift **started**, using a noon "business day" boundary, for agents
whose roster ``shift`` is ``'night'``. The time-based raw tables
(``adherent_time``, ``occupancy_time``, ``shrinkage_slots``, ``jobs_raw``) all
apply it, so their date attribution — and the NTPJ jobs<->DIME required-flag
join — stay aligned.

Boundary (why noon)
-------------------
Night shifts are scheduled in the evening (~20:00+) and their tails end in the
early morning (~06:00–07:00), leaving a wide empty gap (~08:00–19:00) in the
middle of the day. So we subtract 12h from the local timestamp and take the
date: the evening head (e.g. 22:00 on day N → 10:00 day N) and the following
early-morning tail (e.g. 03:00 on day N+1 → 15:00 day N) both land on day N,
while a fresh evening head the next night stays put. Morning / mid shifts are
never touched (they aren't ``'night'``).

Gating (no retroactive change)
------------------------------
Only activity whose calendar date is on/after ``NIGHT_SHIFT_CUTOVER``
(2026-07-01) is re-attributed, and a rolled-back date is never allowed to fall
before the cutover. So every pre-July-2026 metric is byte-for-byte unchanged,
and the June 30 → July 1 boundary shift keeps its legacy (split) attribution.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Roster shift label that crosses midnight.
NIGHT_SHIFT_LABEL = "night"

# Activity strictly before this date keeps legacy calendar-day attribution, so
# historical metrics never change.
NIGHT_SHIFT_CUTOVER: date = date(2026, 7, 1)

# Night "business day" boundary: noon. Subtract 12h before taking the date so a
# shift's evening start and its following early morning land on the start date.
NIGHT_BOUNDARY_HOURS = 12


def night_agent_months(agent_info: DataFrame) -> DataFrame:
    """Return ``(agent, snapshot_month, is_night=True)`` for night-shift rows.

    ``snapshot_month`` is normalized to a month-start ``DATE`` so it joins
    cleanly against an activity's month. Non-night / NULL-shift rows are dropped,
    so a left-join miss means "not a night agent that month".
    """
    return (
        agent_info.filter(F.lower(F.col("shift")) == NIGHT_SHIFT_LABEL)
        .select(
            F.col("agent"),
            F.trunc(F.to_date(F.col("snapshot_month")), "month").alias(
                "snapshot_month"
            ),
        )
        .distinct()
        .withColumn("is_night", F.lit(True))
    )


def shift_start_date(
    df: DataFrame,
    *,
    agent_col: str,
    local_ts_col: str,
    calendar_date_col: str,
    night_months: DataFrame,
    cutover: date = NIGHT_SHIFT_CUTOVER,
) -> DataFrame:
    """Re-attribute each row to the day its (night) shift started.

    Replaces ``calendar_date_col`` in place (keeping every other column and the
    column order) with:

      * Night-shift agent AND calendar date >= ``cutover`` AND the noon-boundary
        date is also >= ``cutover``  →  ``DATE(local_ts - 12h)``.
      * Everything else  →  the original calendar date (unchanged).

    Args:
        df: frame to re-attribute; must contain ``agent_col``, ``local_ts_col``
            (the activity's LOCAL timestamp) and ``calendar_date_col``.
        night_months: output of :func:`night_agent_months`.
    """
    nm = night_months.select(
        F.col("agent").alias("_nm_agent"),
        F.col("snapshot_month").alias("_nm_month"),
        F.col("is_night").alias("_is_night"),
    )
    month = F.trunc(F.to_date(F.col(local_ts_col)), "month")
    joined = df.withColumn("_month", month).join(
        nm,
        (F.col(agent_col) == F.col("_nm_agent"))
        & (F.col("_month") == F.col("_nm_month")),
        "left",
    )

    is_night = F.col("_is_night").isNotNull()
    cal = F.to_date(F.col(calendar_date_col))
    candidate = F.to_date(
        F.col(local_ts_col).cast("timestamp")
        - F.expr(f"INTERVAL {NIGHT_BOUNDARY_HOURS} HOURS")
    )
    cut = F.lit(cutover)
    eligible = is_night & (cal >= cut) & (candidate >= cut)
    business = F.when(eligible, candidate).otherwise(cal)

    return joined.withColumn(calendar_date_col, business).drop(
        "_month", "_nm_agent", "_nm_month", "_is_night"
    )
