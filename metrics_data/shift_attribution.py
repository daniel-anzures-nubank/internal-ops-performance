"""shift_attribution — re-attribute night-shift activity to the day the shift started.

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

import numpy as np
import pandas as pd

# Roster shift label that crosses midnight.
NIGHT_SHIFT_LABEL = "night"

# Activity strictly before this date keeps legacy calendar-day attribution, so
# historical metrics never change.
NIGHT_SHIFT_CUTOVER = pd.Timestamp("2026-07-01")

# Night "business day" boundary: noon. Subtract 12h before taking the date so a
# shift's evening start and its following early morning land on the start date.
NIGHT_BOUNDARY_HOURS = 12


def _to_naive(series: pd.Series) -> pd.Series:
    """Coerce a datetime Series to tz-naive ``datetime64[ns]``."""
    s = pd.to_datetime(series)
    if s.dt.tz is not None:
        return s.dt.tz_localize(None)
    return s


def night_agent_months(agent_info: pd.DataFrame) -> pd.DataFrame:
    """Return ``(agent, snapshot_month, is_night=True)`` for night-shift rows.

    ``snapshot_month`` is normalized to a tz-naive month-start Timestamp so it
    joins cleanly against an activity's month. Non-night / NULL-shift rows are
    dropped, so a left-join miss means "not a night agent that month".
    """
    shift = agent_info["shift"].astype("string").str.lower()
    df = agent_info.loc[
        shift == NIGHT_SHIFT_LABEL, ["agent", "snapshot_month"]
    ].copy()
    if df.empty:
        return pd.DataFrame(
            {
                "agent": pd.Series(dtype="object"),
                "snapshot_month": pd.Series(dtype="datetime64[ns]"),
                "is_night": pd.Series(dtype="bool"),
            }
        )
    df["snapshot_month"] = (
        _to_naive(df["snapshot_month"]).dt.to_period("M").dt.to_timestamp()
    )
    df = df.drop_duplicates()
    df["is_night"] = True
    return df


def shift_start_date(
    df: pd.DataFrame,
    *,
    agent_col: str,
    local_ts_col: str,
    calendar_date_col: str,
    night_months: pd.DataFrame,
    cutover: pd.Timestamp = NIGHT_SHIFT_CUTOVER,
) -> pd.Series:
    """Re-attribute each row to the day its (night) shift started.

    Returns a Series of python ``date`` objects aligned to ``df.index``:

      * Night-shift agent AND calendar date >= ``cutover`` AND the noon-boundary
        date is also >= ``cutover``  →  ``DATE(local_ts - 12h)``.
      * Everything else  →  the original calendar date (unchanged).

    Args:
        df: frame to re-attribute; must contain ``agent_col``, ``local_ts_col``
            (the activity's LOCAL timestamp) and ``calendar_date_col``.
        night_months: output of :func:`night_agent_months`.
    """
    if df.empty:
        return pd.Series([], dtype="object", index=df.index)

    local_ts = _to_naive(df[local_ts_col])
    cal_ts = _to_naive(df[calendar_date_col]).dt.normalize()

    month = local_ts.dt.to_period("M").dt.to_timestamp()
    lookup = pd.DataFrame(
        {"agent": df[agent_col].to_numpy(), "snapshot_month": month.to_numpy()}
    )
    # ``is_night`` in ``night_months`` is always True, so a left-join match is
    # exactly ``notna()`` — this also sidesteps the object-dtype fillna downcast.
    is_night = (
        lookup.merge(night_months, on=["agent", "snapshot_month"], how="left")[
            "is_night"
        ]
        .notna()
        .to_numpy()
    )

    candidate = (local_ts - pd.Timedelta(hours=NIGHT_BOUNDARY_HOURS)).dt.normalize()
    cutover64 = np.datetime64(pd.Timestamp(cutover))
    eligible = (
        is_night
        & (cal_ts.to_numpy() >= cutover64)
        & (candidate.to_numpy() >= cutover64)
    )
    business = np.where(eligible, candidate.to_numpy(), cal_ts.to_numpy())
    return pd.to_datetime(pd.Series(business, index=df.index)).dt.date
