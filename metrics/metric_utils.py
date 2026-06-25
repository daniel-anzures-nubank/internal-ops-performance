"""Shared helpers for the metrics layer.

Every metric table has the same tidy "long" shape — one row per
``(agent, date_reference, date_granularity)`` with ``metric / numerator /
denominator / metric_value`` — and the same day/week/month aggregation rules:

* ``day``      → the date itself
* ``week``     → Monday of that week (matches Spark ``DATE_TRUNC('WEEK', ...)``)
* ``month``    → first day of the month
* ``quarter``  → first day of the calendar quarter (Jan/Apr/Jul/Oct 1)
* ``semester`` → first day of the half-year (Jan 1 or Jul 1)
* ``year``     → first day of the year
* hierarchy/dimension fields take their **most-recent value within the bucket**
  (legacy ``FIRST_VALUE(... ORDER BY date DESC)``)
* ``metric_value`` = ``numerator / denominator * 100`` (percentage; NULL when
  the denominator is 0)

``aggregate_long`` centralizes that so each metric module only has to produce a
per-(agent, date) frame with a numerator and denominator column.
"""

from __future__ import annotations

import pandas as pd

# Roster dimension columns carried through every metric (most-recent value
# within each period bucket). Order matters — it defines the output column order.
DIM_COLS: tuple[str, ...] = (
    "xforce",
    "xplead",
    "team",
    "squad",
    "district",
    "shift",
)

GRANULARITIES: tuple[str, ...] = (
    "day",
    "week",
    "month",
    "quarter",
    "semester",
    "year",
)

# The shared tidy output columns, in order.
METRIC_COLUMNS: tuple[str, ...] = (
    "agent",
    *DIM_COLS,
    "date_reference",
    "date_granularity",
    "metric",
    "numerator",
    "denominator",
    "metric_value",
)


def bucket_dates(dates: pd.Series, granularity: str) -> pd.Series:
    """Map each date to its period-bucket start (tz-naive datetime)."""
    d = pd.to_datetime(dates)
    if getattr(d.dt, "tz", None) is not None:
        d = d.dt.tz_localize(None)
    if granularity == "day":
        return d.dt.normalize()
    if granularity == "week":
        return (d - pd.to_timedelta(d.dt.weekday, unit="D")).dt.normalize()
    if granularity == "month":
        return d.dt.to_period("M").dt.to_timestamp()
    if granularity == "quarter":
        return d.dt.to_period("Q").dt.to_timestamp()
    if granularity == "semester":
        # H1 → Jan 1, H2 → Jul 1 of the same year.
        year_start = d.dt.to_period("Y").dt.to_timestamp()
        second_half = year_start + pd.offsets.MonthBegin(6)
        return year_start.where(d.dt.month <= 6, second_half)
    if granularity == "year":
        return d.dt.to_period("Y").dt.to_timestamp()
    raise ValueError(f"unknown granularity: {granularity!r}")


def empty_metric_frame() -> pd.DataFrame:
    """An empty frame with the shared metric columns (for empty inputs)."""
    return pd.DataFrame({c: pd.Series(dtype="object") for c in METRIC_COLUMNS})


def latest_dims(
    work: pd.DataFrame,
    *,
    sort_col: str = "_date",
    keys: tuple[str, ...] = ("agent", "date_reference"),
    dim_cols: tuple[str, ...] = DIM_COLS,
) -> pd.DataFrame:
    """The most-recent dimension values within each ``keys`` group.

    Mirrors legacy ``FIRST_VALUE(... ORDER BY date DESC)``: sort ascending by
    ``sort_col`` and take the last row per group.
    """
    return (
        work.sort_values(sort_col)
        .groupby(list(keys), as_index=False, dropna=False)
        .tail(1)[[*keys, *dim_cols]]
    )


def aggregate_long(
    df: pd.DataFrame,
    *,
    numerator_col: str,
    denominator_col: str,
    metric_name: str,
    date_col: str = "date",
    dim_cols: tuple[str, ...] = DIM_COLS,
    granularities: tuple[str, ...] = GRANULARITIES,
    scale: float = 100.0,
) -> pd.DataFrame:
    """Aggregate a per-(agent, date) frame into tidy day/week/month metric rows.

    Args:
        df: rows carrying ``agent``, ``date_col``, the ``dim_cols``, and the
            numerator/denominator columns. Multiple rows per (agent, date) are
            summed.
        numerator_col / denominator_col: columns summed into ``numerator`` /
            ``denominator``.
        metric_name: value for the ``metric`` column.
        date_col: the source date column (default ``date``).
        scale: multiplier for ``metric_value = numerator / denominator * scale``.
            ``100.0`` for ratio metrics expressed as a percentage; pass ``1.0``
            when the numerator is already on a 0-100 scale (e.g. averaging
            quality scores, where the denominator is a row count).

    Returns:
        Tidy long-format frame with :data:`METRIC_COLUMNS`, sorted by
        granularity / date_reference / agent.
    """
    if df.empty:
        return empty_metric_frame()

    parts: list[pd.DataFrame] = []
    for granularity in granularities:
        work = df.copy()
        work["_date"] = pd.to_datetime(work[date_col])
        if getattr(work["_date"].dt, "tz", None) is not None:
            work["_date"] = work["_date"].dt.tz_localize(None)
        work["date_reference"] = bucket_dates(work["_date"], granularity)

        sums = work.groupby(["agent", "date_reference"], as_index=False).agg(
            numerator=(numerator_col, "sum"),
            denominator=(denominator_col, "sum"),
        )
        latest = latest_dims(work, dim_cols=dim_cols)
        out = sums.merge(latest, on=["agent", "date_reference"], how="left")
        out["date_granularity"] = granularity
        out["metric"] = metric_name
        out["metric_value"] = (
            out["numerator"] / out["denominator"]
        ).where(out["denominator"] > 0) * scale
        out["date_reference"] = out["date_reference"].dt.date
        parts.append(out)

    result = pd.concat(parts, ignore_index=True)[list(METRIC_COLUMNS)]
    return result.sort_values(
        ["date_granularity", "date_reference", "agent"]
    ).reset_index(drop=True)
