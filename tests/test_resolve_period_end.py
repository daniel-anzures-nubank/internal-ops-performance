"""Unit tests for ``--period-end`` resolution in ``db``.

Pure Python — no SparkSession, no IO. ``resolve_period_end`` accepts an ISO
date string or the ``max_dime`` sentinel (which queries ``MAX(dime_date)``
from the DIME ETL table via the provided Spark session); the Spark path is
exercised with a duck-typed stub so the parsing stays testable locally.
"""

from __future__ import annotations

from datetime import date, datetime

import pytest

import db


class _StubSpark:
    """Duck-typed SparkSession: ``sql()`` returns rows for ``collect()``."""

    def __init__(self, rows: list[tuple]) -> None:
        self._rows = rows
        self.queries: list[str] = []

    def sql(self, query: str) -> "_StubSpark._Result":
        self.queries.append(query)
        return self._Result(self._rows)

    class _Result:
        def __init__(self, rows: list[tuple]) -> None:
            self._rows = rows

        def collect(self) -> list[tuple]:
            return self._rows


# ---------------------------------------------------------------------------
# ISO date strings
# ---------------------------------------------------------------------------


def test_iso_date_string_parses_without_spark() -> None:
    assert db.resolve_period_end("2026-06-30") == date(2026, 6, 30)


def test_iso_date_string_ignores_spark() -> None:
    spark = _StubSpark([(date(2026, 6, 21),)])
    assert db.resolve_period_end("2026-05-01", spark) == date(2026, 5, 1)
    assert spark.queries == []


# ---------------------------------------------------------------------------
# max_dime sentinel
# ---------------------------------------------------------------------------


def test_sentinel_resolves_to_max_dime_date() -> None:
    spark = _StubSpark([(date(2026, 6, 30),)])
    assert db.resolve_period_end(db.MAX_DIME_SENTINEL, spark) == date(2026, 6, 30)
    assert len(spark.queries) == 1
    assert "MAX(dime_date)" in spark.queries[0]
    assert db.DIME_TABLE in spark.queries[0]


def test_sentinel_normalizes_datetime_to_date() -> None:
    spark = _StubSpark([(datetime(2026, 6, 30, 0, 0),)])
    assert db.resolve_period_end(db.MAX_DIME_SENTINEL, spark) == date(2026, 6, 30)


def test_sentinel_without_spark_raises() -> None:
    with pytest.raises(ValueError, match=db.MAX_DIME_SENTINEL):
        db.resolve_period_end(db.MAX_DIME_SENTINEL)


@pytest.mark.parametrize("rows", [[], [(None,)]])
def test_sentinel_with_no_date_raises(rows: list[tuple]) -> None:
    spark = _StubSpark(rows)
    with pytest.raises(RuntimeError, match=r"MAX\(dime_date\)"):
        db.resolve_period_end(db.MAX_DIME_SENTINEL, spark)


# ---------------------------------------------------------------------------
# Everything else
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        "yesterday",
        "max_dime ",  # stray whitespace is not the sentinel
        "2026-13-01",  # not a real date
        "",
        None,
    ],
)
def test_garbage_raises_value_error_naming_accepted_forms(value) -> None:
    with pytest.raises(ValueError, match=r"YYYY-MM-DD.*max_dime"):
        db.resolve_period_end(value)
