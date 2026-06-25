"""Shared pytest fixtures for the PySpark test suite.

The pipeline is Spark-native, so tests build small Spark DataFrames with
``spark.createDataFrame`` and assert on collected rows. A single local
SparkSession is shared across the whole test session (Spark startup is the
expensive part).

Note: PySpark has no wheels for Python >= 3.13, so this suite must run on a
Python 3.11/3.12 interpreter (or directly on a Databricks cluster). It will not
collect on the repo's local 3.14 interpreter.
"""

from __future__ import annotations

import pytest


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("io-performance-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
