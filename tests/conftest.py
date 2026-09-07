from __future__ import annotations

import os
import shutil

import pytest


def _java_available() -> bool:
    java_home = os.environ.get("JAVA_HOME")
    if java_home and os.path.exists(os.path.join(java_home, "bin", "java")):
        return True
    return shutil.which("java") is not None


@pytest.fixture(scope="session")
def spark():
    """A plain local Spark session: no Iceberg, no Kafka, no services. Skips without a JDK."""
    if not _java_available():
        pytest.skip("no JDK on PATH/JAVA_HOME; Spark tests skipped")
    from pyspark.sql import SparkSession

    s = (SparkSession.builder.master("local[2]").appName("tests")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.session.timeZone", "UTC")
         .getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()
