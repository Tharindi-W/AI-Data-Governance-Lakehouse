"""Delta Lake + PySpark session factory (singleton pattern)."""
from __future__ import annotations

import os
from functools import lru_cache

from dotenv import load_dotenv

load_dotenv()


@lru_cache(maxsize=1)
def get_spark_session():
    """Return a singleton SparkSession configured for local Delta Lake."""
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    app_name = os.getenv("SPARK_APP_NAME", "DataGovernanceLakehouse")
    master = os.getenv("SPARK_MASTER", "local[*]")
    log_level = os.getenv("SPARK_LOG_LEVEL", "WARN")

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.adaptive.enabled", "true")
        # Suppress verbose Delta Lake logs
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel(log_level)
    return spark


def stop_spark():
    """Gracefully stop the SparkSession."""
    get_spark_session.cache_clear()
    from pyspark.sql import SparkSession

    active = SparkSession.getActiveSession()
    if active:
        active.stop()
