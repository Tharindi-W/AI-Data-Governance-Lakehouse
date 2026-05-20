"""Bronze layer: ingest raw CDR CSV → Delta Lake with audit columns.

Databricks pattern: preserve raw data unchanged, add audit columns only.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DoubleType, IntegerType, LongType,
    StringType, StructField, StructType,
)
from rich.console import Console

from src.utils.logger import get_logger
from src.utils.paths import BRONZE_PATH, LINEAGE_PATH, RAW_PATH, ensure_paths
from src.utils.spark_session import get_spark_session

log = get_logger(__name__)
console = Console()

BATCH_ID = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")

CDR_SCHEMA = StructType([
    StructField("square_id",           IntegerType(), False),
    StructField("time_interval",       LongType(),    False),  # Unix ms
    StructField("country_code",        IntegerType(), True),
    StructField("sms_in",              DoubleType(),  True),
    StructField("sms_out",             DoubleType(),  True),
    StructField("call_in",             DoubleType(),  True),
    StructField("call_out",            DoubleType(),  True),
    StructField("internet",            DoubleType(),  True),
    StructField("is_anomaly_injected", BooleanType(), True),   # generator label
    StructField("anomaly_type",        StringType(),  True),
])


def _add_audit_columns(df, source_file: str):
    return (
        df.withColumn("_bronze_ingested_at",  F.current_timestamp())
          .withColumn("_source_file",         F.lit(source_file))
          .withColumn("_batch_id",            F.lit(BATCH_ID))
          .withColumn("_pipeline_version",    F.lit("2.0.0"))
    )


def ingest_cdr(spark) -> dict:
    source_path = str(RAW_PATH / "cdr_records.csv")
    target_path = str(BRONZE_PATH / "cdr_records")

    log.info("Ingesting [bold]cdr_records[/bold] → bronze")

    df = (spark.read
               .schema(CDR_SCHEMA)
               .option("header", "true")
               .csv(source_path))
    df = _add_audit_columns(df, source_path)

    (df.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .save(target_path))

    count = df.count()
    log.info(f"  [green]✓[/green] cdr_records: {count:,} rows written to Delta")

    return {
        "table":        "cdr_records",
        "source":       source_path,
        "target":       target_path,
        "rows_written": count,
        "batch_id":     BATCH_ID,
        "ingested_at":  datetime.now(tz=timezone.utc).isoformat(),
    }


def main():
    ensure_paths()
    console.print("\n[bold cyan]═══ Bronze Layer: Raw Ingestion ═══[/bold cyan]")

    spark = get_spark_session()
    record = ingest_cdr(spark)

    lineage = {
        "layer":         "bronze",
        "batch_id":      BATCH_ID,
        "pipeline":      "bronze_ingestion",
        "run_timestamp": datetime.now(tz=timezone.utc).isoformat(),
        "tables":        [record],
    }
    lineage_path = LINEAGE_PATH / f"bronze_lineage_{BATCH_ID}.json"
    lineage_path.write_text(json.dumps(lineage, indent=2))
    log.info(f"Lineage written to {lineage_path}")

    console.print(f"\n[bold green]Bronze ingestion complete. Batch: {BATCH_ID}[/bold green]\n")


if __name__ == "__main__":
    main()
