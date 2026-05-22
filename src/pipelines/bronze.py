"""Bronze layer: ingest raw Telecom Italia CDR .txt files → Delta Lake.

Real dataset format (Harvard Dataverse doi:10.7910/DVN/EGZHFV):
  - Tab-separated, NO header
  - One file per day: sms-call-internet-mi-YYYY-MM-DD.txt
  - Columns: square_id, time_interval, country_code,
             sms_in, sms_out, call_in, call_out, internet
  - time_interval: Unix milliseconds (10-min slots)
  - square_id: 1–10000 (100×100 Milan grid)

Bronze rule: preserve raw data exactly — only add audit columns.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, LongType,
    StructField, StructType,
)
from rich.console import Console

from src.utils.logger import get_logger
from src.utils.paths import BRONZE_PATH, LINEAGE_PATH, RAW_PATH, ensure_paths
from src.utils.spark_session import get_spark_session

log = get_logger(__name__)
console = Console()

BATCH_ID = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")

# Real Telecom Italia CDR schema — 8 columns, no header
CDR_SCHEMA = StructType([
    StructField("square_id",     IntegerType(), True),
    StructField("time_interval", LongType(),    True),  # Unix ms
    StructField("country_code",  IntegerType(), True),
    StructField("sms_in",        DoubleType(),  True),
    StructField("sms_out",       DoubleType(),  True),
    StructField("call_in",       DoubleType(),  True),
    StructField("call_out",      DoubleType(),  True),
    StructField("internet",      DoubleType(),  True),
])


def _add_audit_columns(df, source_glob: str):
    return (
        df.withColumn("_bronze_ingested_at", F.current_timestamp())
          .withColumn("_source_file",        F.input_file_name())
          .withColumn("_batch_id",           F.lit(BATCH_ID))
          .withColumn("_pipeline_version",   F.lit("3.0.0"))
    )


def ingest_cdr(spark) -> dict:
    # Read all daily .txt files from data/raw/
    source_glob = str(RAW_PATH / "sms-call-internet-mi-*.txt")
    target_path = str(BRONZE_PATH / "cdr_records")

    log.info(f"Ingesting CDR files from {source_glob}")

    df = (spark.read
               .schema(CDR_SCHEMA)
               .option("sep", "\t")
               .option("header", "false")
               .csv(source_glob))

    df = _add_audit_columns(df, source_glob)

    # Partition by date derived from time_interval for efficient downstream reads
    df = df.withColumn(
        "_date_partition",
        F.to_date((F.col("time_interval") / 1000).cast("timestamp"))
    )

    (df.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .partitionBy("_date_partition")
       .save(target_path))

    count = df.count()
    files = [f.name for f in RAW_PATH.glob("sms-call-internet-mi-*.txt")]
    log.info(f"  [green]✓[/green] {len(files)} daily files → {count:,} rows written to Delta")

    return {
        "table":        "cdr_records",
        "source_files": sorted(files),
        "target":       target_path,
        "rows_written": count,
        "batch_id":     BATCH_ID,
        "ingested_at":  datetime.now(tz=timezone.utc).isoformat(),
    }


def main():
    ensure_paths()
    txt_files = list(RAW_PATH.glob("sms-call-internet-mi-*.txt"))
    if not txt_files:
        console.print("[red]No CDR data files found in data/raw/.[/red]")
        console.print("[yellow]Run: make download[/yellow]")
        return

    console.print(f"\n[bold cyan]═══ Bronze Layer: Raw Ingestion ═══[/bold cyan]")
    console.print(f"  Found {len(txt_files)} daily file(s) in data/raw/")

    spark = get_spark_session()
    record = ingest_cdr(spark)

    lineage = {
        "layer":         "bronze",
        "batch_id":      BATCH_ID,
        "pipeline":      "bronze_ingestion",
        "run_timestamp": datetime.now(tz=timezone.utc).isoformat(),
        "tables":        [record],
    }
    (LINEAGE_PATH / f"bronze_lineage_{BATCH_ID}.json").write_text(
        json.dumps(lineage, indent=2)
    )

    console.print(f"\n[bold green]Bronze complete — {record['rows_written']:,} rows. Batch: {BATCH_ID}[/bold green]\n")


if __name__ == "__main__":
    main()
