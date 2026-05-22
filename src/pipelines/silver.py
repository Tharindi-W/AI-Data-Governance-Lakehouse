"""Silver layer: clean, validate, and flag CDR records.

Steps:
  1. Cast time_interval to readable timestamp
  2. Clip / flag negative activity values
  3. Flag future timestamps
  4. Flag null-activity records
  5. Deduplicate on (square_id, time_interval, country_code)
  6. Pandera schema validation
  7. Quality report
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

import pandas as pd
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import TimestampType
from rich.console import Console

from src.quality.reporter import QualityReporter
from src.quality.validators import validate_cdr
from src.utils.logger import get_logger
from src.utils.paths import BRONZE_PATH, LINEAGE_PATH, SILVER_PATH, ensure_paths
from src.utils.spark_session import get_spark_session

log = get_logger(__name__)
console = Console()
BATCH_ID = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")

ACTIVITY_COLS = ["sms_in", "sms_out", "call_in", "call_out", "internet"]


def clean_cdr(spark) -> tuple[DataFrame, dict]:
    df = spark.read.format("delta").load(str(BRONZE_PATH / "cdr_records"))
    before = df.count()

    # Derive human-readable timestamp from Unix-ms field
    df = df.withColumn(
        "event_time",
        (F.col("time_interval") / 1000).cast(TimestampType()),
    )

    # Temporal flags
    df = (df
          .withColumn("_is_future_timestamp",
                      F.col("time_interval") > (F.unix_timestamp(F.current_timestamp()) * 1000))
          .withColumn("hour_of_day",   F.hour("event_time"))
          .withColumn("day_of_week",   F.dayofweek("event_time"))  # 1=Sun … 7=Sat
          .withColumn("date",          F.to_date("event_time")))

    # Activity quality flags (before coercion so flags reflect raw values)
    neg_cond = " OR ".join([f"{c} < 0" for c in ACTIVITY_COLS])
    null_cond = " AND ".join([f"{c} IS NULL" for c in ACTIVITY_COLS])
    df = (df
          .withColumn("_has_negative_activity", F.expr(neg_cond))
          .withColumn("_is_null_activity",      F.expr(null_cond)))

    # Coerce: clip negatives to 0
    for col in ACTIVITY_COLS:
        df = df.withColumn(col, F.greatest(F.col(col), F.lit(0.0)))

    # Fill remaining nulls with 0 (isolated nulls, not wholesale null_activity)
    df = df.fillna(0.0, subset=ACTIVITY_COLS)

    # Deduplicate on natural key
    df = (df.dropDuplicates(["square_id", "time_interval", "country_code"])
            .withColumn("_silver_processed_at", F.current_timestamp())
            .withColumn("_silver_batch_id",     F.lit(BATCH_ID)))

    after = df.count()
    suspicious = df.filter(
        F.col("_has_negative_activity") | F.col("_is_future_timestamp") | F.col("_is_null_activity")
    ).count()

    return df, {
        "table":              "cdr_records",
        "rows_before":        before,
        "rows_after":         after,
        "dropped":            before - after,
        "suspicious_flagged": suspicious,
    }


def write_silver(df: DataFrame, table_name: str):
    path = str(SILVER_PATH / table_name)
    (df.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .save(path))
    log.info(f"  [green]✓[/green] Wrote {table_name} to silver Delta")


def main():
    ensure_paths()
    console.print("\n[bold cyan]═══ Silver Layer: Cleaning & Validation ═══[/bold cyan]")

    spark = get_spark_session()
    reporter = QualityReporter(layer="silver", batch_id=BATCH_ID)

    df, stats = clean_cdr(spark)
    write_silver(df, "cdr_records")
    reporter.add_table_stats("cdr_records", stats)

    console.print("\n[bold]Running schema validation (sample 100k rows)...[/bold]")
    pdf = (spark.read.format("delta")
               .load(str(SILVER_PATH / "cdr_records"))
               .sample(fraction=0.1, seed=42)
               .limit(100_000)
               .toPandas())
    validation = {"cdr_records": validate_cdr(pdf)}
    reporter.add_validation_results(validation)

    status = "[green]PASS[/green]" if validation["cdr_records"]["passed"] else "[red]FAIL[/red]"
    log.info(f"  Validation cdr_records: {status} "
             f"({validation['cdr_records']['checks_passed']}/{validation['cdr_records']['checks_total']} checks)")

    report_path = reporter.save()
    console.print(f"\n[bold green]Quality report: {report_path}[/bold green]")

    lineage = {
        "layer":         "silver",
        "batch_id":      BATCH_ID,
        "pipeline":      "silver_transformation",
        "run_timestamp": datetime.now(tz=timezone.utc).isoformat(),
        "tables": [{
            **stats,
            "source":   str(BRONZE_PATH / "cdr_records"),
            "target":   str(SILVER_PATH / "cdr_records"),
            "batch_id": BATCH_ID,
        }],
    }
    lp = LINEAGE_PATH / f"silver_lineage_{BATCH_ID}.json"
    lp.write_text(json.dumps(lineage, indent=2))
    console.print(f"[bold green]Silver pipeline complete. Batch: {BATCH_ID}[/bold green]\n")


if __name__ == "__main__":
    main()
