"""Gold layer: business-ready aggregations from silver CDR Delta table.

Produces:
  gold/hourly_grid_activity   — avg/max activity per square per hour-of-day
  gold/daily_country_usage    — total activity per country per date
  gold/grid_heatmap           — cumulative activity + tier per square
  gold/anomaly_summary        — ML-detected anomaly breakdown
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F
from rich.console import Console

from src.utils.logger import get_logger
from src.utils.paths import GOLD_PATH, LINEAGE_PATH, SILVER_PATH, ensure_paths
from src.utils.spark_session import get_spark_session

log = get_logger(__name__)
console = Console()
BATCH_ID = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
ACTIVITY = ["sms_in", "sms_out", "call_in", "call_out", "internet"]


def build_hourly_grid_activity(spark):
    """Average and peak activity per grid square per hour-of-day."""
    df = spark.read.format("delta").load(str(SILVER_PATH / "cdr_records"))

    return (
        df.filter(~F.col("_is_future_timestamp"))
          .groupBy("square_id", "hour_of_day", "day_of_week")
          .agg(
              F.avg("sms_in").alias("avg_sms_in"),
              F.avg("sms_out").alias("avg_sms_out"),
              F.avg("call_in").alias("avg_call_in"),
              F.avg("call_out").alias("avg_call_out"),
              F.avg("internet").alias("avg_internet"),
              F.max("internet").alias("peak_internet"),
              F.count("*").alias("record_count"),
          )
          .withColumn("_gold_processed_at", F.current_timestamp())
          .withColumn("_gold_batch_id", F.lit(BATCH_ID))
          .orderBy("square_id", "hour_of_day")
    )


def build_daily_country_usage(spark):
    """Total CDR activity per country code per calendar date."""
    df = spark.read.format("delta").load(str(SILVER_PATH / "cdr_records"))

    return (
        df.filter(~F.col("_is_future_timestamp"))
          .groupBy("country_code", "date")
          .agg(
              F.sum("sms_in").alias("total_sms_in"),
              F.sum("sms_out").alias("total_sms_out"),
              F.sum("call_in").alias("total_call_in"),
              F.sum("call_out").alias("total_call_out"),
              F.sum("internet").alias("total_internet"),
              F.count("*").alias("record_count"),
              F.countDistinct("square_id").alias("active_squares"),
          )
          .withColumn("_gold_processed_at", F.current_timestamp())
          .withColumn("_gold_batch_id", F.lit(BATCH_ID))
          .orderBy("date", "country_code")
    )


def build_grid_heatmap(spark):
    """Cumulative activity + tier classification per grid square."""
    df = spark.read.format("delta").load(str(SILVER_PATH / "cdr_records"))

    total_activity = sum(F.col(c) for c in ACTIVITY)

    agg = (
        df.filter(~F.col("_is_future_timestamp"))
          .groupBy("square_id")
          .agg(
              F.sum("sms_in").alias("total_sms_in"),
              F.sum("sms_out").alias("total_sms_out"),
              F.sum("call_in").alias("total_call_in"),
              F.sum("call_out").alias("total_call_out"),
              F.sum("internet").alias("total_internet"),
              F.max("internet").alias("peak_internet"),
              F.count("*").alias("record_count"),
          )
          .withColumn("total_activity",
                      F.col("total_sms_in") + F.col("total_sms_out") +
                      F.col("total_call_in") + F.col("total_call_out") +
                      F.col("total_internet"))
          .withColumn("grid_row", ((F.col("square_id") - 1) / 100).cast("int"))
          .withColumn("grid_col",  ((F.col("square_id") - 1) % 100).cast("int"))
    )

    # Compute activity tier using percentile approximation via window
    from pyspark.sql.window import Window
    w = Window.orderBy("total_activity")
    agg = (
        agg.withColumn("_rank_pct", F.percent_rank().over(w))
           .withColumn("activity_tier",
                       F.when(F.col("_rank_pct") >= 0.80, "High")
                        .when(F.col("_rank_pct") >= 0.40, "Medium")
                        .otherwise("Low"))
           .drop("_rank_pct")
           .withColumn("_gold_processed_at", F.current_timestamp())
           .withColumn("_gold_batch_id", F.lit(BATCH_ID))
    )
    return agg


def build_anomaly_summary(spark):
    anomaly_path = str(SILVER_PATH / "anomaly_scores")
    if not Path(anomaly_path).exists():
        log.warning("Anomaly scores not found — run `make model` first. Skipping.")
        return None

    scores = spark.read.format("delta").load(anomaly_path)
    return (
        scores.groupBy("anomaly_type_predicted")
              .agg(
                  F.count("*").alias("count"),
                  F.avg("reconstruction_error").alias("avg_reconstruction_error"),
                  F.avg("internet").alias("avg_internet"),
              )
              .withColumn("_gold_processed_at", F.current_timestamp())
              .withColumn("_gold_batch_id", F.lit(BATCH_ID))
    )


def write_gold(df, table_name: str) -> int:
    path = str(GOLD_PATH / table_name)
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
    count = df.count()
    log.info(f"  [green]✓[/green] {table_name}: {count:,} rows")
    return count


def main():
    ensure_paths()
    console.print("\n[bold cyan]═══ Gold Layer: CDR Aggregations ═══[/bold cyan]")

    spark = get_spark_session()
    lineage_records = []

    tables = {
        "hourly_grid_activity":  build_hourly_grid_activity,
        "daily_country_usage":   build_daily_country_usage,
        "grid_heatmap":          build_grid_heatmap,
    }

    for name, builder in tables.items():
        log.info(f"Building [bold]{name}[/bold]...")
        count = write_gold(builder(spark), name)
        lineage_records.append({
            "table": name, "rows": count,
            "source": "silver/cdr_records",
            "target": str(GOLD_PATH / name),
            "batch_id": BATCH_ID,
        })

    anomaly_df = build_anomaly_summary(spark)
    if anomaly_df is not None:
        count = write_gold(anomaly_df, "anomaly_summary")
        lineage_records.append({
            "table": "anomaly_summary", "rows": count,
            "source": "silver/anomaly_scores",
            "target": str(GOLD_PATH / "anomaly_summary"),
            "batch_id": BATCH_ID,
        })

    lineage = {
        "layer": "gold", "batch_id": BATCH_ID,
        "pipeline": "gold_aggregations",
        "run_timestamp": datetime.now(tz=timezone.utc).isoformat(),
        "tables": lineage_records,
    }
    (LINEAGE_PATH / f"gold_lineage_{BATCH_ID}.json").write_text(json.dumps(lineage, indent=2))
    console.print(f"\n[bold green]Gold pipeline complete. Batch: {BATCH_ID}[/bold green]\n")


if __name__ == "__main__":
    main()
