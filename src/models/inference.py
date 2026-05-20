"""Score all silver CDR records with the trained autoencoder and write anomaly flags.

Writes: silver/anomaly_scores  (Delta table)
"""
from __future__ import annotations

import pickle
from pathlib import Path

import numpy as np
import torch
from rich.console import Console

from src.models.autoencoder import CDRAutoencoder
from src.models.train import engineer_features
from src.utils.logger import get_logger
from src.utils.paths import MODEL_SAVE_PATH, SILVER_PATH, ensure_paths
from src.utils.spark_session import get_spark_session

log = get_logger(__name__)
console = Console()

ACTIVITY_COLS = ["sms_in", "sms_out", "call_in", "call_out", "internet"]


def classify_anomaly(error: float, threshold: float, row) -> str:
    if error <= threshold:
        return "normal"
    # Use row-level signals from silver flags where available
    if row.get("_is_future_timestamp", False):
        return "future_timestamp"
    if row.get("_is_null_activity", False):
        return "null_activity"
    if row.get("_has_negative_activity", False):
        return "negative_activity"
    # Heuristic: very large error → activity spike; low error band → dead zone
    max_activity = max(float(row.get(c, 0) or 0) for c in ACTIVITY_COLS)
    if max_activity < 0.01:
        return "dead_zone"
    return "activity_spike"


def run_inference():
    ensure_paths()
    model_path  = str(MODEL_SAVE_PATH / "autoencoder.pt")
    scaler_path = str(MODEL_SAVE_PATH / "scaler.pkl")

    if not Path(model_path).exists():
        raise FileNotFoundError(f"Model not found at {model_path}. Run `make model` first.")

    console.print("\n[bold cyan]═══ CDR Anomaly Detection: Inference ═══[/bold cyan]")

    model, threshold = CDRAutoencoder.load(model_path)
    with open(scaler_path, "rb") as f:
        scaler = pickle.load(f)

    spark = get_spark_session()
    pdf = spark.read.format("delta").load(str(SILVER_PATH / "cdr_records")).toPandas()
    log.info(f"Scoring {len(pdf):,} CDR records...")

    features        = engineer_features(pdf)
    features_scaled = scaler.transform(features).astype(np.float32)

    with torch.no_grad():
        errors = model.reconstruction_error(torch.from_numpy(features_scaled)).numpy()

    pdf["reconstruction_error"] = errors
    pdf["is_anomaly_predicted"] = errors > threshold
    pdf["anomaly_type_predicted"] = [
        classify_anomaly(e, threshold, row)
        for e, (_, row) in zip(errors, pdf.iterrows())
    ]

    n_anomalies = int(pdf["is_anomaly_predicted"].sum())
    log.info(f"  Anomalies detected: {n_anomalies:,} ({n_anomalies / len(pdf) * 100:.1f}%)")

    result_cols = [
        "square_id", "time_interval", "country_code",
        "sms_in", "sms_out", "call_in", "call_out", "internet",
        "hour_of_day", "day_of_week",
        "reconstruction_error", "is_anomaly_predicted", "anomaly_type_predicted",
        "_has_negative_activity", "_is_future_timestamp", "_is_null_activity",
    ]
    result_cols = [c for c in result_cols if c in pdf.columns]
    result_spark = spark.createDataFrame(pdf[result_cols])

    (result_spark.write
                 .format("delta")
                 .mode("overwrite")
                 .option("overwriteSchema", "true")
                 .save(str(SILVER_PATH / "anomaly_scores")))

    log.info("  [green]✓[/green] Anomaly scores written to silver/anomaly_scores")

    console.print("\n[bold]Anomaly Breakdown:[/bold]")
    for k, v in pdf.groupby("anomaly_type_predicted").size().items():
        console.print(f"  {k:<26}: {v:,}")

    console.print(f"\n[bold green]Inference complete. Threshold: {threshold:.6f}[/bold green]\n")
    return n_anomalies, threshold


if __name__ == "__main__":
    run_inference()
