"""Train CDRAutoencoder on clean (non-anomalous) silver CDR records.

Process:
  1. Load silver cdr_records
  2. Filter to clean records only (no negative / future / null flags)
  3. Engineer 7 features
  4. Fit StandardScaler
  5. Train autoencoder for EPOCHS
  6. Compute P95 reconstruction-error threshold
  7. Save model, scaler, and metadata
"""
from __future__ import annotations

import json
import os
import pickle
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import torch
import torch.nn as nn
from rich.console import Console
from rich.progress import track
from sklearn.preprocessing import StandardScaler
from torch.utils.data import DataLoader, TensorDataset

from pyspark.sql import functions as F

from src.models.autoencoder import CDRAutoencoder
from src.utils.logger import get_logger
from src.utils.paths import MODEL_SAVE_PATH, SILVER_PATH, ensure_paths
from src.utils.spark_session import get_spark_session

log = get_logger(__name__)
console = Console()

THRESHOLD_PCT = float(os.getenv("ANOMALY_THRESHOLD_PERCENTILE", 95))
EPOCHS        = 50
BATCH_SIZE    = 512
LR            = 1e-3
SEED          = 42


def engineer_features(pdf) -> np.ndarray:
    """Extract 7 normalised features from a CDR pandas DataFrame."""
    import pandas as pd

    for col in ["sms_in", "sms_out", "call_in", "call_out", "internet"]:
        pdf[col] = pd.to_numeric(pdf[col], errors="coerce").fillna(0.0).clip(lower=0)

    hour = pd.to_numeric(pdf.get("hour_of_day",  pd.Series(np.zeros(len(pdf)))), errors="coerce").fillna(0)
    dow  = pd.to_numeric(pdf.get("day_of_week",  pd.Series(np.zeros(len(pdf)))), errors="coerce").fillna(0)

    return np.column_stack([
        pdf["sms_in"].values,
        pdf["sms_out"].values,
        pdf["call_in"].values,
        pdf["call_out"].values,
        pdf["internet"].values,
        (hour / 23.0).values,
        (dow  /  6.0).values,
    ]).astype(np.float32)


def train(features_clean: np.ndarray) -> tuple[CDRAutoencoder, float, StandardScaler]:
    torch.manual_seed(SEED)
    np.random.seed(SEED)

    scaler = StandardScaler()
    X = scaler.fit_transform(features_clean).astype(np.float32)

    loader = DataLoader(TensorDataset(torch.from_numpy(X)),
                        batch_size=BATCH_SIZE, shuffle=True, drop_last=False)

    model     = CDRAutoencoder(input_dim=X.shape[1])
    optimizer = torch.optim.Adam(model.parameters(), lr=LR, weight_decay=1e-5)
    scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(optimizer, T_max=EPOCHS)
    criterion = nn.MSELoss()

    model.train()
    for epoch in track(range(EPOCHS), description="Training CDR autoencoder..."):
        epoch_loss = 0.0
        for (batch,) in loader:
            optimizer.zero_grad()
            loss = criterion(model(batch), batch)
            loss.backward()
            nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            optimizer.step()
            epoch_loss += loss.item()
        scheduler.step()
        if (epoch + 1) % 10 == 0:
            log.info(f"  Epoch {epoch+1}/{EPOCHS} — loss: {epoch_loss/len(loader):.6f}")

    model.eval()
    with torch.no_grad():
        errors = model.reconstruction_error(torch.from_numpy(X)).numpy()

    threshold = float(np.percentile(errors, THRESHOLD_PCT))
    log.info(f"  Anomaly threshold (P{int(THRESHOLD_PCT)}): {threshold:.6f}")
    return model, threshold, scaler


def main():
    ensure_paths()
    MODEL_SAVE_PATH.mkdir(parents=True, exist_ok=True)
    console.print("\n[bold cyan]═══ CDR Anomaly Detection: Training ═══[/bold cyan]")

    spark = get_spark_session()
    df = spark.read.format("delta").load(str(SILVER_PATH / "cdr_records"))

    # Train only on records with no quality flags raised
    clean_pdf = df.filter(
        (~F.col("_has_negative_activity")) &
        (~F.col("_is_future_timestamp")) &
        (~F.col("_is_null_activity"))
    ).toPandas()

    log.info(f"Clean training samples: {len(clean_pdf):,}")
    features = engineer_features(clean_pdf)
    model, threshold, scaler = train(features)

    model_path  = str(MODEL_SAVE_PATH / "autoencoder.pt")
    scaler_path = str(MODEL_SAVE_PATH / "scaler.pkl")
    meta_path   = str(MODEL_SAVE_PATH / "model_meta.json")

    model.save(model_path, threshold)
    with open(scaler_path, "wb") as f:
        pickle.dump(scaler, f)

    meta = {
        "trained_at":           datetime.now(tz=timezone.utc).isoformat(),
        "training_samples":     len(clean_pdf),
        "epochs":               EPOCHS,
        "threshold_percentile": THRESHOLD_PCT,
        "anomaly_threshold":    threshold,
        "features":             CDRAutoencoder.FEATURE_NAMES,
        "architecture":         "7→32→16→4→16→32→7",
        "dataset":              "Telecom Italia Milan CDR (synthetic replica)",
    }
    Path(meta_path).write_text(json.dumps(meta, indent=2))

    console.print(f"\n[green]✓[/green] Model    : {model_path}")
    console.print(f"[green]✓[/green] Scaler   : {scaler_path}")
    console.print(f"[green]✓[/green] Threshold: {threshold:.6f}")
    console.print(f"\n[bold green]Training complete.[/bold green]\n")


if __name__ == "__main__":
    main()
