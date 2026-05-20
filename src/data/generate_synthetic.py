"""Synthetic Telecom Italia Milan CDR data generator.

Schema mirrors the real Telecom Italia Big Data Challenge dataset:
  square_id       — Grid square ID 1-10000 (100×100 grid of Milan)
  time_interval   — Unix timestamp ms, start of 10-min slot (Nov 2013 epoch)
  country_code    — ISO 3166-1 numeric (39=Italy dominant)
  sms_in          — Incoming SMS activity (relative, anonymised units)
  sms_out         — Outgoing SMS activity
  call_in         — Incoming call activity
  call_out        — Outgoing call activity
  internet        — Internet activity

Produces: data/raw/cdr_records.csv  (~650 k rows by default)

Reference: Telecom Italia Big Data Challenge 2014
  https://doi.org/10.7910/DVN/EGZHFV
"""
from __future__ import annotations

import os
import random
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from rich.console import Console

load_dotenv()
console = Console()

SEED           = int(os.getenv("RANDOM_SEED", 42))
NUM_SQUARES    = int(os.getenv("NUM_SQUARES", 500))     # subset of 10 000
NUM_DAYS       = int(os.getenv("NUM_DAYS", 7))
ANOMALY_RATE   = float(os.getenv("ANOMALY_INJECTION_RATE", 0.05))

np.random.seed(SEED)
random.seed(SEED)

# Dataset epoch: 2013-11-01 00:00 UTC (matches real Telecom Italia CDR)
DATASET_START = datetime(2013, 11, 1, 0, 0, 0, tzinfo=timezone.utc)
INTERVAL_MS   = 600_000          # 10 minutes in milliseconds
INTERVALS_PER_DAY = 144

# Hourly activity curve (index = hour 0-23), same for SMS/Call/Internet
HOURLY_CURVE = np.array([
    0.05, 0.03, 0.02, 0.02, 0.03, 0.10,   # 00–05 night
    0.20, 0.48, 0.72, 0.87, 0.92, 0.96,   # 06–11 morning ramp
    1.00, 0.95, 0.88, 0.84, 0.88, 0.95,   # 12–17 midday
    1.00, 0.90, 0.70, 0.50, 0.30, 0.15,   # 18–23 evening drop
])
# Expand to 10-min resolution: 144 values per day
INTERVAL_CURVE = np.repeat(HOURLY_CURVE, 6)

# Foreign country codes added to a random subset of squares
FOREIGN_CODES    = [44, 49, 33, 1, 7, 86]   # UK, DE, FR, US, RU, CN
FOREIGN_SQ_FRAC  = 0.12   # 12 % of squares get a foreign-country series


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _select_squares() -> np.ndarray:
    rng = np.random.default_rng(SEED)
    return rng.choice(np.arange(1, 10_001), size=NUM_SQUARES, replace=False)


def _build_baselines(square_ids: np.ndarray) -> pd.DataFrame:
    """Per-square baseline activity using 2-D Gaussian centred on Milan's centre."""
    rng = np.random.default_rng(SEED + 1)
    rows = (square_ids - 1) // 100
    cols = (square_ids - 1) % 100
    dist = np.sqrt((rows - 50) ** 2 + (cols - 50) ** 2)
    geo  = np.exp(-dist / 35.0) + 0.25          # 0.25 – 1.25

    return pd.DataFrame({
        "square_id":      square_ids,
        "base_sms":       rng.lognormal(1.9, 0.65, NUM_SQUARES) * geo,
        "base_call":      rng.lognormal(1.6, 0.55, NUM_SQUARES) * geo,
        "base_internet":  rng.lognormal(2.3, 0.80, NUM_SQUARES) * geo,
    })


def _temporal_multipliers() -> np.ndarray:
    """Shape (NUM_DAYS × 144,) — combines diurnal curve with weekend dampening."""
    n = NUM_DAYS * INTERVALS_PER_DAY
    slots = np.tile(INTERVAL_CURVE, NUM_DAYS)
    dow = np.array([
        (DATASET_START + timedelta(minutes=10 * i)).weekday()
        for i in range(n)
    ])
    weekend = np.where(dow >= 5, 0.62, 1.0)
    return slots * weekend


def _time_slots() -> np.ndarray:
    """Unix-ms timestamps for each 10-min interval."""
    start_ms = int(DATASET_START.timestamp() * 1_000)
    n = NUM_DAYS * INTERVALS_PER_DAY
    return start_ms + np.arange(n, dtype=np.int64) * INTERVAL_MS


# ──────────────────────────────────────────────────────────────────────────────
# Core generation
# ──────────────────────────────────────────────────────────────────────────────

def _generate_series(
    square_ids: np.ndarray,
    baselines: pd.DataFrame,
    temporal: np.ndarray,
    time_slots: np.ndarray,
    country_code: int,
    rng: np.random.Generator,
    activity_scale: float = 1.0,
) -> pd.DataFrame:
    """Vectorised: generate all records for a set of squares under one country code."""
    n_sq = len(square_ids)
    n_t  = len(time_slots)

    bl = baselines.set_index("square_id").loc[square_ids]
    base_sms  = bl["base_sms"].values[:, None]       # (n_sq, 1)
    base_call = bl["base_call"].values[:, None]
    base_inet = bl["base_internet"].values[:, None]
    t = temporal[None, :]                              # (1, n_t)

    noise = rng.lognormal(0, 0.28, (n_sq, n_t, 5))

    sms_in   = np.maximum(0, base_sms  * t * noise[:, :, 0] * activity_scale)
    sms_out  = np.maximum(0, base_sms  * t * noise[:, :, 1] * activity_scale * 0.88)
    call_in  = np.maximum(0, base_call * t * noise[:, :, 2] * activity_scale)
    call_out = np.maximum(0, base_call * t * noise[:, :, 3] * activity_scale * 0.93)
    internet = np.maximum(0, base_inet * t * noise[:, :, 4] * activity_scale)

    n_rows = n_sq * n_t
    return pd.DataFrame({
        "square_id":          np.tile(square_ids[:, None], (1, n_t)).ravel(),
        "time_interval":      np.tile(time_slots[None, :], (n_sq, 1)).ravel(),
        "country_code":       country_code,
        "sms_in":             sms_in.ravel().round(4),
        "sms_out":            sms_out.ravel().round(4),
        "call_in":            call_in.ravel().round(4),
        "call_out":           call_out.ravel().round(4),
        "internet":           internet.ravel().round(4),
        "is_anomaly_injected": False,
        "anomaly_type":        np.full(n_rows, None, dtype=object),
    })


def _inject_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    rng  = np.random.default_rng(SEED + 99)
    n    = len(df)
    n_an = int(n * ANOMALY_RATE)
    idx  = rng.choice(n, size=n_an, replace=False)
    types = rng.choice(
        ["activity_spike", "dead_zone", "negative_activity", "future_timestamp", "null_activity"],
        size=n_an,
        p=[0.35, 0.20, 0.20, 0.15, 0.10],
    )

    activity_cols = ["sms_in", "sms_out", "call_in", "call_out", "internet"]
    df = df.copy()
    df.loc[idx, "is_anomaly_injected"] = True
    df.loc[idx, "anomaly_type"] = types

    # activity_spike — 15–50× multiplier on all activity metrics
    spike_idx = idx[types == "activity_spike"]
    if len(spike_idx):
        mult = rng.uniform(15, 50, len(spike_idx))
        for col in activity_cols:
            df.loc[spike_idx, col] = df.loc[spike_idx, col].values * mult

    # dead_zone — total silence (suspicious absence of signal)
    dead_idx = idx[types == "dead_zone"]
    if len(dead_idx):
        df.loc[dead_idx, activity_cols] = 0.0

    # negative_activity — one metric goes negative (data corruption)
    neg_idx = idx[types == "negative_activity"]
    if len(neg_idx):
        cols_choice = rng.integers(0, 5, len(neg_idx))
        for row_i, col_i in zip(neg_idx, cols_choice):
            df.at[row_i, activity_cols[col_i]] = float(-rng.uniform(1, 120))

    # future_timestamp — timestamp far beyond dataset window
    fut_idx = idx[types == "future_timestamp"]
    if len(fut_idx):
        future_base = int(datetime(2025, 1, 1, tzinfo=timezone.utc).timestamp() * 1_000)
        df.loc[fut_idx, "time_interval"] = (
            future_base + rng.integers(0, 365 * 24 * 3_600_000, len(fut_idx))
        ).astype(np.int64)

    # null_activity — all five metrics set to NaN
    null_idx = idx[types == "null_activity"]
    if len(null_idx):
        df.loc[null_idx, activity_cols] = np.nan

    return df


# ──────────────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────────────

def main():
    from src.utils.paths import RAW_PATH, ensure_paths
    ensure_paths()

    console.print("\n[bold cyan]Generating Telecom Italia Milan CDR data...[/bold cyan]")
    console.print(f"  Squares : {NUM_SQUARES:,} of 10 000")
    console.print(f"  Days    : {NUM_DAYS} (from {DATASET_START.date()})")
    console.print(f"  Interval: 10 min → {NUM_DAYS * INTERVALS_PER_DAY:,} slots")

    square_ids = _select_squares()
    baselines  = _build_baselines(square_ids)
    temporal   = _temporal_multipliers()
    time_slots = _time_slots()
    rng_main   = np.random.default_rng(SEED + 10)

    # Primary series: Italy (country_code = 39)
    df_italy = _generate_series(square_ids, baselines, temporal, time_slots, 39, rng_main)
    console.print(f"  [green]✓[/green] Italy records   : {len(df_italy):,}")

    # Foreign series: ~12 % of squares, reduced activity scale
    n_foreign_sq = max(1, int(NUM_SQUARES * FOREIGN_SQ_FRAC))
    foreign_sqs  = rng_main.choice(square_ids, size=n_foreign_sq, replace=False)
    foreign_code = int(rng_main.choice(FOREIGN_CODES))
    df_foreign   = _generate_series(
        foreign_sqs, baselines, temporal, time_slots,
        foreign_code, rng_main, activity_scale=0.18,
    )
    console.print(f"  [green]✓[/green] Foreign records : {len(df_foreign):,} (code {foreign_code})")

    df = pd.concat([df_italy, df_foreign], ignore_index=True)

    # Inject anomalies
    df = _inject_anomalies(df)
    n_anom = int(df["is_anomaly_injected"].sum())
    console.print(f"\n  [yellow]Anomalies injected: {n_anom:,} ({n_anom / len(df) * 100:.1f}%)[/yellow]")
    breakdown = df[df["is_anomaly_injected"]]["anomaly_type"].value_counts()
    for atype, cnt in breakdown.items():
        console.print(f"    {atype:<22}: {cnt:,}")

    out_path = RAW_PATH / "cdr_records.csv"
    df.to_csv(out_path, index=False)
    console.print(f"\n  [bold green]✓ Written {len(df):,} rows → {out_path}[/bold green]\n")


if __name__ == "__main__":
    main()
