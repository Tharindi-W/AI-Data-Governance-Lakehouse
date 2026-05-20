"""Centralised path resolution from environment variables."""
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

ROOT = Path(__file__).resolve().parents[2]

RAW_PATH = Path(os.getenv("RAW_DATA_PATH", ROOT / "data" / "raw"))
BRONZE_PATH = Path(os.getenv("BRONZE_PATH", ROOT / "data" / "bronze"))
SILVER_PATH = Path(os.getenv("SILVER_PATH", ROOT / "data" / "silver"))
GOLD_PATH = Path(os.getenv("GOLD_PATH", ROOT / "data" / "gold"))
GOVERNANCE_PATH = Path(os.getenv("GOVERNANCE_PATH", ROOT / "governance"))
REPORTS_PATH = GOVERNANCE_PATH / "reports"
LINEAGE_PATH = GOVERNANCE_PATH / "lineage"
CATALOG_PATH = GOVERNANCE_PATH / "catalog"
MODEL_SAVE_PATH = Path(os.getenv("MODEL_SAVE_PATH", ROOT / "src" / "models" / "trained"))


def ensure_paths():
    for p in [RAW_PATH, BRONZE_PATH, SILVER_PATH, GOLD_PATH,
              REPORTS_PATH, LINEAGE_PATH, CATALOG_PATH, MODEL_SAVE_PATH]:
        p.mkdir(parents=True, exist_ok=True)
