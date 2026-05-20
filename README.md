# AI-Powered Data Governance & Quality Lakehouse

[![CI](https://github.com/YOUR_USERNAME/data-governance-lakehouse/actions/workflows/ci.yml/badge.svg)](https://github.com/YOUR_USERNAME/data-governance-lakehouse/actions)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://python.org)
[![PySpark](https://img.shields.io/badge/PySpark-3.5-orange.svg)](https://spark.apache.org)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.2-00ADD8.svg)](https://delta.io)
[![PyTorch](https://img.shields.io/badge/PyTorch-2.1%2B-EE4C2C.svg)](https://pytorch.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A production-grade data engineering platform that combines **Databricks Medallion Architecture**, **PyTorch anomaly detection**, and **Claude AI governance agents** to simulate how modern enterprise companies manage large-scale data platforms with automation, governance, and AI-assisted workflows.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                      E-Commerce Raw Data                            │
│              customers │ products │ orders │ order_items            │
└─────────────────────────────┬───────────────────────────────────────┘
                              │ CSV ingestion
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     BRONZE LAYER (Delta Lake)                       │
│  Raw data preserved exactly + audit columns                         │
│  _ingested_at │ _source_file │ _batch_id │ _pipeline_version        │
└─────────────────────────────┬───────────────────────────────────────┘
                              │ PySpark transformations
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     SILVER LAYER (Delta Lake)                       │
│  Type coercion │ Null handling │ Deduplication                      │
│  Pandera schema validation │ Suspicious record flagging             │
│  Quality reports (JSON + Markdown)                                  │
└──────────────┬──────────────────────────────┬───────────────────────┘
               │ clean orders                 │ all orders
               ▼                              ▼
┌──────────────────────────┐    ┌─────────────────────────────────────┐
│   PyTorch Autoencoder    │    │         GOLD LAYER (Delta)          │
│   Trains on clean data   │    │  daily_revenue │ customer_ltv       │
│   Reconstruction error   │───▶│  product_performance                │
│   threshold = P95        │    │  anomaly_summary                    │
└──────────────────────────┘    └──────────────────┬──────────────────┘
                                                   │ business metrics
                                                   ▼
                              ┌────────────────────────────────────────┐
                              │      Claude AI Governance Agent        │
                              │  Executive summary │ Risk assessment   │
                              │  Data lineage │ Recommendations        │
                              │  Data catalog │ SLA status             │
                              └────────────────────────────────────────┘
```

---

## Features

| Feature | Technology | Description |
|---------|-----------|-------------|
| **Lakehouse Architecture** | PySpark + Delta Lake | Full Medallion (Bronze/Silver/Gold) with ACID transactions |
| **Data Ingestion** | PySpark + Delta Lake | Schema enforcement, audit columns, transaction logs |
| **Data Quality** | Pandera | Schema validation, null analysis, business rule checks |
| **Anomaly Detection** | PyTorch Autoencoder | Unsupervised ML to flag suspicious transactions |
| **Orchestration** | Prefect | DAG-based pipeline with retries and logging |
| **AI Governance** | Claude API | LLM-generated data quality reports and catalogs |
| **Data Lineage** | Custom JSON | Full Bronze → Silver → Gold lineage tracking |
| **Testing** | pytest | Unit tests for validators, models, and data quality |
| **CI/CD** | GitHub Actions | Automated linting and testing on push |

---

## Quick Start

### Prerequisites
- Python 3.10+
- Java 11+ (required by PySpark)
- `ANTHROPIC_API_KEY` (for the AI governance agent)

### Installation

```bash
# Clone the repo
git clone https://github.com/YOUR_USERNAME/data-governance-lakehouse
cd data-governance-lakehouse

# Install dependencies (recommended: uv)
pip install uv
uv sync

# OR standard pip
pip install -e ".[dev]"

# Configure environment
cp .env.example .env
# Edit .env and add your ANTHROPIC_API_KEY
```

### Run the Full Pipeline

```bash
# Run everything end-to-end
make all

# OR step by step:
make data    # Generate 10k synthetic orders with anomalies
make bronze  # Ingest to Delta Lake (raw)
make silver  # Clean, validate, flag suspicious records
make model   # Train PyTorch anomaly detector
make gold    # Build business aggregations
make agent   # Generate AI governance report
```

### Prefect Orchestration

```bash
# Run as a Prefect flow (with task tracking and retries)
make flow

# View Prefect UI (optional)
prefect server start  # in another terminal
```

### Jupyter Notebooks

```bash
jupyter lab
# Open notebooks/ in order: 01 → 02 → 03 → 04
```

---

## Project Structure

```
data-governance-lakehouse/
├── src/
│   ├── data/
│   │   └── generate_synthetic.py    # Synthetic e-commerce data with injected anomalies
│   ├── pipelines/
│   │   ├── bronze.py                # Raw ingestion → Delta Lake
│   │   ├── silver.py                # Cleaning + validation + quality reports
│   │   └── gold.py                  # Business aggregations
│   ├── models/
│   │   ├── autoencoder.py           # PyTorch Autoencoder architecture
│   │   ├── train.py                 # Training loop with StandardScaler
│   │   └── inference.py            # Score all orders, write anomaly_scores Delta table
│   ├── quality/
│   │   ├── validators.py            # Pandera schemas for each table
│   │   └── reporter.py             # JSON + Markdown quality reports
│   ├── agents/
│   │   └── governance_agent.py      # Claude-powered governance doc generation
│   └── utils/
│       ├── spark_session.py         # Delta Lake SparkSession (singleton)
│       ├── paths.py                 # Centralised path resolution
│       └── logger.py               # Rich-formatted logging
├── flows/
│   └── main_flow.py                 # Prefect orchestration
├── notebooks/
│   ├── 01_bronze_ingestion.ipynb   # Delta Lake ingestion walkthrough
│   ├── 02_silver_transformation.ipynb
│   ├── 03_anomaly_detection.ipynb  # PyTorch training + evaluation
│   └── 04_gold_and_governance.ipynb
├── data/
│   ├── raw/                         # CSV source files
│   ├── bronze/                      # Delta tables (raw)
│   ├── silver/                      # Delta tables (clean)
│   └── gold/                        # Delta tables (aggregated)
├── governance/
│   ├── reports/                     # Quality reports + AI governance docs
│   ├── lineage/                     # Pipeline lineage JSON
│   └── catalog/                     # AI-generated data catalog
├── tests/                           # pytest test suite
├── .github/workflows/ci.yml        # GitHub Actions CI
├── Makefile                         # One-command interface
└── pyproject.toml                   # Modern Python packaging (uv-compatible)
```

---

## Anomaly Detection

The pipeline injects 5 types of anomalies into the synthetic data to demonstrate ML-based detection:

| Anomaly Type | Description | Business Impact |
|---|---|---|
| `negative_amount` | Orders with negative totals | Data corruption / refund fraud |
| `extreme_amount` | Orders 10-100x normal range | Potential fraud or pricing error |
| `future_date` | Orders timestamped in the future | ETL pipeline clock drift |
| `zero_quantity` | Line items with zero units | Data entry error |
| `invalid_customer` | Orphaned orders (no matching customer) | Referential integrity failure |

The **PyTorch Autoencoder** (6→32→16→8→16→32→6) trains on clean data only, then uses reconstruction error to flag anomalies at the 95th percentile threshold.

---

## AI Governance Agent

The governance agent uses Claude to produce:

1. **Executive Summary** — non-technical business health overview
2. **Quality Scorecard** — per-table validation results
3. **Anomaly Risk Assessment** — business impact in dollar terms
4. **Data Lineage Narrative** — human-readable lineage summary
5. **Data Catalog** — table descriptions, PII columns, SLAs, ownership
6. **Governance Recommendations** — prioritised, actionable items

Output: `governance/reports/governance_report_<timestamp>.md`

---

## Data Lineage

Every pipeline run writes a lineage JSON to `governance/lineage/`:

```json
{
  "layer": "silver",
  "batch_id": "20240115_143022",
  "pipeline": "silver_transformation",
  "tables": [
    {
      "table": "orders",
      "rows_before": 10000,
      "rows_after": 9847,
      "dropped": 153,
      "suspicious_flagged": 482,
      "source": "data/bronze/orders",
      "target": "data/silver/orders"
    }
  ]
}
```

---

## Running Tests

```bash
make test
# or
pytest tests/ -v
```

Tests cover:
- Synthetic data generation and schema correctness
- Pandera validation (pass/fail cases)
- PyTorch autoencoder forward pass and anomaly scoring
- Quality reporter markdown generation

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Python 3.10+ |
| Compute | Apache Spark 3.5 (local mode) |
| Storage | Delta Lake 3.2 (Parquet + transaction log) |
| ML | PyTorch 2.1, scikit-learn |
| Data Quality | Pandera |
| Orchestration | Prefect 2.x |
| AI Agent | Anthropic Claude (claude-sonnet-4-6) |
| Testing | pytest |
| Packaging | uv + pyproject.toml |
| CI | GitHub Actions |

---

## License

MIT — see [LICENSE](LICENSE).

---

*Built to demonstrate enterprise data engineering patterns: Medallion architecture, ML-powered data quality, and AI-assisted governance.*
