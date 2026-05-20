.PHONY: setup install data bronze silver model gold agent flow all clean test lint help

PYTHON := python
UV := uv

help:
	@echo "Telecom Italia CDR Lakehouse - Available Commands"
	@echo "──────────────────────────────────────────────────"
	@echo "  make setup        Install dependencies with uv"
	@echo "  make install      Install with pip (fallback)"
	@echo "  make data         Generate synthetic CDR data (Telecom Italia Milan schema)"
	@echo "  make bronze       Ingest CDR CSV → Delta Lake (bronze)"
	@echo "  make silver       Clean, validate, flag CDR records (silver)"
	@echo "  make model        Train PyTorch CDR anomaly detection model"
	@echo "  make gold         Build CDR aggregations (gold)"
	@echo "  make agent        Run AI governance agent (requires ANTHROPIC_API_KEY)"
	@echo "  make flow         Run full Prefect orchestrated pipeline"
	@echo "  make all          Run complete pipeline end-to-end"
	@echo "  make test         Run test suite"
	@echo "  make lint         Lint with ruff"
	@echo "  make clean        Remove generated artifacts"

setup:
	$(UV) sync

install:
	pip install -e ".[dev]"

data:
	$(PYTHON) -m src.data.generate_synthetic

bronze:
	$(PYTHON) -m src.pipelines.bronze

silver:
	$(PYTHON) -m src.pipelines.silver

model:
	$(PYTHON) -m src.models.train

gold:
	$(PYTHON) -m src.pipelines.gold

agent:
	$(PYTHON) -m src.agents.governance_agent

flow:
	$(PYTHON) flows/main_flow.py

all:
	@echo "Running full pipeline..."
	$(MAKE) data
	$(MAKE) bronze
	$(MAKE) silver
	$(MAKE) model
	$(MAKE) gold
	$(MAKE) agent
	@echo "Pipeline complete. Check governance/reports/ for output."

test:
	$(PYTHON) -m pytest tests/ -v --tb=short

lint:
	ruff check src/ tests/ flows/

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
	rm -f derby.log
	rm -rf metastore_db spark-warehouse
	@echo "Cleaned Python artifacts"
