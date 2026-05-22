.PHONY: setup install download bronze silver model gold agent flow all clean test lint help

VENV   := .venv
PYTHON := $(shell [ -f $(VENV)/bin/python ] && echo $(VENV)/bin/python || echo python)
UV     := uv
export PYTHONPATH := .

# Number of days to download (default 7 = Mon 2013-11-04 through Sun 2013-11-10)
DAYS ?= 7

help:
	@echo "Telecom Italia CDR Lakehouse — Available Commands"
	@echo "──────────────────────────────────────────────────"
	@echo "  make setup          Install dependencies (uv)"
	@echo "  make install        Install dependencies (pip fallback)"
	@echo "  make download       Download real CDR data from Harvard Dataverse"
	@echo "                      Override days: make download DAYS=3"
	@echo "  make bronze         Ingest raw .txt files → Delta Lake (bronze)"
	@echo "  make silver         Clean, validate, flag CDR records (silver)"
	@echo "  make model          Train PyTorch anomaly detector + run inference"
	@echo "  make gold           Build CDR aggregation tables (gold)"
	@echo "  make agent          Run AI governance agent (needs ANTHROPIC_API_KEY)"
	@echo "  make all            Download + full pipeline end-to-end"
	@echo "  make clean          Remove generated data/model/report artifacts"
	@echo "  make test           Run test suite"
	@echo "  make lint           Lint with ruff"

setup:
	$(UV) sync

install:
	$(PYTHON) -m pip install -e ".[dev]"

download:
	$(PYTHON) -m src.data.download_raw --days $(DAYS) $(if $(TOKEN),--token $(TOKEN),)

bronze:
	$(PYTHON) -m src.pipelines.bronze

silver:
	$(PYTHON) -m src.pipelines.silver

model:
	$(PYTHON) -m src.models.train
	$(PYTHON) -m src.models.inference

gold:
	$(PYTHON) -m src.pipelines.gold

agent:
	$(PYTHON) -m src.agents.governance_agent

flow:
	$(PYTHON) flows/main_flow.py

all:
	@echo "Running full pipeline..."
	$(MAKE) download
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
	rm -rf data/bronze data/silver data/gold
	rm -rf src/models/trained/*.pt src/models/trained/*.pkl src/models/trained/*.json
	rm -rf governance/reports/*.md governance/reports/*.json governance/lineage/*.json
	@echo "Cleaned all generated artifacts."
