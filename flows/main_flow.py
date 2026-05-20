"""Prefect orchestration: full lakehouse pipeline as a single flow.

Run: python flows/main_flow.py
  or: prefect run flows/main_flow.py
"""
from __future__ import annotations

from datetime import datetime, timezone

from prefect import flow, task
from prefect.logging import get_run_logger
from rich.console import Console

console = Console()


@task(name="generate-cdr-data", retries=0)
def task_generate_data():
    from src.data.generate_synthetic import main as gen_main
    gen_main()
    return "data_generated"


@task(name="bronze-ingestion", retries=1)
def task_bronze():
    from src.pipelines.bronze import main as bronze_main
    bronze_main()
    return "bronze_complete"


@task(name="silver-transformation", retries=1)
def task_silver():
    from src.pipelines.silver import main as silver_main
    silver_main()
    return "silver_complete"


@task(name="train-anomaly-model", retries=0)
def task_train_model():
    from src.models.train import main as train_main
    train_main()
    return "model_trained"


@task(name="run-anomaly-inference", retries=0)
def task_inference():
    from src.models.inference import run_inference
    n_anomalies, threshold = run_inference()
    return {"anomalies": n_anomalies, "threshold": threshold}


@task(name="gold-aggregations", retries=1)
def task_gold():
    from src.pipelines.gold import main as gold_main
    gold_main()
    return "gold_complete"


@task(name="ai-governance-agent", retries=0)
def task_governance():
    from src.agents.governance_agent import main as agent_main
    agent_main()
    return "governance_complete"


@flow(
    name="data-governance-lakehouse",
    description="End-to-end AI-powered data governance pipeline on Telecom Italia Milan CDR: raw → bronze → silver → ML → gold → AI governance",
    version="1.0.0",
)
def lakehouse_flow(skip_data_gen: bool = False, skip_model_train: bool = False):
    logger = get_run_logger()
    start = datetime.now(tz=timezone.utc)
    logger.info(f"Lakehouse pipeline started at {start.isoformat()}")

    # Phase 1: Generate data (optional skip for reruns)
    if not skip_data_gen:
        task_generate_data()

    # Phase 2: Bronze ingestion
    task_bronze()

    # Phase 3: Silver transformation + validation
    task_silver()

    # Phase 4: ML model (train once, inference always)
    if not skip_model_train:
        task_train_model()
    task_inference()

    # Phase 5: Gold aggregations
    task_gold()

    # Phase 6: AI governance
    task_governance()

    elapsed = (datetime.now(tz=timezone.utc) - start).total_seconds()
    logger.info(f"Pipeline complete in {elapsed:.1f}s")
    console.print(f"\n[bold green]Full pipeline complete in {elapsed:.1f}s[/bold green]")
    console.print("Check [cyan]governance/reports/[/cyan] for the AI-generated governance report.")


if __name__ == "__main__":
    lakehouse_flow()
