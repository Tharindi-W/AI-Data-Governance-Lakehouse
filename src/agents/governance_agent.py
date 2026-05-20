"""AI Governance Agent powered by Claude.

Reads CDR quality reports, anomaly scores, and lineage metadata, then generates:
  - Executive summary of network data health
  - Anomaly risk assessment (activity spikes, dead zones, data integrity issues)
  - Data lineage narrative
  - Governance recommendations
  - Data catalog entries for each CDR table

Output: governance/reports/governance_report_<batch>.md
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import anthropic
from dotenv import load_dotenv
from rich.console import Console
from rich.markdown import Markdown

from src.utils.logger import get_logger
from src.utils.paths import (
    CATALOG_PATH, GOLD_PATH, LINEAGE_PATH,
    REPORTS_PATH, SILVER_PATH, ensure_paths,
)
from src.utils.spark_session import get_spark_session

load_dotenv()
log = get_logger(__name__)
console = Console()

MODEL    = "claude-sonnet-4-6"
BATCH_ID = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")


def load_quality_reports() -> dict:
    reports = {}
    if not REPORTS_PATH.exists():
        return reports
    for path in REPORTS_PATH.glob("silver_quality_*.json"):
        with open(path) as f:
            reports["silver"] = json.load(f)
        break
    return reports


def load_lineage() -> list[dict]:
    lineage = []
    if not LINEAGE_PATH.exists():
        return lineage
    for path in sorted(LINEAGE_PATH.glob("*.json")):
        with open(path) as f:
            lineage.append(json.load(f))
    return lineage


def load_anomaly_stats(spark) -> dict:
    anomaly_path = str(SILVER_PATH / "anomaly_scores")
    if not Path(anomaly_path).exists():
        return {"available": False}

    try:
        df  = spark.read.format("delta").load(anomaly_path)
        pdf = df.toPandas()
        total       = len(pdf)
        n_anomalies = int(pdf["is_anomaly_predicted"].sum())
        breakdown   = (
            pdf.groupby("anomaly_type_predicted")
               .agg(count=("square_id", "size"),
                    avg_internet=("internet", "mean"))
               .reset_index()
               .to_dict(orient="records")
        )
        high_risk = pdf[pdf["reconstruction_error"] > pdf["reconstruction_error"].quantile(0.99)]
        return {
            "available":           True,
            "total_records":       total,
            "anomalies_detected":  n_anomalies,
            "anomaly_rate_pct":    round(n_anomalies / total * 100, 2),
            "breakdown":           breakdown,
            "high_risk_count":     len(high_risk),
            "top_internet_spikes": high_risk["internet"].nlargest(5).tolist(),
        }
    except Exception as e:
        return {"available": False, "error": str(e)}


def load_gold_metrics(spark) -> dict:
    metrics = {}
    tables  = ["hourly_grid_activity", "daily_country_usage", "grid_heatmap"]
    for table in tables:
        path = str(GOLD_PATH / table)
        if Path(path).exists():
            try:
                df = spark.read.format("delta").load(path)
                metrics[table] = {"row_count": df.count(), "columns": df.columns}
                if table == "daily_country_usage":
                    from pyspark.sql import functions as F
                    s = df.agg(
                        F.sum("total_internet").alias("total_internet"),
                        F.countDistinct("country_code").alias("unique_countries"),
                        F.countDistinct("date").alias("active_days"),
                    ).collect()[0]
                    metrics[table]["summary"] = {
                        "total_internet":    float(s["total_internet"] or 0),
                        "unique_countries":  int(s["unique_countries"] or 0),
                        "active_days":       int(s["active_days"] or 0),
                    }
                elif table == "grid_heatmap":
                    from pyspark.sql import functions as F
                    tier_dist = df.groupBy("activity_tier").count().collect()
                    metrics[table]["tier_distribution"] = {
                        r["activity_tier"]: r["count"] for r in tier_dist
                    }
            except Exception as e:
                metrics[table] = {"error": str(e)}
    return metrics


def build_context_prompt(quality: dict, lineage: list, anomaly: dict, gold: dict) -> str:
    return f"""You are a senior Data Governance Officer at a major European telecommunications company.
You have just completed the daily processing of the Milan CDR (Call Detail Records) lakehouse pipeline
and have access to the following metrics from the Telecom Italia Milan dataset.
Generate a comprehensive governance report in Markdown.

## Quality Report Data
{json.dumps(quality, indent=2, default=str)}

## Data Lineage
{json.dumps(lineage, indent=2, default=str)}

## Anomaly Detection Results
{json.dumps(anomaly, indent=2, default=str)}

## Gold Layer Network Metrics
{json.dumps(gold, indent=2, default=str)}

---

Write a governance report with the following sections:

1. **Executive Summary** (3-4 sentences, non-technical, business-focused — focus on network data quality and CDR coverage)
2. **Data Quality Scorecard** (table: each layer/table, quality score, status, key issues)
3. **Anomaly Risk Assessment** (what was detected — activity spikes, dead zones, data corruption — and recommended actions)
4. **Data Lineage Summary** (how CDR data flows raw → bronze → silver → gold, including what was cleaned at each step)
5. **Data Catalog** (for each CDR table: description, owner team, sensitivity — CDR data is confidential, PII considerations, refresh schedule, SLA)
6. **Governance Recommendations** (3-5 prioritised, actionable items with effort/impact ratings)
7. **SLA Status** (define reasonable CDR pipeline SLA targets and show actual vs target)

Use professional enterprise telecommunications governance tone.
Include specific numbers from the data provided.
Note that CDR data carries regulatory obligations (GDPR Article 6, ePrivacy Directive).
Format as clean Markdown. Start with a title and date."""


def generate_report(client: anthropic.Anthropic, prompt: str) -> str:
    console.print("[dim]Calling Claude API...[/dim]")
    response = client.messages.create(
        model=MODEL,
        max_tokens=4096,
        system=(
            "You are an expert Data Governance Officer in a telecommunications company. "
            "Produce formal enterprise documentation with precise language. "
            "Use markdown tables and headers. Be specific with numbers — never use 'some' or 'many'. "
            "Always reference GDPR and ePrivacy obligations when discussing CDR data sensitivity."
        ),
        messages=[{"role": "user", "content": prompt}],
    )
    return response.content[0].text


def generate_catalog(client: anthropic.Anthropic) -> str:
    tables_desc = "\n".join([
        "- cdr_records (bronze): raw CDR records from Milan 100×100 grid — square_id, time_interval, country_code, sms_in, sms_out, call_in, call_out, internet",
        "- cdr_records (silver): cleaned CDR — negative activity clipped, timestamps validated, quality flags added",
        "- anomaly_scores (silver): ML-predicted anomaly labels per CDR record",
        "- hourly_grid_activity (gold): avg/peak activity per grid square per hour-of-day",
        "- daily_country_usage (gold): total CDR activity per country code per date",
        "- grid_heatmap (gold): cumulative activity + tier classification per grid square",
    ])
    response = client.messages.create(
        model=MODEL,
        max_tokens=2048,
        messages=[{"role": "user", "content": f"""Generate a data catalog JSON for these Telecom Italia CDR lakehouse tables:
{tables_desc}

Return a JSON array where each item has:
- table_name, layer (bronze/silver/gold), description, owner_team,
  sensitivity (public/internal/confidential/restricted),
  pii_columns (list — note that CDR data contains location-inferred identifiers),
  refresh_schedule, sla_freshness_hours,
  primary_key, partition_key (if applicable), tags (list),
  regulatory_notes (GDPR/ePrivacy obligations for this table)

Return ONLY valid JSON, no markdown fences."""}],
    )
    return response.content[0].text


def main():
    ensure_paths()
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        console.print("[red]ERROR: ANTHROPIC_API_KEY not set in .env[/red]")
        return

    console.print("\n[bold cyan]═══ AI Governance Agent (Claude) ═══[/bold cyan]")

    client = anthropic.Anthropic(api_key=api_key)
    spark  = get_spark_session()

    console.print("[dim]Loading pipeline metrics...[/dim]")
    quality = load_quality_reports()
    lineage = load_lineage()
    anomaly = load_anomaly_stats(spark)
    gold    = load_gold_metrics(spark)

    prompt      = build_context_prompt(quality, lineage, anomaly, gold)
    report_text = generate_report(client, prompt)

    report_path = REPORTS_PATH / f"governance_report_{BATCH_ID}.md"
    report_path.write_text(report_text)
    log.info(f"[green]✓[/green] Governance report: {report_path}")

    console.print("[dim]Generating CDR data catalog...[/dim]")
    catalog_text = generate_catalog(client)
    try:
        catalog_data = json.loads(catalog_text)
        catalog_path = CATALOG_PATH / f"data_catalog_{BATCH_ID}.json"
        catalog_path.write_text(json.dumps(catalog_data, indent=2))
        log.info(f"[green]✓[/green] Data catalog: {catalog_path}")
    except json.JSONDecodeError:
        catalog_path = CATALOG_PATH / f"data_catalog_{BATCH_ID}_raw.md"
        catalog_path.write_text(catalog_text)
        log.warning(f"Catalog saved as raw markdown: {catalog_path}")

    console.print("\n[bold]─── Governance Report Preview ───[/bold]")
    console.print(Markdown("\n".join(report_text.split("\n")[:40])))
    console.print(f"\n[dim]... full report at {report_path}[/dim]")
    console.print(f"\n[bold green]Governance agent complete. Batch: {BATCH_ID}[/bold green]\n")


if __name__ == "__main__":
    main()
