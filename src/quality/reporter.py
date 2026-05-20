"""Quality report generation — produces JSON and Markdown artefacts."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.utils.paths import REPORTS_PATH


class QualityReporter:
    def __init__(self, layer: str, batch_id: str):
        self.layer = layer
        self.batch_id = batch_id
        self.timestamp = datetime.now(tz=timezone.utc).isoformat()
        self.table_stats: dict[str, Any] = {}
        self.validation_results: dict[str, Any] = {}

    def add_table_stats(self, table_name: str, stats: dict):
        self.table_stats[table_name] = stats

    def add_validation_results(self, results: dict):
        self.validation_results = results

    def _compute_overall_score(self) -> float:
        if not self.validation_results:
            return 0.0
        scores = []
        for result in self.validation_results.values():
            total = result.get("checks_total", 1)
            passed = result.get("checks_passed", 0)
            scores.append(passed / total if total > 0 else 0)
        return round(sum(scores) / len(scores) * 100, 1)

    def to_dict(self) -> dict:
        return {
            "layer": self.layer,
            "batch_id": self.batch_id,
            "generated_at": self.timestamp,
            "overall_quality_score": self._compute_overall_score(),
            "table_stats": self.table_stats,
            "validation_results": self.validation_results,
        }

    def to_markdown(self) -> str:
        score = self._compute_overall_score()
        score_emoji = "🟢" if score >= 90 else "🟡" if score >= 70 else "🔴"

        lines = [
            f"# Data Quality Report — {self.layer.upper()} Layer",
            f"**Batch:** `{self.batch_id}`  |  **Generated:** {self.timestamp}",
            f"\n## Overall Quality Score: {score_emoji} {score}%\n",
            "## Table Statistics\n",
            "| Table | Rows Before | Rows After | Dropped | Notes |",
            "|-------|-------------|------------|---------|-------|",
        ]
        for table, stats in self.table_stats.items():
            dropped = stats.get("dropped", 0)
            notes = f"{stats.get('suspicious_flagged', '')} suspicious" if stats.get("suspicious_flagged") else ""
            lines.append(
                f"| {table} | {stats.get('rows_before', 'N/A'):,} | {stats.get('rows_after', 'N/A'):,} | {dropped} | {notes} |"
            )

        lines.append("\n## Schema Validation Results\n")
        for table, result in self.validation_results.items():
            status = "✅ PASS" if result["passed"] else "❌ FAIL"
            lines.append(f"### {table} — {status}")
            lines.append(f"- Checks: {result['checks_passed']}/{result['checks_total']} passed")
            lines.append(f"- Rows: {result['row_count']:,} | Duplicates: {result['duplicate_count']}")
            if result.get("failures"):
                lines.append("- **Failures:**")
                for f in result["failures"]:
                    lines.append(f"  - `{f}`")
            lines.append("")

        return "\n".join(lines)

    def save(self) -> Path:
        REPORTS_PATH.mkdir(parents=True, exist_ok=True)
        json_path = REPORTS_PATH / f"{self.layer}_quality_{self.batch_id}.json"
        md_path = REPORTS_PATH / f"{self.layer}_quality_{self.batch_id}.md"

        json_path.write_text(json.dumps(self.to_dict(), indent=2))
        md_path.write_text(self.to_markdown())

        return md_path
