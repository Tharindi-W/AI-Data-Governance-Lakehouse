"""Pandera schema validation for the silver CDR layer."""
from __future__ import annotations

import os
from typing import Any

import pandas as pd
import pandera.pandas as pa
from pandera.pandas import Check, Column, DataFrameSchema

os.environ.setdefault("DISABLE_PANDERA_IMPORT_WARNING", "True")

CDR_SCHEMA = DataFrameSchema(
    columns={
        "square_id":     Column(int,   Check.in_range(1, 10_000),           nullable=False),
        "time_interval": Column(int,   Check.greater_than(0),               nullable=False),
        "country_code":  Column(int,   nullable=True),
        "sms_in":        Column(float, Check.greater_than_or_equal_to(0),   nullable=True),
        "sms_out":       Column(float, Check.greater_than_or_equal_to(0),   nullable=True),
        "call_in":       Column(float, Check.greater_than_or_equal_to(0),   nullable=True),
        "call_out":      Column(float, Check.greater_than_or_equal_to(0),   nullable=True),
        "internet":      Column(float, Check.greater_than_or_equal_to(0),   nullable=True),
        "hour_of_day":   Column(int,   Check.in_range(0, 23),               nullable=True),
        "day_of_week":   Column(int,   Check.in_range(1, 7),                nullable=True),
    },
    coerce=True,
    strict=False,
)


def _run_schema(schema: DataFrameSchema, df: pd.DataFrame, table: str) -> dict[str, Any]:
    checks_total = len(schema.columns)
    failures: list[str] = []
    failed_cols: set[str] = set()

    try:
        schema.validate(df, lazy=True)
        checks_passed = checks_total
    except pa.errors.SchemaErrors as exc:
        fc = exc.failure_cases if exc.failure_cases is not None else pd.DataFrame()
        for _, row in fc.iterrows():
            col = str(row.get("schema_context", row.get("column", "unknown")))
            chk = str(row.get("check", "failed"))
            if col not in failed_cols:
                failures.append(f"{col}: {chk}")
                failed_cols.add(col)
        checks_passed = checks_total - len(failed_cols)
    except pa.errors.SchemaError as exc:
        failures.append(str(exc)[:200])
        checks_passed = 0

    return {
        "table":           table,
        "passed":          len(failures) == 0,
        "checks_total":    checks_total,
        "checks_passed":   checks_passed,
        "failures":        failures,
        "null_counts":     df.isnull().sum().to_dict(),
        "row_count":       len(df),
        "duplicate_count": int(df.duplicated(subset=["square_id", "time_interval", "country_code"],
                                             keep=False).sum()),
    }


def validate_cdr(df: pd.DataFrame) -> dict[str, Any]:
    return _run_schema(CDR_SCHEMA, df, "cdr_records")
