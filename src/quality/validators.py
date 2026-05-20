"""Pandera schema validation for the silver CDR layer."""
from __future__ import annotations

from typing import Any

import pandas as pd
import pandera as pa
from pandera import Check, Column, DataFrameSchema

CDR_SCHEMA = DataFrameSchema(
    columns={
        "square_id":    Column(int,   Check.in_range(1, 10_000),           nullable=False),
        "time_interval": Column(int,  Check.greater_than(0),               nullable=False),
        "country_code": Column(int,   nullable=True),
        "sms_in":       Column(float, Check.greater_than_or_equal_to(0),   nullable=True),
        "sms_out":      Column(float, Check.greater_than_or_equal_to(0),   nullable=True),
        "call_in":      Column(float, Check.greater_than_or_equal_to(0),   nullable=True),
        "call_out":     Column(float, Check.greater_than_or_equal_to(0),   nullable=True),
        "internet":     Column(float, Check.greater_than_or_equal_to(0),   nullable=True),
        "hour_of_day":  Column(int,   Check.in_range(0, 23),               nullable=True),
        "day_of_week":  Column(int,   Check.in_range(1, 7),                nullable=True),
    },
    coerce=True,
    strict=False,
)


def _run_schema(schema: DataFrameSchema, df: pd.DataFrame, table: str) -> dict[str, Any]:
    checks_total  = len(schema.columns)
    checks_passed = 0
    failures: list[str] = []

    for col_name, col_schema in schema.columns.items():
        series = df[col_name] if col_name in df.columns else pd.Series(dtype=object)
        try:
            col_schema.validate(series)
            checks_passed += 1
        except (pa.errors.SchemaError, KeyError) as exc:
            failures.append(f"{col_name}: {str(exc)[:120]}")

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
