"""Tests for data quality validation logic."""
import pandas as pd
import pytest


def test_validate_orders_passes_clean_data():
    from src.quality.validators import validate_orders

    df = pd.DataFrame({
        "order_id": ["ORD_001", "ORD_002"],
        "customer_id": ["CUST_001", "CUST_002"],
        "status": ["completed", "shipped"],
        "payment_method": ["credit_card", "paypal"],
        "channel": ["web", "mobile"],
        "shipping_cost": [5.99, 0.0],
        "tax_amount": [4.50, 2.10],
    })
    result = validate_orders(df)
    assert result["passed"] is True
    assert result["checks_passed"] == result["checks_total"]


def test_validate_orders_fails_invalid_status():
    from src.quality.validators import validate_orders

    df = pd.DataFrame({
        "order_id": ["ORD_001"],
        "customer_id": ["CUST_001"],
        "status": ["INVALID_STATUS"],
        "payment_method": ["credit_card"],
        "channel": ["web"],
        "shipping_cost": [5.99],
        "tax_amount": [4.50],
    })
    result = validate_orders(df)
    assert result["passed"] is False


def test_validate_customers_passes_clean_data():
    from src.quality.validators import validate_customers

    df = pd.DataFrame({
        "customer_id": ["CUST_001", "CUST_002"],
        "email": ["a@b.com", "c@d.com"],
        "segment": ["GOLD", "SILVER"],
        "is_active": [True, False],
    })
    result = validate_customers(df)
    assert result["passed"] is True


def test_validate_products_rejects_negative_price():
    from src.quality.validators import validate_products

    df = pd.DataFrame({
        "product_id": ["PROD_001"],
        "category": ["Electronics"],
        "base_price": [-10.0],
        "stock_qty": [100],
    })
    result = validate_products(df)
    assert result["passed"] is False


def test_quality_reporter_generates_markdown():
    from src.quality.reporter import QualityReporter

    reporter = QualityReporter(layer="silver", batch_id="TEST_001")
    reporter.add_table_stats("orders", {
        "rows_before": 1000, "rows_after": 990, "dropped": 10, "suspicious_flagged": 5
    })
    reporter.add_validation_results({
        "orders": {"passed": True, "checks_total": 5, "checks_passed": 5,
                   "failures": [], "null_counts": {}, "row_count": 990, "duplicate_count": 0}
    })

    md = reporter.to_markdown()
    assert "Silver" in md
    assert "990" in md
    assert "PASS" in md
    assert "100.0%" in reporter.to_dict()["overall_quality_score"].__str__() or \
           reporter.to_dict()["overall_quality_score"] == 100.0
