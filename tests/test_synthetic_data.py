"""Tests for synthetic data generation."""
import os
import pandas as pd
import pytest


def test_customers_generated(tmp_data_dir):
    from src.data.generate_synthetic import generate_customers
    df = generate_customers()
    assert len(df) == int(os.getenv("NUM_CUSTOMERS", 50))
    assert set(["customer_id", "email", "segment", "joined_date"]).issubset(df.columns)
    assert df["customer_id"].nunique() == len(df), "customer_ids must be unique"


def test_products_generated(tmp_data_dir):
    from src.data.generate_synthetic import generate_products
    df = generate_products()
    assert len(df) == int(os.getenv("NUM_PRODUCTS", 20))
    assert (df["base_price"] > 0).all(), "All prices must be positive"


def test_orders_have_anomalies(tmp_data_dir):
    from src.data.generate_synthetic import generate_customers, generate_products, generate_orders_and_items
    customers = generate_customers()
    products = generate_products()
    orders, items = generate_orders_and_items(customers, products)

    expected_orders = int(os.getenv("NUM_ORDERS", 200))
    assert len(orders) == expected_orders

    # At 5% anomaly rate with 200 orders, we expect some anomalies
    anomaly_count = orders["is_anomaly_injected"].sum()
    assert anomaly_count > 0, "Should have injected some anomalies"
    assert anomaly_count < expected_orders, "Should not be all anomalies"


def test_order_items_reference_valid_orders(tmp_data_dir):
    from src.data.generate_synthetic import generate_customers, generate_products, generate_orders_and_items
    customers = generate_customers()
    products = generate_products()
    orders, items = generate_orders_and_items(customers, products)

    # Every item should reference a product that exists
    valid_products = set(products["product_id"])
    # Items product_ids should be a subset of valid products
    item_products = set(items["product_id"])
    assert item_products.issubset(valid_products), "Items should only reference valid products"


def test_full_data_generation_writes_files(tmp_data_dir):
    from pathlib import Path
    from src.data.generate_synthetic import main
    main()

    raw_path = Path(os.getenv("RAW_DATA_PATH", f"{tmp_data_dir}/raw"))
    assert (raw_path / "customers.csv").exists()
    assert (raw_path / "products.csv").exists()
    assert (raw_path / "orders.csv").exists()
    assert (raw_path / "order_items.csv").exists()
