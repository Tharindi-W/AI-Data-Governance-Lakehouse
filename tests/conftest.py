"""Shared pytest fixtures."""
import os
import shutil
import tempfile

import pytest


@pytest.fixture(scope="session")
def tmp_data_dir():
    d = tempfile.mkdtemp(prefix="lakehouse_test_")
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture(autouse=True)
def patch_paths(tmp_data_dir, monkeypatch):
    """Redirect all data paths to a temp dir during tests."""
    monkeypatch.setenv("RAW_DATA_PATH", f"{tmp_data_dir}/raw")
    monkeypatch.setenv("BRONZE_PATH", f"{tmp_data_dir}/bronze")
    monkeypatch.setenv("SILVER_PATH", f"{tmp_data_dir}/silver")
    monkeypatch.setenv("GOLD_PATH", f"{tmp_data_dir}/gold")
    monkeypatch.setenv("GOVERNANCE_PATH", f"{tmp_data_dir}/governance")
    monkeypatch.setenv("MODEL_SAVE_PATH", f"{tmp_data_dir}/models")
    monkeypatch.setenv("NUM_CUSTOMERS", "50")
    monkeypatch.setenv("NUM_PRODUCTS", "20")
    monkeypatch.setenv("NUM_ORDERS", "200")
    monkeypatch.setenv("RANDOM_SEED", "42")
