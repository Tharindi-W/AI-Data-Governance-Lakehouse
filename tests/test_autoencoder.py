"""Tests for the PyTorch autoencoder model."""
import numpy as np
import torch
import pytest


def test_autoencoder_forward_pass():
    from src.models.autoencoder import OrderAutoencoder
    model = OrderAutoencoder(input_dim=6, latent_dim=8)
    x = torch.randn(32, 6)
    out = model(x)
    assert out.shape == x.shape


def test_reconstruction_error_shape():
    from src.models.autoencoder import OrderAutoencoder
    model = OrderAutoencoder()
    x = torch.randn(10, 6)
    errors = model.reconstruction_error(x)
    assert errors.shape == (10,)
    assert (errors >= 0).all()


def test_anomalies_have_higher_error():
    """Autoencoder trained on zeros should assign high error to large-value inputs."""
    from src.models.autoencoder import OrderAutoencoder
    import torch.nn as nn

    model = OrderAutoencoder(input_dim=6, latent_dim=8)
    optimizer = torch.optim.Adam(model.parameters(), lr=1e-2)

    # Train on near-zero inputs
    for _ in range(100):
        x = torch.randn(64, 6) * 0.1
        optimizer.zero_grad()
        loss = nn.MSELoss()(model(x), x)
        loss.backward()
        optimizer.step()

    model.eval()
    normal = torch.randn(20, 6) * 0.1
    anomalous = torch.ones(20, 6) * 10.0

    normal_err = model.reconstruction_error(normal).mean().item()
    anomaly_err = model.reconstruction_error(anomalous).mean().item()
    assert anomaly_err > normal_err, "Anomalous inputs should have higher reconstruction error"


def test_feature_engineering():
    from src.models.train import engineer_features
    import pandas as pd

    df = pd.DataFrame({
        "order_timestamp": ["2024-01-15 14:30:00"] * 5,
        "total_amount": [100.0, 200.0, 50.0, 300.0, 150.0],
        "shipping_cost": [5.0, 10.0, 3.0, 0.0, 8.0],
        "tax_amount": [8.0, 16.0, 4.0, 24.0, 12.0],
        "subtotal": [87.0, 174.0, 43.0, 276.0, 130.0],
    })
    features = engineer_features(df)
    assert features.shape == (5, 6)
    assert features.dtype == np.float32
    assert not np.any(np.isnan(features))
