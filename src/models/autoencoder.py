"""PyTorch Autoencoder for unsupervised anomaly detection on CDR records.

Architecture: 7 → 32 → 16 → 4 → 16 → 32 → 7
Anomaly score = mean squared reconstruction error across the 7 features.
"""
from __future__ import annotations

import torch
import torch.nn as nn


class CDRAutoencoder(nn.Module):
    """Bottleneck autoencoder that learns normal CDR activity patterns."""

    INPUT_DIM = 7
    FEATURE_NAMES = [
        "sms_in_norm",
        "sms_out_norm",
        "call_in_norm",
        "call_out_norm",
        "internet_norm",
        "hour_of_day_norm",   # hour / 23
        "day_of_week_norm",   # dayofweek / 6
    ]

    def __init__(self, input_dim: int = INPUT_DIM, latent_dim: int = 4):
        super().__init__()
        self.encoder = nn.Sequential(
            nn.Linear(input_dim, 32),
            nn.BatchNorm1d(32),
            nn.ReLU(),
            nn.Linear(32, 16),
            nn.BatchNorm1d(16),
            nn.ReLU(),
            nn.Linear(16, latent_dim),
        )
        self.decoder = nn.Sequential(
            nn.Linear(latent_dim, 16),
            nn.BatchNorm1d(16),
            nn.ReLU(),
            nn.Linear(16, 32),
            nn.BatchNorm1d(32),
            nn.ReLU(),
            nn.Linear(32, input_dim),
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.decoder(self.encoder(x))

    def reconstruction_error(self, x: torch.Tensor) -> torch.Tensor:
        with torch.no_grad():
            recon = self.forward(x)
            return torch.mean((x - recon) ** 2, dim=1)

    @classmethod
    def load(cls, path: str) -> tuple["CDRAutoencoder", float]:
        checkpoint = torch.load(path, map_location="cpu", weights_only=True)
        model = cls(input_dim=checkpoint["input_dim"], latent_dim=checkpoint["latent_dim"])
        model.load_state_dict(checkpoint["state_dict"])
        model.eval()
        return model, checkpoint["threshold"]

    def save(self, path: str, threshold: float, input_dim: int = INPUT_DIM, latent_dim: int = 4):
        torch.save({
            "state_dict":    self.state_dict(),
            "threshold":     threshold,
            "input_dim":     input_dim,
            "latent_dim":    latent_dim,
            "feature_names": self.FEATURE_NAMES,
        }, path)
