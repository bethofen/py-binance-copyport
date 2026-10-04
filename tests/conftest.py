"""Shared fixtures: fake clients from binance_copytrade.testing (no network) and a frozen clock."""
from __future__ import annotations

import pytest

from binance_copytrade import CopyTradeService, ServiceConfig
from binance_copytrade.testing import BatchFakeClient, FakeClient, freeze_clock


@pytest.fixture
def fake_client():
    return FakeClient()


@pytest.fixture
def svc(tmp_path, fake_client):
    return CopyTradeService(ServiceConfig(data_dir=str(tmp_path / "data"), delay=0), client=fake_client)


@pytest.fixture
def bsvc(tmp_path):
    return CopyTradeService(ServiceConfig(data_dir=str(tmp_path / "data"), delay=0), client=BatchFakeClient())


@pytest.fixture(autouse=True)
def _frozen_clock(monkeypatch):
    """Payloads are built around testing.NOW (~2026-09-05); freeze the package clock there so ages don't drift."""
    freeze_clock(monkeypatch)
