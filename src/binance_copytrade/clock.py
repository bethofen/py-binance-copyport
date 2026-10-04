"""One clock for the whole package. Call through the module (``clock.now_ms()``) so tests can freeze it:

    monkeypatch.setattr(binance_copytrade.clock, "now_ms", lambda: 1_788_600_000_000.0)
"""
from __future__ import annotations

from datetime import datetime, timezone


def now_ms() -> float:
    return datetime.now(timezone.utc).timestamp() * 1000


def now_iso() -> str:
    return datetime.fromtimestamp(now_ms() / 1000, timezone.utc).isoformat(timespec="seconds")


def today() -> str:
    """UTC date of now_ms(), YYYY-MM-DD (snapshot folder name)."""
    return datetime.fromtimestamp(now_ms() / 1000, timezone.utc).strftime("%Y-%m-%d")


def iso_ms(iso: str | None) -> float | None:
    """ISO-8601 string -> epoch ms, None when missing or unparsable."""
    try:
        return datetime.fromisoformat(iso).timestamp() * 1000 if iso else None
    except (TypeError, ValueError):
        return None
