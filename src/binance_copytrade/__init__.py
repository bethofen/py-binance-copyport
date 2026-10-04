"""binance-copytrade-data: fetch, normalise and store Binance Futures copy-trading lead data.

Unofficial, public, no-login binance.com web endpoints (no SLA). Local files only.

    from binance_copytrade import CopyTradeService, FetchFilter
    svc = CopyTradeService()
    ls = svc.collect(FetchFilter(source="ai", sort="AUM", pages=1))
    svc.save(ls, fmt="json")
"""
from .client import (AuthRequiredError, BinanceCopyClient, BinanceCopyError, MAX_PAGE_SIZE, PORTFOLIO_TYPES,
                     PortfolioClosedError, SORT_TYPES, TIME_RANGES, min_margin_from_detail, to_float)
from .filters import BADGES, RECOMMENDED_GROUPS, SOURCES, FetchFilter
from .leadset import LeadSet
from .normalize import CORE_COLUMNS, DAY_MS, RAW_SECTIONS, batch_to_raw, flatten_lead, order_columns
from .service import CopyTradeService, Sections, ServiceConfig, has_sections
from .store import SCHEMA_VERSION, LocalStore

__version__ = "0.2.0"

__all__ = [
    "AuthRequiredError", "PortfolioClosedError", "Sections", "has_sections", "SCHEMA_VERSION",
    "BinanceCopyClient", "BinanceCopyError", "MAX_PAGE_SIZE", "PORTFOLIO_TYPES", "SORT_TYPES", "TIME_RANGES",
    "min_margin_from_detail", "to_float", "BADGES", "RECOMMENDED_GROUPS", "SOURCES", "FetchFilter", "LeadSet",
    "CORE_COLUMNS", "DAY_MS", "RAW_SECTIONS", "batch_to_raw", "flatten_lead", "order_columns", "CopyTradeService",
    "ServiceConfig", "LocalStore", "__version__",
]
