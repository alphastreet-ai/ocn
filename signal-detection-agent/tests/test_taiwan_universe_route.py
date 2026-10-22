"""GET /taiwan-signals/universe - served live from news-retrieval's table."""
from __future__ import annotations

import asyncio
import os
import sys

import pytest
from fastapi import HTTPException

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import routes.jobs as jobs  # noqa: E402

_COMPANIES = [{"ticker": "2449", "company_name": "京元電子", "translated_company_name": "King Yuan Electronics",
               "customers": [{"name": "NVIDIA", "relationship": "Customer: ..."}]}]


def test_universe_is_returned_unchanged(monkeypatch) -> None:
    async def fake():
        return _COMPANIES
    monkeypatch.setattr(jobs, "fetch_taiwan_universe", fake)
    assert asyncio.run(jobs.get_taiwan_signal_universe(caller={})) == {"companies": _COMPANIES}


def test_unavailable_table_is_503_not_an_empty_universe(monkeypatch) -> None:
    async def fake():
        return None
    monkeypatch.setattr(jobs, "fetch_taiwan_universe", fake)
    with pytest.raises(HTTPException) as e:
        asyncio.run(jobs.get_taiwan_signal_universe(caller={}))
    assert e.value.status_code == 503
