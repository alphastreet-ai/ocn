"""Placeholder count must match the parameter tuple, per insert.

A field was added to the Japan insert's column list and VALUES clause
without adding its value to the params tuple: 12 placeholders, 10
params. psycopg2 raises IndexError at execute time, so every row
failed and the table came back empty - caught only against the live
DB, because nothing here exercised the statement.

These call each insert with a fake connection that checks the arity
the driver would have checked, without needing a database.
"""
from __future__ import annotations

import os
import sys
from unittest.mock import patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'shared', 'src'))


class _ArityCheckingConn:
    """Stands in for a DB connection and asserts %s count == len(params)."""

    def __init__(self):
        self.calls = []

    def execute(self, sql, params=None):
        # Only count the driver's own placeholders. '%%' is an escaped
        # literal percent and binds nothing.
        n_placeholders = sql.replace("%%", "").count("%s")
        n_params = len(params or ())
        assert n_placeholders == n_params, (
            f"{n_placeholders} placeholders but {n_params} params"
        )
        self.calls.append((sql, params))
        return self

    def fetchall(self):
        return []

    def fetchone(self):
        return None

    def commit(self):
        pass


@pytest.fixture
def conn():
    import contextlib
    c = _ArityCheckingConn()

    @contextlib.contextmanager
    def _get_db():
        yield c

    with patch("models.jobs.get_db", _get_db):
        yield c


_ARTICLE = {
    "url": "irbank-buyback://4063/S100TKGJ",
    "title": "自己株式の取得状況",
    "published": "2026-09-30T00:00:00+00:00",
}


def _japan_result(**over):
    r = {
        "source_id": "irbank-buyback://4063/S100TKGJ",
        "signal": "signal",
        "signal_score": 0.72,
        "reason": "Buyback progressed.",
        "metadata": {"code": "4063", "source_category": "jp_buyback"},
        "entities": [{"name": "Nvidia", "type": "company"}],
    }
    r.update(over)
    return r


class TestJapanInsertArity:
    def test_insert_binds_every_placeholder(self, conn):
        from models.jobs import insert_japan_signal_classification
        insert_japan_signal_classification(1, _ARTICLE, _japan_result())
        assert conn.calls, "insert never executed"

    def test_watching_upsert_path_also_binds(self, conn):
        """The jp_watching branch swaps in a DO UPDATE clause, so it is
        a different SQL string and needs its own arity check."""
        from models.jobs import insert_japan_signal_classification
        insert_japan_signal_classification(
            1, _ARTICLE,
            _japan_result(metadata={"code": "4063",
                                    "source_category": "jp_watching"}),
        )
        sql = conn.calls[0][0]
        assert "DO UPDATE SET" in sql

    def test_entities_are_stored_and_normalized(self, conn):
        from models.jobs import insert_japan_signal_classification
        insert_japan_signal_classification(1, _ARTICLE, _japan_result())
        params = conn.calls[0][1]
        assert '"Nvidia"' in params[-2]
        assert params[-1] == ["nvidia"]

    def test_no_entities_still_binds(self, conn):
        from models.jobs import insert_japan_signal_classification
        insert_japan_signal_classification(1, _ARTICLE, _japan_result(entities=[]))
        assert conn.calls[0][1][-1] == []


class TestMiddleEastInsertArity:
    def test_insert_binds_every_placeholder(self, conn):
        from models.jobs import insert_middle_east_signal_classification
        insert_middle_east_signal_classification(
            1, {"url": "er-me://1", "title": "t", "published": None},
            {"signal": "noise", "reason": "r", "entities": [], "metadata": {}})
        assert conn.calls, "insert never executed"
