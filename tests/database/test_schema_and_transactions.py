from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable, Mapping

import pytest
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from database import (
    FactorRepository,
    SchemaManager,
    SQLiteClient,
    StrategyRepository,
    _validate_identifier,
)


def _setup_client(tmp_path, filename: str) -> SQLiteClient:
    client = SQLiteClient(tmp_path / filename)
    schema = SchemaManager(client)
    schema.ensure_schema()
    return client


def _make_factor_rows(timestamp: str) -> list[Mapping[str, object]]:
    return [
        {
            "symbol": "SYM_A",
            "timeframe": "1m",
            "factor": "factor_a",
            "sharpe_ratio": 1.1,
            "stability": 0.8,
            "trades_count": 10,
            "win_rate": 0.55,
            "profit_factor": 1.3,
            "max_drawdown": 0.04,
            "information_coefficient": 0.02,
            "exploration_date": timestamp,
        },
        {
            "symbol": "SYM_B",
            "timeframe": "5m",
            "factor": "factor_b",
            "sharpe_ratio": 1.2,
            "stability": 0.75,
            "trades_count": 12,
            "win_rate": 0.58,
            "profit_factor": 1.25,
            "max_drawdown": 0.05,
            "information_coefficient": 0.03,
            "exploration_date": timestamp,
        },
    ]


def _make_strategy_rows(timestamp: str) -> list[Mapping[str, object]]:
    return [
        {
            "symbol": "STRAT_A",
            "strategy_name": "strategy_a",
            "factors": ["factor_a", "factor_b"],
            "sharpe_ratio": 1.15,
            "stability": 0.72,
            "trades_count": 8,
            "win_rate": 0.6,
            "profit_factor": 1.22,
            "max_drawdown": 0.06,
            "average_information_coefficient": 0.025,
            "creation_date": timestamp,
        },
        {
            "symbol": "STRAT_B",
            "strategy_name": "strategy_b",
            "factors": ["factor_c", "factor_d"],
            "sharpe_ratio": 1.18,
            "stability": 0.74,
            "trades_count": 9,
            "win_rate": 0.59,
            "profit_factor": 1.24,
            "max_drawdown": 0.05,
            "average_information_coefficient": 0.027,
            "creation_date": timestamp,
        },
    ]


def _install_failing_connect(monkeypatch: pytest.MonkeyPatch, exception_factory: Callable[[], Exception]) -> None:
    original_connect = sqlite3.connect

    class FailingConnection(sqlite3.Connection):
        def executemany(self, sql: str, seq_of_parameters):  # type: ignore[override]
            iterator = iter(seq_of_parameters)
            try:
                first_row = next(iterator)
            except StopIteration:
                return super().executemany(sql, seq_of_parameters)
            super().execute(sql, tuple(first_row))
            raise exception_factory()

    def connect_with_failure(*args, **kwargs):
        kwargs = dict(kwargs)
        kwargs["factory"] = FailingConnection
        return original_connect(*args, **kwargs)

    monkeypatch.setattr(sqlite3, "connect", connect_with_failure)


def test_invalid_identifier_and_no_dirty_schema(tmp_path) -> None:
    client = _setup_client(tmp_path, "schema.sqlite")
    schema = SchemaManager(client)

    with client.connect() as conn:
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(factor_exploration_results)")
        before_columns = [row[1] for row in cursor.fetchall()]

        with pytest.raises(ValueError):
            schema._ensure_column(cursor, "factor_exploration_results", "invalid-column", "TEXT")

        cursor.execute("PRAGMA table_info(factor_exploration_results)")
        after_columns = [row[1] for row in cursor.fetchall()]

    assert before_columns == after_columns

    with pytest.raises(ValueError):
        _validate_identifier("invalid-column")


@pytest.mark.parametrize(
    "repo_class, rows_factory, table_name, exception_factory",
    [
        pytest.param(
            FactorRepository,
            _make_factor_rows,
            "factor_exploration_results",
            lambda: sqlite3.IntegrityError(
                "UNIQUE constraint failed: factor_exploration_results.symbol, "
                "factor_exploration_results.timeframe, factor_exploration_results.factor_name"
            ),
            id="factor-integrity",
        ),
        pytest.param(
            StrategyRepository,
            _make_strategy_rows,
            "combination_strategies",
            lambda: sqlite3.IntegrityError(
                "UNIQUE constraint failed: combination_strategies.symbol, combination_strategies.strategy_name"
            ),
            id="strategy-integrity",
        ),
    ],
)
def test_save_many_rolls_back_on_integrity_error(
    tmp_path, monkeypatch, repo_class, rows_factory, table_name, exception_factory
) -> None:
    client = _setup_client(tmp_path, f"{table_name}.sqlite")
    repo = repo_class(client)
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    rows = rows_factory(timestamp)

    _install_failing_connect(monkeypatch, exception_factory)

    with pytest.raises(sqlite3.IntegrityError):
        repo.save_many(rows)

    with client.connect() as conn:
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        assert cursor.fetchone()[0] == 0


def test_save_many_rolls_back_on_runtime_error(tmp_path, monkeypatch) -> None:
    client = _setup_client(tmp_path, "runtime.sqlite")
    repo = FactorRepository(client)
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    rows = _make_factor_rows(timestamp)

    _install_failing_connect(monkeypatch, lambda: RuntimeError("transaction failure"))

    with pytest.raises(RuntimeError):
        repo.save_many(rows)

    with client.connect() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM factor_exploration_results")
        assert cursor.fetchone()[0] == 0


def test_ensure_schema_idempotent(tmp_path) -> None:
    client = _setup_client(tmp_path, "idempotent.sqlite")
    schema = SchemaManager(client)

    with client.connect() as conn:
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(factor_exploration_results)")
        initial_columns = {row[1] for row in cursor.fetchall()}
        cursor.execute("PRAGMA index_list('factor_exploration_results')")
        initial_factor_indexes = {row[1] for row in cursor.fetchall()}
        cursor.execute("PRAGMA table_info(combination_strategies)")
        initial_strategy_columns = {row[1] for row in cursor.fetchall()}
        cursor.execute("PRAGMA index_list('combination_strategies')")
        initial_strategy_indexes = {row[1] for row in cursor.fetchall()}

    # Re-run schema creation to check for idempotency.
    schema.ensure_schema()
    schema.ensure_schema()

    with client.connect() as conn:
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(factor_exploration_results)")
        columns_after = {row[1] for row in cursor.fetchall()}
        cursor.execute("PRAGMA index_list('factor_exploration_results')")
        factor_indexes_after = {row[1] for row in cursor.fetchall()}
        cursor.execute("PRAGMA table_info(combination_strategies)")
        strategy_columns_after = {row[1] for row in cursor.fetchall()}
        cursor.execute("PRAGMA index_list('combination_strategies')")
        strategy_indexes_after = {row[1] for row in cursor.fetchall()}

    assert columns_after == initial_columns
    assert "information_coefficient" in columns_after
    assert factor_indexes_after == initial_factor_indexes
    assert "idx_factor_symbol_timeframe" in factor_indexes_after
    assert strategy_columns_after == initial_strategy_columns
    assert "average_information_coefficient" in strategy_columns_after
    assert strategy_indexes_after == initial_strategy_indexes
    assert "idx_strategy_symbol" in strategy_indexes_after
