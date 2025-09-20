"""SQLite persistence layer with repository abstractions."""
from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, List, Mapping, Protocol


DEFAULT_DB_PATH = Path(".local_results/hk_factor_results.sqlite")


class ConnectionProvider(Protocol):
    """Protocol for connection providers used by repositories."""

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        ...


def _validate_identifier(value: str) -> str:
    """Allow only alphanumeric characters and underscore for SQL identifiers."""

    if not value.replace("_", "").isalnum():
        raise ValueError(f"Invalid SQL identifier: {value!r}")
    return value


class SQLiteClient(ConnectionProvider):
    """Thin wrapper responsible for managing SQLite connections."""

    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)
        if self.path.parent and not self.path.parent.exists():
            self.path.parent.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.path)
        try:
            yield connection
        finally:
            connection.close()


@dataclass
class FactorResult:
    symbol: str
    timeframe: str
    factor_name: str
    sharpe_ratio: float
    stability: float
    trades_count: int
    win_rate: float
    profit_factor: float
    max_drawdown: float
    information_coefficient: float
    exploration_date: str


@dataclass
class StrategyResult:
    symbol: str
    strategy_name: str
    factor_combination: List[str]
    sharpe_ratio: float
    stability: float
    trades_count: int
    win_rate: float
    profit_factor: float
    max_drawdown: float
    average_information_coefficient: float
    creation_date: str


class SchemaManager:
    """Responsible for schema and index management."""

    def __init__(self, client: ConnectionProvider) -> None:
        self._client = client

    def ensure_schema(self) -> None:
        with self._client.connect() as conn:
            cursor = conn.cursor()
            cursor.executescript(
                """
                CREATE TABLE IF NOT EXISTS factor_exploration_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    factor_name TEXT NOT NULL,
                    sharpe_ratio REAL NOT NULL,
                    stability REAL NOT NULL,
                    trades_count INTEGER NOT NULL,
                    win_rate REAL NOT NULL,
                    profit_factor REAL NOT NULL,
                    max_drawdown REAL NOT NULL,
                    information_coefficient REAL NOT NULL DEFAULT 0,
                    exploration_date TEXT NOT NULL,
                    UNIQUE(symbol, timeframe, factor_name)
                );

                CREATE TABLE IF NOT EXISTS combination_strategies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    strategy_name TEXT NOT NULL,
                    factor_combination TEXT NOT NULL,
                    sharpe_ratio REAL NOT NULL,
                    stability REAL NOT NULL,
                    trades_count INTEGER NOT NULL,
                    win_rate REAL NOT NULL,
                    profit_factor REAL NOT NULL,
                    max_drawdown REAL NOT NULL,
                    average_information_coefficient REAL NOT NULL DEFAULT 0,
                    creation_date TEXT NOT NULL,
                    UNIQUE(symbol, strategy_name)
                );

                CREATE TABLE IF NOT EXISTS system_config (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    description TEXT
                );
                """
            )
            self._ensure_column(cursor, "factor_exploration_results", "information_coefficient", "REAL NOT NULL DEFAULT 0")
            self._ensure_column(
                cursor,
                "combination_strategies",
                "average_information_coefficient",
                "REAL NOT NULL DEFAULT 0",
            )
            self._ensure_index(cursor, "idx_factor_symbol_timeframe", "factor_exploration_results", ("symbol", "timeframe"))
            self._ensure_index(cursor, "idx_strategy_symbol", "combination_strategies", ("symbol",))
            conn.commit()

    def _ensure_column(self, cursor: sqlite3.Cursor, table: str, column: str, definition: str) -> None:
        table = _validate_identifier(table)
        column = _validate_identifier(column)
        cursor.execute(f"PRAGMA table_info({table})")
        existing = {row[1] for row in cursor.fetchall()}
        if column not in existing:
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    def _ensure_index(self, cursor: sqlite3.Cursor, name: str, table: str, columns: Iterable[str]) -> None:
        name = _validate_identifier(name)
        table = _validate_identifier(table)
        column_list = ", ".join(_validate_identifier(col) for col in columns)
        cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name=?", (name,))
        if cursor.fetchone() is None:
            cursor.execute(f"CREATE INDEX {name} ON {table} ({column_list})")


class FactorRepository:
    """Repository for persisting factor exploration results."""

    def __init__(self, client: ConnectionProvider) -> None:
        self._client = client

    def save_many(self, results: Iterable[Mapping[str, object]]) -> None:
        with self._client.connect() as conn:
            rows = [
                (
                    r["symbol"],
                    r["timeframe"],
                    r["factor"],
                    r["sharpe_ratio"],
                    r["stability"],
                    r["trades_count"],
                    r["win_rate"],
                    r["profit_factor"],
                    r["max_drawdown"],
                    r.get("information_coefficient", 0.0),
                    r["exploration_date"],
                )
                for r in results
            ]
            conn.executemany(
                """
                INSERT OR REPLACE INTO factor_exploration_results (
                    symbol, timeframe, factor_name, sharpe_ratio, stability,
                    trades_count, win_rate, profit_factor, max_drawdown,
                    information_coefficient, exploration_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            conn.commit()

    def load_by_symbol(self, symbol: str) -> List[FactorResult]:
        with self._client.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT symbol, timeframe, factor_name, sharpe_ratio, stability,
                       trades_count, win_rate, profit_factor, max_drawdown,
                       information_coefficient, exploration_date
                FROM factor_exploration_results
                WHERE symbol = ?
                ORDER BY timeframe, sharpe_ratio DESC
                """,
                (symbol,),
            )
            rows = cursor.fetchall()
        return [FactorResult(*row) for row in rows]


class StrategyRepository:
    """Repository for combination strategies."""

    def __init__(self, client: ConnectionProvider) -> None:
        self._client = client

    def save_many(self, strategies: Iterable[Mapping[str, object]]) -> None:
        with self._client.connect() as conn:
            rows = [
                (
                    s["symbol"],
                    s["strategy_name"],
                    json.dumps(s["factors"]),
                    s["sharpe_ratio"],
                    s["stability"],
                    s["trades_count"],
                    s["win_rate"],
                    s["profit_factor"],
                    s["max_drawdown"],
                    s.get("average_information_coefficient", 0.0),
                    s["creation_date"],
                )
                for s in strategies
            ]
            conn.executemany(
                """
                INSERT OR REPLACE INTO combination_strategies (
                    symbol, strategy_name, factor_combination, sharpe_ratio,
                    stability, trades_count, win_rate, profit_factor, max_drawdown,
                    average_information_coefficient, creation_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            conn.commit()

    def load_by_symbol(self, symbol: str) -> List[StrategyResult]:
        with self._client.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT symbol, strategy_name, factor_combination, sharpe_ratio, stability,
                       trades_count, win_rate, profit_factor, max_drawdown,
                       average_information_coefficient, creation_date
                FROM combination_strategies
                WHERE symbol = ?
                ORDER BY sharpe_ratio DESC
                """,
                (symbol,),
            )
            rows = cursor.fetchall()
        return [
            StrategyResult(row[0], row[1], json.loads(row[2]), *row[3:])
            for row in rows
        ]


class ConfigRepository:
    """Repository for system configuration values."""

    def __init__(self, client: ConnectionProvider) -> None:
        self._client = client

    def upsert(self, key: str, value: str, description: str | None = None) -> None:
        with self._client.connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO system_config (key, value, description) VALUES (?, ?, ?)",
                (key, value, description),
            )
            conn.commit()

    def get(self, key: str) -> str | None:
        with self._client.connect() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT value FROM system_config WHERE key = ?", (key,))
            row = cursor.fetchone()
        return row[0] if row else None


class DatabaseManager:
    """Facade exposing repositories for backward compatibility."""

    def __init__(self, path: Path | str | None = None) -> None:
        db_path = Path(path) if path is not None else DEFAULT_DB_PATH
        self._client = SQLiteClient(db_path)
        self.schema = SchemaManager(self._client)
        self.factors = FactorRepository(self._client)
        self.strategies = StrategyRepository(self._client)
        self.config = ConfigRepository(self._client)
        self.schema.ensure_schema()

    # ------------------------------------------------------------------
    def reset_database(self) -> None:
        with self._client.connect() as conn:
            conn.executescript(
                """
                DELETE FROM factor_exploration_results;
                DELETE FROM combination_strategies;
                DELETE FROM system_config;
                VACUUM;
                """
            )
            conn.commit()

    # ------------------------------------------------------------------
    def save_exploration_results(self, results: Iterable[Mapping[str, object]]) -> None:
        self.factors.save_many(results)

    def load_exploration_results(self, symbol: str) -> List[FactorResult]:
        return self.factors.load_by_symbol(symbol)

    def save_combination_strategies(self, strategies: Iterable[Mapping[str, object]]) -> None:
        self.strategies.save_many(strategies)

    def load_combination_strategies(self, symbol: str) -> List[StrategyResult]:
        return self.strategies.load_by_symbol(symbol)

