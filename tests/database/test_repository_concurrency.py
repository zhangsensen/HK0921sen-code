from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timezone
import time
from pathlib import Path
from typing import Callable, Iterator, List, Mapping, Tuple

import pytest
import sqlite3
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from database import ConnectionProvider, FactorRepository, SchemaManager, SQLiteClient, StrategyRepository


class SharedMemoryClient(ConnectionProvider):
    """Connection provider using a shared in-memory SQLite database."""

    def __init__(self, name: str = "concurrency") -> None:
        self._uri = f"file:{name}?mode=memory&cache=shared"
        # Keep a root connection open so the shared in-memory database persists.
        self._root_connection = sqlite3.connect(self._uri, uri=True, timeout=10.0)

    def close(self) -> None:
        self._root_connection.close()

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self._uri, uri=True, timeout=10.0)
        try:
            yield connection
        finally:
            connection.close()


@pytest.fixture(params=["file", "memory"], name="connection_provider")
def connection_provider_fixture(tmp_path, request) -> Tuple[ConnectionProvider, Callable[[], None]]:
    """Provide both file-backed and shared in-memory SQLite connection providers."""

    if request.param == "file":
        db_path = tmp_path / "concurrency.sqlite"
        client = SQLiteClient(db_path)

        def cleanup() -> None:
            if db_path.exists():
                db_path.unlink()

        return client, cleanup

    client = SharedMemoryClient("concurrency")

    def cleanup() -> None:
        client.close()

    return client, cleanup


def test_parallel_factor_and_strategy_saves(connection_provider) -> None:
    """Write factor and strategy batches in parallel threads."""

    client, cleanup = connection_provider
    try:
        schema = SchemaManager(client)
        schema.ensure_schema()

        factor_repo = FactorRepository(client)
        strategy_repo = StrategyRepository(client)

        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        def factor_batch(batch_id: int) -> List[Mapping[str, object]]:
            return [
                {
                    "symbol": f"SYM_{batch_id}_{row_id}",
                    "timeframe": "1m",
                    "factor": f"factor_{batch_id}_{row_id}",
                    "sharpe_ratio": 1.0 + batch_id + row_id / 10,
                    "stability": 0.8,
                    "trades_count": 5 + row_id,
                    "win_rate": 0.55,
                    "profit_factor": 1.3,
                    "max_drawdown": 0.05,
                    "information_coefficient": 0.01 * (batch_id + row_id),
                    "exploration_date": timestamp,
                }
                for row_id in range(3)
            ]

        def strategy_batch(batch_id: int) -> List[Mapping[str, object]]:
            return [
                {
                    "symbol": f"STRAT_{batch_id}_{row_id}",
                    "strategy_name": f"strategy_{batch_id}_{row_id}",
                    "factors": [f"factor_{batch_id}", f"factor_{batch_id}_{row_id}"],
                    "sharpe_ratio": 1.2 + batch_id / 10,
                    "stability": 0.7,
                    "trades_count": 7 + row_id,
                    "win_rate": 0.6,
                    "profit_factor": 1.25,
                    "max_drawdown": 0.06,
                    "average_information_coefficient": 0.02 * (batch_id + row_id),
                    "creation_date": timestamp,
                }
                for row_id in range(3)
            ]

        factor_payloads = [factor_batch(i) for i in range(4)]
        strategy_payloads = [strategy_batch(i) for i in range(4)]

        def run_with_retry(operation: Callable[[], None]) -> None:
            attempts = 0
            while True:
                try:
                    operation()
                    return
                except sqlite3.OperationalError as exc:
                    if "locked" not in str(exc).lower() or attempts >= 5:
                        raise
                    attempts += 1
                    time.sleep(0.05)

        def save_factor_batches() -> None:
            for batch in factor_payloads:
                run_with_retry(lambda b=batch: factor_repo.save_many(b))

        def save_strategy_batches() -> None:
            for batch in strategy_payloads:
                run_with_retry(lambda b=batch: strategy_repo.save_many(b))

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [
                pool.submit(save_factor_batches),
                pool.submit(save_strategy_batches),
            ]
            for future in futures:
                # Ensure any sqlite errors in worker threads are surfaced.
                future.result()

        with client.connect() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM factor_exploration_results")
            factor_count = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM combination_strategies")
            strategy_count = cursor.fetchone()[0]

        assert factor_count == sum(len(batch) for batch in factor_payloads)
        assert strategy_count == sum(len(batch) for batch in strategy_payloads)
    finally:
        cleanup()
