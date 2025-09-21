"""Helper entry point to execute the slow CI test suite."""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def main(argv: list[str] | None = None) -> int:
    args = list(argv or sys.argv[1:])
    repo_root = Path(__file__).resolve().parents[1]
    command = [sys.executable, "-m", "pytest", "-m", "slow"] + args
    result = subprocess.run(command, cwd=repo_root)
    return result.returncode


if __name__ == "__main__":  # pragma: no cover - CLI helper
    raise SystemExit(main())
