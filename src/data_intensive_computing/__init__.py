"""Urban data platform package."""

from __future__ import annotations

from typing import Sequence

__all__ = ["main"]


def main(argv: Sequence[str] | None = None) -> int:
    """Run the project command-line interface."""
    from data_intensive_computing.task1.cli import main as cli_main

    return cli_main(argv)
