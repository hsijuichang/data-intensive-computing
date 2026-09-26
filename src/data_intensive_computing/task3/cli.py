"""CLI integration for Task 3 monitoring and historical backfill."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path
from typing import TYPE_CHECKING

from data_intensive_computing.task3.queries import MONITORING_QUERIES

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


TASK3_COMMANDS = frozenset({"monitor", "task3", "backfill-monitoring"})


def add_task3_subcommands(subparsers: "argparse._SubParsersAction") -> None:
    monitor = subparsers.add_parser(
        "monitor",
        aliases=["task3"],
        help="Run Task 3 Spark SQL operational monitoring queries.",
    )
    monitor.add_argument(
        "queries",
        nargs="*",
        choices=tuple(MONITORING_QUERIES),
        default=list(MONITORING_QUERIES),
    )
    monitor.add_argument("--limit", type=int, default=100)

    subparsers.add_parser(
        "backfill-monitoring",
        help="Import existing Task 1 JSONL execution results into monitoring Delta.",
    )


def run_task3_command(
    args: argparse.Namespace,
    spark: "SparkSession",
    project_root: Path,
) -> int:
    if args.command == "backfill-monitoring":
        from data_intensive_computing.task3 import backfill_task1_artifact

        result = backfill_task1_artifact(spark, project_root)
        print(json.dumps(asdict(result), indent=2))
        return 0
    if args.command not in {"monitor", "task3"}:
        raise ValueError(f"Unsupported Task 3 command: {args.command}")

    from data_intensive_computing.task3 import run_monitoring_queries

    results = run_monitoring_queries(spark, project_root, tuple(args.queries))
    for name, frame in results.items():
        print(f"\n=== {name} ===")
        frame.show(args.limit, truncate=False)
    return 0
