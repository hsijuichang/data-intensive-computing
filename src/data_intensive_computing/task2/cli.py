"""Project command line entry point extended with the Task 2 workflow."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path
from typing import TYPE_CHECKING, Sequence

from data_intensive_computing.benchmark_helpers import find_project_root
from data_intensive_computing.task1.config import TASK1_DATASET_ORDER
from data_intensive_computing.task2.config import PRODUCT_ORDER

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


TASK2_COMMANDS = frozenset({"refresh-analytics", "task2"})


def add_task2_subcommands(subparsers: "argparse._SubParsersAction") -> None:
    """Register the Task 2 command and its alias on the platform parser."""
    refresh = subparsers.add_parser(
        "refresh-analytics",
        aliases=["task2"],
        help="Incrementally update the integrated table and affected analytical products.",
    )
    refresh.add_argument(
        "--taxi-run-id",
        help=(
            "Only integrate taxi rows from this Task 1 run. "
            "When omitted, all processed trips missing from the integrated table are used."
        ),
    )
    refresh.add_argument(
        "products",
        nargs="*",
        choices=PRODUCT_ORDER,
        default=list(PRODUCT_ORDER),
        help=(
            "Analytical product tables to refresh. "
            "When omitted, all six Week 2 products are refreshed."
        ),
    )


def run_task2_command(
    args: argparse.Namespace,
    spark: "SparkSession",
    project_root: Path,
) -> int:
    """Execute a parsed Task 2 command using the caller's Spark session."""
    if args.command not in TASK2_COMMANDS:
        raise ValueError(f"Unsupported Task 2 command: {args.command}")

    from data_intensive_computing.task2.pipeline import (
        maintain_analytical_consistency,
    )

    result = maintain_analytical_consistency(
        spark,
        project_root,
        taxi_run_id=args.taxi_run_id,
        product_names=tuple(args.products),
        progress_callback=lambda item: print(json.dumps(asdict(item), default=str)),
    )
    print(json.dumps(asdict(result), indent=2, default=str))
    return 0


def build_parser() -> argparse.ArgumentParser:
    """Build the common Task 1 and Task 2 command parser."""
    parser = argparse.ArgumentParser(prog="data-intensive-computing")
    parser.add_argument(
        "--project-root",
        type=Path,
        help="Repository root; defaults to the nearest directory containing pyproject.toml.",
    )
    parser.add_argument("--driver-memory", default="8g")
    parser.add_argument("--shuffle-partitions", type=int, default=32)
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate = subparsers.add_parser(
        "generate-updates",
        help="Generate the three simulated incremental releases.",
    )
    generate.add_argument("--taxi-fraction", type=float, default=0.05)
    generate.add_argument("--taxi-duplicate-fraction", type=float, default=0.01)
    generate.add_argument("--hours", type=int, default=168)
    generate.add_argument("--seed", type=int, default=42)

    apply = subparsers.add_parser(
        "apply-updates",
        help="Validate and merge generated releases into existing Delta tables.",
    )
    apply.add_argument(
        "datasets",
        nargs="*",
        choices=TASK1_DATASET_ORDER,
        default=list(TASK1_DATASET_ORDER),
    )

    task1 = subparsers.add_parser(
        "task1",
        help="Generate and then apply all Task 1 updates.",
    )
    task1.add_argument("--taxi-fraction", type=float, default=0.05)
    task1.add_argument("--taxi-duplicate-fraction", type=float, default=0.01)
    task1.add_argument("--hours", type=int, default=168)
    task1.add_argument("--seed", type=int, default=42)

    add_task2_subcommands(subparsers)
    return parser


def _project_root(value: Path | None) -> Path:
    if value is None:
        return find_project_root(Path.cwd().resolve())
    root = value.resolve()
    if not (root / "pyproject.toml").is_file():
        raise FileNotFoundError(f"{root} is not a project root (pyproject.toml missing)")
    return root


def _print_results(results: list[object]) -> None:
    print(json.dumps([asdict(result) for result in results], indent=2, default=str))


def main(argv: Sequence[str] | None = None) -> int:
    """Run either the existing Task 1 jobs or the Task 2 refresh."""
    args = build_parser().parse_args(argv)
    project_root = _project_root(args.project_root)

    from data_intensive_computing.spark import build_spark_session

    spark = build_spark_session(
        "Urban-Data-Platform",
        driver_memory=args.driver_memory,
        shuffle_partitions=args.shuffle_partitions,
    )
    try:
        if args.command in TASK2_COMMANDS:
            return run_task2_command(args, spark, project_root)

        if args.command in {"generate-updates", "task1"}:
            from data_intensive_computing.task1.updates import generate_all_updates

            generated = generate_all_updates(
                spark,
                project_root,
                taxi_fraction=args.taxi_fraction,
                taxi_duplicate_fraction=args.taxi_duplicate_fraction,
                hours=args.hours,
                seed=args.seed,
            )
            _print_results(generated)
            if args.command == "generate-updates":
                return 0

        from data_intensive_computing.task1.pipeline import run_incremental_updates

        datasets = (
            tuple(args.datasets)
            if args.command == "apply-updates"
            else TASK1_DATASET_ORDER
        )
        _print_results(run_incremental_updates(spark, project_root, datasets))
        return 0
    finally:
        spark.stop()
