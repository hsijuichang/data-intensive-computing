"""Command-line entry points for reproducible platform jobs."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path
from typing import Sequence

from data_intensive_computing.benchmark_helpers import find_project_root
from data_intensive_computing.task1.config import TASK1_DATASET_ORDER


def build_parser() -> argparse.ArgumentParser:
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
    return parser


def _root_from_args(value: Path | None) -> Path:
    if value is not None:
        root = value.resolve()
        if not (root / "pyproject.toml").is_file():
            raise FileNotFoundError(f"{root} is not a project root (pyproject.toml missing)")
        return root
    return find_project_root(Path.cwd().resolve())


def _print_results(results: list[object]) -> None:
    print(json.dumps([asdict(result) for result in results], indent=2, default=str))


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    project_root = _root_from_args(args.project_root)

    from data_intensive_computing.spark import build_spark_session

    spark = build_spark_session(
        "Urban-Data-Platform-Task1",
        driver_memory=args.driver_memory,
        shuffle_partitions=args.shuffle_partitions,
    )
    try:
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
