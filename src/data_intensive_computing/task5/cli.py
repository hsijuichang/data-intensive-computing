"""Command-line entry point for the Task 5 evaluation.

Run from the project root, one step at a time::

    uv run python -m data_intensive_computing.task5.cli prepare
    uv run python -m data_intensive_computing.task5.cli phase-a --reps 5
    uv run python -m data_intensive_computing.task5.cli phase-b --reps 5
    uv run python -m data_intensive_computing.task5.cli summary

If a phase is interrupted, reset every table to the starting snapshot with::

    uv run python -m data_intensive_computing.task5.cli restore
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

from data_intensive_computing.benchmark_helpers import find_project_root
from data_intensive_computing.task5.config import DEFAULT_REPETITIONS


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m data_intensive_computing.task5.cli")
    parser.add_argument("--driver-memory", default="12g")
    parser.add_argument("--shuffle-partitions", type=int, default=32)
    commands = parser.add_subparsers(dest="command", required=True)

    prepare = commands.add_parser(
        "prepare",
        help="Build missing products, generate and check updates, snapshot S0.",
    )
    prepare.add_argument("--regenerate", action="store_true",
                         help="Regenerate update files even if they exist.")
    prepare.add_argument("--resnapshot", action="store_true",
                         help="Replace the existing S0 snapshot with the current versions.")

    for name, text in (
        ("phase-a", "Measure Task 1 update time, validation and monitoring overhead."),
        ("phase-b", "Measure Task 2 incremental refresh against full recompute."),
    ):
        phase = commands.add_parser(name, help=text)
        phase.add_argument("--reps", type=int, default=DEFAULT_REPETITIONS)

    commands.add_parser("restore", help="Restore every table under test to snapshot S0.")
    commands.add_parser("summary", help="Compute medians and overheads from the results.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    root = find_project_root(Path.cwd().resolve())

    if args.command == "summary":
        from data_intensive_computing.task5.summary import print_summary, summarize

        print_summary(summarize(root))
        return 0

    from data_intensive_computing.spark import build_spark_session
    from data_intensive_computing.task5 import experiments

    spark = build_spark_session(
        "Urban-Data-Platform-Task5",
        driver_memory=args.driver_memory,
        shuffle_partitions=args.shuffle_partitions,
    )
    try:
        if args.command == "prepare":
            experiments.prepare(
                spark, root, regenerate=args.regenerate, resnapshot=args.resnapshot
            )
        elif args.command == "phase-a":
            experiments.run_phase_a(spark, root, args.reps)
        elif args.command == "phase-b":
            experiments.run_phase_b(spark, root, args.reps)
        elif args.command == "restore":
            experiments.restore_s0(spark, root)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
