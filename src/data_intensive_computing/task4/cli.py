"""Standalone Task 4 CLI for validating transformed Delta batches."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path
from typing import Sequence

from data_intensive_computing.task4.config import TASK4_DATASET_ORDER


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m data_intensive_computing.task4.cli")
    parser.add_argument("dataset", choices=TASK4_DATASET_ORDER)
    parser.add_argument("candidate_path", type=Path)
    parser.add_argument("target_path", type=Path)
    parser.add_argument("--quarantine-path", type=Path)
    parser.add_argument("--run-id")
    parser.add_argument("--no-quarantine", action="store_true")
    parser.add_argument("--driver-memory", default="8g")
    parser.add_argument("--shuffle-partitions", type=int, default=32)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    from data_intensive_computing.spark import build_spark_session
    from data_intensive_computing.task4.engine import validate_dataset

    spark = build_spark_session(
        "Urban-Data-Platform-Task4",
        driver_memory=args.driver_memory,
        shuffle_partitions=args.shuffle_partitions,
    )
    try:
        candidate = spark.read.format("delta").load(str(args.candidate_path))
        result = validate_dataset(
            spark,
            args.dataset,
            candidate,
            target_path=args.target_path,
            run_id=args.run_id,
            quarantine_path=args.quarantine_path,
            write_quarantine=not args.no_quarantine,
        )
        print(json.dumps(asdict(result.summary), indent=2, default=str))
        result.unpersist()
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())

