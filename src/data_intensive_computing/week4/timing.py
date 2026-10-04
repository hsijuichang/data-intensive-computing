"""Week 4 Task 4 timing harness.

Runs Week 4 notebooks end to end with ``jupyter nbconvert`` and records the
wall-clock time of every run. Each notebook starts in a fresh kernel, so every
measurement includes kernel start-up, JVM start-up, and Spark session creation.
The ``spark_baseline`` workflow measures that fixed cost on its own.

Run from the project root::

    # Main comparison: 1 discarded warm-up round + 5 measured rounds
    uv run python -m data_intensive_computing.week4.timing \
        --workflows spark_baseline A_dataset B_incremental --warmup 1 --repeats 5

    # Same downstream code on both datasets (sanity check)
    uv run python -m data_intensive_computing.week4.timing \
        --workflows A_downstream B_downstream --warmup 1 --repeats 3

    # Full platform rebuild, run inside a separate copy of the project
    uv run python -m data_intensive_computing.week4.timing \
        --workflows B_full_rebuild --project-root /path/to/copy --warmup 1 --repeats 3

    # Print the summary of everything recorded so far
    uv run python -m data_intensive_computing.week4.timing --summary-only

Workflows run round-robin: one run of every workflow per round, with the start
position rotated each round, so that slow drifts in machine state affect every
workflow equally. Warm-up rounds are recorded but excluded from the summary.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import statistics
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

HOME_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_RESULTS = HOME_ROOT / "artifacts" / "task4_timing_results.jsonl"
DEFAULT_SUMMARY = HOME_ROOT / "artifacts" / "task4_timing_summary.json"


@dataclass(frozen=True)
class Step:
    notebook: str
    env: dict = field(default_factory=dict)
    # Optional JSON artifact whose "timings_seconds" holds in-notebook stage timings.
    manifest: str | None = None


WORKFLOWS: dict[str, list[Step]] = {
    # Fixed cost of one notebook: kernel, JVM, Delta packages, Spark session.
    "spark_baseline": [
        Step("notebooks/w4_task4_spark_baseline.ipynb"),
    ],
    # Approach A: raw files -> training dataset.
    "A_dataset": [
        Step(
            "notebooks/w4_task4_approach_a_raw_dataset.ipynb",
            env={"TASK4_SKIP_EQUIVALENCE_CHECK": "1"},
            manifest="artifacts/task4_approach_a_dataset_run.json",
        ),
    ],
    # Approach B, per experiment: integrated platform table -> training dataset.
    "B_incremental": [
        Step("notebooks/w4_task1.ipynb"),
    ],
    # Shared feature pipeline and model on each dataset.
    "A_downstream": [
        Step("notebooks/w4_task2.ipynb", env={"WEEK4_VARIANT": "raw"}),
        Step(
            "notebooks/w4_task3.ipynb",
            env={"WEEK4_VARIANT": "raw"},
            manifest="artifacts/task4_approach_a_fare_metrics.json",
        ),
    ],
    "B_downstream": [
        Step("notebooks/w4_task2.ipynb", env={"WEEK4_VARIANT": "platform"}),
        Step(
            "notebooks/w4_task3.ipynb",
            env={"WEEK4_VARIANT": "platform"},
            manifest="artifacts/week4_fare_metrics.json",
        ),
    ],
    # Approach B including the platform build. Overwrites data/processed/*.
    "B_full_rebuild": [
        Step("notebooks/ingestion_framework.ipynb"),
        Step("notebooks/integrated_taxi_trips.ipynb"),
        Step("notebooks/w4_task1.ipynb"),
    ],
}

# Workflows that overwrite the shared Week 1-3 platform tables.
SHARED_TABLE_WRITERS = {"B_full_rebuild"}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def machine_info(project_root: Path) -> dict:
    info = {
        "platform": platform.platform(),
        "machine": platform.machine(),
        "cpu_count": os.cpu_count(),
        "python": platform.python_version(),
    }
    try:
        info["memory_bytes"] = int(
            subprocess.run(["sysctl", "-n", "hw.memsize"], capture_output=True, text=True, check=True).stdout
        )
    except (OSError, subprocess.CalledProcessError, ValueError):
        pass
    try:
        info["git_commit"] = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=project_root, capture_output=True, text=True, check=True
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        pass
    return info


def run_step(step: Step, project_root: Path, output_dir: Path) -> dict:
    """Execute one notebook in a fresh kernel and return its timing record."""
    notebook = project_root / step.notebook
    if not notebook.is_file():
        raise FileNotFoundError(notebook)
    output_dir.mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()
    for name in ("WEEK4_VARIANT", "TASK4_SKIP_EQUIVALENCE_CHECK"):
        env.pop(name, None)
    env.update(step.env)

    command = [
        sys.executable, "-m", "jupyter", "nbconvert",
        "--to", "notebook",
        "--execute",
        "--ExecutePreprocessor.timeout=-1",
        "--ExecutePreprocessor.kernel_name=python3",
        str(notebook),
        "--output-dir", str(output_dir),
    ]
    log_path = output_dir / f"{notebook.stem}.log"
    started_wall = time.time()
    started = time.perf_counter()
    with log_path.open("w", encoding="utf-8") as log:
        completed = subprocess.run(command, env=env, stdout=log, stderr=subprocess.STDOUT)
    seconds = time.perf_counter() - started

    record = {
        "notebook": step.notebook,
        "env": step.env,
        "seconds": round(seconds, 4),
        "returncode": completed.returncode,
        "log": str(log_path),
    }
    if step.manifest:
        manifest_path = project_root / step.manifest
        if manifest_path.is_file() and manifest_path.stat().st_mtime >= started_wall:
            record["stage_timings"] = json.loads(manifest_path.read_text(encoding="utf-8")).get("timings_seconds")
    return record


def run_session(
    workflows: list[str],
    project_root: Path,
    warmup: int,
    repeats: int,
    results_path: Path,
    output_root: Path,
) -> None:
    session_id = uuid.uuid4().hex[:12]
    info = machine_info(project_root)
    rounds = warmup + repeats
    print(f"Session {session_id}: {rounds} rounds ({warmup} warm-up) of {workflows}")
    print(f"Project root: {project_root}")
    results_path.parent.mkdir(parents=True, exist_ok=True)

    for round_index in range(rounds):
        shift = round_index % len(workflows)
        order = workflows[shift:] + workflows[:shift]
        is_warmup = round_index < warmup
        for workflow in order:
            output_dir = output_root / session_id / f"round_{round_index:02d}" / workflow
            started_at = utc_now()
            steps = []
            for step in WORKFLOWS[workflow]:
                record = run_step(step, project_root, output_dir)
                steps.append(record)
                if record["returncode"] != 0:
                    break
            succeeded = all(s["returncode"] == 0 for s in steps) and len(steps) == len(WORKFLOWS[workflow])
            result = {
                "session_id": session_id,
                "workflow": workflow,
                "round": round_index,
                "warmup": is_warmup,
                "succeeded": succeeded,
                "seconds": round(sum(s["seconds"] for s in steps), 4),
                "steps": steps,
                "started_at": started_at,
                "project_root": str(project_root),
                "machine": info,
            }
            with results_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(result) + "\n")

            label = "warm-up" if is_warmup else "measured"
            print(f"  round {round_index} [{label}] {workflow:16s} {result['seconds']:9.2f} s")
            if not succeeded:
                failed = steps[-1]
                raise SystemExit(
                    f"{workflow} failed in {failed['notebook']}; see {failed['log']} "
                    f"and the executed notebook in {output_dir}"
                )


def summarize(results_path: Path, summary_path: Path) -> dict:
    """Median, min, and max of measured, successful runs per workflow."""
    if not results_path.is_file():
        raise SystemExit(f"No results at {results_path}")
    runs = [json.loads(line) for line in results_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    measured = [r for r in runs if r["succeeded"] and not r["warmup"]]

    summary: dict = {"generated_at": utc_now(), "results_path": str(results_path), "workflows": {}}
    for workflow in WORKFLOWS:
        selected = [r for r in measured if r["workflow"] == workflow]
        if not selected:
            continue
        totals = [r["seconds"] for r in selected]
        per_notebook: dict[str, list[float]] = {}
        per_stage: dict[str, list[float]] = {}
        for run in selected:
            for step in run["steps"]:
                per_notebook.setdefault(step["notebook"], []).append(step["seconds"])
                for stage, value in (step.get("stage_timings") or {}).items():
                    per_stage.setdefault(stage, []).append(value)
        summary["workflows"][workflow] = {
            "n": len(totals),
            "median_seconds": statistics.median(totals),
            "min_seconds": min(totals),
            "max_seconds": max(totals),
            "sessions": sorted({r["session_id"] for r in selected}),
            "notebook_median_seconds": {k: statistics.median(v) for k, v in per_notebook.items()},
            "stage_median_seconds": {k: statistics.median(v) for k, v in per_stage.items()},
        }

    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

    print(f"\n{'workflow':16s} {'n':>3s} {'median s':>10s} {'min s':>9s} {'max s':>9s}")
    for workflow, values in summary["workflows"].items():
        print(
            f"{workflow:16s} {values['n']:3d} {values['median_seconds']:10.2f} "
            f"{values['min_seconds']:9.2f} {values['max_seconds']:9.2f}"
        )
    print(f"\nSummary written to {summary_path}")
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m data_intensive_computing.week4.timing")
    parser.add_argument("--workflows", nargs="+", choices=sorted(WORKFLOWS))
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument(
        "--project-root",
        type=Path,
        default=HOME_ROOT,
        help="Project whose notebooks and data are used. Use a copy for B_full_rebuild.",
    )
    parser.add_argument("--results", type=Path, default=DEFAULT_RESULTS)
    parser.add_argument("--summary", type=Path, default=DEFAULT_SUMMARY)
    parser.add_argument("--output-root", type=Path, default=Path("/tmp/task4_timing"))
    parser.add_argument(
        "--allow-shared-rebuild",
        action="store_true",
        help="Allow B_full_rebuild to overwrite the platform tables of this repository.",
    )
    parser.add_argument("--summary-only", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if not args.summary_only:
        if not args.workflows:
            raise SystemExit("--workflows is required unless --summary-only is given")
        project_root = args.project_root.resolve()
        rewrites_shared = SHARED_TABLE_WRITERS.intersection(args.workflows)
        if rewrites_shared and project_root == HOME_ROOT and not args.allow_shared_rebuild:
            raise SystemExit(
                f"{sorted(rewrites_shared)} overwrites data/processed in {project_root}. "
                "Run it with --project-root pointing to a copy of the project."
            )
        run_session(args.workflows, project_root, args.warmup, args.repeats, args.results, args.output_root)
    summarize(args.results, args.summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
