"""Turn raw Task 5 measurements into the numbers the evaluation report needs.

Pure Python (no Spark): it only reads the JSON artifacts written by the
experiments, so it can be re-run at any time without touching the tables.
"""

from __future__ import annotations

import json
import statistics
from collections import defaultdict
from pathlib import Path

from data_intensive_computing.task1.config import TASK1_DATASET_ORDER
from data_intensive_computing.task2.config import PRODUCT_ORDER
from data_intensive_computing.task5.config import (
    B1_INCREMENTAL_MONITORING_ON,
    B2_INCREMENTAL_MONITORING_OFF,
    B3_FULL_RECOMPUTE,
    results_path,
    storage_path,
    summary_path,
)


def _latest_session_rows(rows: list[dict], phase: str) -> list[dict]:
    """Use only the most recent complete run of a phase."""
    phase_rows = [row for row in rows if row.get("phase") == phase]
    if not phase_rows:
        return []
    latest = phase_rows[-1]["session_id"]
    return [
        row
        for row in phase_rows
        if row["session_id"] == latest
        and row.get("kind") == "measurement"
        and not row.get("warmup")
    ]


def _stats(values: list[float]) -> dict:
    return {
        "n": len(values),
        "median": round(statistics.median(values), 3),
        "min": round(min(values), 3),
        "max": round(max(values), 3),
    }


def _grouped(rows: list[dict], field: str) -> dict[tuple[str, str], dict]:
    values: dict[tuple[str, str], list[float]] = defaultdict(list)
    for row in rows:
        if field in row:
            values[(row["config"], row["scope"])].append(float(row[field]))
    return {key: _stats(items) for key, items in values.items()}


def _overhead(with_it: dict | None, without_it: dict | None) -> dict | None:
    if not with_it or not without_it:
        return None
    extra = with_it["median"] - without_it["median"]
    return {
        "extra_seconds": round(extra, 3),
        "extra_percent": round(100.0 * extra / without_it["median"], 1)
        if without_it["median"]
        else None,
    }


def _task1_summary(rows: list[dict]) -> dict:
    wall = _grouped(rows, "wall_seconds")
    result = {}
    for scope in (*TASK1_DATASET_ORDER, "all_datasets"):
        a1 = wall.get(("A1_validation_on_monitoring_on", scope))
        a2 = wall.get(("A2_validation_on_monitoring_off", scope))
        a3 = wall.get(("A3_validation_off_monitoring_off", scope))
        result[scope] = {
            "incremental_update_seconds": a1,
            "without_monitoring_seconds": a2,
            "without_validation_or_monitoring_seconds": a3,
            "monitoring_overhead": _overhead(a1, a2),
            "validation_overhead": _overhead(a2, a3),
        }
    return result


def _task2_summary(rows: list[dict]) -> dict:
    wall = _grouped(rows, "wall_seconds")
    internal = _grouped(rows, "internal_seconds")
    b1 = wall.get((B1_INCREMENTAL_MONITORING_ON, "total"))
    b2 = wall.get((B2_INCREMENTAL_MONITORING_OFF, "total"))
    b3 = wall.get((B3_FULL_RECOMPUTE, "total"))
    breakdown = {}
    for scope in ("integration", *PRODUCT_ORDER):
        breakdown[scope] = {
            "incremental_seconds": internal.get((B1_INCREMENTAL_MONITORING_ON, scope)),
            "full_recompute_seconds": internal.get((B3_FULL_RECOMPUTE, scope)),
        }
    affected = next(
        (row.get("affected_periods") for row in rows if row.get("scope") == "total"
         and row.get("affected_periods")),
        None,
    )
    return {
        "incremental_refresh_seconds": b1,
        "incremental_without_monitoring_seconds": b2,
        "full_recompute_seconds": b3,
        "monitoring_overhead": _overhead(b1, b2),
        "full_recompute_over_incremental": round(b3["median"] / b1["median"], 2)
        if b1 and b3 and b1["median"]
        else None,
        "affected_periods": affected,
        "breakdown": breakdown,
    }


def _storage_summary(root: Path) -> dict:
    path = storage_path(root)
    if not path.exists():
        return {}
    data = json.loads(path.read_text(encoding="utf-8"))
    result = {}
    for phase, entry in data.items():
        tables = {}
        for name, after in entry["after"].items():
            before = entry["before"].get(name, {"exists": False})
            if not after.get("exists"):
                continue
            disk_before = before.get("disk_bytes", 0) if before.get("exists") else 0
            current_before = before.get("current_version_bytes", 0) if before.get("exists") else 0
            change = {
                "disk_bytes_before": disk_before,
                "disk_bytes_after": after["disk_bytes"],
                "disk_bytes_added": after["disk_bytes"] - disk_before,
                "current_version_bytes_added": after.get("current_version_bytes", 0)
                - current_before,
                # Parquet files on disk that the latest version no longer uses.
                "kept_for_time_travel_bytes": after.get("parquet_bytes_on_disk", 0)
                - after.get("current_version_bytes", 0),
                "delta_log_bytes": after.get("delta_log_bytes", 0),
            }
            if change["disk_bytes_added"] or change["current_version_bytes_added"]:
                tables[name] = change
        result[phase] = tables
    return result


def summarize(root: Path) -> dict:
    path = results_path(root)
    if not path.exists():
        raise FileNotFoundError(f"No measurements yet: {path}")
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]
    summary = {
        "task1_incremental_update": _task1_summary(_latest_session_rows(rows, "A")),
        "task2_analytical_refresh": _task2_summary(_latest_session_rows(rows, "B")),
        "storage": _storage_summary(root),
    }
    summary_path(root).write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary


def _fmt(stats: dict | None) -> str:
    if not stats:
        return "-"
    return f"{stats['median']:.1f}s (n={stats['n']}, {stats['min']:.1f}-{stats['max']:.1f})"


def _fmt_overhead(value: dict | None) -> str:
    if not value:
        return "-"
    return f"{value['extra_seconds']:+.1f}s ({value['extra_percent']:+.1f}%)"


def _mb(value: int) -> str:
    return f"{value / 1_000_000:.1f} MB"


def print_summary(summary: dict) -> None:
    print("\nTask 1 incremental update (median of wall-clock seconds)")
    for scope, item in summary["task1_incremental_update"].items():
        print(f"  {scope}")
        print(f"    update time (A1):     {_fmt(item['incremental_update_seconds'])}")
        print(f"    monitoring overhead:  {_fmt_overhead(item['monitoring_overhead'])}")
        print(f"    validation overhead:  {_fmt_overhead(item['validation_overhead'])}")

    task2 = summary["task2_analytical_refresh"]
    print("\nTask 2 analytical refresh")
    print(f"  incremental (B1):       {_fmt(task2['incremental_refresh_seconds'])}")
    print(f"  full recompute (B3):    {_fmt(task2['full_recompute_seconds'])}")
    print(f"  full / incremental:     {task2['full_recompute_over_incremental']}x")
    print(f"  monitoring overhead:    {_fmt_overhead(task2['monitoring_overhead'])}")
    print(f"  affected periods:       {task2['affected_periods']}")

    print("\nStorage added by one run")
    for phase, tables in summary["storage"].items():
        print(f"  {phase}")
        for name, change in tables.items():
            print(
                f"    {name:<50} disk +{_mb(change['disk_bytes_added'])}, "
                f"current version +{_mb(change['current_version_bytes_added'])}, "
                f"kept for time travel {_mb(change['kept_for_time_travel_bytes'])}"
            )
