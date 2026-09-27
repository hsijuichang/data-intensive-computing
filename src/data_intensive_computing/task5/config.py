"""Experiment settings and paths for the Task 5 evaluation."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from data_intensive_computing.task1.config import TASK1_DATASET_ORDER
from data_intensive_computing.task2.config import PRODUCT_ORDER


DEFAULT_REPETITIONS = 5

# Generated taxi trips are shifted to start right after the original data.
# A shift larger than this means a malformed old timestamp (e.g. 2002) was
# sampled as the earliest trip, which would move all new trips years ahead.
MAX_TAXI_UPDATE_SPAN_DAYS = 200


@dataclass(frozen=True)
class Task1Config:
    """One Phase A configuration of the incremental update pipeline."""

    name: str
    validation_enabled: bool
    monitoring_enabled: bool
    purpose: str


PHASE_A_CONFIGS = (
    Task1Config(
        "A1_validation_on_monitoring_on",
        validation_enabled=True,
        monitoring_enabled=True,
        purpose="production configuration: incremental update time",
    ),
    Task1Config(
        "A2_validation_on_monitoring_off",
        validation_enabled=True,
        monitoring_enabled=False,
        purpose="A1 - A2 = monitoring overhead",
    ),
    Task1Config(
        "A3_validation_off_monitoring_off",
        validation_enabled=False,
        monitoring_enabled=False,
        purpose="A2 - A3 = validation overhead",
    ),
)

B1_INCREMENTAL_MONITORING_ON = "B1_incremental_monitoring_on"
B2_INCREMENTAL_MONITORING_OFF = "B2_incremental_monitoring_off"
B3_FULL_RECOMPUTE = "B3_full_recompute"
PHASE_B_CONFIGS = (
    B1_INCREMENTAL_MONITORING_ON,
    B2_INCREMENTAL_MONITORING_OFF,
    B3_FULL_RECOMPUTE,
)

# Tables changed by the pipeline under test. They are restored between runs.
TASK1_TABLES = TASK1_DATASET_ORDER
TASK2_TABLES = ("integrated_taxi_trips", *(f"analytical_products/{name}" for name in PRODUCT_ORDER))
TRACKED_TABLES = (*TASK1_TABLES, *TASK2_TABLES)

# Records of the runs themselves. They are measured but never restored.
FAILED_TABLES = TASK1_DATASET_ORDER
METADATA_TABLES = ("ingestion_runs", "validation_failure_events", "monitoring_writes")


def processed_dir(root: Path) -> Path:
    return root / "data" / "processed"


def table_path(root: Path, name: str) -> Path:
    return processed_dir(root) / name


def results_path(root: Path) -> Path:
    return root / "artifacts" / "task5_evaluation_results.jsonl"


def snapshot_path(root: Path) -> Path:
    return root / "artifacts" / "task5_snapshot.json"


def storage_path(root: Path) -> Path:
    return root / "artifacts" / "task5_storage.json"


def summary_path(root: Path) -> Path:
    return root / "artifacts" / "task5_summary.json"
