"""Paths and identifiers used by the Task 3 monitoring system."""

from __future__ import annotations

from pathlib import Path


PIPELINE_RUNS_TABLE = "pipeline_runs"
VALIDATION_FAILURES_TABLE = "validation_failure_events"
MONITORING_WRITES_TABLE = "monitoring_writes"


def metadata_dir(project_root: Path) -> Path:
    return project_root / "data" / "metadata"


def pipeline_runs_path(project_root: Path) -> Path:
    # Reuse and evolve the Week 1 table rather than creating a competing log.
    return metadata_dir(project_root) / "ingestion_runs"


def validation_failures_path(project_root: Path) -> Path:
    return metadata_dir(project_root) / VALIDATION_FAILURES_TABLE


def monitoring_writes_path(project_root: Path) -> Path:
    return metadata_dir(project_root) / MONITORING_WRITES_TABLE
