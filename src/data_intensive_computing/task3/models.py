"""Serializable monitoring records stored by the Task 3 framework."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class PipelineRunRecord:
    """One operational event in the unified pipeline-run fact table."""

    run_id: str
    dataset_name: str
    pipeline_type: str
    source_format: str
    source_path: str
    processed_path: str
    failed_path: str
    started_at: str
    completed_at: str
    execution_time_seconds: float
    input_records: int
    processed_records: int
    inserted_records: int
    rejected_records: int
    duplicate_records: int
    warning_records: int
    schema_version: str
    validation_failures: dict[str, int] = field(default_factory=dict)
    schema_changes: tuple[str, ...] = ()
    status: str = "SUCCESS"
    error_message: str | None = None


@dataclass(frozen=True)
class MonitoringWriteResult:
    """Timing information retained for the later monitoring-overhead experiment."""

    run_id: str
    dataset_name: str
    pipeline_type: str
    execution_time_seconds: float
    validation_events_written: int
    record_inserted: bool
    status: str


@dataclass(frozen=True)
class BackfillResult:
    """Result of importing historical JSONL execution artifacts."""

    source_path: str
    records_read: int
    records_written: int
    records_skipped: int
