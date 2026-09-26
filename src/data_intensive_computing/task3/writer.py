"""Idempotent Delta writers for Task 3 operational metadata."""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from data_intensive_computing.task3.config import (
    monitoring_writes_path,
    pipeline_runs_path,
    validation_failures_path,
)
from data_intensive_computing.task3.models import (
    MonitoringWriteResult,
    PipelineRunRecord,
)
from data_intensive_computing.task3.schema import (
    MONITORING_WRITE_SCHEMA,
    PIPELINE_RUN_SCHEMA,
    VALIDATION_FAILURE_SCHEMA,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import StructType


def _timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def _delta_exists(path: Path) -> bool:
    return (path / "_delta_log").exists()


def _ensure_table(
    spark: "SparkSession",
    path: Path,
    schema: "StructType",
) -> None:
    if _delta_exists(path):
        existing_columns = set(
            spark.read.format("delta").load(str(path)).schema.fieldNames()
        )
        if set(schema.fieldNames()).issubset(existing_columns):
            return
        (
            spark.createDataFrame([], schema)
            .write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(str(path))
        )
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    spark.createDataFrame([], schema).write.format("delta").mode("overwrite").save(
        str(path)
    )


def ensure_monitoring_tables(spark: "SparkSession", project_root: Path) -> None:
    """Create empty monitoring Delta tables so every SQL query is always valid."""
    _ensure_table(spark, pipeline_runs_path(project_root), PIPELINE_RUN_SCHEMA)
    _ensure_table(
        spark,
        validation_failures_path(project_root),
        VALIDATION_FAILURE_SCHEMA,
    )
    _ensure_table(
        spark,
        monitoring_writes_path(project_root),
        MONITORING_WRITE_SCHEMA,
    )


def _record_row(record: PipelineRunRecord) -> dict:
    failures = {key: int(value) for key, value in record.validation_failures.items()}
    return {
        "run_id": record.run_id,
        "dataset_name": record.dataset_name,
        "source_format": record.source_format,
        "source_path": record.source_path,
        "processed_path": record.processed_path,
        "failed_path": record.failed_path,
        "started_at": _timestamp(record.started_at),
        "completed_at": _timestamp(record.completed_at),
        "execution_time_seconds": float(record.execution_time_seconds),
        "input_records": int(record.input_records),
        "processed_records": int(record.processed_records),
        "rejected_records": int(record.rejected_records),
        "duplicate_records": int(record.duplicate_records),
        "schema_version": record.schema_version,
        "source_schema_json": None,
        "output_schema_json": None,
        "status": record.status,
        "error_message": record.error_message,
        "pipeline_type": record.pipeline_type,
        "inserted_records": int(record.inserted_records),
        "warning_records": int(record.warning_records),
        "validation_failure_count": sum(failures.values()),
        "validation_failures_json": json.dumps(failures, sort_keys=True),
        "schema_changes_json": json.dumps(list(record.schema_changes)),
        "recorded_at": datetime.now(timezone.utc).replace(tzinfo=None),
    }


def _merge_pipeline_run(
    spark: "SparkSession",
    project_root: Path,
    record: PipelineRunRecord,
) -> bool:
    from delta.tables import DeltaTable

    path = pipeline_runs_path(project_root)
    source = spark.createDataFrame([_record_row(record)], PIPELINE_RUN_SCHEMA)
    if not _delta_exists(path):
        path.parent.mkdir(parents=True, exist_ok=True)
        source.write.format("delta").mode("overwrite").save(str(path))
        return True

    # A tiny key lookup supplies an accurate backfill count without scanning
    # the table into Python.
    from pyspark.sql import functions as F

    already_recorded = (
        spark.read.format("delta")
        .load(str(path))
        .filter(
            (F.col("run_id") == record.run_id)
            & (F.col("dataset_name") == record.dataset_name)
        )
        .limit(1)
        .count()
        > 0
    )
    (
        DeltaTable.forPath(spark, str(path))
        .alias("target")
        .merge(
            source.alias("updates"),
            "target.run_id = updates.run_id "
            "AND target.dataset_name = updates.dataset_name",
        )
        .withSchemaEvolution()
        .whenNotMatchedInsertAll()
        .execute()
    )
    return not already_recorded


def _validation_rows(record: PipelineRunRecord) -> list[dict]:
    recorded_at = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = []
    for code, count in sorted(record.validation_failures.items()):
        event_key = f"{record.run_id}|{record.dataset_name}|{code}"
        rows.append(
            {
                "event_id": hashlib.sha256(event_key.encode("utf-8")).hexdigest(),
                "run_id": record.run_id,
                "dataset_name": record.dataset_name,
                "pipeline_type": record.pipeline_type,
                "failure_code": code,
                "failure_count": int(count),
                "recorded_at": recorded_at,
            }
        )
    return rows


def _merge_validation_events(
    spark: "SparkSession",
    project_root: Path,
    record: PipelineRunRecord,
) -> int:
    from delta.tables import DeltaTable

    rows = _validation_rows(record)
    if not rows:
        return 0
    path = validation_failures_path(project_root)
    source = spark.createDataFrame(rows, VALIDATION_FAILURE_SCHEMA)
    if not _delta_exists(path):
        path.parent.mkdir(parents=True, exist_ok=True)
        source.write.format("delta").mode("overwrite").save(str(path))
        return len(rows)
    (
        DeltaTable.forPath(spark, str(path))
        .alias("target")
        .merge(source.alias("updates"), "target.event_id = updates.event_id")
        .whenNotMatchedInsertAll()
        .execute()
    )
    return len(rows)


def record_pipeline_run(
    spark: "SparkSession",
    project_root: Path,
    record: PipelineRunRecord,
) -> MonitoringWriteResult:
    """Write one run plus normalized validation failures to Delta tables."""
    started = time.perf_counter()
    record_inserted = _merge_pipeline_run(spark, project_root, record)
    validation_events = _merge_validation_events(spark, project_root, record)
    elapsed = time.perf_counter() - started
    result = MonitoringWriteResult(
        run_id=record.run_id,
        dataset_name=record.dataset_name,
        pipeline_type=record.pipeline_type,
        execution_time_seconds=round(elapsed, 4),
        validation_events_written=validation_events,
        record_inserted=record_inserted,
        status="SUCCESS",
    )

    path = monitoring_writes_path(project_root)
    write_row = {
        **asdict(result),
        "recorded_at": datetime.now(timezone.utc).replace(tzinfo=None),
    }
    mode = "append" if _delta_exists(path) else "overwrite"
    (
        spark.createDataFrame([write_row], MONITORING_WRITE_SCHEMA)
        .write.format("delta")
        .mode(mode)
        .save(str(path))
    )
    return result
