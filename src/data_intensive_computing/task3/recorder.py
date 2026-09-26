"""Adapters from pipeline result objects to unified monitoring records."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

from data_intensive_computing.task1.config import DATASETS
from data_intensive_computing.task3.models import (
    BackfillResult,
    PipelineRunRecord,
)
from data_intensive_computing.task3.writer import record_pipeline_run

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from data_intensive_computing.task1.pipeline import IncrementalRunResult
    from data_intensive_computing.task2.models import AnalyticalConsistencyResult


def incremental_record(
    result: "IncrementalRunResult",
    project_root: Path,
) -> PipelineRunRecord:
    """Map Task 1 naming onto explicit input/processed/inserted semantics."""
    config = DATASETS[result.dataset_name]
    return PipelineRunRecord(
        run_id=result.run_id,
        dataset_name=result.dataset_name,
        pipeline_type="incremental_update",
        source_format=config.source_format,
        source_path=str(config.update_path(project_root)),
        processed_path=str(config.processed_path(project_root)),
        failed_path=str(config.failed_path(project_root)),
        started_at=result.started_at,
        completed_at=result.completed_at,
        execution_time_seconds=result.execution_time_seconds,
        input_records=result.processed_records,
        processed_records=result.inserted_records,
        inserted_records=result.inserted_records,
        rejected_records=result.rejected_records,
        duplicate_records=result.duplicate_records,
        warning_records=result.warning_records,
        schema_version=result.schema_version,
        validation_failures=dict(result.validation_failures),
        schema_changes=tuple(result.schema_changes),
        status=result.status,
    )


def record_incremental_result(
    spark: "SparkSession",
    project_root: Path,
    result: "IncrementalRunResult",
):
    return record_pipeline_run(spark, project_root, incremental_record(result, project_root))


def analytical_records(
    result: "AnalyticalConsistencyResult",
    project_root: Path,
) -> tuple[PipelineRunRecord, ...]:
    """Create one integration record plus one record per refreshed product."""
    processed = project_root / "data" / "processed"
    failed = project_root / "data" / "failed"
    integration = result.integration
    records = [
        PipelineRunRecord(
            run_id=result.run_id,
            dataset_name="integrated_taxi_trips",
            pipeline_type="analytical_refresh",
            source_format="delta",
            source_path=str(processed / "taxi_trips"),
            processed_path=str(processed / "integrated_taxi_trips"),
            failed_path=str(failed / "integrated_taxi_trips"),
            started_at=result.started_at,
            completed_at=result.completed_at,
            execution_time_seconds=integration.execution_time_seconds,
            input_records=integration.candidate_records,
            processed_records=integration.integrated_records,
            inserted_records=integration.integrated_records,
            rejected_records=max(
                0,
                integration.pending_records - integration.integrated_records,
            ),
            duplicate_records=max(
                0,
                integration.candidate_records - integration.pending_records,
            ),
            warning_records=0,
            schema_version=result.schema_version,
            status=integration.status,
        )
    ]
    product_dir = processed / "analytical_products"
    for product in result.product_results:
        records.append(
            PipelineRunRecord(
                run_id=result.run_id,
                dataset_name=product.product_name,
                pipeline_type="analytical_product_refresh",
                source_format="delta",
                source_path=str(processed / "integrated_taxi_trips"),
                processed_path=str(product_dir / product.product_name),
                failed_path=str(failed / "analytical_products" / product.product_name),
                started_at=result.started_at,
                completed_at=result.completed_at,
                execution_time_seconds=product.execution_time_seconds,
                input_records=product.rows_written,
                processed_records=product.rows_written,
                inserted_records=product.rows_written,
                rejected_records=0,
                duplicate_records=0,
                warning_records=0,
                schema_version=result.schema_version,
                status=product.status,
            )
        )
    return tuple(records)


def record_analytical_result(
    spark: "SparkSession",
    project_root: Path,
    result: "AnalyticalConsistencyResult",
):
    return tuple(
        record_pipeline_run(spark, project_root, record)
        for record in analytical_records(result, project_root)
    )


def _historical_incremental_record(
    raw: dict,
    project_root: Path,
) -> PipelineRunRecord:
    dataset_name = raw["dataset_name"]
    config = DATASETS[dataset_name]
    return PipelineRunRecord(
        run_id=raw["run_id"],
        dataset_name=dataset_name,
        pipeline_type="incremental_update",
        source_format=config.source_format,
        source_path=str(config.update_path(project_root)),
        processed_path=str(config.processed_path(project_root)),
        failed_path=str(config.failed_path(project_root)),
        started_at=raw["started_at"],
        completed_at=raw["completed_at"],
        execution_time_seconds=float(raw["execution_time_seconds"]),
        input_records=int(raw["processed_records"]),
        processed_records=int(raw["inserted_records"]),
        inserted_records=int(raw["inserted_records"]),
        rejected_records=int(raw.get("rejected_records", 0)),
        duplicate_records=int(raw.get("duplicate_records", 0)),
        warning_records=int(raw.get("warning_records", 0)),
        schema_version=raw["schema_version"],
        validation_failures={
            key: int(value)
            for key, value in raw.get("validation_failures", {}).items()
        },
        schema_changes=tuple(raw.get("schema_changes", ())),
        status=raw.get("status", "SUCCESS"),
        error_message=raw.get("error_message"),
    )


def backfill_task1_artifact(
    spark: "SparkSession",
    project_root: Path,
    source_path: Path | None = None,
) -> BackfillResult:
    """Idempotently import existing Task 1 JSONL timings into Delta."""
    source_path = source_path or (
        project_root / "artifacts" / "task1_incremental_results.jsonl"
    )
    if not source_path.exists():
        raise FileNotFoundError(f"Task 1 monitoring artifact not found: {source_path}")

    records_read = 0
    records_written = 0
    with source_path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            records_read += 1
            try:
                raw = json.loads(line)
                record = _historical_incremental_record(raw, project_root)
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
                raise ValueError(
                    f"Invalid monitoring record at {source_path}:{line_number}"
                ) from error
            write_result = record_pipeline_run(spark, project_root, record)
            records_written += int(write_result.record_inserted)
    return BackfillResult(
        source_path=str(source_path),
        records_read=records_read,
        records_written=records_written,
        records_skipped=records_read - records_written,
    )
