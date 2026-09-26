"""Reusable Task 4 validation engine."""

from __future__ import annotations

import time
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Mapping

from data_intensive_computing.task4.config import DATASET_CONFIGS
from data_intensive_computing.task4.models import (
    DatasetValidationConfig,
    PathLike,
    ReferenceRule,
    SchemaValidationResult,
    ValidatedBatch,
    ValidationSummary,
)
from data_intensive_computing.task4.quarantine import append_quarantine
from data_intensive_computing.task4.schema import validate_schema_change

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame, SparkSession


def _delta_exists(path: Path) -> bool:
    return (path / "_delta_log").exists()


def _default_quarantine_path(target_path: Path, dataset_name: str) -> Path:
    return target_path.parent.parent / "failed" / dataset_name


def _empty_string_or_null(column: str) -> "Column":
    from pyspark.sql import functions as F

    return F.col(column).isNull() | (F.trim(F.col(column).cast("string")) == "")


def _all_present(columns: tuple[str, ...]) -> "Column":
    from pyspark.sql import functions as F

    condition = F.lit(True)
    for column in columns:
        condition &= ~_empty_string_or_null(column)
    return condition


def _add_code(
    codes: "Column",
    condition: "Column",
    code: str,
) -> "Column":
    from pyspark.sql import functions as F

    return F.when(
        F.coalesce(condition, F.lit(False)),
        F.array_union(codes, F.array(F.lit(code))),
    ).otherwise(codes)


def _annotate_for_quarantine(
    frame: "DataFrame",
    *,
    dataset_name: str,
    run_id: str,
    schema_version: str,
) -> "DataFrame":
    from pyspark.sql import functions as F

    return (
        frame.withColumn("_validation_run_id", F.lit(run_id))
        .withColumn("_validated_at", F.current_timestamp())
        .withColumn("_validation_dataset", F.lit(dataset_name))
        .withColumn("_validation_schema_version", F.lit(schema_version))
    )


def _failure_counts(invalid: "DataFrame") -> dict[str, int]:
    from pyspark.sql import functions as F

    return {
        row["code"]: int(row["count"])
        for row in (
            invalid.select(F.explode("_validation_errors").alias("code"))
            .groupBy("code")
            .count()
            .collect()
        )
    }


def _schema_rejection(
    frame: "DataFrame",
    *,
    config: DatasetValidationConfig,
    schema_result: SchemaValidationResult,
    run_id: str,
    started: float,
    quarantine_path: Path | None,
    write_quarantine: bool,
) -> ValidatedBatch:
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    input_records = frame.count()
    errors = F.array(*[F.lit(code) for code in schema_result.errors])
    invalid = _annotate_for_quarantine(
        frame.withColumn("_validation_errors", errors).withColumn(
            "_validation_warnings", F.expr("array()").cast("array<string>")
        ),
        dataset_name=config.name,
        run_id=run_id,
        schema_version=config.schema_version,
    ).persist(StorageLevel.MEMORY_AND_DISK)
    invalid.count()
    valid = frame.limit(0).persist(StorageLevel.MEMORY_AND_DISK)
    warnings = invalid.limit(0).persist(StorageLevel.MEMORY_AND_DISK)
    if write_quarantine and quarantine_path is not None:
        append_quarantine(invalid, quarantine_path)
    failure_counts = {code: input_records for code in schema_result.errors}
    summary = ValidationSummary(
        run_id=run_id,
        dataset_name=config.name,
        input_records=input_records,
        valid_records=0,
        rejected_records=input_records,
        duplicate_records=0,
        warning_records=0,
        schema_version=config.schema_version,
        schema_changes=tuple(f"added:{name}" for name in schema_result.added_columns),
        failure_counts=failure_counts,
        execution_time_seconds=round(time.perf_counter() - started, 4),
        status="REJECTED_SCHEMA",
        quarantine_path=str(quarantine_path) if quarantine_path is not None else None,
    )
    return ValidatedBatch(valid, invalid, warnings, summary, schema_result)


def _reference_frame(
    spark: "SparkSession",
    rule: ReferenceRule,
    target_path: Path,
    supplied: Mapping[str, "DataFrame"],
) -> "DataFrame":
    if rule.reference_name in supplied:
        return supplied[rule.reference_name]
    sibling = target_path.parent / rule.reference_name
    if _delta_exists(sibling):
        return spark.read.format("delta").load(str(sibling))
    raise ValueError(
        f"Missing reference DataFrame '{rule.reference_name}' for rule {rule.code}"
    )


def _apply_reference_rule(
    frame: "DataFrame",
    reference: "DataFrame",
    rule: ReferenceRule,
) -> "DataFrame":
    from pyspark.sql import functions as F

    if len(rule.local_columns) != len(rule.reference_columns):
        raise ValueError(f"Reference rule {rule.code} has mismatched key lengths")
    missing_local = sorted(set(rule.local_columns) - set(frame.columns))
    missing_reference = sorted(set(rule.reference_columns) - set(reference.columns))
    if missing_local or missing_reference:
        raise ValueError(
            f"Reference rule {rule.code} missing local={missing_local}, "
            f"reference={missing_reference}"
        )

    marker = f"_reference_match_{abs(hash(rule.code))}"
    lookup = reference.select(
        *[
            F.col(reference_name).alias(local_name)
            for local_name, reference_name in zip(
                rule.local_columns, rule.reference_columns, strict=True
            )
        ]
    ).dropDuplicates()
    lookup = lookup.withColumn(marker, F.lit(True))
    checked = frame.join(F.broadcast(lookup), list(rule.local_columns), "left")
    condition = _all_present(rule.local_columns) & F.col(marker).isNull()
    destination = (
        "_validation_errors" if rule.severity == "ERROR" else "_validation_warnings"
    )
    return checked.withColumn(
        destination,
        _add_code(F.col(destination), condition, rule.code),
    ).drop(marker)


def validate_dataset(
    spark: "SparkSession",
    dataset_name: str,
    frame: "DataFrame",
    *,
    target_path: PathLike,
    run_id: str | None = None,
    reference_frames: Mapping[str, "DataFrame"] | None = None,
    quarantine_path: PathLike | None = None,
    write_quarantine: bool = True,
) -> ValidatedBatch:
    """Validate a transformed batch without allowing bad rows to stop valid rows.

    Duplicate records are rejected and included in ``rejected_records`` while
    also being reported separately in ``duplicate_records``.
    """
    from pyspark import StorageLevel
    from pyspark.sql import Window, functions as F

    if dataset_name not in DATASET_CONFIGS:
        raise ValueError(f"Unknown dataset: {dataset_name}")
    config = DATASET_CONFIGS[dataset_name]
    target_path = Path(target_path)
    if not _delta_exists(target_path):
        raise FileNotFoundError(f"Missing target Delta table: {target_path}")
    target = spark.read.format("delta").load(str(target_path))
    run_id = run_id or str(uuid.uuid4())
    started = time.perf_counter()
    resolved_quarantine = (
        Path(quarantine_path)
        if quarantine_path is not None
        else _default_quarantine_path(target_path, dataset_name)
    )

    schema_result = validate_schema_change(frame, target, config.schema_policy)
    if not schema_result.supported:
        return _schema_rejection(
            frame,
            config=config,
            schema_result=schema_result,
            run_id=run_id,
            started=started,
            quarantine_path=resolved_quarantine,
            write_quarantine=write_quarantine,
        )

    required_for_engine = set(config.required_columns) | set(config.primary_key)
    required_for_engine.update(
        column for rule in config.reference_rules for column in rule.local_columns
    )
    missing_engine_columns = sorted(required_for_engine - set(frame.columns))
    if missing_engine_columns:
        raise ValueError(
            f"Validation configuration for {dataset_name} references missing columns: "
            f"{missing_engine_columns}"
        )

    errors = F.expr("array()").cast("array<string>")
    warnings = F.expr("array()").cast("array<string>")
    checked = frame.withColumn("_validation_errors", errors).withColumn(
        "_validation_warnings", warnings
    )
    for column in config.required_columns:
        checked = checked.withColumn(
            "_validation_errors",
            _add_code(
                F.col("_validation_errors"),
                _empty_string_or_null(column),
                f"incomplete_record:{column}",
            ),
        )
    for rule in config.rules:
        destination = (
            "_validation_errors"
            if rule.severity == "ERROR"
            else "_validation_warnings"
        )
        checked = checked.withColumn(
            destination,
            _add_code(F.col(destination), rule.condition(checked), rule.code),
        )

    supplied_references = reference_frames or {}
    for rule in config.reference_rules:
        reference = _reference_frame(
            spark, rule, target_path, supplied_references
        )
        checked = _apply_reference_rule(checked, reference, rule)

    duplicate_window = Window.partitionBy(*config.primary_key).orderBy(
        F.monotonically_increasing_id()
    )
    checked = checked.withColumn(
        "_duplicate_rank", F.row_number().over(duplicate_window)
    ).withColumn(
        "_validation_errors",
        _add_code(
            F.col("_validation_errors"),
            _all_present(config.primary_key) & (F.col("_duplicate_rank") > 1),
            "duplicate_within_batch",
        ),
    )

    target_keys = target.select(*config.primary_key).dropDuplicates().withColumn(
        "_existing_primary_key", F.lit(True)
    )
    checked = checked.join(
        target_keys, list(config.primary_key), "left"
    ).withColumn(
        "_validation_errors",
        _add_code(
            F.col("_validation_errors"),
            _all_present(config.primary_key) & F.col("_existing_primary_key").isNotNull(),
            "duplicate_existing_record",
        ),
    ).drop("_existing_primary_key", "_duplicate_rank")
    checked = checked.persist(StorageLevel.MEMORY_AND_DISK)

    invalid_base = checked.filter(F.size("_validation_errors") > 0)
    warnings_base = checked.filter(F.size("_validation_warnings") > 0)
    valid = (
        checked.filter(F.size("_validation_errors") == 0)
        .drop("_validation_errors", "_validation_warnings")
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    invalid = _annotate_for_quarantine(
        invalid_base,
        dataset_name=dataset_name,
        run_id=run_id,
        schema_version=config.schema_version,
    ).persist(StorageLevel.MEMORY_AND_DISK)
    warning_rows = _annotate_for_quarantine(
        warnings_base,
        dataset_name=dataset_name,
        run_id=run_id,
        schema_version=config.schema_version,
    ).persist(StorageLevel.MEMORY_AND_DISK)

    input_records = checked.count()
    valid_records = valid.count()
    rejected_records = invalid.count()
    warning_records = warning_rows.count()
    duplicate_records = invalid.filter(
        F.array_contains("_validation_errors", "duplicate_within_batch")
        | F.array_contains("_validation_errors", "duplicate_existing_record")
    ).count()
    failure_counts = _failure_counts(invalid)
    if input_records != valid_records + rejected_records:
        raise AssertionError(
            "Validation accounting mismatch: "
            f"{input_records} != {valid_records} + {rejected_records}"
        )
    if write_quarantine:
        append_quarantine(invalid, resolved_quarantine)
    checked.unpersist()

    summary = ValidationSummary(
        run_id=run_id,
        dataset_name=dataset_name,
        input_records=input_records,
        valid_records=valid_records,
        rejected_records=rejected_records,
        duplicate_records=duplicate_records,
        warning_records=warning_records,
        schema_version=config.schema_version,
        schema_changes=tuple(f"added:{name}" for name in schema_result.added_columns),
        failure_counts=failure_counts,
        execution_time_seconds=round(time.perf_counter() - started, 4),
        status="SUCCESS_WITH_REJECTIONS" if rejected_records else "SUCCESS",
        quarantine_path=str(resolved_quarantine) if write_quarantine else None,
    )
    return ValidatedBatch(valid, invalid, warning_rows, summary, schema_result)
