"""Incremental validation and insert-only Delta MERGE pipeline for Task 1."""

from __future__ import annotations

import json
import re
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from data_intensive_computing.task1.config import (
    DATASETS,
    TASK1_DATASET_ORDER,
    DatasetConfig,
)

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame, SparkSession


@dataclass(frozen=True)
class IncrementalRunResult:
    run_id: str
    dataset_name: str
    started_at: str
    completed_at: str
    execution_time_seconds: float
    processed_records: int
    inserted_records: int
    duplicate_records: int
    rejected_records: int
    warning_records: int
    schema_version: str
    schema_changes: tuple[str, ...]
    validation_failures: dict[str, int]
    quarantine_path: str | None
    status: str


def _snake_case(name: str) -> str:
    name = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    return re.sub(r"[^a-zA-Z0-9]+", "_", name).strip("_").lower()


def _standardize_columns(frame: "DataFrame") -> "DataFrame":
    for current in frame.columns:
        if current.startswith("_"):
            continue
        normalized = _snake_case(current)
        if current != normalized:
            frame = frame.withColumnRenamed(current, normalized)
    return frame


def _all_present(columns: tuple[str, ...] | list[str]) -> "Column":
    from pyspark.sql import functions as F

    condition = F.lit(True)
    for name in columns:
        condition &= F.col(name).isNotNull() & (F.trim(F.col(name).cast("string")) != "")
    return condition


def _stable_hash(columns: list[str]) -> "Column":
    from pyspark.sql import functions as F

    return F.sha2(
        F.concat_ws(
            "||",
            *[F.coalesce(F.col(name).cast("string"), F.lit("<null>")) for name in columns],
        ),
        256,
    )


def _has_columns(frame: "DataFrame", *names: str) -> bool:
    return set(names).issubset(frame.columns)


def _transform_taxi(frame: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F

    frame = _standardize_columns(frame)
    for old, new in (
        ("pu_location_id", "pickup_location_id"),
        ("do_location_id", "dropoff_location_id"),
        ("ratecode_id", "rate_code_id"),
        ("tpep_pickup_datetime", "pickup_datetime"),
        ("tpep_dropoff_datetime", "dropoff_datetime"),
    ):
        if old in frame.columns:
            frame = frame.withColumnRenamed(old, new)
    for name, data_type in (
        ("vendor_id", "long"),
        ("pickup_datetime", "timestamp"),
        ("dropoff_datetime", "timestamp"),
        ("passenger_count", "int"),
        ("rate_code_id", "long"),
        ("pickup_location_id", "int"),
        ("dropoff_location_id", "int"),
        ("payment_type", "long"),
    ):
        if name in frame.columns:
            frame = frame.withColumn(name, F.col(name).cast(data_type))
    for name in (
        "trip_distance",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
        "airport_fee",
    ):
        if name in frame.columns:
            frame = frame.withColumn(name, F.col(name).cast("double"))
    key = [
        "vendor_id",
        "pickup_datetime",
        "dropoff_datetime",
        "pickup_location_id",
        "dropoff_location_id",
        "trip_distance",
        "total_amount",
    ]
    trip_id = (
        F.when(_all_present(key), _stable_hash(key))
        if _has_columns(frame, *key)
        else F.lit(None).cast("string")
    )
    duration = (
        F.expr("timestampdiff(SECOND, pickup_datetime, dropoff_datetime)")
        / F.lit(60.0)
        if _has_columns(frame, "pickup_datetime", "dropoff_datetime")
        else F.lit(None).cast("double")
    )
    pickup = (
        F.col("pickup_datetime")
        if "pickup_datetime" in frame.columns
        else F.lit(None).cast("timestamp")
    )
    return (
        frame.withColumn("trip_id", trip_id)
        .withColumn("trip_duration_minutes", duration)
        .withColumn("pickup_date", F.to_date(pickup))
        .withColumn("pickup_hour", F.date_trunc("hour", pickup))
        .withColumn("pickup_year", F.year(pickup).cast("int"))
        .withColumn("pickup_month", F.month(pickup).cast("int"))
    )


def _transform_weather(frame: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F

    frame = _standardize_columns(frame)
    for name in ("year", "month", "day", "hour", "rhum", "wdir", "cldc", "coco"):
        if name in frame.columns:
            frame = frame.withColumn(name, F.col(name).cast("int"))
    for name in ("temp", "prcp", "snwd", "wspd", "wpgt", "pres", "humidity"):
        if name in frame.columns:
            frame = frame.withColumn(name, F.col(name).cast("double"))
    timestamp = (
        F.make_timestamp("year", "month", "day", "hour", F.lit(0), F.lit(0))
        if _has_columns(frame, "year", "month", "day", "hour")
        else F.lit(None).cast("timestamp")
    )
    return (
        frame.withColumn("observation_timestamp", timestamp)
        .withColumn("observation_date", F.to_date("observation_timestamp"))
    )


def _transform_air_quality(frame: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F

    frame = _standardize_columns(frame)
    for name in ("latitude", "longitude", "sample_measurement", "mdl", "uncertainty"):
        if name in frame.columns:
            frame = frame.withColumn(name, F.col(name).cast("double"))
    if "aqi" in frame.columns:
        frame = frame.withColumn("aqi", F.col("aqi").cast("int"))
    local_timestamp = (
        F.to_timestamp(
            F.concat_ws(" ", "date_local", "time_local"), "yyyy-MM-dd HH:mm"
        )
        if _has_columns(frame, "date_local", "time_local")
        else F.lit(None).cast("timestamp")
    )
    utc_timestamp = (
        F.to_timestamp(
            F.concat_ws(" ", "date_gmt", "time_gmt"), "yyyy-MM-dd HH:mm"
        )
        if _has_columns(frame, "date_gmt", "time_gmt")
        else F.lit(None).cast("timestamp")
    )
    last_change = (
        F.to_date("date_of_last_change", "yyyy-MM-dd")
        if "date_of_last_change" in frame.columns
        else F.lit(None).cast("date")
    )
    station_columns = (
        "state_code",
        "county_code",
        "site_num",
        "parameter_code",
        "poc",
    )
    station_id = (
        F.concat_ws("-", *station_columns)
        if _has_columns(frame, *station_columns)
        else F.lit(None).cast("string")
    )
    frame = (
        frame.withColumn("observation_timestamp", local_timestamp)
        .withColumn("observation_timestamp_utc", utc_timestamp)
        .withColumn("date_of_last_change", last_change)
        .withColumn("station_id", station_id)
    )
    key = [
        "state_code",
        "county_code",
        "site_num",
        "parameter_code",
        "poc",
        "observation_timestamp",
        "method_code",
    ]
    observation_id = (
        F.when(_all_present(key), _stable_hash(key))
        if _has_columns(frame, *key)
        else F.lit(None).cast("string")
    )
    return (
        frame.withColumn("observation_id", observation_id)
        .withColumn("observation_date", F.to_date("observation_timestamp"))
        .withColumn("observation_year", F.year("observation_timestamp").cast("int"))
        .withColumn("observation_month", F.month("observation_timestamp").cast("int"))
        .drop("date_local", "time_local", "date_gmt", "time_gmt")
    )


TRANSFORMERS = {
    "taxi_trips": _transform_taxi,
    "weather": _transform_weather,
    "air_quality": _transform_air_quality,
}


def _read_update(
    spark: "SparkSession",
    config: DatasetConfig,
    project_root: Path,
) -> "DataFrame":
    path = config.update_path(project_root)
    if not path.exists():
        raise FileNotFoundError(f"Missing update dataset: {path}. Run generate-updates first.")
    if config.source_format == "parquet":
        frame = spark.read.parquet(str(path))
    else:
        frame = spark.read.option("header", True).option("inferSchema", False).csv(str(path))
    return frame


def apply_incremental_update(
    spark: "SparkSession",
    project_root: Path,
    dataset_name: str,
) -> IncrementalRunResult:
    """Transform one release, validate it with Task 4, and merge valid rows."""
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    from data_intensive_computing.task4 import validate_dataset

    config = DATASETS[dataset_name]
    target_path = config.processed_path(project_root)
    if not (target_path / "_delta_log").exists():
        raise FileNotFoundError(
            f"Missing baseline Delta table: {target_path}. Run the Week 1 ingestion first."
        )

    run_id = str(uuid.uuid4())
    started_clock = time.perf_counter()
    started_at = datetime.now(timezone.utc)
    raw = _read_update(spark, config, project_root)
    processed_records = raw.count()
    enriched = (
        raw.withColumn("_source_file", F.input_file_name())
        .withColumn("_ingestion_run_id", F.lit(run_id))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_schema_version", F.lit(config.schema_version))
    )
    transformed = TRANSFORMERS[dataset_name](enriched)
    # Task 4 owns schema, value, completeness, reference, and duplicate
    # validation. Only its valid frame is allowed to reach the Delta MERGE.
    validation = validate_dataset(
        spark,
        dataset_name,
        transformed,
        target_path=target_path,
        run_id=run_id,
        quarantine_path=config.failed_path(project_root),
    )
    summary = validation.summary
    inserts = validation.valid
    inserted_records = summary.valid_records

    if inserted_records:
        condition = " AND ".join(
            f"target.`{name}` = updates.`{name}`" for name in config.primary_key
        )
        (
            DeltaTable.forPath(spark, str(target_path))
            .alias("target")
            .merge(inserts.alias("updates"), condition)
            .withSchemaEvolution()
            .whenNotMatchedInsertAll()
            .execute()
        )

    validation.unpersist()
    completed_at = datetime.now(timezone.utc)
    elapsed = time.perf_counter() - started_clock
    if processed_records != summary.input_records:
        raise AssertionError(
            "Task 1/Task 4 input mismatch: "
            f"{processed_records} != {summary.input_records}"
        )
    if processed_records != inserted_records + summary.rejected_records:
        raise AssertionError(
            "Incremental accounting mismatch: "
            f"{processed_records} != {inserted_records} + {summary.rejected_records}"
        )
    return IncrementalRunResult(
        run_id=run_id,
        dataset_name=dataset_name,
        started_at=started_at.isoformat(),
        completed_at=completed_at.isoformat(),
        execution_time_seconds=round(elapsed, 4),
        processed_records=processed_records,
        inserted_records=inserted_records,
        duplicate_records=summary.duplicate_records,
        rejected_records=summary.rejected_records,
        warning_records=summary.warning_records,
        schema_version=config.schema_version,
        schema_changes=summary.schema_changes,
        validation_failures=dict(summary.failure_counts),
        quarantine_path=summary.quarantine_path,
        status=summary.status,
    )


def run_incremental_updates(
    spark: "SparkSession",
    project_root: Path,
    datasets: tuple[str, ...] = TASK1_DATASET_ORDER,
) -> list[IncrementalRunResult]:
    """Apply selected releases and append reusable timing results for evaluation."""
    unknown = sorted(set(datasets) - set(DATASETS))
    if unknown:
        raise ValueError(f"Unknown datasets: {unknown}")
    results = [
        apply_incremental_update(spark, project_root, dataset_name)
        for dataset_name in datasets
    ]
    output_path = project_root / "artifacts" / "task1_incremental_results.jsonl"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("a", encoding="utf-8") as handle:
        for result in results:
            handle.write(json.dumps(asdict(result), sort_keys=True) + "\n")
    return results
