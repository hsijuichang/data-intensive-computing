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
    schema_version: str
    validation_failures: dict[str, int]
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


def _add_error(errors: "Column", condition: "Column", code: str) -> "Column":
    from pyspark.sql import functions as F

    return F.when(
        condition,
        F.array_union(errors, F.array(F.lit(code))),
    ).otherwise(errors)


def _transform_taxi(frame: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F

    frame = (
        _standardize_columns(frame)
        .withColumnRenamed("pu_location_id", "pickup_location_id")
        .withColumnRenamed("do_location_id", "dropoff_location_id")
        .withColumnRenamed("ratecode_id", "rate_code_id")
        .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
        .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
        .withColumn("vendor_id", F.col("vendor_id").cast("long"))
        .withColumn("pickup_datetime", F.col("pickup_datetime").cast("timestamp"))
        .withColumn("dropoff_datetime", F.col("dropoff_datetime").cast("timestamp"))
        .withColumn("passenger_count", F.col("passenger_count").cast("int"))
        .withColumn("rate_code_id", F.col("rate_code_id").cast("long"))
        .withColumn("pickup_location_id", F.col("pickup_location_id").cast("int"))
        .withColumn("dropoff_location_id", F.col("dropoff_location_id").cast("int"))
        .withColumn("payment_type", F.col("payment_type").cast("long"))
    )
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
    return (
        frame.withColumn("trip_id", F.when(_all_present(key), _stable_hash(key)))
        .withColumn(
            "trip_duration_minutes",
            F.expr("timestampdiff(SECOND, pickup_datetime, dropoff_datetime)") / F.lit(60.0),
        )
        .withColumn("pickup_date", F.to_date("pickup_datetime"))
        .withColumn("pickup_hour", F.date_trunc("hour", "pickup_datetime"))
        .withColumn("pickup_year", F.year("pickup_datetime").cast("int"))
        .withColumn("pickup_month", F.month("pickup_datetime").cast("int"))
    )


def _transform_weather(frame: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F

    frame = _standardize_columns(frame)
    for name in ("year", "month", "day", "hour", "rhum", "wdir", "cldc", "coco"):
        frame = frame.withColumn(name, F.col(name).cast("int"))
    for name in ("temp", "prcp", "snwd", "wspd", "wpgt", "pres", "humidity"):
        frame = frame.withColumn(name, F.col(name).cast("double"))
    return (
        frame.withColumn(
            "observation_timestamp",
            F.make_timestamp("year", "month", "day", "hour", F.lit(0), F.lit(0)),
        )
        .withColumn("observation_date", F.to_date("observation_timestamp"))
    )


def _transform_air_quality(frame: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F

    frame = _standardize_columns(frame)
    for name in ("latitude", "longitude", "sample_measurement", "mdl", "uncertainty"):
        frame = frame.withColumn(name, F.col(name).cast("double"))
    frame = (
        frame.withColumn("aqi", F.col("aqi").cast("int"))
        .withColumn(
            "observation_timestamp",
            F.to_timestamp(F.concat_ws(" ", "date_local", "time_local"), "yyyy-MM-dd HH:mm"),
        )
        .withColumn(
            "observation_timestamp_utc",
            F.to_timestamp(F.concat_ws(" ", "date_gmt", "time_gmt"), "yyyy-MM-dd HH:mm"),
        )
        .withColumn("date_of_last_change", F.to_date("date_of_last_change", "yyyy-MM-dd"))
        .withColumn(
            "station_id",
            F.concat_ws("-", "state_code", "county_code", "site_num", "parameter_code", "poc"),
        )
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
    return (
        frame.withColumn("observation_id", F.when(_all_present(key), _stable_hash(key)))
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


def _validate_update_schema(frame: "DataFrame", config: DatasetConfig) -> None:
    expected = set(config.raw_columns) | set(config.evolved_columns)
    actual = set(frame.columns)
    missing = sorted(expected - actual)
    unexpected = sorted(actual - expected)
    if missing or unexpected:
        raise ValueError(
            json.dumps(
                {
                    "dataset": config.name,
                    "missing_columns": missing,
                    "unexpected_columns": unexpected,
                    "supported_schema_changes": list(config.evolved_columns),
                },
                indent=2,
            )
        )


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
    _validate_update_schema(frame, config)
    return frame


def _validation_rules(dataset_name: str) -> list[tuple[str, "Column"]]:
    from pyspark.sql import functions as F

    if dataset_name == "taxi_trips":
        return [
            ("dropoff_before_pickup", F.col("dropoff_datetime") < F.col("pickup_datetime")),
            ("negative_trip_distance", F.col("trip_distance") < 0),
            ("negative_passenger_count", F.col("passenger_count") < 0),
            ("negative_fare_amount", F.col("fare_amount") < 0),
            ("negative_total_amount", F.col("total_amount") < 0),
            ("invalid_trip_distance", F.isnan("trip_distance")),
            ("invalid_total_amount", F.isnan("total_amount")),
        ]
    if dataset_name == "weather":
        return [
            ("invalid_relative_humidity", ~F.col("rhum").between(0, 100)),
            ("invalid_humidity", ~F.col("humidity").between(20, 100)),
            ("negative_precipitation", F.col("prcp") < 0),
            ("invalid_wind_direction", ~F.col("wdir").between(0, 360)),
            ("negative_wind_speed", F.col("wspd") < 0),
            ("invalid_pressure", ~F.col("pres").between(850, 1100)),
        ]
    if dataset_name == "air_quality":
        return [
            ("invalid_latitude", ~F.col("latitude").between(-90, 90)),
            ("invalid_longitude", ~F.col("longitude").between(-180, 180)),
            ("negative_sample_measurement", F.col("sample_measurement") < 0),
            ("invalid_sample_measurement", F.isnan("sample_measurement")),
            ("invalid_aqi", ~F.col("aqi").between(0, 500)),
        ]
    raise KeyError(f"No validation rules for {dataset_name}")


def _with_validation_errors(
    frame: "DataFrame",
    config: DatasetConfig,
) -> "DataFrame":
    from pyspark.sql import functions as F

    errors = F.expr("array()").cast("array<string>")
    for name in config.required_columns:
        missing = F.col(name).isNull() | (F.trim(F.col(name).cast("string")) == "")
        errors = _add_error(errors, missing, f"missing_required:{name}")
    for code, condition in _validation_rules(config.name):
        errors = _add_error(errors, F.coalesce(condition, F.lit(False)), code)
    return frame.withColumn("_validation_errors", errors)


def _validation_failure_counts(invalid: "DataFrame") -> dict[str, int]:
    from pyspark.sql import functions as F

    return {
        row["error"]: row["count"]
        for row in (
            invalid.select(F.explode("_validation_errors").alias("error"))
            .groupBy("error")
            .count()
            .collect()
        )
    }


def _append_quarantine(frame: "DataFrame", path: Path) -> None:
    if frame.limit(1).count() == 0:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    (
        frame.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(str(path))
    )


def apply_incremental_update(
    spark: "SparkSession",
    project_root: Path,
    dataset_name: str,
) -> IncrementalRunResult:
    """Validate one release, quarantine bad/duplicate rows, and insert new rows."""
    from delta.tables import DeltaTable
    from pyspark.sql import Window, functions as F

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
    # Cut the long cast/hash/validation lineage before it is reused by counts,
    # quarantine writes, joins, and MERGE. This avoids oversized generated
    # code and keeps later evaluation timings focused on incremental work.
    validated = _with_validation_errors(transformed, config).localCheckpoint(
        eager=True
    )

    invalid = validated.filter(F.size("_validation_errors") > 0)
    validation_failures = _validation_failure_counts(invalid)
    rejected_records = invalid.count()
    valid = validated.filter(F.size("_validation_errors") == 0).drop("_validation_errors")

    duplicate_window = Window.partitionBy(*config.primary_key).orderBy(
        F.col("_source_file"),
        F.col("_ingested_at"),
    )
    ranked = valid.withColumn("_duplicate_rank", F.row_number().over(duplicate_window))
    batch_duplicates = (
        ranked.filter(F.col("_duplicate_rank") > 1)
        .drop("_duplicate_rank")
        .withColumn("_validation_errors", F.array(F.lit("duplicate_within_update")))
    )
    candidates = ranked.filter(F.col("_duplicate_rank") == 1).drop("_duplicate_rank")

    target_keys = (
        spark.read.format("delta")
        .load(str(target_path))
        .select(*config.primary_key)
        .dropDuplicates()
    )
    existing_duplicates = (
        candidates.join(target_keys, list(config.primary_key), "left_semi")
        .withColumn("_validation_errors", F.array(F.lit("duplicate_existing_record")))
    )
    inserts = candidates.join(
        target_keys,
        list(config.primary_key),
        "left_anti",
    ).localCheckpoint(eager=True)
    inserted_records = inserts.count()
    batch_duplicate_count = batch_duplicates.count()
    existing_duplicate_count = existing_duplicates.count()
    duplicate_records = batch_duplicate_count + existing_duplicate_count

    quarantine = invalid.unionByName(batch_duplicates, allowMissingColumns=True).unionByName(
        existing_duplicates,
        allowMissingColumns=True,
    )
    _append_quarantine(quarantine, config.failed_path(project_root))

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

    validated.unpersist()
    inserts.unpersist()
    completed_at = datetime.now(timezone.utc)
    elapsed = time.perf_counter() - started_clock
    if processed_records != inserted_records + duplicate_records + rejected_records:
        raise AssertionError(
            "Incremental accounting mismatch: "
            f"{processed_records} != {inserted_records} + {duplicate_records} + {rejected_records}"
        )
    return IncrementalRunResult(
        run_id=run_id,
        dataset_name=dataset_name,
        started_at=started_at.isoformat(),
        completed_at=completed_at.isoformat(),
        execution_time_seconds=round(elapsed, 4),
        processed_records=processed_records,
        inserted_records=inserted_records,
        duplicate_records=duplicate_records,
        rejected_records=rejected_records,
        schema_version=config.schema_version,
        validation_failures=validation_failures,
        status="SUCCESS",
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
