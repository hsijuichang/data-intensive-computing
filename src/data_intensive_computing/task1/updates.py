"""Generate realistic incremental releases for the three municipal datasets."""

from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from data_intensive_computing.task1.config import AIR_RAW_COLUMNS, DATASETS

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame, SparkSession


@dataclass(frozen=True)
class UpdateGenerationResult:
    dataset_name: str
    output_path: str
    original_records: int
    new_records: int
    duplicate_records: int
    total_update_records: int
    schema_changes: tuple[str, ...]


def _require_fraction(name: str, value: float, minimum: float, maximum: float) -> None:
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} must be between {minimum:.2%} and {maximum:.2%}; got {value:.2%}")


def _replace_output(path: Path) -> None:
    """Spark overwrite handles existing data; this only creates the parent."""
    path.parent.mkdir(parents=True, exist_ok=True)


def _clamp(column: "Column", minimum: float, maximum: float) -> "Column":
    from pyspark.sql import functions as F

    return F.least(F.lit(maximum), F.greatest(F.lit(minimum), column))


def _exact_random_subset(
    frame: "DataFrame",
    target_records: int,
    approximate_fraction: float,
    seed: int,
) -> "DataFrame":
    """Avoid a full random sort in the normal case while returning an exact count."""
    from pyspark.sql import functions as F

    sampled = (
        frame.sample(
            withReplacement=False,
            fraction=min(1.0, approximate_fraction * 1.20),
            seed=seed,
        )
        .limit(target_records)
        .cache()
    )
    if sampled.count() == target_records:
        return sampled
    sampled.unpersist()
    return frame.orderBy(F.rand(seed)).limit(target_records).cache()


def pm25_aqi(concentration: "Column") -> "Column":
    """Return the US EPA PM2.5 AQI interpolation, capped to the 0–500 range."""
    from pyspark.sql import functions as F

    concentration = F.floor(_clamp(concentration, 0.0, 500.4) * F.lit(10.0)) / F.lit(10.0)
    breakpoints = (
        (0.0, 9.0, 0, 50),
        (9.1, 35.4, 51, 100),
        (35.5, 55.4, 101, 150),
        (55.5, 125.4, 151, 200),
        (125.5, 225.4, 201, 300),
        (225.5, 325.4, 301, 500),
    )
    result = None
    for low_c, high_c, low_i, high_i in breakpoints:
        interpolated = F.round(
            (F.lit(high_i - low_i) / F.lit(high_c - low_c))
            * (concentration - F.lit(low_c))
            + F.lit(low_i)
        )
        condition = (concentration >= F.lit(low_c)) & (
            concentration <= F.lit(high_c)
        )
        result = (
            F.when(condition, interpolated)
            if result is None
            else result.when(condition, interpolated)
        )
    return _clamp(result.otherwise(F.lit(500.0)), 0.0, 500.0).cast("int")


def generate_taxi_update(
    spark: "SparkSession",
    project_root: Path,
    *,
    new_fraction: float = 0.05,
    duplicate_fraction: float = 0.01,
    seed: int = 42,
) -> UpdateGenerationResult:
    """Generate shifted taxi trips plus exact copies that exercise deduplication."""
    from pyspark.sql import functions as F

    _require_fraction("new_fraction", new_fraction, 0.05, 0.10)
    _require_fraction("duplicate_fraction", duplicate_fraction, 0.01, 0.02)
    config = DATASETS["taxi_trips"]
    source = spark.read.parquet(str(config.source_path(project_root))).select(*config.raw_columns)
    original_records = source.count()
    if original_records == 0:
        raise ValueError("Cannot generate taxi updates from an empty source")

    new_records = max(1, math.ceil(original_records * new_fraction))
    duplicate_records = max(1, math.ceil(original_records * duplicate_fraction))
    latest_pickup_epoch = source.agg(
        F.max(F.unix_timestamp("tpep_pickup_datetime"))
    ).first()[0]
    if latest_pickup_epoch is None:
        raise ValueError("Taxi source contains no pickup timestamps")

    valid_source = source.filter(
        F.col("tpep_pickup_datetime").isNotNull()
        & F.col("tpep_dropoff_datetime").isNotNull()
        & (F.col("tpep_dropoff_datetime") >= F.col("tpep_pickup_datetime"))
        & F.col("PULocationID").isNotNull()
        & F.col("DOLocationID").isNotNull()
        & (F.col("trip_distance") >= 0)
        & ~F.isnan("trip_distance")
        & (F.col("fare_amount") >= 0)
        & (F.col("total_amount") >= 0)
        & ~F.isnan("total_amount")
        & (F.col("passenger_count").isNull() | (F.col("passenger_count") >= 0))
    )
    if valid_source.limit(new_records).count() < new_records:
        raise ValueError("Taxi source does not contain enough valid rows for the requested update")

    selected = _exact_random_subset(valid_source, new_records, new_fraction, seed)
    selected_min_epoch = selected.agg(
        F.min(F.unix_timestamp("tpep_pickup_datetime"))
    ).first()[0]
    shift_seconds = latest_pickup_epoch - selected_min_epoch + 1
    new_trips = (
        selected.withColumn(
            "tpep_pickup_datetime",
            F.timestamp_seconds(
                F.unix_timestamp("tpep_pickup_datetime") + F.lit(shift_seconds)
            ),
        )
        .withColumn(
            "tpep_dropoff_datetime",
            F.timestamp_seconds(
                F.unix_timestamp("tpep_dropoff_datetime") + F.lit(shift_seconds)
            ),
        )
    )
    duplicates = _exact_random_subset(
        valid_source,
        duplicate_records,
        duplicate_fraction,
        seed + 2,
    )
    update = new_trips.unionByName(duplicates).select(*config.raw_columns)

    output_path = config.update_path(project_root)
    _replace_output(output_path)
    update.write.mode("overwrite").parquet(str(output_path))
    selected.unpersist()
    duplicates.unpersist()
    return UpdateGenerationResult(
        dataset_name=config.name,
        output_path=str(output_path),
        original_records=original_records,
        new_records=new_records,
        duplicate_records=duplicate_records,
        total_update_records=new_records + duplicate_records,
        schema_changes=(),
    )


def generate_weather_update(
    spark: "SparkSession",
    project_root: Path,
    *,
    hours: int = 168,
) -> UpdateGenerationResult:
    """Generate the next consecutive weather hours and add numeric humidity."""
    from pyspark.sql import functions as F

    if hours <= 0:
        raise ValueError("hours must be positive")
    config = DATASETS["weather"]
    source = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(str(config.source_path(project_root)))
    )
    original_records = source.count()
    observed = source.withColumn(
        "_timestamp",
        F.make_timestamp("year", "month", "day", "hour", F.lit(0), F.lit(0)),
    )
    template = (
        observed.filter(
            F.col("_timestamp").isNotNull()
            & F.col("temp").isNotNull()
            & F.col("rhum").between(0, 100)
            & (F.col("prcp") >= 0)
            & F.col("wdir").between(0, 360)
            & (F.col("wspd") >= 0)
            & F.col("pres").between(850, 1100)
        )
        .orderBy(F.col("_timestamp").desc())
        .limit(1)
    )
    latest_epoch = observed.agg(F.max(F.unix_timestamp("_timestamp"))).first()[0]
    if latest_epoch is None or template.count() == 0:
        raise ValueError("Weather source has no valid timestamped template row")

    generated = template.crossJoin(spark.range(hours).withColumnRenamed("id", "_hour_index"))
    timestamp = F.timestamp_seconds(
        F.lit(latest_epoch) + (F.col("_hour_index") + F.lit(1)) * F.lit(3600)
    )
    wave = F.sin(F.col("_hour_index") * F.lit(math.pi / 12.0))
    generated = (
        generated.withColumn("_timestamp", timestamp)
        .withColumn("year", F.year(timestamp))
        .withColumn("month", F.month(timestamp))
        .withColumn("day", F.dayofmonth(timestamp))
        .withColumn("hour", F.hour(timestamp))
        .withColumn("temp", F.round(F.col("temp").cast("double") + wave * F.lit(2.5), 1))
        .withColumn("rhum", F.round(_clamp(F.col("rhum").cast("double") - wave * F.lit(7.0), 20, 100)).cast("int"))
        .withColumn("humidity", F.round(_clamp(F.col("rhum").cast("double") + wave * F.lit(3.0), 20, 100), 1))
        .withColumn("prcp", F.greatest(F.lit(0.0), F.col("prcp").cast("double")))
        .withColumn("wdir", F.pmod(F.col("wdir").cast("int") + F.col("_hour_index") * F.lit(7), F.lit(361)).cast("int"))
        .withColumn("wspd", F.round(F.greatest(F.lit(0.0), F.col("wspd").cast("double") + wave), 1))
        .withColumn("pres", F.round(_clamp(F.col("pres").cast("double") + wave * F.lit(1.5), 850, 1100), 1))
        .select(*config.raw_columns, *config.evolved_columns)
    )

    output_path = config.update_path(project_root)
    _replace_output(output_path)
    generated.coalesce(1).write.mode("overwrite").option("header", True).csv(str(output_path))
    return UpdateGenerationResult(
        dataset_name=config.name,
        output_path=str(output_path),
        original_records=original_records,
        new_records=hours,
        duplicate_records=0,
        total_update_records=hours,
        schema_changes=("added humidity: numeric relative humidity percentage (20–100)",),
    )


def generate_air_quality_update(
    spark: "SparkSession",
    project_root: Path,
    *,
    hours: int = 168,
) -> UpdateGenerationResult:
    """Generate the next hours for every observed monitor and add PM2.5 AQI."""
    from pyspark.sql import Window, functions as F

    if hours <= 0:
        raise ValueError("hours must be positive")
    config = DATASETS["air_quality"]
    source = spark.read.option("header", True).option("inferSchema", False).csv(
        str(config.source_path(project_root))
    )
    original_records = source.count()
    local_timestamp = F.to_timestamp(
        F.concat_ws(" ", F.col("Date Local"), F.col("Time Local")),
        "yyyy-MM-dd HH:mm",
    )
    utc_timestamp = F.to_timestamp(
        F.concat_ws(" ", F.col("Date GMT"), F.col("Time GMT")),
        "yyyy-MM-dd HH:mm",
    )
    prepared = (
        source.withColumn("_local_timestamp", local_timestamp)
        .withColumn("_utc_timestamp", utc_timestamp)
        .withColumn("_measurement", F.col("Sample Measurement").cast("double"))
    )
    latest_epoch = prepared.agg(
        F.max(F.unix_timestamp("_local_timestamp"))
    ).first()[0]
    if latest_epoch is None:
        raise ValueError("Air-quality source contains no local timestamps")

    monitor_key = [
        "State Code",
        "County Code",
        "Site Num",
        "Parameter Code",
        "POC",
        "Method Code",
    ]
    templates = (
        prepared.filter(
            F.col("_local_timestamp").isNotNull()
            & F.col("_utc_timestamp").isNotNull()
            & F.col("_measurement").isNotNull()
            & ~F.isnan("_measurement")
            & (F.col("_measurement") >= 0)
        )
        .withColumn(
            "_template_rank",
            F.row_number().over(
                Window.partitionBy(*monitor_key).orderBy(F.col("_local_timestamp").desc())
            ),
        )
        .filter(F.col("_template_rank") == 1)
        .drop("_template_rank")
    )
    monitor_count = templates.count()
    if monitor_count == 0:
        raise ValueError("Air-quality source has no valid monitor template rows")

    generated = templates.crossJoin(spark.range(hours).withColumnRenamed("id", "_hour_index"))
    new_local = F.timestamp_seconds(
        F.lit(latest_epoch) + (F.col("_hour_index") + F.lit(1)) * F.lit(3600)
    )
    utc_offset = F.unix_timestamp("_utc_timestamp") - F.unix_timestamp("_local_timestamp")
    new_utc = F.timestamp_seconds(F.unix_timestamp(new_local) + utc_offset)
    wave = F.sin(F.col("_hour_index") * F.lit(math.pi / 12.0))
    measurement = F.round(
        F.greatest(F.lit(0.0), F.col("_measurement") * (F.lit(1.0) + wave * F.lit(0.08))),
        1,
    )
    generated = (
        generated.withColumn("Date Local", F.date_format(new_local, "yyyy-MM-dd"))
        .withColumn("Time Local", F.date_format(new_local, "HH:mm"))
        .withColumn("Date GMT", F.date_format(new_utc, "yyyy-MM-dd"))
        .withColumn("Time GMT", F.date_format(new_utc, "HH:mm"))
        .withColumn("Sample Measurement", F.format_string("%.1f", measurement))
        .withColumn("Date of Last Change", F.date_format(new_local, "yyyy-MM-dd"))
        .withColumn("aqi", pm25_aqi(measurement))
        .select(*AIR_RAW_COLUMNS, "aqi")
    )

    output_path = config.update_path(project_root)
    _replace_output(output_path)
    generated.coalesce(1).write.mode("overwrite").option("header", True).csv(str(output_path))
    new_records = monitor_count * hours
    return UpdateGenerationResult(
        dataset_name=config.name,
        output_path=str(output_path),
        original_records=original_records,
        new_records=new_records,
        duplicate_records=0,
        total_update_records=new_records,
        schema_changes=("added aqi: integer US EPA Air Quality Index (0–500)",),
    )


def generate_all_updates(
    spark: "SparkSession",
    project_root: Path,
    *,
    taxi_fraction: float = 0.05,
    taxi_duplicate_fraction: float = 0.01,
    hours: int = 168,
    seed: int = 42,
) -> list[UpdateGenerationResult]:
    """Generate all Task 1 releases and save a machine-readable manifest."""
    results = [
        generate_taxi_update(
            spark,
            project_root,
            new_fraction=taxi_fraction,
            duplicate_fraction=taxi_duplicate_fraction,
            seed=seed,
        ),
        generate_weather_update(spark, project_root, hours=hours),
        generate_air_quality_update(spark, project_root, hours=hours),
    ]
    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "parameters": {
            "taxi_fraction": taxi_fraction,
            "taxi_duplicate_fraction": taxi_duplicate_fraction,
            "hours": hours,
            "seed": seed,
        },
        "datasets": [asdict(result) for result in results],
    }
    manifest_path = project_root / "artifacts" / "task1_update_manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return results
