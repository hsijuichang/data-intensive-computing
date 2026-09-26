"""Incrementally enrich new taxi trips and merge them into the integrated table."""

from __future__ import annotations

import time
from pathlib import Path
from typing import TYPE_CHECKING

from data_intensive_computing.task2.config import TASK2_SCHEMA_VERSION
from data_intensive_computing.task2.models import IntegrationRefreshResult

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


def _require_delta(path: Path, label: str) -> None:
    if not (path / "_delta_log").exists():
        raise FileNotFoundError(f"Missing {label} Delta table: {path}")


def _require_columns(frame: "DataFrame", label: str, columns: set[str]) -> None:
    missing = sorted(columns - set(frame.columns))
    if missing:
        raise ValueError(f"{label} is missing required columns: {missing}")


def _candidate_trips(
    trips: "DataFrame",
    integrated: "DataFrame",
    taxi_run_id: str | None,
) -> tuple["DataFrame", "DataFrame"]:
    """Return selected Task 1 rows and the subset not already integrated."""
    from pyspark.sql import functions as F

    candidates = trips
    if taxi_run_id is not None:
        if "_ingestion_run_id" not in trips.columns:
            raise ValueError(
                "taxi_trips has no _ingestion_run_id column; cannot select a Task 1 run"
            )
        candidates = trips.filter(F.col("_ingestion_run_id") == taxi_run_id)

    existing_keys = integrated.select("trip_id").dropDuplicates()
    pending = candidates.join(existing_keys, "trip_id", "left_anti")
    return candidates, pending


def enrich_taxi_trips(
    spark: "SparkSession",
    project_root: Path,
    trips: "DataFrame",
) -> "DataFrame":
    """Add zone, exact-hour weather, and borough-hour air quality context."""
    from pyspark.sql import functions as F

    processed = project_root / "data" / "processed"
    weather_path = processed / "weather"
    air_quality_path = processed / "air_quality"
    zones_path = processed / "taxi_zones"
    for path, label in (
        (weather_path, "weather"),
        (air_quality_path, "air_quality"),
        (zones_path, "taxi_zones"),
    ):
        _require_delta(path, label)

    weather = spark.read.format("delta").load(str(weather_path))
    air_quality = spark.read.format("delta").load(str(air_quality_path))
    zones = spark.read.format("delta").load(str(zones_path))

    _require_columns(
        trips,
        "taxi_trips",
        {"trip_id", "pickup_hour", "pickup_location_id", "dropoff_location_id"},
    )
    _require_columns(
        weather,
        "weather",
        {"observation_timestamp", "temp", "rhum", "prcp", "wspd"},
    )
    _require_columns(
        air_quality,
        "air_quality",
        {
            "observation_timestamp",
            "state_code",
            "county_code",
            "parameter_code",
            "sample_measurement",
            "station_id",
            "units_of_measure",
        },
    )
    _require_columns(zones, "taxi_zones", {"location_id", "zone", "borough"})

    bounds = trips.agg(
        F.min("pickup_hour").alias("first_hour"),
        F.max("pickup_hour").alias("last_hour"),
    ).first()
    if bounds is None or bounds["first_hour"] is None:
        return trips

    first_hour = bounds["first_hour"]
    last_hour = bounds["last_hour"]
    weather = weather.filter(
        F.col("observation_timestamp").between(first_hour, last_hour)
    )
    air_quality = air_quality.filter(
        F.col("observation_timestamp").between(first_hour, last_hour)
    )

    pickup_zones = F.broadcast(
        zones.select(
            F.col("location_id").alias("pickup_zone_location_id"),
            F.col("zone").alias("pickup_zone"),
            F.col("borough").alias("pickup_borough"),
        )
    )
    dropoff_zones = F.broadcast(
        zones.select(
            F.col("location_id").alias("dropoff_zone_location_id"),
            F.col("zone").alias("dropoff_zone"),
            F.col("borough").alias("dropoff_borough"),
        )
    )
    with_zones = (
        trips.join(
            pickup_zones,
            trips.pickup_location_id == pickup_zones.pickup_zone_location_id,
            "left",
        )
        .drop("pickup_zone_location_id")
        .join(
            dropoff_zones,
            trips.dropoff_location_id == dropoff_zones.dropoff_zone_location_id,
            "left",
        )
        .drop("dropoff_zone_location_id")
    )

    evolved_humidity = (
        F.col("humidity") if "humidity" in weather.columns else F.lit(None).cast("double")
    )
    weather_hourly = F.broadcast(
        weather.select(
            F.col("observation_timestamp").alias("weather_observation_timestamp"),
            F.col("temp").alias("weather_temperature_c"),
            # Preserve the Week 2 integer type while exposing the evolved
            # decimal-valued source in a separate additive column.
            F.coalesce(evolved_humidity, F.col("rhum").cast("double"))
            .cast("int")
            .alias("weather_relative_humidity_pct"),
            evolved_humidity.alias("weather_evolved_humidity_pct"),
            F.col("prcp").alias("weather_precipitation_mm"),
            F.col("snwd").alias("weather_snow_depth_mm"),
            F.col("wdir").alias("weather_wind_direction_deg"),
            F.col("wspd").alias("weather_wind_speed_kmh"),
            F.col("wpgt").alias("weather_wind_gust_kmh"),
            F.col("pres").alias("weather_pressure_hpa"),
            F.col("cldc").alias("weather_cloud_cover_code"),
            F.col("coco").alias("weather_condition_code"),
        )
    )
    with_weather = (
        with_zones.join(
            weather_hourly,
            with_zones.pickup_hour == weather_hourly.weather_observation_timestamp,
            "left",
        ).withColumn(
            "weather_match_status",
            F.when(
                F.col("weather_observation_timestamp").isNotNull(), F.lit("exact_hour")
            ).otherwise(F.lit("missing")),
        )
    )

    nyc_counties = spark.createDataFrame(
        [
            ("005", "Bronx"),
            ("047", "Brooklyn"),
            ("061", "Manhattan"),
            ("081", "Queens"),
            ("085", "Staten Island"),
        ],
        ["county_code", "air_quality_borough"],
    )
    nyc_air_quality = (
        air_quality.filter(
            (F.col("state_code") == "36") & (F.col("parameter_code") == "88101")
        ).join(F.broadcast(nyc_counties), "county_code", "inner")
    )
    aqi_value = F.col("aqi") if "aqi" in air_quality.columns else F.lit(None).cast("int")
    air_quality_hourly = (
        nyc_air_quality.groupBy("air_quality_borough", "observation_timestamp")
        .agg(
            F.avg("sample_measurement").alias("air_quality_pm25_mean"),
            F.min("sample_measurement").alias("air_quality_pm25_min"),
            F.max("sample_measurement").alias("air_quality_pm25_max"),
            F.count("sample_measurement").alias("air_quality_observation_count"),
            F.countDistinct("station_id").alias("air_quality_station_count"),
            F.first("units_of_measure", ignorenulls=True).alias("air_quality_units"),
            F.avg(aqi_value).alias("air_quality_aqi_mean"),
        )
        .withColumnRenamed(
            "observation_timestamp", "air_quality_observation_timestamp"
        )
    )

    return (
        with_weather.join(
            F.broadcast(air_quality_hourly),
            (with_weather.pickup_borough == air_quality_hourly.air_quality_borough)
            & (
                with_weather.pickup_hour
                == air_quality_hourly.air_quality_observation_timestamp
            ),
            "left",
        )
        .drop("air_quality_borough")
        .withColumn(
            "air_quality_match_status",
            F.when(
                F.col("air_quality_observation_timestamp").isNotNull(),
                F.lit("borough_exact_hour"),
            ).otherwise(F.lit("missing")),
        )
        .withColumn("_integration_strategy_version", F.lit(TASK2_SCHEMA_VERSION))
        .withColumn("_integrated_at", F.current_timestamp())
    )


def update_integrated_taxi_trips(
    spark: "SparkSession",
    project_root: Path,
    *,
    taxi_run_id: str | None = None,
) -> IntegrationRefreshResult:
    """Enrich pending Task 1 taxi rows and insert them idempotently."""
    from delta.tables import DeltaTable
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    started = time.perf_counter()
    processed = project_root / "data" / "processed"
    taxi_path = processed / "taxi_trips"
    integrated_path = processed / "integrated_taxi_trips"
    _require_delta(taxi_path, "taxi_trips")
    _require_delta(integrated_path, "integrated_taxi_trips")

    trips = spark.read.format("delta").load(str(taxi_path))
    integrated = spark.read.format("delta").load(str(integrated_path))
    candidates, pending = _candidate_trips(trips, integrated, taxi_run_id)
    candidate_records = candidates.count()
    pending = pending.persist(StorageLevel.MEMORY_AND_DISK)
    pending_records = pending.count()
    if pending_records == 0:
        pending.unpersist()
        return IntegrationRefreshResult(
            source_taxi_run_id=taxi_run_id,
            candidate_records=candidate_records,
            pending_records=0,
            integrated_records=0,
            affected_periods=(),
            weather_match_rate=None,
            air_quality_match_rate=None,
            execution_time_seconds=round(time.perf_counter() - started, 4),
            status="SKIPPED",
        )

    enriched = enrich_taxi_trips(spark, project_root, pending).persist(
        StorageLevel.MEMORY_AND_DISK
    )
    quality = enriched.agg(
        F.count("*").alias("rows"),
        F.avg((F.col("weather_match_status") == "exact_hour").cast("double")).alias(
            "weather_match_rate"
        ),
        F.avg(
            (F.col("air_quality_match_status") == "borough_exact_hour").cast("double")
        ).alias("air_quality_match_rate"),
    ).first()
    integrated_records = int(quality["rows"])
    if integrated_records != pending_records:
        raise AssertionError(
            "Integration changed trip cardinality: "
            f"pending={pending_records}, enriched={integrated_records}"
        )

    affected_periods = tuple(
        sorted(
            (int(row["pickup_year"]), int(row["pickup_month"]))
            for row in enriched.select("pickup_year", "pickup_month")
            .where(F.col("pickup_year").isNotNull() & F.col("pickup_month").isNotNull())
            .distinct()
            .collect()
        )
    )
    (
        DeltaTable.forPath(spark, str(integrated_path))
        .alias("target")
        .merge(enriched.alias("updates"), "target.trip_id = updates.trip_id")
        .withSchemaEvolution()
        .whenNotMatchedInsertAll()
        .execute()
    )
    enriched.unpersist()
    pending.unpersist()
    return IntegrationRefreshResult(
        source_taxi_run_id=taxi_run_id,
        candidate_records=candidate_records,
        pending_records=pending_records,
        integrated_records=integrated_records,
        affected_periods=affected_periods,
        weather_match_rate=(
            round(float(quality["weather_match_rate"]), 6)
            if quality["weather_match_rate"] is not None
            else None
        ),
        air_quality_match_rate=(
            round(float(quality["air_quality_match_rate"]), 6)
            if quality["air_quality_match_rate"] is not None
            else None
        ),
        execution_time_seconds=round(time.perf_counter() - started, 4),
        status="SUCCESS",
    )
