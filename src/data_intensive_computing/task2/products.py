"""Incremental builders and Delta publishers for Week 2 analytical products."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Callable

from data_intensive_computing.task2.config import (
    PRODUCTS,
    TASK2_SCHEMA_VERSION,
    ProductConfig,
)
from data_intensive_computing.task2.models import ProductRefreshResult, YearMonth

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


AIRPORT_TRANSFER_SQL = """
WITH airport_trips AS (
    SELECT pickup_year, pickup_month,
        CASE
            WHEN pickup_location_id IN (1, 132, 138)
             AND dropoff_location_id IN (1, 132, 138) THEN 'airport_to_airport'
            WHEN pickup_location_id IN (1, 132, 138) THEN 'from_airport'
            WHEN dropoff_location_id IN (1, 132, 138) THEN 'to_airport'
        END AS direction,
        CASE WHEN pickup_location_id IN (1, 132, 138)
             THEN pickup_location_id ELSE dropoff_location_id END AS airport_location_id,
        CASE WHEN pickup_location_id IN (1, 132, 138)
             THEN pickup_zone ELSE dropoff_zone END AS airport_zone,
        CASE WHEN pickup_location_id IN (1, 132, 138)
             THEN dropoff_location_id ELSE pickup_location_id END AS counterpart_location_id,
        CASE WHEN pickup_location_id IN (1, 132, 138)
             THEN dropoff_zone ELSE pickup_zone END AS counterpart_zone,
        CASE WHEN pickup_location_id IN (1, 132, 138)
             THEN dropoff_borough ELSE pickup_borough END AS counterpart_borough,
        trip_id, trip_distance, trip_duration_minutes, total_amount
    FROM _task2_affected_integrated
    WHERE trip_id IS NOT NULL
      AND (pickup_location_id IN (1, 132, 138)
        OR dropoff_location_id IN (1, 132, 138))
)
SELECT pickup_year, pickup_month, direction, airport_location_id,
    COALESCE(airport_zone, 'Unknown') AS airport_zone,
    counterpart_location_id,
    COALESCE(counterpart_zone, 'Unknown') AS counterpart_zone,
    COALESCE(counterpart_borough, 'Unknown') AS counterpart_borough,
    COUNT(trip_id) AS trip_count,
    ROUND(AVG(trip_distance), 3) AS avg_trip_distance,
    ROUND(AVG(trip_duration_minutes), 3) AS avg_trip_duration_minutes,
    ROUND(AVG(total_amount), 2) AS avg_total_amount
FROM airport_trips
GROUP BY pickup_year, pickup_month, direction, airport_location_id,
    COALESCE(airport_zone, 'Unknown'), counterpart_location_id,
    COALESCE(counterpart_zone, 'Unknown'),
    COALESCE(counterpart_borough, 'Unknown')
"""

FARE_AND_TIP_SQL = """
WITH grouped AS (
    SELECT pickup_year, pickup_month,
        COALESCE(pickup_borough, 'Unknown') AS pickup_borough,
        payment_type, COUNT(trip_id) AS trip_count,
        COUNT(tip_amount) AS trips_with_recorded_tip_value,
        ROUND(AVG(fare_amount), 2) AS avg_base_fare,
        ROUND(AVG(total_amount), 2) AS avg_total_amount,
        ROUND(AVG(tip_amount), 2) AS avg_recorded_tip_amount,
        SUM(CASE WHEN tip_amount > 0 THEN 1 ELSE 0 END)
            AS trips_with_positive_recorded_tip,
        SUM(CASE WHEN fare_amount > 0 AND tip_amount >= 0 THEN tip_amount END)
            AS eligible_recorded_tip_total,
        SUM(CASE WHEN fare_amount > 0 AND tip_amount >= 0 THEN fare_amount END)
            AS eligible_base_fare_total
    FROM _task2_affected_integrated
    WHERE trip_id IS NOT NULL
    GROUP BY pickup_year, pickup_month,
        COALESCE(pickup_borough, 'Unknown'), payment_type
)
SELECT pickup_year, pickup_month, pickup_borough, payment_type,
    trip_count, trips_with_recorded_tip_value,
    avg_base_fare, avg_total_amount, avg_recorded_tip_amount,
    trips_with_positive_recorded_tip,
    ROUND(100.0 * trips_with_positive_recorded_tip / NULLIF(trip_count, 0), 2)
        AS positive_recorded_tip_share_pct,
    ROUND(100.0 * eligible_recorded_tip_total
          / NULLIF(eligible_base_fare_total, 0), 2)
        AS recorded_tip_pct_of_base_fare
FROM grouped
"""

ORIGIN_DESTINATION_SQL = """
SELECT pickup_year, pickup_month, pickup_location_id,
    COALESCE(pickup_zone, 'Unknown') AS pickup_zone,
    COALESCE(pickup_borough, 'Unknown') AS pickup_borough,
    dropoff_location_id,
    COALESCE(dropoff_zone, 'Unknown') AS dropoff_zone,
    COALESCE(dropoff_borough, 'Unknown') AS dropoff_borough,
    COUNT(trip_id) AS trip_count,
    ROUND(AVG(trip_distance), 3) AS avg_trip_distance,
    ROUND(AVG(trip_duration_minutes), 3) AS avg_trip_duration_minutes,
    ROUND(AVG(total_amount), 2) AS avg_total_amount
FROM _task2_affected_integrated
WHERE trip_id IS NOT NULL
GROUP BY pickup_year, pickup_month, pickup_location_id,
    COALESCE(pickup_zone, 'Unknown'), COALESCE(pickup_borough, 'Unknown'),
    dropoff_location_id, COALESCE(dropoff_zone, 'Unknown'),
    COALESCE(dropoff_borough, 'Unknown')
"""

TRIP_DISTANCE_SQL = """
WITH classified AS (
    SELECT pickup_year, pickup_month,
        COALESCE(pickup_borough, 'Unknown') AS pickup_borough,
        trip_id, trip_distance, fare_amount, trip_duration_minutes,
        CASE
            WHEN trip_distance = 0 THEN '00_zero'
            WHEN trip_distance < 1 THEN '01_under_1'
            WHEN trip_distance < 3 THEN '02_1_to_under_3'
            WHEN trip_distance < 10 THEN '03_3_to_under_10'
            ELSE '04_10_or_more'
        END AS distance_band
    FROM _task2_affected_integrated
    WHERE trip_id IS NOT NULL AND trip_distance IS NOT NULL AND trip_distance >= 0
), band_summary AS (
    SELECT pickup_year, pickup_month, pickup_borough, distance_band,
        COUNT(trip_id) AS trip_count,
        ROUND(AVG(trip_distance), 3) AS avg_trip_distance,
        ROUND(AVG(fare_amount), 2) AS avg_base_fare,
        ROUND(AVG(trip_duration_minutes), 2) AS avg_trip_duration_minutes
    FROM classified
    GROUP BY pickup_year, pickup_month, pickup_borough, distance_band
)
SELECT pickup_year, pickup_month, pickup_borough, distance_band, trip_count,
    ROUND(100.0 * trip_count / NULLIF(SUM(trip_count) OVER (
        PARTITION BY pickup_year, pickup_month, pickup_borough
    ), 0), 2) AS trip_share_pct,
    avg_trip_distance, avg_base_fare, avg_trip_duration_minutes
FROM band_summary
"""

WEATHER_SENSITIVE_SQL = """
WITH classified_hours AS (
    SELECT DISTINCT weather_observation_timestamp AS weather_hour,
        YEAR(weather_observation_timestamp) AS pickup_year,
        MONTH(weather_observation_timestamp) AS pickup_month,
        CASE
            WHEN weather_condition_code IN (1, 2) THEN 'clear_or_fair'
            WHEN weather_condition_code IN (3, 4) THEN 'cloudy'
            WHEN weather_condition_code IN (5, 6) THEN 'fog'
            WHEN weather_condition_code IN (7, 8, 9, 10, 11, 17, 18) THEN 'rain'
            WHEN weather_condition_code IN (12, 13, 14, 15, 16, 19, 20, 21, 22)
                THEN 'snow_or_sleet'
            WHEN weather_condition_code BETWEEN 23 AND 27 THEN 'storm'
            ELSE 'unknown'
        END AS weather_condition
    FROM _task2_affected_integrated
    WHERE weather_match_status = 'exact_hour'
      AND weather_observation_timestamp IS NOT NULL
), valid_hours AS (
    SELECT * FROM classified_hours WHERE weather_condition <> 'unknown'
), zones AS (
    SELECT DISTINCT pickup_location_id,
        COALESCE(pickup_zone, 'Unknown') AS pickup_zone,
        COALESCE(pickup_borough, 'Unknown') AS pickup_borough
    FROM _task2_full_integrated
    WHERE pickup_location_id IS NOT NULL
), zone_hour_trips AS (
    SELECT pickup_location_id, pickup_hour, COUNT(trip_id) AS trip_count
    FROM _task2_affected_integrated
    WHERE trip_id IS NOT NULL
      AND weather_match_status = 'exact_hour'
      AND pickup_location_id IS NOT NULL
    GROUP BY pickup_location_id, pickup_hour
), zone_weather_hours AS (
    SELECT z.pickup_location_id, z.pickup_zone, z.pickup_borough,
        h.pickup_year, h.pickup_month, h.weather_condition,
        COALESCE(t.trip_count, 0) AS trip_count
    FROM zones z CROSS JOIN valid_hours h
    LEFT JOIN zone_hour_trips t
      ON z.pickup_location_id = t.pickup_location_id
     AND h.weather_hour = t.pickup_hour
), category_summary AS (
    SELECT pickup_year, pickup_month, pickup_location_id,
        pickup_zone, pickup_borough, weather_condition,
        COUNT(*) AS weather_hour_count,
        SUM(trip_count) AS trip_count,
        AVG(trip_count) AS avg_hourly_demand
    FROM zone_weather_hours
    GROUP BY pickup_year, pickup_month, pickup_location_id,
        pickup_zone, pickup_borough, weather_condition
), with_baseline AS (
    SELECT *,
        SUM(trip_count) OVER (
            PARTITION BY pickup_year, pickup_month, pickup_location_id
        ) / SUM(weather_hour_count) OVER (
            PARTITION BY pickup_year, pickup_month, pickup_location_id
        ) AS zone_month_avg_hourly_demand
    FROM category_summary
)
SELECT pickup_year, pickup_month, pickup_location_id,
    pickup_zone, pickup_borough, weather_condition,
    weather_hour_count, trip_count,
    ROUND(avg_hourly_demand, 3) AS avg_hourly_demand,
    ROUND(zone_month_avg_hourly_demand, 3) AS zone_month_avg_hourly_demand,
    ROUND(100.0 * (avg_hourly_demand - zone_month_avg_hourly_demand)
          / NULLIF(zone_month_avg_hourly_demand, 0), 2)
        AS demand_vs_zone_month_pct
FROM with_baseline
"""

ZONE_HOURLY_SQL = """
SELECT pickup_year, pickup_month, pickup_date, pickup_hour, pickup_location_id,
    COALESCE(pickup_zone, 'Unknown') AS pickup_zone,
    COALESCE(pickup_borough, 'Unknown') AS pickup_borough,
    COUNT(trip_id) AS trip_count,
    ROUND(AVG(trip_distance), 3) AS avg_trip_distance,
    ROUND(AVG(trip_duration_minutes), 3) AS avg_trip_duration_minutes
FROM _task2_affected_integrated
WHERE trip_id IS NOT NULL
GROUP BY pickup_year, pickup_month, pickup_date, pickup_hour,
    pickup_location_id, COALESCE(pickup_zone, 'Unknown'),
    COALESCE(pickup_borough, 'Unknown')
"""

PRODUCT_SQL = {
    "airport_transfer_summary": AIRPORT_TRANSFER_SQL,
    "fare_and_tip_patterns": FARE_AND_TIP_SQL,
    "origin_destination_flow": ORIGIN_DESTINATION_SQL,
    "trip_distance_distribution": TRIP_DISTANCE_SQL,
    "weather_sensitive_zone_demand": WEATHER_SENSITIVE_SQL,
    "zone_hourly_demand_profile": ZONE_HOURLY_SQL,
}


def _affected_frame(
    spark: "SparkSession",
    integrated: "DataFrame",
    periods: tuple[YearMonth, ...],
) -> "DataFrame":
    from pyspark.sql import functions as F

    period_frame = spark.createDataFrame(periods, "pickup_year int, pickup_month int")
    return integrated.join(
        F.broadcast(period_frame), ["pickup_year", "pickup_month"], "left_semi"
    )


def _add_product_metadata(
    frame: "DataFrame",
    created_at: datetime,
    refreshed_at: datetime,
) -> "DataFrame":
    from pyspark.sql import functions as F

    return (
        frame.withColumn("source_table", F.lit("integrated_taxi_trips"))
        .withColumn("created_at", F.lit(created_at).cast("timestamp"))
        .withColumn("refreshed_at", F.lit(refreshed_at).cast("timestamp"))
        .withColumn("schema_version", F.lit(TASK2_SCHEMA_VERSION))
    )


def _product_created_at(
    spark: "SparkSession",
    path: Path,
    fallback: datetime,
) -> datetime:
    from pyspark.sql import functions as F

    if not (path / "_delta_log").exists():
        return fallback
    old = spark.read.format("delta").load(str(path))
    if "created_at" not in old.columns:
        return fallback
    row = old.agg(F.min("created_at").alias("created_at")).first()
    return row["created_at"] if row and row["created_at"] is not None else fallback


def _merge_product_month(
    spark: "SparkSession",
    path: Path,
    source: "DataFrame",
    config: ProductConfig,
    period: YearMonth,
) -> None:
    from delta.tables import DeltaTable

    year, month = period
    if not (path / "_delta_log").exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        (
            source.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(str(path))
        )
        return

    condition = " AND ".join(
        f"target.`{key}` <=> updates.`{key}`" for key in config.merge_keys
    )
    target_period = (
        f"target.`pickup_year` = {int(year)} "
        f"AND target.`pickup_month` = {int(month)}"
    )
    (
        DeltaTable.forPath(spark, str(path))
        .alias("target")
        .merge(source.alias("updates"), condition)
        .withSchemaEvolution()
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete(target_period)
        .execute()
    )


def refresh_analytical_products(
    spark: "SparkSession",
    project_root: Path,
    periods: tuple[YearMonth, ...],
    *,
    product_names: tuple[str, ...],
    progress_callback: Callable[[ProductRefreshResult], None] | None = None,
) -> tuple[ProductRefreshResult, ...]:
    """Recompute complete affected months and atomically merge product rows."""
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    unknown = sorted(set(product_names) - set(PRODUCTS))
    if unknown:
        raise ValueError(f"Unknown analytical products: {unknown}")
    if not periods:
        return ()

    integrated_path = project_root / "data" / "processed" / "integrated_taxi_trips"
    if not (integrated_path / "_delta_log").exists():
        raise FileNotFoundError(f"Missing integrated Delta table: {integrated_path}")
    integrated = spark.read.format("delta").load(str(integrated_path))
    affected = _affected_frame(spark, integrated, periods).persist(
        StorageLevel.MEMORY_AND_DISK
    )
    affected.count()
    integrated.createOrReplaceTempView("_task2_full_integrated")
    affected.createOrReplaceTempView("_task2_affected_integrated")

    product_dir = project_root / "data" / "processed" / "analytical_products"
    results: list[ProductRefreshResult] = []
    for name in product_names:
        started = time.perf_counter()
        config = PRODUCTS[name]
        path = product_dir / name
        refreshed_at = datetime.now(timezone.utc)
        created_at = _product_created_at(spark, path, refreshed_at)
        calculated = _add_product_metadata(
            spark.sql(PRODUCT_SQL[name]), created_at, refreshed_at
        ).persist(StorageLevel.MEMORY_AND_DISK)
        rows_written = calculated.count()
        for year, month in periods:
            month_frame = calculated.filter(
                (F.col("pickup_year") == year) & (F.col("pickup_month") == month)
            )
            _merge_product_month(spark, path, month_frame, config, (year, month))
        calculated.unpersist()
        result = ProductRefreshResult(
            product_name=name,
            affected_periods=periods,
            rows_written=rows_written,
            execution_time_seconds=round(time.perf_counter() - started, 4),
            status="SUCCESS",
        )
        results.append(result)
        if progress_callback is not None:
            progress_callback(result)

    affected.unpersist()
    return tuple(results)

