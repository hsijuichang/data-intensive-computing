"""Dataset-specific rule registration for Task 4."""

from __future__ import annotations

from data_intensive_computing.task4.models import (
    DatasetValidationConfig,
    ReferenceRule,
    SchemaPolicy,
)
from data_intensive_computing.task4.rules import (
    non_negative,
    not_nan,
    numeric_range,
    timestamp_order,
)


DATASET_CONFIGS = {
    "taxi_trips": DatasetValidationConfig(
        name="taxi_trips",
        primary_key=("trip_id",),
        required_columns=(
            "trip_id",
            "pickup_datetime",
            "dropoff_datetime",
            "pickup_location_id",
            "dropoff_location_id",
        ),
        rules=(
            timestamp_order(
                "pickup_datetime",
                "dropoff_datetime",
                code="dropoff_before_pickup",
            ),
            non_negative("trip_distance", code="negative_trip_distance"),
            non_negative("passenger_count", code="negative_passenger_count"),
            non_negative("fare_amount", code="negative_fare_amount"),
            non_negative("total_amount", code="negative_total_amount"),
            not_nan("trip_distance", code="invalid_trip_distance"),
            not_nan("total_amount", code="invalid_total_amount"),
        ),
        reference_rules=(
            ReferenceRule(
                code="missing_reference:pickup_location_id",
                reference_name="taxi_zones",
                local_columns=("pickup_location_id",),
                reference_columns=("location_id",),
                description="pickup_location_id must exist in taxi_zones",
            ),
            ReferenceRule(
                code="missing_reference:dropoff_location_id",
                reference_name="taxi_zones",
                local_columns=("dropoff_location_id",),
                reference_columns=("location_id",),
                description="dropoff_location_id must exist in taxi_zones",
            ),
        ),
    ),
    "weather": DatasetValidationConfig(
        name="weather",
        primary_key=("observation_timestamp",),
        required_columns=("observation_timestamp", "humidity"),
        rules=(
            numeric_range("rhum", 0, 100, code="invalid_relative_humidity"),
            numeric_range("humidity", 20, 100, code="invalid_humidity"),
            non_negative("prcp", code="negative_precipitation"),
            numeric_range("wdir", 0, 360, code="invalid_wind_direction"),
            non_negative("wspd", code="negative_wind_speed"),
            numeric_range("pres", 850, 1100, code="invalid_pressure"),
        ),
        schema_policy=SchemaPolicy(
            allowed_additions={"humidity": frozenset({"double", "float", "int"})}
        ),
    ),
    "air_quality": DatasetValidationConfig(
        name="air_quality",
        primary_key=("observation_id",),
        required_columns=(
            "observation_id",
            "station_id",
            "parameter_code",
            "observation_timestamp",
            "observation_timestamp_utc",
            "sample_measurement",
            "aqi",
        ),
        rules=(
            numeric_range("latitude", -90, 90, code="invalid_latitude"),
            numeric_range("longitude", -180, 180, code="invalid_longitude"),
            non_negative("sample_measurement", code="negative_sample_measurement"),
            not_nan("sample_measurement", code="invalid_sample_measurement"),
            numeric_range("aqi", 0, 500, code="invalid_aqi"),
        ),
        schema_policy=SchemaPolicy(
            allowed_additions={"aqi": frozenset({"int", "bigint", "long"})}
        ),
    ),
    "taxi_zones": DatasetValidationConfig(
        name="taxi_zones",
        primary_key=("location_id",),
        required_columns=("location_id", "borough", "zone"),
        rules=(numeric_range("location_id", minimum=1, code="invalid_location_id"),),
    ),
}

TASK4_DATASET_ORDER = tuple(DATASET_CONFIGS)

