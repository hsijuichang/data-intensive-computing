"""Configuration for incremental integration and analytical products."""

from __future__ import annotations

from dataclasses import dataclass


TASK2_SCHEMA_VERSION = "1.1.0"


@dataclass(frozen=True)
class ProductConfig:
    name: str
    merge_keys: tuple[str, ...]


PRODUCTS = {
    "airport_transfer_summary": ProductConfig(
        "airport_transfer_summary",
        (
            "pickup_year",
            "pickup_month",
            "direction",
            "airport_location_id",
            "counterpart_location_id",
        ),
    ),
    "fare_and_tip_patterns": ProductConfig(
        "fare_and_tip_patterns",
        ("pickup_year", "pickup_month", "pickup_borough", "payment_type"),
    ),
    "origin_destination_flow": ProductConfig(
        "origin_destination_flow",
        (
            "pickup_year",
            "pickup_month",
            "pickup_location_id",
            "dropoff_location_id",
        ),
    ),
    "trip_distance_distribution": ProductConfig(
        "trip_distance_distribution",
        ("pickup_year", "pickup_month", "pickup_borough", "distance_band"),
    ),
    "weather_sensitive_zone_demand": ProductConfig(
        "weather_sensitive_zone_demand",
        (
            "pickup_year",
            "pickup_month",
            "pickup_location_id",
            "weather_condition",
        ),
    ),
    "zone_hourly_demand_profile": ProductConfig(
        "zone_hourly_demand_profile",
        ("pickup_hour", "pickup_location_id"),
    ),
}

PRODUCT_ORDER = tuple(PRODUCTS)

