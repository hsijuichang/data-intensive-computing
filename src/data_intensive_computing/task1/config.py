"""Configuration shared by the Task 1 update generator and incremental pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


TAXI_RAW_COLUMNS = (
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "store_and_fwd_flag",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "Airport_fee",
)

WEATHER_RAW_COLUMNS = (
    "year",
    "month",
    "day",
    "hour",
    "temp",
    "temp_source",
    "rhum",
    "rhum_source",
    "prcp",
    "prcp_source",
    "snwd",
    "snwd_source",
    "wdir",
    "wdir_source",
    "wspd",
    "wspd_source",
    "wpgt",
    "wpgt_source",
    "pres",
    "pres_source",
    "cldc",
    "cldc_source",
    "coco",
    "coco_source",
)

AIR_RAW_COLUMNS = (
    "State Code",
    "County Code",
    "Site Num",
    "Parameter Code",
    "POC",
    "Latitude",
    "Longitude",
    "Datum",
    "Parameter Name",
    "Date Local",
    "Time Local",
    "Date GMT",
    "Time GMT",
    "Sample Measurement",
    "Units of Measure",
    "MDL",
    "Uncertainty",
    "Qualifier",
    "Method Type",
    "Method Code",
    "Method Name",
    "State Name",
    "County Name",
    "Date of Last Change",
)


@dataclass(frozen=True)
class DatasetConfig:
    name: str
    update_name: str
    source_pattern: str
    source_format: str
    raw_columns: tuple[str, ...]
    evolved_columns: tuple[str, ...]
    primary_key: tuple[str, ...]
    required_columns: tuple[str, ...]
    partition_columns: tuple[str, ...] = ()
    schema_version: str = "1.1.0"

    def source_path(self, root: Path) -> Path:
        return root / "data" / "raw" / self.source_pattern

    def update_path(self, root: Path) -> Path:
        return root / "data" / "updates" / self.update_name

    def processed_path(self, root: Path) -> Path:
        return root / "data" / "processed" / self.name

    def failed_path(self, root: Path) -> Path:
        return root / "data" / "failed" / self.name


DATASETS = {
    "taxi_trips": DatasetConfig(
        name="taxi_trips",
        update_name="taxi_trips_update.parquet",
        source_pattern="yellow_tripdata_2024-*.parquet",
        source_format="parquet",
        raw_columns=TAXI_RAW_COLUMNS,
        evolved_columns=(),
        primary_key=("trip_id",),
        required_columns=(
            "trip_id",
            "pickup_datetime",
            "dropoff_datetime",
            "pickup_location_id",
            "dropoff_location_id",
        ),
        partition_columns=("pickup_year", "pickup_month"),
    ),
    "weather": DatasetConfig(
        name="weather",
        update_name="weather_update.csv",
        source_pattern="weather.csv",
        source_format="csv",
        raw_columns=WEATHER_RAW_COLUMNS,
        evolved_columns=("humidity",),
        primary_key=("observation_timestamp",),
        required_columns=("observation_timestamp", "humidity"),
    ),
    "air_quality": DatasetConfig(
        name="air_quality",
        update_name="air_quality_update.csv",
        source_pattern="hourly_88101_2024.csv",
        source_format="csv",
        raw_columns=AIR_RAW_COLUMNS,
        evolved_columns=("aqi",),
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
        partition_columns=("observation_year", "observation_month"),
    ),
}

TASK1_DATASET_ORDER = ("taxi_trips", "weather", "air_quality")
