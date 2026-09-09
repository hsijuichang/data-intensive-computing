# Common Data Model

The model defines common naming, timestamp, data-type, and missing-value rules. 

## 1. Common rules

| Area | Rule |
|---|---|
| Column names | Use lowercase `snake_case`. Convert spaces and punctuation to underscores, split CamelCase, and expand unclear abbreviations where needed, such as `PULocationID → pickup_location_id` and `DOLocationID → dropoff_location_id`. |
| Timestamps | Store timestamps as Spark `TimestampType`. Use `yyyy-MM-dd HH:mm:ss` when displayed or serialized as text. The Spark session time zone is `America/New_York`. |
| Dates | Store dates as `DateType`, displayed as `yyyy-MM-dd`. Store year, month, day, and hour components as integers. |
| Identifiers | Use `IntegerType` or `LongType` for numeric identifiers. Keep Air Quality codes as strings to preserve leading zeroes. |
| Measurements | Use `DoubleType` for monetary values, distance, duration, coordinates, weather measurements, and air-quality measurements. |
| Missing values | Keep unknown optional values as `null`; never replace them with `0` or `"unknown"`. Missing required or primary-key fields cause the record to be written to `failed`. |
| Invalid values | Invalid timestamps, invalid numerical values, and duplicate primary keys are written to `failed` with `_validation_errors`; only valid records enter `processed`. |
| Metadata | Add `_source_file`, `_ingestion_run_id`, `_ingested_at`, and `_schema_version` to every output record. Rejected records also contain `_validation_errors`. |

## 2. Dataset-specific transformations

### Taxi Trips

| Transformation | Rule |
|---|---|
| Rename | Standardize all names to `snake_case`; additionally rename `tpep_pickup_datetime → pickup_datetime`, `tpep_dropoff_datetime → dropoff_datetime`, `PULocationID → pickup_location_id`, `DOLocationID → dropoff_location_id`, and `RatecodeID → rate_code_id`. |
| Cast | Cast pickup/dropoff times to `TimestampType`; location IDs and passenger count to `IntegerType`; vendor, rate, and payment IDs to `LongType`; distance and all monetary fields to `DoubleType`. |
| Generate key | Create `trip_id` as a SHA-256 hash of vendor, pickup/dropoff timestamps, pickup/dropoff locations, trip distance, and total amount. Generate it only when every component is present. |
| Derive time fields | Calculate `trip_duration_minutes`; derive `pickup_date`, hourly `pickup_hour`, `pickup_year`, and `pickup_month`. |
| Validate | Reject missing required fields, duplicate `trip_id`, dropoff before pickup, negative distance/count/fare/total, and NaN distance or total. |
| Partition | Store processed records by `pickup_year` and `pickup_month`. |

### Weather

| Transformation | Rule |
|---|---|
| Rename | Apply the common `snake_case` rule; existing measurement and `*_source` names remain readable after standardization. |
| Cast | Cast `year`, `month`, `day`, `hour`, relative humidity, wind direction, cloud cover, and weather-condition code to `IntegerType`. Cast temperature, precipitation, snow depth, wind speed/gust, and pressure to `DoubleType`. Preserve all `*_source` fields as strings. |
| Normalize time | Combine `year`, `month`, `day`, and `hour` into hourly `observation_timestamp`; derive `observation_date`. |
| Key | Use `observation_timestamp` as the processed primary key. |
| Validate | Reject a missing/duplicate timestamp, humidity outside 0–100, negative precipitation or wind speed, wind direction outside 0–360, and pressure outside 850–1100. |
| Partition | Do not partition because the dataset is comparatively small. |

### Air Quality

| Transformation | Rule |
|---|---|
| Rename | Convert spaced source names to `snake_case`, for example `Sample Measurement → sample_measurement` and `Date of Last Change → date_of_last_change`. |
| Cast | Cast latitude, longitude, sample measurement, MDL, and uncertainty to `DoubleType`; parse `date_of_last_change` as `DateType`. Preserve state/county/site, parameter, POC, and method codes as strings. |
| Normalize time | Combine `Date Local` and `Time Local` into `observation_timestamp`; combine `Date GMT` and `Time GMT` into `observation_timestamp_utc`; derive `observation_date`, `observation_year`, and `observation_month`. Remove the four original date/time component fields after parsing. |
| Generate identifiers | Create `station_id` from state, county, site, parameter, and POC codes. Create `observation_id` as a SHA-256 hash of those codes plus local observation timestamp and method code. |
| Validate | Reject missing required fields, duplicate `observation_id`, latitude/longitude outside their valid ranges, and negative or NaN sample measurements. |
| Partition | Store processed records by `observation_year` and `observation_month`. |

### Taxi Zone Lookup

| Transformation | Rule |
|---|---|
| Rename | Apply `snake_case`, including `LocationID → location_id`. |
| Cast and clean | Cast `location_id` to `IntegerType`; trim surrounding whitespace from `borough`, `zone`, and `service_zone`. |
| Key | Use `location_id` as the processed primary key. |
| Validate | Reject missing/duplicate IDs, IDs less than or equal to zero, and missing borough or zone values. |
| Partition | Do not partition because this is a small lookup table. |

## 3. Missing and rejected records

The model preserves valid rows while preventing invalid data from entering analytical tables:

- Optional missing values remain `null` in `processed`.
- Missing required fields, invalid values, and duplicate keys are recorded in `_validation_errors` and written to `data/failed/<dataset>`.
- A failed cast becomes `null`; it causes rejection when the target field is required.
- For duplicate keys, the first record is retained and later occurrences are rejected.
- No invalid record is silently corrected or replaced with a default value.

## 4. Processed model summary

| Dataset | Primary key | Main timestamp | Partition columns |
|---|---|---|---|
| Taxi Trips | `trip_id` | `pickup_datetime` | `pickup_year`, `pickup_month` |
| Weather | `observation_timestamp` | `observation_timestamp` | None |
| Air Quality | `observation_id` | `observation_timestamp` and `observation_timestamp_utc` | `observation_year`, `observation_month` |
| Taxi Zone Lookup | `location_id` | None | None |

