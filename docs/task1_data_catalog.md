# Task 1 — Data Catalog and Source Data Assessment

## Scope and method

This catalog covers all source files currently present in `data/`:

- Three Parquet files containing January–March 2024 yellow-taxi trips.
- One taxi-zone lookup CSV.
- One nationwide hourly PM2.5 CSV for 2024.
- One hourly weather CSV for 2024.

The configuration-driven profiler scans complete files rather than samples. It
records source schemas, row counts, null counts, exact candidate-key duplicate
counts, exact categorical cardinalities, temporal ranges, numerical ranges, and
configured quality checks. CSV columns are deliberately read as strings during
profiling so identifiers such as state and county codes do not lose leading
zeros. The reproducible detailed result is stored in
`artifacts/task1_profile.json`.

Column names will be standardized to lower `snake_case` during ingestion. For
example, `PULocationID` becomes `pu_location_id`, `Date Local` becomes
`date_local`, and `Airport_fee` becomes `airport_fee`.

## Dataset: yellow_tripdata

**Sources and size.** The dataset contains 9,554,778 rows across three Parquet
files:

- January: 2,964,624 rows.
- February: 3,007,526 rows.
- March: 3,582,628 rows.

**Primary entity.** One completed New York City yellow-taxi trip.

**Record identifier.** The source does not provide an official trip ID.
`VendorID`, pickup timestamp, drop-off timestamp, pickup location, drop-off
location, trip distance, and total amount form a useful candidate fingerprint,
but they do not constitute a guaranteed natural key: one duplicate candidate
key exists in the supplied data. The ingestion layer should retain the source
fields, use this fingerprint for duplicate detection, and generate a surrogate
`trip_id`. It must not silently claim that the fingerprint is a true source
primary key.

**Likely join attributes.**

- `PULocationID` joins to `taxi_zone_lookup.LocationID`.
- `DOLocationID` joins to `taxi_zone_lookup.LocationID`.
- Pickup timestamps can join to hourly facts only after applying an explicit
  time-zone and time-bucketing rule.

**Temporal attributes.**

- `tpep_pickup_datetime`.
- `tpep_dropoff_datetime`.

The observed pickup range is 2002-12-31 22:17:10 through
2024-04-01 00:34:55. Although the files are labelled January–March 2024, 21
pickup timestamps fall outside 2024 Q1. The drop-off range extends through
2024-04-02 18:08:46, and 180 records have a drop-off time earlier than their
pickup time.

**Categorical attributes.**

- `VendorID`: 3 distinct values.
- `RatecodeID`: 7 distinct non-null values.
- `store_and_fwd_flag`: 2 distinct non-null values.
- `payment_type`: 5 distinct values.
- `PULocationID`: 262 distinct values.
- `DOLocationID`: 261 distinct values.

Location IDs are categorical foreign keys even though their physical source
type is integer.

**Attributes likely to grow.** Trip rows, timestamps, passenger counts,
distance, fare components, tips, tolls, surcharges, and total amounts grow as
new monthly trip files arrive. Code domains such as payment type change much
more slowly.

**Important source-quality findings.**

- `passenger_count`, `RatecodeID`, `store_and_fwd_flag`,
  `congestion_surcharge`, and `Airport_fee` each have 751,962 nulls (7.87%).
- Trip distance ranges from 0 to 312,722.3; 158 rows exceed 1,000 and require
  review rather than automatic acceptance.
- Total amount ranges from -1,000 to 9,792. Negative monetary records may be
  refunds or corrections, so a blanket non-negative rule would reject
  potentially legitimate data.

## Dataset: taxi_zone_lookup

**Source and size.** `taxi_zone_lookup.csv` contains 265 rows and 4 columns.

**Primary entity.** One New York City taxi zone.

**Primary key.** `LocationID` uniquely identifies every row. It has no nulls,
no duplicates, and ranges from 1 to 265.

**Likely join attributes.**

- `LocationID` joins to yellow-trip `PULocationID`.
- `LocationID` joins to yellow-trip `DOLocationID`.

**Temporal attributes.** None are present. Consequently, the source cannot
represent historical zone-name or service-zone changes without adding
effective dates in a future version.

**Categorical attributes.**

- `Borough`: 8 distinct values.
- `Zone`: 262 distinct values; zone labels are not unique.
- `service_zone`: 5 distinct values.

**Attributes likely to grow.** This is a slowly changing reference dataset.
Rows grow only when zones are introduced, while descriptive values may be
corrected. It should conceptually be treated as a lookup/dimension table.

**Important source-quality findings.** No null primary keys, duplicate primary
keys, or invalid location IDs were found.

## Dataset: air_quality_hourly

**Source and size.** `hourly_88101_2024.csv` contains 8,139,551 rows and 24
columns.

**Primary entity.** One hourly PM2.5 observation made by a particular
instrument at an air-quality monitoring site.

**Primary key.** The combination of `State Code`, `County Code`, `Site Num`,
`Parameter Code`, `POC`, `Date Local`, and `Time Local` is unique in the
supplied file. It contains no null components and no duplicates. Codes must
remain strings so leading zeros are preserved.

**Likely join attributes.**

- `State Code` + `County Code` + `Site Num` identify a monitoring site and can
  join to an AQS site reference dataset.
- `Latitude` + `Longitude` support spatial joins.
- `Parameter Code`, `Method Code`, and geographic codes can join to their
  respective reference datasets.
- `Date Local` + `Time Local` can join to weather only after confirming that
  the weather observation represents the same place and time zone. The
  supplied weather file has no location identifier, so this join cannot
  currently be treated as reliable.

**Temporal attributes.**

- `Date Local` + `Time Local` normalize to `local_timestamp`, ranging from
  2024-01-01 00:00 through 2024-12-31 23:26.
- `Date GMT` + `Time GMT` normalize to `gmt_timestamp`, ranging from
  2024-01-01 04:00 through 2025-01-01 09:00.
- `Date of Last Change` is source-record maintenance metadata, ranging from
  2024-02-02 through 2026-06-16; it is not the observation time.

All temporal strings parsed successfully. However, 16,514 local timestamps
are not aligned to the start of an hour, so downstream hourly joins need an
explicit floor/rounding policy rather than equality on the raw timestamp.

**Categorical attributes.**

- Geographic/site codes: `State Code` (53), `County Code` (126),
  `Site Num` (266), and `POC` (12).
- Pollutant fields: `Parameter Code` (1) and `Parameter Name` (1).
- Measurement descriptors: `Datum` (2), `Units of Measure` (1),
  `Qualifier` (40 non-null values), and `Method Type` (1).
- Method fields: `Method Code` (14) and `Method Name` (14).
- Geographic labels: `State Name` (53) and `County Name` (489).

**Attributes likely to grow.** Observation rows and timestamps grow hourly.
The site, pollutant, instrument, and method domains may also expand as new
locations, parameters, and equipment are introduced.

**Important source-quality findings.**

- `Uncertainty` is null in all 8,139,551 rows and contributes no information in
  this extract.
- `Qualifier` is null in 7,487,648 rows (91.9909%), which is expected for
  unqualified observations and should not automatically fail ingestion.
- Latitude and longitude are within valid geographic ranges.
- `Sample Measurement` ranges from -10 to 1,435. Negative PM2.5 readings can
  arise from instrument noise and require domain policy, not a generic
  non-negative rejection rule.

## Dataset: weather_hourly

**Source and size.** `weather.csv` contains 8,784 rows and 24 columns. This is
exactly 366 days × 24 hours for leap year 2024.

**Primary entity.** One hourly weather observation for the single implicit
location represented by the file.

**Primary key.** `year`, `month`, `day`, and `hour` uniquely identify all rows
in this file, with no missing or duplicate key components. This key is valid
only while the dataset contains one location. A `station_id` or location key
must become part of the primary key if observations from additional locations
are added.

**Likely join attributes.**

- `year` + `month` + `day` + `hour` can be normalized to
  `observation_timestamp` for hourly joins.
- No station ID, latitude, longitude, or explicit time zone is present.
  Therefore, temporal equality alone is insufficient for a defensible join to
  nationwide air-quality observations.

**Temporal attributes.** The four date parts normalize without errors to
`observation_timestamp`, spanning 2024-01-01 00:00 through
2024-12-31 23:00.

**Categorical attributes.**

- Measurement provenance: `temp_source`, `rhum_source`, `prcp_source`,
  `snwd_source`, `wdir_source`, `wspd_source`, `wpgt_source`, `pres_source`,
  `cldc_source`, and `coco_source`.
- `coco` is a categorical weather-condition code with 15 distinct non-null
  values.

**Attributes likely to grow.** Hourly observation rows and timestamps grow
over time. If the model is extended, station/location identifiers will also
grow. Source-code domains change slowly.

**Important source-quality findings.**

- `snwd`, `snwd_source`, `wpgt`, and `wpgt_source` are 100% null.
- `prcp` has 553 nulls (6.2955%).
- `coco` and `coco_source` each have 6 nulls (0.0683%).
- All timestamps are valid, the candidate key is unique, and configured
  physical ranges for temperature, humidity, precipitation, wind, pressure,
  and cloud cover contain no invalid values.

## Conclusions for downstream design

The taxi trips and air-quality observations are large, continuously growing
fact datasets. Weather is also a time-series fact dataset but is currently
small and lacks a location key. Taxi zones are a lookup/dimension dataset.

The most important preprocessing requirements are:

1. Preserve geographic codes as strings and standardize all names to
   `snake_case`.
2. Construct canonical timestamp columns while retaining original values.
3. Generate a surrogate taxi `trip_id` and quarantine or review duplicate
   candidate fingerprints.
4. Keep quality outcomes separate from domain warnings: negative fares,
   negative PM2.5, and missing qualifiers are not automatically invalid.
5. Add a weather station/location identifier before attempting spatially
   meaningful weather and air-quality joins.
