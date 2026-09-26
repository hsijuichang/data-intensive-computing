# Data-Intensive Computing — Urban Data Platform

This project implements a reusable analytical platform with Apache Spark and
Delta Lake. It ingests and validates New York City taxi, weather, air-quality,
and taxi-zone data; builds an integrated trip table; executes Spark SQL
analytics; evaluates query optimizations; and publishes reusable analytical
Delta products.

The `data/` directory is intentionally excluded from the repository because
the raw and generated datasets are too large.

## Prerequisites

- Python and [uv](https://docs.astral.sh/uv/) as specified in `pyproject.toml`
- Java 17 for PySpark 4.x
- A LaTeX distribution with `pgfplots` to rebuild the benchmark report

Install the Python environment from the project root:

```powershell
uv sync
java -version
uv run jupyter lab
```

`java -version` must report Java 17.

### Windows

The project automatically configures `JAVA_HOME`, `HADOOP_HOME`,
`winutils.exe`, `hadoop.dll`, and Spark temporary directories through
`src/data_intensive_computing/spark_env.py`.

Run only one Spark notebook kernel at a time. After a notebook finishes,
shut down its kernel before starting another one. Multiple local Spark
contexts can exhaust driver memory or terminate the Py4J JVM connection.

## Input data

Create `data/raw/` and add:

- `yellow_tripdata_2024-*.parquet`
- `weather.csv`
- `hourly_88101_2024.csv`
- `taxi_zone_lookup.csv`

## Project structure

```text
data-intensive-computing/
├── artifacts/
│   ├── task1_update_manifest.json
│   ├── task1_incremental_results.jsonl
│   ├── task2_refresh_results.jsonl
│   ├── task3_benchmark_results.jsonl
│   └── ten_city_scaling_results.json
├── notebooks/
│   ├── data_products/
│   │   ├── product_utils.py
│   │   ├── README.md
│   │   ├── airport_transfer_summary.ipynb
│   │   ├── fare_and_tip_patterns.ipynb
│   │   ├── origin_destination_flow.ipynb
│   │   ├── trip_distance_distribution.ipynb
│   │   ├── weather_sensitive_zone_demand.ipynb
│   │   └── zone_hourly_demand_profile.ipynb
│   ├── optimization/
│   │   ├── partition_pruning.ipynb
│   │   ├── broadcast_join.ipynb
│   │   ├── caching.ipynb
│   │   ├── aqe_zone_weather_demand_variation.ipynb
│   │   ├── aqe_monthly_zone_demand.ipynb
│   │   ├── aqe_monthly_zone_demand_reverse.ipynb
│   │   ├── aqe_distance_by_weather.ipynb
│   │   └── aqe_distance_by_weather_reverse.ipynb
│   ├── data_catalog.ipynb
│   ├── ingestion_framework.ipynb
│   ├── integrated_taxi_trips.ipynb
│   ├── analytical_queries.ipynb
│   ├── evaluate.ipynb
│   └── benchmark.ipynb
├── reports/
│   ├── lab1/
│   │   ├── architecture.png
│   │   ├── data_catalog.png
│   │   ├── benchmark_report.pdf
│   │   ├── design_report.pdf
│   │   └── common_data_model.md
│   ├── lab2/
│   │   ├── Iab2_benchmark.pdf
│   │   └── lab2_short_report.pdf
├── src/
│   └── data_intensive_computing/
│       ├── task1/
│       │   ├── __init__.py
│       │   ├── cli.py
│       │   ├── config.py
│       │   ├── pipeline.py
│       │   └── updates.py
│       ├── task2/
│       │   ├── __init__.py
│       │   ├── cli.py
│       │   ├── config.py
│       │   ├── integration.py
│       │   ├── models.py
│       │   ├── pipeline.py
│       │   └── products.py
│       ├── task3/
│       │   ├── __init__.py
│       │   ├── cli.py
│       │   ├── config.py
│       │   ├── models.py
│       │   ├── queries.py
│       │   ├── recorder.py
│       │   ├── schema.py
│       │   └── writer.py
│       ├── task4/
│       │   ├── __init__.py
│       │   ├── cli.py
│       │   ├── config.py
│       │   ├── engine.py
│       │   ├── models.py
│       │   ├── quarantine.py
│       │   ├── rules.py
│       │   └── schema.py
│       ├── __init__.py
│       ├── benchmark_helpers.py
│       └── spark.py
├── tools/
│   └── hadoop/bin/
│       ├── hadoop.dll
│       └── winutils.exe
├── .gitignore
├── pyproject.toml
└── uv.lock
```

`notebooks/benchmark.ipynb` is the earlier partition-strategy experiment.
The current analytical optimization experiments are under
`notebooks/optimization/`.

## Week 3 Task 1: incremental updates

Task 1 is implemented as the reusable Python package
`src/data_intensive_computing/task1/`; it does not require a notebook. First
run the Week 1 ingestion once so the baseline Delta tables exist, then generate
and apply the incremental releases:

```powershell
uv run data-intensive-computing generate-updates
uv run data-intensive-computing apply-updates
```

The equivalent one-step command is:

```powershell
uv run data-intensive-computing task1
```

Useful generation options are `--taxi-fraction` (0.05–0.10),
`--taxi-duplicate-fraction` (0.01–0.02), `--hours`, and `--seed`. Both taxi
fractions use the original taxi row count as their denominator. For example:

```powershell
uv run data-intensive-computing task1 --taxi-fraction 0.08 --taxi-duplicate-fraction 0.015 --hours 168
```

The generated Spark datasets are stored at:

- `data/updates/taxi_trips_update.parquet`
- `data/updates/weather_update.csv`
- `data/updates/air_quality_update.csv`

These paths are Spark dataset directories containing Parquet or CSV part
files. `artifacts/task1_update_manifest.json` documents the generated counts
and schema changes. Incremental timing and record counts are appended to
`artifacts/task1_incremental_results.jsonl` for later evaluation.

The pipeline validates each release, quarantines invalid and duplicate rows
under `data/failed/`, and uses an insert-only Delta `MERGE`. Existing rows are
never overwritten. Delta schema evolution adds numeric `humidity` to weather
and integer `aqi` to air quality; previous records retain `null` for those new
columns. Re-running `apply-updates` is idempotent for the processed tables:
previously inserted rows are recognized by their Week 1 primary keys and
ignored. The failed-data audit intentionally records duplicate observations
from each execution separately by ingestion run ID.

The Python API used by the CLI is also available to the evaluation code:

- `task1.updates.generate_all_updates(...)`
- `task1.pipeline.run_incremental_updates(...)`
- `task1.pipeline.apply_incremental_update(...)`

## Week 3 Task 2: analytical consistency

After Task 1 has merged the processed Delta updates, incrementally enrich every
processed taxi trip that is still missing from `integrated_taxi_trips` and
refresh only the affected year/month slices of all six analytical products:

```powershell
uv run data-intensive-computing refresh-analytics
```

```powershell
uv run data-intensive-computing refresh-analytics airport_transfer_summary
uv run data-intensive-computing refresh-analytics fare_and_tip_patterns
uv run data-intensive-computing refresh-analytics origin_destination_flow
uv run data-intensive-computing refresh-analytics trip_distance_distribution
uv run data-intensive-computing refresh-analytics weather_sensitive_zone_demand
uv run data-intensive-computing refresh-analytics zone_hourly_demand_profile
```

The alias `task2` is equivalent. A combined production pipeline can pass the
taxi result returned by Task 1 to avoid scanning unrelated source rows:

The Python implementation lives in `src/data_intensive_computing/task2/`.
It preserves every Week 2 output column, adds evolved humidity and AQI context
to new integrated rows, uses an idempotent `trip_id` Delta merge, and fully
recomputes each affected month before merging the corresponding product rows.
Structured integration and per-product timing results are returned to callers
for Task 5 monitoring and appended to
`artifacts/task2_refresh_results.jsonl`. A second run with no pending taxi rows
is skipped without rewriting analytical products.

## Week 3 Task 3: platform monitoring

Task 1 incremental updates and Task 2 analytical refreshes now automatically
record operational metadata in Delta tables:

- `data/metadata/ingestion_runs`: unified pipeline execution facts, extending
  the Week 1 table with pipeline type, inserted/warning counts, schema changes,
  and validation-failure summaries.
- `data/metadata/validation_failure_events`: normalized validation failures by
  run, dataset, and rule code.
- `data/metadata/monitoring_writes`: monitoring-write timings used to measure
  Task 5 monitoring overhead.

Existing Task 1 JSONL measurements can be imported once without rerunning the
large incremental update. The operation is idempotent:

```powershell
uv run data-intensive-computing backfill-monitoring
```

Run all required Spark SQL monitoring queries with:

```powershell
uv run data-intensive-computing monitor
```

The `task3` alias is equivalent. Individual queries can also be selected:

```powershell
uv run data-intensive-computing monitor validation_failures
uv run data-intensive-computing monitor longest_processing_time
uv run data-intensive-computing monitor rejected_by_execution
uv run data-intensive-computing monitor processing_time_trend
uv run data-intensive-computing monitor monitoring_overhead
```

The SQL definitions and reusable Python API are in
`src/data_intensive_computing/task3/queries.py`. Evaluation experiments can
compare normal execution with `monitoring_enabled=False` on the Task 1 and
Task 2 Python pipeline functions. Monitoring-write failures are reported as
warnings and do not roll back an otherwise successful data update.


## Week 3 Task 4: extensible validation

Task 4 is implemented independently under
`src/data_intensive_computing/task4/`; it is not yet wired into the Task 1
ingestion pipeline. The public API validates an already transformed DataFrame:

The framework detects incomplete records, invalid values, missing taxi-zone
references, duplicates within the batch and against the target Delta table,
and unsupported schema changes. Rejected rows are appended to
`data/failed/<dataset>` with validation metadata. Duplicate rows are included
in `rejected_records` and are also reported separately as
`duplicate_records`, so the accounting invariant is always
`input_records = valid_records + rejected_records`.

Allowed additive evolution is configured for weather `humidity` and
air-quality `aqi`. New rules are registered in `task4/config.py` by composing
the generic factories in `task4/rules.py`; the validation engine does not need
to change.

For standalone checks of transformed Delta data, use the Task 4 module CLI:

```powershell
uv run data-intensive-computing apply-updates
```

```powershell
uv run data-intensive-computing apply-updates taxi_trips
uv run data-intensive-computing apply-updates weather
uv run data-intensive-computing apply-updates air_quality
```

## Complete execution order

1. Build the processed Delta tables:
   - `notebooks/data_catalog.ipynb`
   - `notebooks/ingestion_framework.ipynb`
   - `notebooks/integrated_taxi_trips.ipynb`
2. Generate and apply Week 3 Task 1 updates using the commands above.
3. Run the Week 3 Task 2 analytical consistency refresh.
4. Run the analytical queries:
   - `notebooks/analytical_queries.ipynb`
5. Run every notebook under `notebooks/optimization/`.
6. Run every product notebook under `notebooks/data_products/` only when a
   full Week 2 rebuild is intentionally required.
7. Run the final evaluation:
   - `notebooks/evaluate.ipynb`
