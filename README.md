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
│       │   ├── cli.py
│       │   ├── config.py
│       │   ├── pipeline.py
│       │   └── updates.py
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

## Complete execution order

1. Build the processed Delta tables:
   - `notebooks/data_catalog.ipynb`
   - `notebooks/ingestion_framework.ipynb`
   - `notebooks/integrated_taxi_trips.ipynb`
2. Generate and apply Week 3 Task 1 updates using the commands above.
3. Run the analytical queries:
   - `notebooks/analytical_queries.ipynb`
4. Run every notebook under `notebooks/optimization/`.
5. Run every product notebook under `notebooks/data_products/`.
6. Run the final evaluation:
   - `notebooks/evaluate.ipynb`




