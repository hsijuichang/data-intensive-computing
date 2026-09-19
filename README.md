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
│   └── task3_benchmark_results.jsonl
├── data/
│   ├── raw/
│   ├── processed/
│   │   ├── taxi_trips/
│   │   ├── weather/
│   │   ├── air_quality/
│   │   ├── taxi_zones/
│   │   ├── integrated_taxi_trips/
│   │   └── analytical_products/
│   ├── failed/
│   └── metadata/
├── notebooks/
│   ├── data_catalog.ipynb
│   ├── ingestion_framework.ipynb
│   ├── integrated_taxi_trips.ipynb
│   ├── analytical_queries.ipynb
│   ├── optimization/
│   ├── data_products/
│   ├── evaluate.ipynb
│   └── benchmark.ipynb
├── reports/
│   ├── benchmark_report.tex
│   ├── benchmark_report.pdf
│   ├── design_report.pdf
│   └── common_data_model.md
└── src/data_intensive_computing/
    ├── benchmark_helpers.py
    └── spark_env.py
```

`notebooks/benchmark.ipynb` is the earlier partition-strategy experiment.
The current analytical optimization experiments are under
`notebooks/optimization/`.

## Complete execution order

### 1. Build the Week 1 Delta tables

Run these notebooks in order:

1. `notebooks/data_catalog.ipynb`
2. `notebooks/ingestion_framework.ipynb`
3. `notebooks/integrated_taxi_trips.ipynb`

They create the standardized source Delta tables and
`data/processed/integrated_taxi_trips`.

### 2. Run the analytical query library

Run `notebooks/analytical_queries.ipynb`.

It implements:

- monthly demand by pickup zone;
- trip distance by weather condition;
- air-quality and hourly-demand analysis;
- weather-related zone-demand variation;
- peak travel hours by weekday;
- monthly demand trends.

Each query prints its execution time and stores its SQL text and collected
result in the notebook session.

### 3. Reproduce the optimization experiments

Run the final experiment notebooks under `notebooks/optimization/` in fresh
kernels:

- `partition_pruning.ipynb`
- `broadcast_join.ipynb`
- `caching.ipynb`
- `aqe_zone_weather_demand_variation.ipynb`
- `aqe_monthly_zone_demand.ipynb`
- `aqe_distance_by_weather.ipynb`

The two reverse-order notebooks diagnose AQE measurement-order effects:

- `aqe_monthly_zone_demand_reverse.ipynb`
- `aqe_distance_by_weather_reverse.ipynb`

The common warm-up, median timing, plan capture, result comparison, and JSONL
helpers are in `src/data_intensive_computing/benchmark_helpers.py`.
Historical runs are appended to `artifacts/task3_benchmark_results.jsonl`;
the final notebook outputs are the authoritative results.

### 4. Generate analytical data products

Run all notebooks under `notebooks/data_products/`, one at a time:

1. `zone_hourly_demand_profile.ipynb`
2. `airport_transfer_summary.ipynb`
3. `weather_sensitive_zone_demand.ipynb`
4. `origin_destination_flow.ipynb`
5. `fare_and_tip_patterns.ipynb`
6. `trip_distance_distribution.ipynb`

Each notebook writes a Delta table under
`data/processed/analytical_products/`. Every product includes
`source_table`, `created_at`, `refreshed_at`, and `schema_version`.
The zone-hour and origin-destination products are partitioned by pickup year
and month.

### 5. Evaluate the platform

After all data products exist, run `notebooks/evaluate.ipynb`.

The notebook consolidates:

- original analytical execution times;
- controlled before/after optimization timings;
- optimization coverage and physical-plan changes;
- AQE reverse-order diagnostics;
- result-equivalence checks;
- analytical-product row counts, file counts, and active Delta storage.

The current measured product overhead is 5.800 MB, or 0.619% of the
936.188 MB integrated Delta table.

## Benchmark methodology

Controlled optimization experiments use one discarded warm-up followed by
five timed repetitions and report the median. Queries are fully materialized
with `collect()`. Baseline and optimized results are compared in both
directions using Spark `subtract()`.

The validated results are:

- partition pruning: 1.25x;
- broadcast join: 3.57x;
- shared-intermediate caching: 5.86x;
- AQE: no demonstrated improvement for the three tested queries.

The AQE notebooks initially showed 1.05x–1.37x timing differences, but the
winner reversed when measurement order was reversed. `EXPLAIN FORMATTED`
also showed no `AQEShuffleRead` and no difference between AQE initial and
final plans.

## Reports

The benchmark analysis is in `reports/benchmark_report.tex`. Rebuild it from
the project root with:

```powershell
pdflatex -output-directory=reports reports/benchmark_report.tex
pdflatex -output-directory=reports reports/benchmark_report.tex
```

The report includes timing charts, storage overhead, execution-plan changes,
performance conclusions, trade-offs, and recommendations for expansion to
ten cities.
