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

1. Build the processed Delta tables:
   - `notebooks/data_catalog.ipynb`
   - `notebooks/ingestion_framework.ipynb`
   - `notebooks/integrated_taxi_trips.ipynb`
2. Run the analytical queries:
   - `notebooks/analytical_queries.ipynb`
3. Run every notebook under `notebooks/optimization/`.
4. Run every product notebook under `notebooks/data_products/`.
5. Run the final evaluation:
   - `notebooks/evaluate.ipynb`




