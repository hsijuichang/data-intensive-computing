# Data-Intensive Computing - Lab 1

This repository contains the Week 1 implementation for Tasks 1, 3, 4, 5, and
6. The `data/` folder is not included in the submission because the raw and
generated datasets are too large.

## Prerequisites

- Python (see `pyproject.toml` for the minimum version) and [uv](https://docs.astral.sh/uv/)
- **Java 17** — required by PySpark 4.x (PySpark 4.x will not start on Java 11
  or earlier; it fails with `UnsupportedClassVersionError`).

## Project structure

```text
data-intensive-computing/
├── data/
│   ├── raw/
│   │   ├── yellow_tripdata_2024-*.parquet
│   │   ├── weather.csv
│   │   ├── hourly_88101_2024.csv
│   │   └── taxi_zone_lookup.csv
│   ├── processed/   # Valid standardized Delta data 
│   ├── failed/      # Rejected records and validation errors
│   └── metadata/    # Ingestion statistics and schema information
├── notebooks/
│   ├── data_catalog.ipynb          # Task 1: study data and create the catalog
│   ├── ingestion_framework.ipynb   # Tasks 3 and 4: ingestion and common data model
│   ├── integrated_taxi_trips.ipynb # Task 5: integration pipeline
│   └── benchmark.ipynb             # Task 6: partition strategy benchmark
├── reports/
│   ├── data_catalog.png            # Exported Task 1 catalog
│   ├── common_data_model.md        # Task 4 model documentation
│   └── benchmark_report.pdf        # Task 6 benchmark report
├── README.md
├── pyproject.toml
└── uv.lock
```

## How to run

1. Create `data/raw` in the project root and place the four downloaded
   datasets there using the filenames shown above.
2. Confirm Java 17 is active: `java -version` should print `17.x.x`.
3. `uv sync` to install dependencies.
4. Run `notebooks/data_catalog.ipynb` to inspect the raw data and export the
   catalog image.
5. Run `notebooks/ingestion_framework.ipynb` to create the `processed`,
   `failed`, and `metadata` Delta outputs.
6. Run `notebooks/integrated_taxi_trips.ipynb` to build the enriched
   `integrated_taxi_trips` Delta table.
7. Run `notebooks/benchmark.ipynb` to reproduce the Task 6 partition
   strategy benchmark; the numbers it prints are discussed in `reports/report.pdf`.