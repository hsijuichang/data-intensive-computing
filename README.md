<<<<<<< HEAD
# data-intensive-computing
=======
# Data-Intensive Computing - Lab 1

This repository contains the Week 1 implementation for Tasks 1, 3, and 4. The `data/` folder is not included in the submission because the raw and generated datasets are too large.

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
│   └── ingestion_framework.ipynb   # Tasks 3 and 4: ingestion and common data model
├── reports/
│   ├── data_catalog.png            # Exported Task 1 catalog
│   └── common_data_model.md        # Task 4 model documentation
├── README.md
├── pyproject.toml
└── uv.lock
```


## How to run

1. Create `data/raw` in the project root and place the four downloaded datasets there using the filenames shown above.
2. Run `notebooks/data_catalog.ipynb` to inspect the raw data and export the catalog image.
3. Run `notebooks/ingestion_framework.ipynb` to create the `processed`, `failed`, and `metadata` Delta outputs.

>>>>>>> 3930545 (task 1, 3 and 4, except data)
