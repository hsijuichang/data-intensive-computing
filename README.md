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
│   ├── task5_evaluation_results.jsonl
│   ├── task5_storage.json
│   ├── task5_summary.json
│   ├── ten_city_scaling_results.json
│   └── week4_fare_metrics.json
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
│   ├── benchmark.ipynb
│   ├── w4_task1.ipynb
│   ├── w4_task2.ipynb
│   └── w4_task3.ipynb
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
│   └── lab3/
│       ├── evaluation_report.pdf
│       └── evaluation_report.tex
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
│       ├── task5/
│       │   ├── __init__.py
│       │   ├── cli.py
│       │   ├── config.py
│       │   ├── experiments.py
│       │   ├── snapshot.py
│       │   └── summary.py
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

The Python implementation lives in `src/data_intensive_computing/task2/`.
It preserves every Week 2 output column, adds evolved humidity and AQI context
to new integrated rows, uses an idempotent `trip_id` Delta merge, and fully
recomputes each affected month before merging the corresponding product rows.
Structured integration and per-product timing results are returned to callers
for Task 5 monitoring and appended to
`artifacts/task2_refresh_results.jsonl`. A second run with no pending taxi rows
is skipped without rewriting analytical products.

To limit a standalone refresh to one Task 1 taxi run or selected products:

```powershell
uv run data-intensive-computing refresh-analytics --taxi-run-id RUN_ID
uv run data-intensive-computing refresh-analytics origin_destination_flow
uv run data-intensive-computing refresh-analytics origin_destination_flow zone_hourly_demand_profile
```

Product names are positional, matching the Task 1 `apply-updates` interface.
Omitting product names refreshes all six analytical product tables. Supplying
one or more names refreshes only those tables.

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

Task 4 is implemented as a reusable package under
`src/data_intensive_computing/task4/`. It is wired into Task 1 immediately
after dataset-specific transformation and before the Delta `MERGE`. 

The caller can pass `validated.valid` to a downstream merge or analytical
stage. `validated.invalid` contains the isolated rows and their validation
codes.

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

For normal incremental processing, Task 1 automatically calls Task 4 and then
merges only valid rows into the target table:

```powershell
uv run data-intensive-computing apply-updates taxi_trips
```

To validate a transformed candidate Delta table without merging it into the
processed target, use the standalone Task 4 CLI:

```powershell
uv run python -m data_intensive_computing.task4.cli taxi_trips PATH_TO_CANDIDATE_DELTA data/processed/taxi_trips
```

The candidate path must contain an already transformed Delta table with the
processed schema. The standalone command never modifies the processed target.
By default, it appends rejected rows to `data/failed/taxi_trips`; add
`--no-quarantine` when a completely read-only validation check is required.

## Week 3 Task 5: platform evaluation

Task 5 is implemented under `src/data_intensive_computing/task5/`. It measures
incremental update time, analytical refresh time (incremental versus full
recompute), storage overhead, validation overhead, and monitoring overhead.
Run the steps from the project root in this order:

```powershell
uv run python -m data_intensive_computing.task5.cli prepare
uv run python -m data_intensive_computing.task5.cli phase-a --reps 5
uv run python -m data_intensive_computing.task5.cli phase-b --reps 5
uv run python -m data_intensive_computing.task5.cli summary
```

- `prepare` builds the six analytical products if they are missing, generates
  the update files if they are missing, checks that every generated taxi trip
  starts after the original data, and records the starting Delta version of
  every evaluated table (snapshot `S0`).
- `phase-a` measures Task 1 in three configurations: production (validation
  and monitoring on), monitoring off, and validation off.
  `apply_incremental_update()` accepts `validation_enabled=False` for this
  purpose only; the default keeps validation on.
- `phase-b` applies Task 1 once, then measures the Task 2 incremental refresh
  with and without monitoring, and a full recompute.
- `summary` computes medians and overheads from the saved results without
  starting Spark.

Every measured run is followed by a Delta `RESTORE` to the snapshot, so each
repetition processes the same input against the same table state. Each phase
starts with one discarded warm-up run and rotates the configuration order.
Phase B restores every table to `S0` when it finishes.

Results are written to:

- `artifacts/task5_evaluation_results.jsonl`: one line per measurement
- `artifacts/task5_storage.json`: table sizes before and after one run
- `artifacts/task5_summary.json`: medians and overheads used in the report

If a phase is interrupted, restore all tables before running it again:

```powershell
uv run python -m data_intensive_computing.task5.cli restore
```

`artifacts/task5_snapshot.json` stores Delta versions of the local tables and
is not committed. `prepare` sets the retention period of the evaluated tables
to 365 days so that `RESTORE` is not blocked. The restored versions leave
unreferenced files on disk; remove them with `VACUUM` after the evaluation.

## Week 4 Tasks 1–2: fare prediction dataset and features

`notebooks/week4_task1_fare_training_dataset.ipynb` builds a trip-level fare
prediction dataset from `integrated_taxi_trips`. It selects Bronx, Brooklyn,
Manhattan, and Queens, defines `fare_amount` as the label, creates temporal
features, and assigns chronological train, validation, and test splits.

`notebooks/week4_task2_fare_feature_pipeline.ipynb` builds a reusable Spark ML
feature pipeline. It handles missing values, encodes categorical columns,
scales continuous features with `RobustScaler`, and produces the final feature
vectors used by the fare machine learning model.

### Task 3 implementation

`notebooks/w4_task3.ipynb` implements the complete reproducible ML workflow.
It loads the Task 1 Delta table, applies the Task 2 preprocessing stages,
trains regularized Spark linear regression on Train only, evaluates Validation
and Test against a Train-mean baseline, and saves the preprocessing and model
together as one `PipelineModel`. The notebook also records the source Delta
version, split profile, parameters, Spark configuration, metrics, and phase
timings in a JSON manifest.

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
7. Run the Week 3 Task 5 evaluation with the commands above.
   `notebooks/evaluate.ipynb` contains the Week 2 optimization evaluation.

## Week 4: reproducible fare prediction

Week 4 predicts the non-negative base fare for one taxi trip. Run
`notebooks/w4_task1.ipynb` first to rebuild the chronological Train,
Validation, and Test Delta table. The current shared raw baseline covers
January through March 2024, so the notebook uses January-February for Train
and divides March into Validation and Test periods.

Run `notebooks/w4_task2.ipynb` to build and inspect the reusable feature
pipeline.

Shut down other Spark notebook kernels first, open
`notebooks/w4_task3.ipynb`, restart its kernel, and run all cells from top to
bottom. The notebook uses `local[2]`, a 4 GB Spark driver, and eight shuffle
partitions to avoid exhausting Windows memory and page-file resources.

Task 3 reads `data/processed/ml/fare_training_dataset` directly. Therefore
Task 1 must have completed successfully. Task 2 can be run independently to
inspect its feature output, but Task 3 refits the same feature stages as part
of the complete pipeline and does not require the persisted Task 2 feature
table.

The default outputs are:

- `data/models/fare_prediction_pipeline`: complete fitted feature/model pipeline
- `artifacts/week4_fare_metrics.json`: split profile, Delta version, parameters,
  Spark configuration, MAE/RMSE/R2, and phase timings

To retrain after new valid data has been integrated, rerun Task 1 and then run
Task 3 from top to bottom. The same feature and model definitions are refitted
on the new Train split, the saved model is replaced, and the new Delta version
and run ID are written to the metrics manifest.

For the later Week 4 Task 4 comparison, `artifacts/week4_fare_metrics.json`
provides the platform run's Delta version, split profile, Spark configuration,
model parameters, metrics, and phase timings. Task 4 remains a separate
workflow and should rerun both approaches under the same configuration.
