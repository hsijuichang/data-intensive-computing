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
│   ├── task4_approach_a_dataset_run.json
│   ├── task4_approach_a_fare_metrics.json
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
│   ├── w4_task3.ipynb
│   ├── w4_task4_approach_a_raw_dataset.ipynb
│   └── w4_task4_compare_approaches.ipynb
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
│       ├── week4/
│       │   ├── __init__.py
│       │   ├── features.py
│       │   └── modeling.py
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

Week 4 predicts the non-negative base fare (`fare_amount`, in US dollars) for
one taxi trip. The workflow is a chain of Delta tables and saved Spark ML
models, so every step can be rerun from its upstream artifact.

### Shared ML components

`src/data_intensive_computing/week4/` holds the definitions that every Week 4
workflow imports:

- `features.py`: feature groups and `build_feature_pipeline()`, which returns
  the unfitted seven-stage preprocessing pipeline (category preparation,
  median imputation, string indexing, one-hot encoding, and robust scaling of
  continuous values). This module is the single source of truth for features.
  To add a feature, add the source column in Task 1 and update only this module.
- `modeling.py`: the regressor configuration, `build_regressor()`, and
  `evaluate_split()`, which computes model and Train-mean baseline MAE, RMSE,
  and R2 in one Spark aggregation.

### Tasks 1–3: platform workflow (Approach B)

| Notebook | Reads | Writes |
|---|---|---|
| `w4_task1.ipynb` | `data/processed/integrated_taxi_trips` | `data/processed/ml/fare_training_dataset` |
| `w4_task2.ipynb` | Task 1 dataset | `data/models/fare_feature_pipeline`, `data/processed/ml/fare_feature_dataset` |
| `w4_task3.ipynb` | Task 2 outputs | `data/models/fare_prediction_pipeline`, `artifacts/week4_fare_metrics.json` |

- **Task 1** keeps boroughs with at least 10,000 Train trips (Bronx, Brooklyn,
  Manhattan, Queens), defines the label, adds temporal and cyclic features,
  excludes post-fare monetary columns that would leak the target, and assigns
  chronological splits. The shared raw baseline covers January–March 2024, so
  Train is January–February, Validation is 1–15 March, and Test is 16–31 March.
- **Task 2** fits the shared feature pipeline on Train only and applies the
  fitted pipeline unchanged to Validation and Test.
- **Task 3** trains the shared regressor on the Train feature vectors, evaluates
  Validation and Test against a Train-mean baseline, saves the preprocessing
  and regressor together as one `PipelineModel`, and records upstream Delta
  versions, parameters, Spark configuration, metrics, and phase timings.

Run the notebooks in order 1 → 2 → 3, with one Spark kernel open at a time.
Task 3 uses `local[2]`, a 4 GB driver, and eight shuffle partitions to avoid
exhausting Windows memory and page-file resources.

To retrain after new valid data has been integrated, rerun Task 1, Task 2, and
Task 3 in order. Task 2 refits preprocessing and rewrites the feature dataset;
Task 3 refits the regressor, replaces the complete saved model, and records the
new upstream Delta versions and run ID.

### Task 4: value of the data platform

Task 4 builds the same training dataset in two ways and compares the effort,
run time, and reproducibility of each.

- **Approach A** (`w4_task4_approach_a_raw_dataset.ipynb`) starts from the six
  files in `data/raw/` and does not read any platform table. It re-implements
  every platform rule that affects the training rows: column typing, the
  `trip_id` hash, the taxi, weather, air-quality, and zone validation rules,
  primary-key deduplication, the zone/weather/air-quality joins, and the Task 1
  logic. It writes `data/processed/ml_raw/fare_training_dataset` and per-stage
  timings to `artifacts/task4_approach_a_dataset_run.json`, then checks that
  the result is identical to the Approach B dataset. Set
  `TASK4_SKIP_EQUIVALENCE_CHECK=1` to skip that check during timing runs.
- **Approach B** is `w4_task1.ipynb`, which starts from `integrated_taxi_trips`.

From feature engineering onward, both approaches run the **same** Task 2 and
Task 3 notebooks. The environment variable `WEEK4_VARIANT` selects the input:

| `WEEK4_VARIANT` | Training dataset | Models | Metrics |
|---|---|---|---|
| unset or `platform` | `data/processed/ml/` | `data/models/` | `artifacts/week4_fare_metrics.json` |
| `raw` | `data/processed/ml_raw/` | `data/models/raw/` | `artifacts/task4_approach_a_fare_metrics.json` |

Run the full comparison from the repository root. `nbconvert` executes each
notebook in a fresh kernel and writes the executed copy to `--output-dir`, so
the notebooks in the repository are not overwritten. `kernel_name=python3`
selects the project `.venv` kernel regardless of the kernel name stored in the
notebook metadata.

```bash
RUN="uv run jupyter nbconvert --to notebook --execute --ExecutePreprocessor.timeout=-1 --ExecutePreprocessor.kernel_name=python3"

# Approach B
$RUN notebooks/w4_task1.ipynb --output-dir /tmp/week4_runs
$RUN notebooks/w4_task2.ipynb --output-dir /tmp/week4_runs
$RUN notebooks/w4_task3.ipynb --output-dir /tmp/week4_runs

# Approach A
$RUN notebooks/w4_task4_approach_a_raw_dataset.ipynb --output-dir /tmp/week4_runs
WEEK4_VARIANT=raw $RUN notebooks/w4_task2.ipynb --output-dir /tmp/week4_runs/raw
WEEK4_VARIANT=raw $RUN notebooks/w4_task3.ipynb --output-dir /tmp/week4_runs/raw
```

Then open `notebooks/w4_task4_compare_approaches.ipynb` and run all cells. It
compares the two runs at three levels: run configuration, fitted preprocessing
statistics, and model metrics.

#### Timing experiments

`src/data_intensive_computing/week4/timing.py` runs each workflow end to end
with `nbconvert`, one fresh kernel per notebook, and appends every run to
`artifacts/task4_timing_results.jsonl`. Workflows run round-robin with a
rotating start position; warm-up rounds are recorded but excluded from
`artifacts/task4_timing_summary.json`.

| Workflow | Notebooks |
|---|---|
| `spark_baseline` | `w4_task4_spark_baseline.ipynb` (fixed kernel, JVM, and Spark start-up cost) |
| `A_dataset` | Approach A dataset, equivalence check disabled |
| `B_incremental` | `w4_task1.ipynb` |
| `A_downstream` / `B_downstream` | Task 2 and Task 3 with `WEEK4_VARIANT=raw` / `platform` |
| `B_full_rebuild` | `ingestion_framework.ipynb`, `integrated_taxi_trips.ipynb`, `w4_task1.ipynb` |

```bash
uv run python -m data_intensive_computing.week4.timing \
    --workflows spark_baseline A_dataset B_incremental --warmup 1 --repeats 5
uv run python -m data_intensive_computing.week4.timing \
    --workflows A_downstream B_downstream --warmup 1 --repeats 3
```

`B_full_rebuild` overwrites every platform table, so it must run in a copy of
the project. The harness refuses to run it against this repository unless
`--allow-shared-rebuild` is given.

```bash
rsync -a \
    --exclude .git --exclude .venv --exclude spark-warehouse \
    --exclude data/processed --exclude data/processed_backup \
    --exclude data/failed --exclude data/metadata \
    --exclude data/models --exclude data/updates \
    ./ ~/task4_rebuild_copy/
uv run python -m data_intensive_computing.week4.timing \
    --workflows B_full_rebuild --project-root ~/task4_rebuild_copy --warmup 1 --repeats 3
```

Print the summary of all recorded runs with `--summary-only`.

Current result: the Approach A and Approach B training datasets are identical
row for row and column for column, and the fitted preprocessing statistics are
identical. Model metrics agree within about `1e-5`; the remaining difference
comes from the iterative L-BFGS optimizer summing gradients in a different
order, not from data preparation.
