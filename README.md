# Data-Intensive Computing

## Task 1: source-data profiling

The Task 1 profiler performs complete-file analysis of the configured CSV and
Parquet datasets and writes a reproducible JSON profile.

```powershell
python -m data_intensive_computing.profiling
```

Outputs:

- `docs/task1_data_catalog.md` — interpreted Data Catalog and findings.
- `artifacts/task1_profile.json` — generated detailed profiling evidence.
- `config/datasets.yaml` — dataset semantics and profiling rules.

Profile one dataset during development:

```powershell
python -m data_intensive_computing.profiling --dataset weather_hourly
```