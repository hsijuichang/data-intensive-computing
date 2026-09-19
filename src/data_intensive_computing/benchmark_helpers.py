"""
Reusable benchmarking harness for Task 3 (query optimization) notebooks.

Each optimization technique (partition pruning, broadcast join, caching, AQE)
gets its own notebook with its own fresh SparkSession. All of them import
this module instead of redefining timing / warm-up / verification logic —
keeps the four notebooks consistent and keeps the methodology fixes we
learned from Task 6 (warm-up runs, median over multiple reps, no session
reuse across experiments) in one place instead of copy-pasted four times.
"""

from __future__ import annotations

import contextlib
import io
import json
import time
from datetime import datetime, timezone
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def find_project_root(start: Path) -> Path:
    """Walk up from `start` until a directory containing pyproject.toml is found."""
    for candidate in (start, *start.parents):
        if (candidate / "pyproject.toml").is_file():
            return candidate
    raise FileNotFoundError("Could not find project root containing pyproject.toml")


def build_spark_session(
    app_name: str,
    shuffle_partitions: str = "8",
    aqe_enabled: bool = True,
    extra_configs: dict[str, str] | None = None,
) -> SparkSession:
    """
    Build a SparkSession with the same base config Kelly used in
    analytical_queries.ipynb, plus an explicit AQE on/off switch so the
    AQE notebook can toggle it without touching the other three.
    """
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "6g")
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        .config("spark.sql.adaptive.enabled", str(aqe_enabled).lower())
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    if extra_configs:
        for key, value in extra_configs.items():
            builder = builder.config(key, value)

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "America/New_York")
    spark.sparkContext.setLogLevel("WARN")
    return spark


def register_delta_tables(spark: SparkSession, processed_dir: Path, tables: list[str] | None = None) -> None:
    """Register the Week 1 Delta tables as temp views, same as Kelly's notebook."""
    if tables is None:
        tables = ["taxi_trips", "weather", "air_quality", "taxi_zones", "integrated_taxi_trips"]

    missing = [name for name in tables if not (processed_dir / name).exists()]
    if missing:
        raise FileNotFoundError(f"Missing Delta tables: {missing}. Run Week 1 ingestion first.")

    for name in tables:
        path = processed_dir / name
        spark.read.format("delta").load(str(path)).createOrReplaceTempView(name)
        print(f"Registered {name}: {path}")


def timed_run(spark: SparkSession, query_name: str, sql_text: str, warmup: bool = True, repetitions: int = 5) -> dict:
    """
    Run a query `repetitions` times and return timing stats.

    - One discarded warm-up run first (unless warmup=False), so JIT/codegen
      cold-start cost doesn't contaminate the first measured run.
    - Reports the MEDIAN, not the mean — Spark run times are right-skewed
      (occasional GC pause / scheduling hiccup), median is more robust.
    - Keep repetitions >= 3 even for fast queries: the shorter the baseline,
      the more a single noisy run can distort a 1-2 rep "average".
    """
    if warmup:
        spark.sql(sql_text).collect()

    times = []
    row_count = None
    for _ in range(repetitions):
        started = time.perf_counter()
        result = spark.sql(sql_text)
        rows = result.collect()
        times.append(time.perf_counter() - started)
        row_count = len(rows)

    times_sorted = sorted(times)
    mid = len(times_sorted) // 2
    median = times_sorted[mid] if len(times_sorted) % 2 else (times_sorted[mid - 1] + times_sorted[mid]) / 2

    return {
        "query_name": query_name,
        "row_count": row_count,
        "repetitions": repetitions,
        "all_times_seconds": [round(t, 4) for t in times],
        "median_seconds": round(median, 4),
        "min_seconds": round(min(times), 4),
        "max_seconds": round(max(times), 4),
    }


def capture_explain(df, mode: str = "formatted") -> str:
    """
    Capture df.explain(mode=...) output as a string (it normally just prints
    to stdout) so it can be saved next to the timing numbers for the report.
    """
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        df.explain(mode=mode)
    return buffer.getvalue()


def verify_same_results(spark: SparkSession, sql_before: str, sql_after: str) -> dict:
    """
    Order-independent check that two queries return the same rows.
    subtract() only catches rows missing on one side, so it has to be run
    both directions to be a real equality check.
    """
    df_before = spark.sql(sql_before)
    df_after = spark.sql(sql_after)

    only_in_before = df_before.subtract(df_after).count()
    only_in_after = df_after.subtract(df_before).count()

    return {
        "correctness_verified": (only_in_before == 0) and (only_in_after == 0),
        "rows_only_in_before": only_in_before,
        "rows_only_in_after": only_in_after,
    }


def append_benchmark_record(record: dict, results_path: Path) -> None:
    """
    Append one record to a JSON-lines file. Each optimization notebook calls
    this independently, so multiple notebooks can contribute to the same
    results file without a read-modify-write race, and it stays git-diff
    friendly (one line added per run instead of a whole JSON array rewritten).

    Expected record fields (per the schema agreed on for Task 5 hand-off):
    query_name, technique, before_time_sec, after_time_sec, speedup,
    plan_change_summary, correctness_verified.
    """
    results_path.parent.mkdir(parents=True, exist_ok=True)
    record = {**record, "recorded_at": datetime.now(timezone.utc).isoformat()}
    with results_path.open("a") as f:
        f.write(json.dumps(record) + "\n")


def timed_run_multi(spark, name: str, sql_texts: list[str], warmup: bool = True, repetitions: int = 5) -> dict:
    """
    Like timed_run, but times a SEQUENCE of queries together as one unit per
    repetition, rather than a single query run repeatedly. Used for caching,
    where the benefit only shows up across multiple queries that share an
    intermediate result — timing one query alone would not exercise that.
    """
    if warmup:
        for sql_text in sql_texts:
            spark.sql(sql_text).collect()

    times = []
    row_counts = []
    for _ in range(repetitions):
        started = time.perf_counter()
        counts = []
        for sql_text in sql_texts:
            rows = spark.sql(sql_text).collect()
            counts.append(len(rows))
        times.append(time.perf_counter() - started)
        row_counts = counts

    times_sorted = sorted(times)
    mid = len(times_sorted) // 2
    median = times_sorted[mid] if len(times_sorted) % 2 else (times_sorted[mid - 1] + times_sorted[mid]) / 2

    return {
        "query_name": name,
        "row_counts": row_counts,
        "repetitions": repetitions,
        "all_times_seconds": [round(t, 4) for t in times],
        "median_seconds": round(median, 4),
        "min_seconds": round(min(times), 4),
        "max_seconds": round(max(times), 4),
    }