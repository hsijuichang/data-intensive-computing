"""Preparation and the two measurement phases of the Task 5 evaluation.

Phase A measures Task 1 (incremental update) under three configurations to
obtain update time, monitoring overhead, and validation overhead. Phase B
starts from the state after one Task 1 run and measures Task 2 (analytical
refresh) incrementally, without monitoring, and as a full recompute.

Every measured run starts from the same Delta versions: tables under test are
restored to a snapshot after each run, so each repetition does identical work.
"""

from __future__ import annotations

import json
import subprocess
import time
import uuid
from datetime import datetime, timezone
from importlib.metadata import version as package_version
from pathlib import Path
from typing import TYPE_CHECKING, Callable

from data_intensive_computing.task1.config import DATASETS, TASK1_DATASET_ORDER
from data_intensive_computing.task2.config import PRODUCT_ORDER
from data_intensive_computing.task5.config import (
    B1_INCREMENTAL_MONITORING_ON,
    B2_INCREMENTAL_MONITORING_OFF,
    B3_FULL_RECOMPUTE,
    MAX_TAXI_UPDATE_SPAN_DAYS,
    PHASE_A_CONFIGS,
    PHASE_B_CONFIGS,
    TASK2_TABLES,
    TRACKED_TABLES,
    Task1Config,
    results_path,
    table_path,
)
from data_intensive_computing.task5.snapshot import (
    extend_retention,
    is_delta,
    load_snapshot,
    measure_storage,
    restore,
    save_snapshot,
    save_storage,
    take_snapshot,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


# --------------------------------------------------------------------------
# Result recording
# --------------------------------------------------------------------------


def _git_info(root: Path) -> dict:
    def run(*args: str) -> str:
        completed = subprocess.run(
            ["git", "--no-optional-locks", *args],
            cwd=root,
            capture_output=True,
            text=True,
            check=False,
        )
        return completed.stdout.strip()

    return {"commit": run("rev-parse", "HEAD"), "dirty": bool(run("status", "--porcelain"))}


class Recorder:
    """Append one JSON line per measurement with shared run context."""

    def __init__(self, spark: "SparkSession", root: Path, phase: str) -> None:
        self.root = root
        self.phase = phase
        self.session_id = str(uuid.uuid4())
        conf = spark.sparkContext.getConf()
        self.context = {
            "session_id": self.session_id,
            "git": _git_info(root),
            "spark": {
                "version": spark.version,
                "delta_spark": package_version("delta-spark"),
                "master": conf.get("spark.master", ""),
                "driver_memory": conf.get("spark.driver.memory", ""),
                "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions"),
            },
        }

    def write(self, record: dict) -> None:
        path = results_path(self.root)
        path.parent.mkdir(parents=True, exist_ok=True)
        row = {
            "recorded_at": datetime.now(timezone.utc).isoformat(),
            "phase": self.phase,
            **record,
            **self.context,
        }
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, sort_keys=True, default=str) + "\n")


def _rotated(items: tuple, repetition: int) -> tuple:
    """Rotate run order each repetition so no configuration always runs first."""
    shift = repetition % len(items)
    return items[shift:] + items[:shift]


def _check_same(reference: dict, key: tuple, value: int, what: str) -> None:
    """Every repetition must do identical work; otherwise restore failed."""
    if key not in reference:
        reference[key] = value
    elif reference[key] != value:
        raise RuntimeError(
            f"{what} changed between repetitions for {key}: "
            f"{reference[key]} -> {value}. The tables were probably not restored "
            "correctly; run `restore` before continuing."
        )


# --------------------------------------------------------------------------
# Full recompute helpers (preparation baseline and Phase B3)
# --------------------------------------------------------------------------


def full_rebuild_products(spark: "SparkSession", root: Path) -> dict[str, float]:
    """Compute all six products from the complete integrated table and overwrite them.

    Uses the same SQL and metadata as Task 2 so the output schema matches what
    the incremental refresh later merges into.
    """
    from pyspark import StorageLevel

    from data_intensive_computing.task2.products import (
        PRODUCT_SQL,
        _add_product_metadata,
    )

    started = time.perf_counter()
    integrated = (
        spark.read.format("delta")
        .load(str(table_path(root, "integrated_taxi_trips")))
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    integrated.count()
    integrated.createOrReplaceTempView("_task2_full_integrated")
    integrated.createOrReplaceTempView("_task2_affected_integrated")
    timings = {"_load_integrated": round(time.perf_counter() - started, 4)}

    now = datetime.now(timezone.utc)
    for name in PRODUCT_ORDER:
        product_started = time.perf_counter()
        (
            _add_product_metadata(spark.sql(PRODUCT_SQL[name]), now, now)
            .write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(str(table_path(root, f"analytical_products/{name}")))
        )
        timings[name] = round(time.perf_counter() - product_started, 4)
    integrated.unpersist()
    return timings


def full_rebuild_integrated(spark: "SparkSession", root: Path) -> float:
    """Re-enrich every processed taxi trip and overwrite integrated_taxi_trips."""
    from data_intensive_computing.task2.integration import enrich_taxi_trips

    started = time.perf_counter()
    trips = spark.read.format("delta").load(str(table_path(root, "taxi_trips")))
    (
        enrich_taxi_trips(spark, root, trips)
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("pickup_year", "pickup_month")
        .save(str(table_path(root, "integrated_taxi_trips")))
    )
    return round(time.perf_counter() - started, 4)


# --------------------------------------------------------------------------
# Preparation
# --------------------------------------------------------------------------


def _step(title: str) -> float:
    print(f"\n=== {title}", flush=True)
    return time.perf_counter()


def _done(started: float) -> float:
    elapsed = round(time.perf_counter() - started, 1)
    print(f"    done in {elapsed} s", flush=True)
    return elapsed


def check_taxi_update(spark: "SparkSession", root: Path) -> dict:
    """Verify generated taxi trips start after the original data and stay close to it."""
    from pyspark.sql import functions as F

    config = DATASETS["taxi_trips"]
    manifest = json.loads(
        (root / "artifacts" / "task1_update_manifest.json").read_text(encoding="utf-8")
    )
    expected_new = next(
        item["new_records"]
        for item in manifest["datasets"]
        if item["dataset_name"] == "taxi_trips"
    )
    # Compare epoch seconds inside Spark. Both sides are first interpreted in
    # the session time zone exactly as ingestion does (raw TLC timestamps are
    # zone-less New York wall-clock times). Collecting timestamps to Python
    # and sending them back with F.lit() would add another time zone
    # conversion and can hide a shift.
    def pickup_epoch():
        return F.unix_timestamp(F.col("tpep_pickup_datetime").cast("timestamp"))

    def as_local_text(epoch: int) -> str:
        return spark.range(1).select(F.from_unixtime(F.lit(epoch))).first()[0]

    raw_max_epoch = int(
        spark.read.parquet(str(config.source_path(root))).agg(F.max(pickup_epoch())).first()[0]
    )
    update = spark.read.parquet(str(config.update_path(root))).withColumn(
        "_epoch", pickup_epoch()
    )
    row = update.agg(
        F.count("*").alias("rows"),
        F.max("_epoch").alias("max_epoch"),
        F.min(F.when(F.col("_epoch") > raw_max_epoch, F.col("_epoch"))).alias(
            "first_new_epoch"
        ),
        F.sum((F.col("_epoch") > raw_max_epoch).cast("long")).alias("after_original"),
    ).first()
    new_by_month = {
        f"{r['y']}-{r['m']:02d}": int(r["n"])
        for r in update.filter(F.col("_epoch") > raw_max_epoch)
        .groupBy(
            F.year(F.col("tpep_pickup_datetime").cast("timestamp")).alias("y"),
            F.month(F.col("tpep_pickup_datetime").cast("timestamp")).alias("m"),
        )
        .agg(F.count("*").alias("n"))
        .orderBy("y", "m")
        .collect()
    }
    check = {
        "session_time_zone": spark.conf.get("spark.sql.session.timeZone"),
        "original_last_pickup": as_local_text(raw_max_epoch),
        "first_new_pickup": as_local_text(int(row["first_new_epoch"]))
        if row["first_new_epoch"] is not None
        else None,
        "last_new_pickup": as_local_text(int(row["max_epoch"])),
        "update_rows": int(row["rows"]),
        "new_trips_after_original": int(row["after_original"]),
        "expected_new_trips": int(expected_new),
        "new_trips_by_month": new_by_month,
    }
    for key, value in check.items():
        print(f"    {key}: {value}")

    problems = []
    if check["new_trips_after_original"] != check["expected_new_trips"]:
        problems.append(
            f"only {check['new_trips_after_original']} of {check['expected_new_trips']} "
            "generated trips start after the original data (time zone shift?)"
        )
    if row["max_epoch"] > raw_max_epoch + MAX_TAXI_UPDATE_SPAN_DAYS * 86400:
        problems.append(
            f"new trips reach {check['last_new_pickup']}, more than "
            f"{MAX_TAXI_UPDATE_SPAN_DAYS} days after the original data; a malformed "
            "old timestamp was probably sampled as the earliest trip"
        )
    if problems:
        raise RuntimeError(
            "Generated taxi update looks wrong: "
            + "; ".join(problems)
            + ". Discuss with the Task 1 owner before measuring."
        )
    return check


def prepare(
    spark: "SparkSession",
    root: Path,
    *,
    regenerate: bool = False,
    resnapshot: bool = False,
) -> None:
    """Build missing baseline state once and record the starting snapshot S0."""
    from data_intensive_computing.task1.updates import generate_all_updates

    recorder = Recorder(spark, root, "prepare")
    existing = load_snapshot(root, "S0")
    if existing is not None and not resnapshot:
        print("Snapshot S0 already exists; restoring to it before checking state.")
        restore(spark, root, existing)

    started = _step("1/4 Analytical products baseline")
    missing = [
        name
        for name in PRODUCT_ORDER
        if not is_delta(table_path(root, f"analytical_products/{name}"))
    ]
    if missing:
        print(f"    missing products: {missing}; building all six from integrated")
        timings = full_rebuild_products(spark, root)
        for name, seconds in timings.items():
            print(f"    {name}: {seconds} s")
        recorder.write({"kind": "prepare", "step": "build_products", "timings": timings})
    else:
        print("    all six products already exist")
    _done(started)

    started = _step("2/4 Generate update files")
    updates_exist = all(
        DATASETS[name].update_path(root).exists() for name in TASK1_DATASET_ORDER
    )
    if updates_exist and not regenerate:
        print("    update files already exist (use --regenerate to rebuild them)")
    else:
        results = generate_all_updates(spark, root)
        for result in results:
            print(
                f"    {result.dataset_name}: {result.new_records} new, "
                f"{result.duplicate_records} duplicate"
            )
    _done(started)

    started = _step("3/4 Check generated taxi trips")
    check = check_taxi_update(spark, root)
    recorder.write({"kind": "prepare", "step": "check_taxi_update", **check})
    _done(started)

    started = _step("4/4 Snapshot S0 (starting versions for every measurement)")
    if existing is not None and not resnapshot:
        print(f"    keeping existing S0: {existing}")
    else:
        # Commit a fresh version with a long retention period on every table,
        # so S0 is recent and RESTORE to it is never blocked by Delta.
        for name in TRACKED_TABLES:
            extend_retention(spark, table_path(root, name))
        versions = take_snapshot(spark, root, TRACKED_TABLES)
        save_snapshot(root, "S0", versions)
        print(f"    saved S0: {versions}")
    _done(started)


def restore_s0(spark: "SparkSession", root: Path) -> None:
    versions = _require_snapshot(root, "S0")
    restored = restore(spark, root, versions)
    print(f"Restored to S0: {restored or 'nothing to restore'}")


def _require_snapshot(root: Path, key: str) -> dict[str, int]:
    versions = load_snapshot(root, key)
    if versions is None:
        raise RuntimeError(f"Snapshot {key} not found. Run `prepare` first.")
    return versions


# --------------------------------------------------------------------------
# Phase A: Task 1 incremental update
# --------------------------------------------------------------------------


def _run_task1(
    spark: "SparkSession",
    root: Path,
    config: Task1Config,
) -> list[dict]:
    """Apply all three update files once, timing each dataset from outside."""
    from data_intensive_computing.task1.pipeline import apply_incremental_update

    measurements = []
    for dataset_name in TASK1_DATASET_ORDER:
        started = time.perf_counter()
        result = apply_incremental_update(
            spark,
            root,
            dataset_name,
            monitoring_enabled=config.monitoring_enabled,
            validation_enabled=config.validation_enabled,
        )
        wall = round(time.perf_counter() - started, 4)
        measurements.append(
            {
                "scope": dataset_name,
                "wall_seconds": wall,
                "internal_seconds": result.execution_time_seconds,
                "run_id": result.run_id,
                "processed_records": result.processed_records,
                "inserted_records": result.inserted_records,
                "rejected_records": result.rejected_records,
                "duplicate_records": result.duplicate_records,
                "status": result.status,
            }
        )
    measurements.append(
        {
            "scope": "all_datasets",
            "wall_seconds": round(sum(m["wall_seconds"] for m in measurements), 4),
        }
    )
    return measurements


def _print_round(label: str, measurements: list[dict]) -> None:
    parts = ", ".join(f"{m['scope']} {m['wall_seconds']:.1f}s" for m in measurements)
    print(f"    {label}: {parts}", flush=True)


def run_phase_a(spark: "SparkSession", root: Path, repetitions: int) -> None:
    s0 = _require_snapshot(root, "S0")
    recorder = Recorder(spark, root, "A")
    restore(spark, root, s0)

    # Warm-up: discarded for timing, used once for storage overhead.
    started = _step("Phase A warm-up (not timed; measures storage)")
    storage_before = measure_storage(spark, root)
    warmup = _run_task1(spark, root, PHASE_A_CONFIGS[0])
    storage_after = measure_storage(spark, root)
    save_storage(root, "phase_a_task1", storage_before, storage_after)
    for item in warmup:
        recorder.write({"kind": "measurement", "config": PHASE_A_CONFIGS[0].name,
                        "repetition": 0, "warmup": True, **item})
    _print_round("warm-up", warmup)
    restore(spark, root, s0)
    round_seconds = _done(started)
    total_runs = repetitions * len(PHASE_A_CONFIGS)
    print(
        f"    estimate for {repetitions} repetitions x {len(PHASE_A_CONFIGS)} "
        f"configurations: about {round(round_seconds * total_runs / 60)} min",
        flush=True,
    )

    reference: dict = {}
    for repetition in range(1, repetitions + 1):
        started = _step(f"Phase A repetition {repetition}/{repetitions}")
        for config in _rotated(PHASE_A_CONFIGS, repetition - 1):
            measurements = _run_task1(spark, root, config)
            for item in measurements:
                if "inserted_records" in item:
                    _check_same(reference, (config.name, item["scope"]),
                                item["inserted_records"], "inserted_records")
                recorder.write({"kind": "measurement", "config": config.name,
                                "repetition": repetition, "warmup": False, **item})
            _print_round(config.name, measurements)
            restore(spark, root, s0)
        _done(started)


# --------------------------------------------------------------------------
# Phase B: Task 2 analytical refresh
# --------------------------------------------------------------------------


def _run_task2_incremental(
    spark: "SparkSession",
    root: Path,
    taxi_run_id: str,
    monitoring_enabled: bool,
) -> list[dict]:
    from data_intensive_computing.task2.pipeline import maintain_analytical_consistency

    started = time.perf_counter()
    result = maintain_analytical_consistency(
        spark,
        root,
        taxi_run_id=taxi_run_id,
        monitoring_enabled=monitoring_enabled,
    )
    wall = round(time.perf_counter() - started, 4)
    if result.integration.status != "SUCCESS":
        raise RuntimeError(
            f"Integration status {result.integration.status}: no pending trips were "
            "found, so integrated_taxi_trips was probably not restored."
        )
    measurements = [
        {
            "scope": "total",
            "wall_seconds": wall,
            "internal_seconds": result.execution_time_seconds,
            "pending_records": result.integration.pending_records,
            "integrated_records": result.integration.integrated_records,
            "affected_periods": [list(p) for p in result.integration.affected_periods],
        },
        {
            "scope": "integration",
            "internal_seconds": result.integration.execution_time_seconds,
        },
    ]
    measurements.extend(
        {
            "scope": product.product_name,
            "internal_seconds": product.execution_time_seconds,
            "rows_written": product.rows_written,
        }
        for product in result.product_results
    )
    return measurements


def _run_task2_full(spark: "SparkSession", root: Path) -> list[dict]:
    started = time.perf_counter()
    integration_seconds = full_rebuild_integrated(spark, root)
    product_timings = full_rebuild_products(spark, root)
    wall = round(time.perf_counter() - started, 4)
    measurements = [
        {"scope": "total", "wall_seconds": wall},
        {"scope": "integration", "internal_seconds": integration_seconds},
    ]
    measurements.extend(
        {"scope": name, "internal_seconds": seconds}
        for name, seconds in product_timings.items()
        if not name.startswith("_")
    )
    return measurements


def run_phase_b(spark: "SparkSession", root: Path, repetitions: int) -> None:
    s0 = _require_snapshot(root, "S0")
    recorder = Recorder(spark, root, "B")
    restore(spark, root, s0)

    # Setup: one production Task 1 run creates the new trips Task 2 must refresh.
    started = _step("Phase B setup: apply Task 1 once (not timed)")
    setup = _run_task1(spark, root, PHASE_A_CONFIGS[0])
    _print_round("setup", setup)
    taxi_run_id = next(m["run_id"] for m in setup if m["scope"] == "taxi_trips")
    s1 = take_snapshot(spark, root, TASK2_TABLES)
    save_snapshot(root, "S1", s1)
    print(f"    taxi run id: {taxi_run_id}; saved S1 for Task 2 tables")
    _done(started)

    runners: dict[str, Callable[[], list[dict]]] = {
        B1_INCREMENTAL_MONITORING_ON: lambda: _run_task2_incremental(
            spark, root, taxi_run_id, monitoring_enabled=True
        ),
        B2_INCREMENTAL_MONITORING_OFF: lambda: _run_task2_incremental(
            spark, root, taxi_run_id, monitoring_enabled=False
        ),
        B3_FULL_RECOMPUTE: lambda: _run_task2_full(spark, root),
    }

    started = _step("Phase B warm-up (not timed; measures storage)")
    storage_before = measure_storage(spark, root)
    warmup = runners[B1_INCREMENTAL_MONITORING_ON]()
    storage_after = measure_storage(spark, root)
    save_storage(root, "phase_b_task2", storage_before, storage_after)
    for item in warmup:
        recorder.write({"kind": "measurement", "config": B1_INCREMENTAL_MONITORING_ON,
                        "repetition": 0, "warmup": True, **item})
    _print_round("warm-up", [m for m in warmup if "wall_seconds" in m])
    restore(spark, root, s1)
    _done(started)

    reference: dict = {}
    for repetition in range(1, repetitions + 1):
        started = _step(f"Phase B repetition {repetition}/{repetitions}")
        for config_name in _rotated(PHASE_B_CONFIGS, repetition - 1):
            measurements = runners[config_name]()
            for item in measurements:
                if "integrated_records" in item:
                    _check_same(reference, (config_name, item["scope"]),
                                item["integrated_records"], "integrated_records")
                recorder.write({"kind": "measurement", "config": config_name,
                                "repetition": repetition, "warmup": False, **item})
            _print_round(config_name, [m for m in measurements if "wall_seconds" in m])
            restore(spark, root, s1)
        _done(started)

    # Leave the platform exactly as it was before the evaluation.
    restore(spark, root, s0)
    print("\nRestored every table to S0.")


__all__ = [
    "full_rebuild_integrated",
    "full_rebuild_products",
    "prepare",
    "restore_s0",
    "run_phase_a",
    "run_phase_b",
]
