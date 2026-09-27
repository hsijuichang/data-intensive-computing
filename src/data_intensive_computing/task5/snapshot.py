"""Delta version snapshots, RESTORE, and storage measurement."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from data_intensive_computing.task5.config import (
    FAILED_TABLES,
    METADATA_TABLES,
    TRACKED_TABLES,
    snapshot_path,
    storage_path,
    table_path,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def is_delta(path: Path) -> bool:
    return (path / "_delta_log").is_dir()


def current_version(spark: "SparkSession", path: Path) -> int:
    from delta.tables import DeltaTable

    return int(
        DeltaTable.forPath(spark, str(path)).history(1).select("version").first()[0]
    )


def take_snapshot(
    spark: "SparkSession",
    root: Path,
    names: tuple[str, ...] = TRACKED_TABLES,
) -> dict[str, int]:
    """Record the current Delta version of every table under test."""
    versions = {}
    for name in names:
        path = table_path(root, name)
        if not is_delta(path):
            raise FileNotFoundError(
                f"Cannot snapshot missing Delta table {path}. Run `prepare` first."
            )
        versions[name] = current_version(spark, path)
    return versions


def save_snapshot(root: Path, key: str, versions: dict[str, int]) -> None:
    path = snapshot_path(root)
    data = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    data[key] = {
        "taken_at": datetime.now(timezone.utc).isoformat(),
        "versions": versions,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def load_snapshot(root: Path, key: str) -> dict[str, int] | None:
    path = snapshot_path(root)
    if not path.exists():
        return None
    entry = json.loads(path.read_text(encoding="utf-8")).get(key)
    return None if entry is None else {k: int(v) for k, v in entry["versions"].items()}


# Delta refuses to time travel to a version older than the table's
# deletedFileRetentionDuration (default 7 days), even if no VACUUM ever ran.
EXPERIMENT_RETENTION = "interval 365 days"
RETENTION_ERROR = "DELTA_UNSUPPORTED_TIME_TRAVEL_BEYOND_DELETED_FILE_RETENTION_DURATION"


def extend_retention(spark: "SparkSession", path: Path) -> None:
    """Allow RESTORE to older versions. This commits a new table version."""
    spark.sql(
        f"ALTER TABLE delta.`{path}` SET TBLPROPERTIES ("
        f"'delta.deletedFileRetentionDuration' = '{EXPERIMENT_RETENTION}', "
        f"'delta.logRetentionDuration' = '{EXPERIMENT_RETENTION}')"
    )


def restore(spark: "SparkSession", root: Path, versions: dict[str, int]) -> list[str]:
    """RESTORE each table to its snapshot version and drop Spark's cache.

    Returns the names of the tables that actually had to be restored.
    """
    from delta.tables import DeltaTable

    restored = []
    for name, version in versions.items():
        path = table_path(root, name)
        if current_version(spark, path) == version:
            continue
        try:
            DeltaTable.forPath(spark, str(path)).restoreToVersion(version)
        except Exception as error:
            if RETENTION_ERROR not in str(error):
                raise
            print(f"    {name}: version {version} is older than the retention "
                  "period; extending retention and retrying", flush=True)
            extend_retention(spark, path)
            DeltaTable.forPath(spark, str(path)).restoreToVersion(version)
        restored.append(name)
    spark.catalog.clearCache()
    return restored


def _disk_usage(path: Path) -> tuple[int, int, int, int]:
    """Return (all bytes on disk, _delta_log bytes, parquet bytes, parquet count)."""
    total = log = parquet_bytes = files = 0
    for directory, _, names in os.walk(path):
        in_log = "_delta_log" in Path(directory).parts
        for name in names:
            size = (Path(directory) / name).stat().st_size
            total += size
            if in_log:
                log += size
            elif name.endswith(".parquet"):
                parquet_bytes += size
                files += 1
    return total, log, parquet_bytes, files


def measure_storage(spark: "SparkSession", root: Path) -> dict[str, dict]:
    """Measure disk usage and current-version size of every relevant table.

    ``disk_bytes`` counts every file under the directory, including data files
    that only older Delta versions still reference. ``current_version_bytes``
    is Delta's own size of the latest version. The difference is what time
    travel keeps on disk.
    """
    locations = {name: table_path(root, name) for name in TRACKED_TABLES}
    locations.update(
        {f"failed/{name}": root / "data" / "failed" / name for name in FAILED_TABLES}
    )
    locations.update(
        {f"metadata/{name}": root / "data" / "metadata" / name for name in METADATA_TABLES}
    )

    result = {}
    for name, path in locations.items():
        if not path.exists():
            result[name] = {"exists": False}
            continue
        disk_bytes, log_bytes, parquet_bytes, parquet_files = _disk_usage(path)
        entry = {
            "exists": True,
            "disk_bytes": disk_bytes,
            "delta_log_bytes": log_bytes,
            "parquet_bytes_on_disk": parquet_bytes,
            "parquet_files_on_disk": parquet_files,
        }
        if is_delta(path):
            detail = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").first()
            entry["current_version_bytes"] = int(detail["sizeInBytes"])
            entry["current_version_files"] = int(detail["numFiles"])
            entry["delta_version"] = current_version(spark, path)
        result[name] = entry
    return result


def save_storage(root: Path, phase: str, before: dict, after: dict) -> None:
    path = storage_path(root)
    data = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    data[phase] = {
        "measured_at": datetime.now(timezone.utc).isoformat(),
        "before": before,
        "after": after,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")
