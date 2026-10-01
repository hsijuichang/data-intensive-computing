"""Spark session construction for command-line platform jobs."""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def _configure_windows_environment() -> None:
    """Configure the repository's bundled Hadoop binaries before importing Spark."""
    project_root = Path(__file__).resolve().parents[2]
    hadoop_home = project_root / "tools" / "hadoop"
    hadoop_bin = hadoop_home / "bin"
    if not (hadoop_bin / "winutils.exe").is_file():
        raise FileNotFoundError(
            f"Windows Spark requires {hadoop_bin / 'winutils.exe'}"
        )

    os.environ.setdefault("HADOOP_HOME", str(hadoop_home))
    os.environ.setdefault("hadoop.home.dir", str(hadoop_home))
    path_parts = os.environ.get("PATH", "").split(os.pathsep)
    if str(hadoop_bin) not in path_parts:
        os.environ["PATH"] = str(hadoop_bin) + os.pathsep + os.environ.get("PATH", "")
    # Spark's Windows worker launcher is unreliable when the virtualenv path
    # contains spaces. The uv-managed base interpreter has no project-path
    # spaces; Spark supplies pyspark to workers through PYTHONPATH.
    worker_python = getattr(sys, "_base_executable", sys.executable)
    os.environ.setdefault("PYSPARK_PYTHON", worker_python)
    spark_temp = Path(tempfile.gettempdir()) / "data-intensive-computing-spark"
    spark_temp.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("SPARK_LOCAL_DIRS", str(spark_temp))


def build_spark_session(
    app_name: str,
    *,
    driver_memory: str = "8g",
    shuffle_partitions: int = 32,
    master: str = "local[*]",
) -> "SparkSession":
    """Create a local Spark session with Delta Lake and the platform time zone."""
    if os.name == "nt":
        _configure_windows_environment()
        try:
            from data_intensive_computing.spark_env import configure_windows_spark_env
        except ImportError:
            configure_windows_spark_env = None
        if configure_windows_spark_env is not None:
            configure_windows_spark_env()

    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.driver.memory", driver_memory)
        .config("spark.default.parallelism", str(shuffle_partitions))
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.databricks.delta.snapshotPartitions", str(shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "America/New_York")
    spark.sparkContext.setLogLevel("WARN")
    return spark
