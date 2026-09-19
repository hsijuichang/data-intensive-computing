"""Shared setup and Delta publishing helpers for the Week 2 data products."""

import os
from datetime import datetime, timezone
from pathlib import Path


PROJECT_ROOT = next(
    parent for parent in (Path(__file__).resolve().parent, *Path(__file__).resolve().parents)
    if (parent / "pyproject.toml").is_file()
)
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
SOURCE_PATH = PROCESSED_DIR / "integrated_taxi_trips"
PRODUCT_DIR = PROCESSED_DIR / "analytical_products"
SCHEMA_VERSION = "1.0.0"


def get_spark():
    """Use the same Delta and time-zone settings as the Week 1 notebooks."""
    if os.name == "nt":
        from data_intensive_computing.spark_env import configure_windows_spark_env
        configure_windows_spark_env()

    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName("Lab2-Analytical-Data-Products")
        .master("local[*]")
        .config("spark.driver.memory", "6g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "America/New_York")
    spark.sparkContext.setLogLevel("WARN")
    return spark


def register_integrated(spark):
    """Register the Week 1 integrated Delta table for the SQL below."""
    if not SOURCE_PATH.exists():
        raise FileNotFoundError(f"Missing {SOURCE_PATH}; run Week 1 integration first")
    spark.read.format("delta").load(str(SOURCE_PATH)).createOrReplaceTempView(
        "integrated_taxi_trips"
    )


def publish_product(spark, frame, name, partition_by=()):
    """Replace one product atomically and retain its original creation timestamp."""
    from pyspark.sql import functions as F

    path = PRODUCT_DIR / name
    refreshed_at = datetime.now(timezone.utc)
    created_at = refreshed_at
    if (path / "_delta_log").exists():
        old = spark.read.format("delta").load(str(path))
        prior = old.select(F.min("created_at").alias("created_at")).first()
        if prior and prior["created_at"] is not None:
            created_at = prior["created_at"]

    # Each row carries the metadata requested for a reusable data product.
    product = (
        frame.withColumn("source_table", F.lit("integrated_taxi_trips"))
        .withColumn("created_at", F.lit(created_at).cast("timestamp"))
        .withColumn("refreshed_at", F.lit(refreshed_at).cast("timestamp"))
        .withColumn("schema_version", F.lit(SCHEMA_VERSION))
    )
    writer = product.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(str(path))
    print(f"Saved {name} to {path}")
    return spark.read.format("delta").load(str(path))
