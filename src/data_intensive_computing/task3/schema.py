"""Delta schemas for operational monitoring tables."""

from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# The first 18 fields match the Week 1 ingestion_runs table exactly. Remaining
# fields are additive schema evolution for Week 3 monitoring.
PIPELINE_RUN_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("dataset_name", StringType(), False),
        StructField("source_format", StringType(), False),
        StructField("source_path", StringType(), False),
        StructField("processed_path", StringType(), False),
        StructField("failed_path", StringType(), False),
        StructField("started_at", TimestampType(), False),
        StructField("completed_at", TimestampType(), False),
        StructField("execution_time_seconds", DoubleType(), False),
        StructField("input_records", LongType(), False),
        StructField("processed_records", LongType(), False),
        StructField("rejected_records", LongType(), False),
        StructField("duplicate_records", LongType(), False),
        StructField("schema_version", StringType(), False),
        StructField("source_schema_json", StringType(), True),
        StructField("output_schema_json", StringType(), True),
        StructField("status", StringType(), False),
        StructField("error_message", StringType(), True),
        StructField("pipeline_type", StringType(), True),
        StructField("inserted_records", LongType(), True),
        StructField("warning_records", LongType(), True),
        StructField("validation_failure_count", LongType(), True),
        StructField("validation_failures_json", StringType(), True),
        StructField("schema_changes_json", StringType(), True),
        StructField("recorded_at", TimestampType(), True),
    ]
)


VALIDATION_FAILURE_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("run_id", StringType(), False),
        StructField("dataset_name", StringType(), False),
        StructField("pipeline_type", StringType(), False),
        StructField("failure_code", StringType(), False),
        StructField("failure_count", LongType(), False),
        StructField("recorded_at", TimestampType(), False),
    ]
)


MONITORING_WRITE_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("dataset_name", StringType(), False),
        StructField("pipeline_type", StringType(), False),
        StructField("execution_time_seconds", DoubleType(), False),
        StructField("validation_events_written", LongType(), False),
        StructField("record_inserted", BooleanType(), False),
        StructField("status", StringType(), False),
        StructField("recorded_at", TimestampType(), False),
    ]
)
