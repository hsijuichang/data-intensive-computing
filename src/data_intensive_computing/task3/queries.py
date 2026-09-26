"""Spark SQL monitoring queries required by Task 3."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from data_intensive_computing.task3.config import (
    MONITORING_WRITES_TABLE,
    PIPELINE_RUNS_TABLE,
    VALIDATION_FAILURES_TABLE,
    monitoring_writes_path,
    pipeline_runs_path,
    validation_failures_path,
)
from data_intensive_computing.task3.writer import ensure_monitoring_tables

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


MONITORING_QUERIES = {
    "validation_failures": f"""
        WITH failure_by_run AS (
            SELECT
                run_id,
                dataset_name,
                SUM(failure_count) AS detailed_failures
            FROM {VALIDATION_FAILURES_TABLE}
            GROUP BY run_id, dataset_name
        ),
        run_failures AS (
            SELECT
                runs.run_id,
                runs.dataset_name,
                COALESCE(events.detailed_failures, runs.rejected_records, 0)
                    AS failure_count
            FROM {PIPELINE_RUNS_TABLE} AS runs
            LEFT JOIN failure_by_run AS events
                ON runs.run_id = events.run_id
               AND runs.dataset_name = events.dataset_name
        )
        SELECT
            dataset_name,
            SUM(failure_count) AS total_validation_failures,
            SUM(CASE WHEN failure_count > 0 THEN 1 ELSE 0 END)
                AS failing_executions
        FROM run_failures
        GROUP BY dataset_name
        ORDER BY total_validation_failures DESC, dataset_name
    """,
    "longest_processing_time": f"""
        SELECT
            dataset_name,
            COALESCE(pipeline_type, 'initial_ingestion') AS pipeline_type,
            COUNT(*) AS execution_count,
            ROUND(AVG(execution_time_seconds), 4) AS average_seconds,
            ROUND(MAX(execution_time_seconds), 4) AS maximum_seconds
        FROM {PIPELINE_RUNS_TABLE}
        GROUP BY dataset_name, COALESCE(pipeline_type, 'initial_ingestion')
        ORDER BY maximum_seconds DESC, dataset_name
    """,
    "rejected_by_execution": f"""
        SELECT
            run_id,
            dataset_name,
            COALESCE(pipeline_type, 'initial_ingestion') AS pipeline_type,
            started_at,
            input_records,
            inserted_records,
            rejected_records,
            duplicate_records
        FROM {PIPELINE_RUNS_TABLE}
        ORDER BY started_at DESC, dataset_name
    """,
    "processing_time_trend": f"""
        WITH timings AS (
            SELECT
                run_id,
                dataset_name,
                COALESCE(pipeline_type, 'initial_ingestion') AS pipeline_type,
                started_at,
                execution_time_seconds,
                LAG(execution_time_seconds) OVER (
                    PARTITION BY dataset_name, pipeline_type
                    ORDER BY started_at
                ) AS previous_seconds
            FROM {PIPELINE_RUNS_TABLE}
        )
        SELECT
            run_id,
            dataset_name,
            pipeline_type,
            started_at,
            execution_time_seconds,
            previous_seconds,
            ROUND(execution_time_seconds - previous_seconds, 4) AS change_seconds
        FROM timings
        ORDER BY dataset_name, pipeline_type, started_at
    """,
    "monitoring_overhead": f"""
        SELECT
            pipeline_type,
            dataset_name,
            COUNT(*) AS write_count,
            ROUND(AVG(execution_time_seconds), 4) AS average_write_seconds,
            ROUND(MAX(execution_time_seconds), 4) AS maximum_write_seconds
        FROM {MONITORING_WRITES_TABLE}
        GROUP BY pipeline_type, dataset_name
        ORDER BY average_write_seconds DESC
    """,
}


def register_monitoring_views(
    spark: "SparkSession",
    project_root: Path,
) -> None:
    """Register stable view names over the three monitoring Delta tables."""
    ensure_monitoring_tables(spark, project_root)
    for name, path in (
        (PIPELINE_RUNS_TABLE, pipeline_runs_path(project_root)),
        (VALIDATION_FAILURES_TABLE, validation_failures_path(project_root)),
        (MONITORING_WRITES_TABLE, monitoring_writes_path(project_root)),
    ):
        spark.read.format("delta").load(str(path)).createOrReplaceTempView(name)


def run_monitoring_queries(
    spark: "SparkSession",
    project_root: Path,
    query_names: tuple[str, ...] | None = None,
) -> dict[str, "DataFrame"]:
    """Execute selected monitoring SQL and return query-name/DataFrame pairs."""
    register_monitoring_views(spark, project_root)
    selected = query_names or tuple(MONITORING_QUERIES)
    unknown = sorted(set(selected) - set(MONITORING_QUERIES))
    if unknown:
        raise ValueError(f"Unknown monitoring queries: {unknown}")
    return {name: spark.sql(MONITORING_QUERIES[name]) for name in selected}
