"""Public Task 2 pipeline for maintaining analytical consistency."""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Callable

from data_intensive_computing.task2.config import PRODUCT_ORDER, TASK2_SCHEMA_VERSION
from data_intensive_computing.task2.integration import update_integrated_taxi_trips
from data_intensive_computing.task2.models import (
    AnalyticalConsistencyResult,
    ProductRefreshResult,
)
from data_intensive_computing.task2.products import refresh_analytical_products

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def maintain_analytical_consistency(
    spark: "SparkSession",
    project_root: Path,
    *,
    taxi_run_id: str | None = None,
    product_names: tuple[str, ...] = PRODUCT_ORDER,
    run_id: str | None = None,
    progress_callback: Callable[[ProductRefreshResult], None] | None = None,
) -> AnalyticalConsistencyResult:
    """Integrate pending trips and refresh only their affected product months."""
    run_id = run_id or str(uuid.uuid4())
    started_clock = time.perf_counter()
    started_at = datetime.now(timezone.utc)

    integration = update_integrated_taxi_trips(
        spark,
        project_root,
        taxi_run_id=taxi_run_id,
    )
    product_results = refresh_analytical_products(
        spark,
        project_root,
        integration.affected_periods,
        product_names=product_names,
        progress_callback=progress_callback,
    )
    completed_at = datetime.now(timezone.utc)
    result = AnalyticalConsistencyResult(
        run_id=run_id,
        started_at=started_at.isoformat(),
        completed_at=completed_at.isoformat(),
        source_taxi_run_id=taxi_run_id,
        integration=integration,
        product_results=product_results,
        execution_time_seconds=round(time.perf_counter() - started_clock, 4),
        schema_version=TASK2_SCHEMA_VERSION,
        status="SUCCESS" if integration.status in {"SUCCESS", "SKIPPED"} else "FAILED",
    )

    artifact = project_root / "artifacts" / "task2_refresh_results.jsonl"
    artifact.parent.mkdir(parents=True, exist_ok=True)
    with artifact.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(asdict(result), sort_keys=True) + "\n")
    return result

