"""Structured results returned by the Task 2 analytical refresh pipeline."""

from __future__ import annotations

from dataclasses import dataclass


YearMonth = tuple[int, int]


@dataclass(frozen=True)
class IntegrationRefreshResult:
    """Outcome of incrementally enriching taxi trips."""

    source_taxi_run_id: str | None
    candidate_records: int
    pending_records: int
    integrated_records: int
    affected_periods: tuple[YearMonth, ...]
    weather_match_rate: float | None
    air_quality_match_rate: float | None
    execution_time_seconds: float
    status: str


@dataclass(frozen=True)
class ProductRefreshResult:
    """Outcome of refreshing one analytical Delta product."""

    product_name: str
    affected_periods: tuple[YearMonth, ...]
    rows_written: int
    execution_time_seconds: float
    status: str


@dataclass(frozen=True)
class AnalyticalConsistencyResult:
    """Top-level result designed for monitoring and evaluation callers."""

    run_id: str
    started_at: str
    completed_at: str
    source_taxi_run_id: str | None
    integration: IntegrationRefreshResult
    product_results: tuple[ProductRefreshResult, ...]
    execution_time_seconds: float
    schema_version: str
    status: str

