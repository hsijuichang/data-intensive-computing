"""Composable generic validation rule factories."""

from __future__ import annotations

from data_intensive_computing.task4.models import Severity, ValidationRule


def numeric_range(
    column: str,
    minimum: float | None = None,
    maximum: float | None = None,
    *,
    code: str | None = None,
    severity: Severity = "ERROR",
) -> ValidationRule:
    """Reject non-null numeric values outside an inclusive range."""

    def condition(_frame):
        from pyspark.sql import functions as F

        value = F.col(column)
        invalid = F.lit(False)
        if minimum is not None:
            invalid |= value < F.lit(minimum)
        if maximum is not None:
            invalid |= value > F.lit(maximum)
        return value.isNotNull() & F.coalesce(invalid, F.lit(False))

    bounds = f"[{minimum if minimum is not None else '-inf'}, {maximum if maximum is not None else 'inf'}]"
    return ValidationRule(
        code=code or f"out_of_range:{column}",
        description=f"{column} must be inside {bounds}",
        condition=condition,
        severity=severity,
    )


def non_negative(
    column: str,
    *,
    code: str | None = None,
    severity: Severity = "ERROR",
) -> ValidationRule:
    return numeric_range(
        column,
        minimum=0,
        code=code or f"negative_value:{column}",
        severity=severity,
    )


def not_nan(
    column: str,
    *,
    code: str | None = None,
    severity: Severity = "ERROR",
) -> ValidationRule:
    def condition(_frame):
        from pyspark.sql import functions as F

        return F.col(column).isNotNull() & F.isnan(column)

    return ValidationRule(
        code=code or f"invalid_nan:{column}",
        description=f"{column} must not be NaN",
        condition=condition,
        severity=severity,
    )


def timestamp_order(
    start_column: str,
    end_column: str,
    *,
    code: str = "invalid_timestamp_order",
    severity: Severity = "ERROR",
) -> ValidationRule:
    def condition(_frame):
        from pyspark.sql import functions as F

        return (
            F.col(start_column).isNotNull()
            & F.col(end_column).isNotNull()
            & (F.col(end_column) < F.col(start_column))
        )

    return ValidationRule(
        code=code,
        description=f"{end_column} must not precede {start_column}",
        condition=condition,
        severity=severity,
    )

