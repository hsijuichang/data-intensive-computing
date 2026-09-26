"""Schema-change detection and supported-evolution policy."""

from __future__ import annotations

from typing import TYPE_CHECKING

from data_intensive_computing.task4.models import (
    SchemaPolicy,
    SchemaValidationResult,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def _schema_map(frame: "DataFrame", policy: SchemaPolicy) -> dict[str, str]:
    return {
        field.name: field.dataType.simpleString()
        for field in frame.schema.fields
        if not (policy.ignore_metadata_columns and field.name.startswith("_"))
    }


def validate_schema_change(
    candidate: "DataFrame",
    target: "DataFrame",
    policy: SchemaPolicy,
) -> SchemaValidationResult:
    """Compare candidate and target schemas without throwing on data changes."""
    actual = _schema_map(candidate, policy)
    expected = _schema_map(target, policy)
    actual_columns = set(actual)
    expected_columns = set(expected)
    missing = tuple(sorted(expected_columns - actual_columns))
    additions = tuple(sorted(actual_columns - expected_columns))
    unexpected = tuple(
        name for name in additions if name not in policy.allowed_additions
    )

    type_changes = {
        name: (expected[name], actual[name])
        for name in sorted(expected_columns & actual_columns)
        if expected[name] != actual[name]
    }
    addition_type_errors = {
        name: actual[name]
        for name in additions
        if name in policy.allowed_additions
        and actual[name] not in policy.allowed_additions[name]
    }

    errors = [f"schema:missing_column:{name}" for name in missing]
    errors.extend(f"schema:unexpected_column:{name}" for name in unexpected)
    errors.extend(
        f"schema:type_change:{name}:{old}->{new}"
        for name, (old, new) in type_changes.items()
    )
    errors.extend(
        f"schema:unsupported_addition_type:{name}:{actual_type}"
        for name, actual_type in addition_type_errors.items()
    )
    return SchemaValidationResult(
        supported=not errors,
        added_columns=additions,
        missing_columns=missing,
        unexpected_columns=unexpected,
        type_changes=type_changes,
        errors=tuple(errors),
    )

