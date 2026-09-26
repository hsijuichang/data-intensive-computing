"""Data models for the reusable Task 4 validation framework."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Literal, Mapping

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame


Severity = Literal["ERROR", "WARNING"]
RuleCondition = Callable[["DataFrame"], "Column"]


@dataclass(frozen=True)
class ValidationRule:
    """A row-level rule whose condition evaluates to true for invalid rows."""

    code: str
    description: str
    condition: RuleCondition
    severity: Severity = "ERROR"


@dataclass(frozen=True)
class ReferenceRule:
    """A foreign-key-style rule evaluated through a left join."""

    code: str
    reference_name: str
    local_columns: tuple[str, ...]
    reference_columns: tuple[str, ...]
    description: str
    severity: Severity = "ERROR"


@dataclass(frozen=True)
class SchemaPolicy:
    """Supported additive schema evolution for one dataset."""

    allowed_additions: Mapping[str, frozenset[str]] = field(default_factory=dict)
    ignore_metadata_columns: bool = True


@dataclass(frozen=True)
class DatasetValidationConfig:
    name: str
    primary_key: tuple[str, ...]
    required_columns: tuple[str, ...]
    rules: tuple[ValidationRule, ...]
    reference_rules: tuple[ReferenceRule, ...] = ()
    schema_policy: SchemaPolicy = field(default_factory=SchemaPolicy)
    schema_version: str = "1.1.0"


@dataclass(frozen=True)
class SchemaValidationResult:
    supported: bool
    added_columns: tuple[str, ...]
    missing_columns: tuple[str, ...]
    unexpected_columns: tuple[str, ...]
    type_changes: Mapping[str, tuple[str, str]]
    errors: tuple[str, ...]


@dataclass(frozen=True)
class ValidationSummary:
    run_id: str
    dataset_name: str
    input_records: int
    valid_records: int
    rejected_records: int
    duplicate_records: int
    warning_records: int
    schema_version: str
    schema_changes: tuple[str, ...]
    failure_counts: Mapping[str, int]
    execution_time_seconds: float
    status: str
    quarantine_path: str | None


@dataclass
class ValidatedBatch:
    """DataFrames plus a serializable validation summary."""

    valid: "DataFrame"
    invalid: "DataFrame"
    warnings: "DataFrame"
    summary: ValidationSummary
    schema: SchemaValidationResult

    def unpersist(self) -> None:
        """Release DataFrames cached by the validation engine."""
        self.valid.unpersist()
        self.invalid.unpersist()
        self.warnings.unpersist()


PathLike = str | Path

