"""Delta-backed isolation for rejected validation records."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def append_quarantine(frame: "DataFrame", path: Path) -> None:
    """Append rejected records when at least one row is present."""
    if frame.limit(1).count() == 0:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    (
        frame.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(str(path))
    )

