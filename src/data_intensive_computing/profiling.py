"""Configuration-driven source-data profiling for Task 1."""

from __future__ import annotations

import argparse
import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import yaml


def standardize_column_name(name: str) -> str:
    """Convert source column names to lower snake_case."""
    value = re.sub(r"[^0-9A-Za-z]+", "_", name.strip())
    value = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", value)
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", value)
    return re.sub(r"_+", "_", value).strip("_").lower()


def quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def quote_literal(value: str | Path) -> str:
    return "'" + str(value).replace("\\", "/").replace("'", "''") + "'"


def missing_expression(expression: str) -> str:
    return (
        f"({expression} IS NULL OR "
        f"trim(CAST({expression} AS VARCHAR)) = '')"
    )


class DatasetProfiler:
    def __init__(self, connection: duckdb.DuckDBPyConnection, data_dir: Path) -> None:
        self.connection = connection
        self.data_dir = data_dir

    def profile(self, config: dict[str, Any]) -> dict[str, Any]:
        files = sorted(self.data_dir.glob(config["file_pattern"]))
        if not files:
            raise FileNotFoundError(
                f"No files match {config['file_pattern']!r} in {self.data_dir}"
            )

        self._create_source_view(config["format"], files)
        schema = self._schema()
        summary = self._summary(config, schema)
        columns = self._column_profiles(schema, summary)
        column_lookup = {column["source_name"]: column for column in columns}
        file_rows = self.connection.execute(
            """
            SELECT __source_file AS source_file, count(*) AS row_count
            FROM source_data
            GROUP BY __source_file
            ORDER BY __source_file
            """
        ).fetchall()

        key_quality = self._key_quality(config["primary_key"])
        custom_quality = self._custom_quality(config.get("custom_checks", []))

        return {
            "dataset_name": config["name"],
            "entity": config["entity"],
            "format": config["format"],
            "source_files": [
                {"path": str(path), "size_bytes": path.stat().st_size}
                for path in files
            ],
            "source_file_rows": [
                {"source_file": str(path), "row_count": count}
                for path, count in file_rows
            ],
            "row_count": summary["row_count"],
            "column_count": len(schema),
            "columns": columns,
            "primary_key": config["primary_key"],
            "primary_key_note": config.get("primary_key_note"),
            "key_quality": key_quality,
            "join_attributes": config["join_attributes"],
            "temporal_attributes": self._temporal_profiles(config, summary),
            "categorical_attributes": [
                {
                    "name": name,
                    "distinct_count": summary[f"categorical_{index}_distinct"],
                    "null_count": column_lookup.get(name, {}).get("null_count"),
                }
                for index, name in enumerate(config.get("categorical", []))
            ],
            "numeric_quality": self._numeric_profiles(config, summary),
            "custom_quality": custom_quality,
            "growth": config["growth"],
        }

    def _create_source_view(self, file_format: str, files: list[Path]) -> None:
        paths = ", ".join(quote_literal(path.resolve()) for path in files)
        if file_format == "parquet":
            reader = f"read_parquet([{paths}], filename = true)"
        elif file_format == "csv":
            if len(files) != 1:
                raise ValueError("CSV profiling currently expects one file per dataset")
            reader = (
                f"read_csv({paths}, header = true, all_varchar = true, "
                "filename = true, parallel = true)"
            )
        else:
            raise ValueError(f"Unsupported source format: {file_format}")

        self.connection.execute("DROP VIEW IF EXISTS source_data")
        self.connection.execute(
            "CREATE TEMP VIEW source_data AS "
            f"SELECT * EXCLUDE (filename), filename AS __source_file FROM {reader}"
        )

    def _schema(self) -> list[dict[str, str]]:
        rows = self.connection.execute(
            "DESCRIBE SELECT * EXCLUDE (__source_file) FROM source_data"
        ).fetchall()
        return [
            {
                "source_name": row[0],
                "standard_name": standardize_column_name(row[0]),
                "source_type": row[1],
            }
            for row in rows
        ]

    def _summary(
        self, config: dict[str, Any], schema: list[dict[str, str]]
    ) -> dict[str, Any]:
        expressions = ["count(*) AS row_count"]

        for index, column in enumerate(schema):
            identifier = quote_identifier(column["source_name"])
            expressions.extend(
                [
                    (
                        "count(*) FILTER "
                        f"(WHERE {missing_expression(identifier)}) "
                        f"AS column_{index}_null_count"
                    ),
                    (
                        f"approx_count_distinct({identifier}) "
                        f"AS column_{index}_approx_distinct"
                    ),
                ]
            )

        for index, temporal in enumerate(config.get("temporal", [])):
            expression = temporal["expression"]
            expressions.extend(
                [
                    (
                        f"count(*) FILTER (WHERE {expression} IS NULL) "
                        f"AS temporal_{index}_invalid_count"
                    ),
                    f"min({expression}) AS temporal_{index}_min",
                    f"max({expression}) AS temporal_{index}_max",
                ]
            )

        for index, categorical in enumerate(config.get("categorical", [])):
            identifier = quote_identifier(categorical)
            expressions.append(
                f"count(DISTINCT {identifier}) AS categorical_{index}_distinct"
            )

        for index, numeric in enumerate(config.get("numeric", [])):
            raw = numeric["expression"]
            cast_value = f"try_cast({raw} AS DOUBLE)"
            parse_invalid = (
                f"NOT {missing_expression(raw)} AND {cast_value} IS NULL"
            )
            condition = numeric.get("condition")
            invalid_condition = parse_invalid
            if condition:
                valid_range = condition.replace("value", cast_value)
                invalid_condition = (
                    f"({parse_invalid}) OR "
                    f"({cast_value} IS NOT NULL AND NOT ({valid_range}))"
                )
            expressions.extend(
                [
                    (
                        f"count(*) FILTER (WHERE {invalid_condition}) "
                        f"AS numeric_{index}_invalid_count"
                    ),
                    f"min({cast_value}) AS numeric_{index}_min",
                    f"max({cast_value}) AS numeric_{index}_max",
                ]
            )

        cursor = self.connection.execute(
            "SELECT\n  " + ",\n  ".join(expressions) + "\nFROM source_data"
        )
        row = cursor.fetchone()
        assert row is not None
        return dict(zip((item[0] for item in cursor.description), row, strict=True))

    def _column_profiles(
        self, schema: list[dict[str, str]], summary: dict[str, Any]
    ) -> list[dict[str, Any]]:
        row_count = summary["row_count"]
        columns = []
        for index, column in enumerate(schema):
            null_count = summary[f"column_{index}_null_count"]
            columns.append(
                {
                    **column,
                    "null_count": null_count,
                    "null_percentage": (
                        round(null_count * 100 / row_count, 4) if row_count else 0
                    ),
                    "approx_distinct_count": summary[
                        f"column_{index}_approx_distinct"
                    ],
                }
            )
        return columns

    def _key_quality(self, key_columns: list[str]) -> dict[str, Any]:
        identifiers = [quote_identifier(column) for column in key_columns]
        missing = " OR ".join(missing_expression(value) for value in identifiers)
        key_hash = "hash(" + ", ".join(identifiers) + ")"
        row = self.connection.execute(
            f"""
            SELECT
                count(*) FILTER (WHERE {missing}) AS missing_key_rows,
                count(*) FILTER (WHERE NOT ({missing}))
                    - count(DISTINCT {key_hash}) FILTER (WHERE NOT ({missing}))
                    AS duplicate_key_rows
            FROM source_data
            """
        ).fetchone()
        assert row is not None
        return {
            "missing_key_rows": row[0],
            "duplicate_key_rows": row[1],
            "duplicate_method": "Exact distinct count over a 64-bit hash of the candidate key.",
        }

    def _custom_quality(self, checks: list[dict[str, str]]) -> list[dict[str, Any]]:
        results = []
        for check in checks:
            count = self.connection.execute(
                f"SELECT count(*) FROM source_data WHERE {check['condition']}"
            ).fetchone()[0]
            results.append({"name": check["name"], "invalid_count": count})
        return results

    @staticmethod
    def _temporal_profiles(
        config: dict[str, Any], summary: dict[str, Any]
    ) -> list[dict[str, Any]]:
        return [
            {
                "name": temporal["name"],
                "invalid_count": summary[f"temporal_{index}_invalid_count"],
                "minimum": summary[f"temporal_{index}_min"],
                "maximum": summary[f"temporal_{index}_max"],
            }
            for index, temporal in enumerate(config.get("temporal", []))
        ]

    @staticmethod
    def _numeric_profiles(
        config: dict[str, Any], summary: dict[str, Any]
    ) -> list[dict[str, Any]]:
        return [
            {
                "name": numeric["name"],
                "valid_condition": numeric.get("condition"),
                "invalid_count": summary[f"numeric_{index}_invalid_count"],
                "minimum": summary[f"numeric_{index}_min"],
                "maximum": summary[f"numeric_{index}_max"],
            }
            for index, numeric in enumerate(config.get("numeric", []))
        ]


def load_config(path: Path) -> list[dict[str, Any]]:
    with path.open(encoding="utf-8") as stream:
        content = yaml.safe_load(stream)
    return content["datasets"]


def run(
    config_path: Path,
    data_dir: Path,
    output_path: Path,
    selected_datasets: set[str] | None = None,
) -> dict[str, Any]:
    configs = load_config(config_path)
    if selected_datasets:
        configs = [
            config for config in configs if config["name"] in selected_datasets
        ]

    connection = duckdb.connect()
    connection.execute("SET threads = 4")
    profiler = DatasetProfiler(connection, data_dir)
    profiles = []
    for config in configs:
        print(f"Profiling {config['name']}...", flush=True)
        profile = profiler.profile(config)
        profiles.append(profile)

    result = {
        "generated_at": datetime.now(UTC).isoformat(),
        "config_path": str(config_path),
        "data_directory": str(data_dir),
        "datasets": profiles,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(result, indent=2, default=str, ensure_ascii=False),
        encoding="utf-8",
    )
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("config/datasets.yaml"))
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    parser.add_argument(
        "--output", type=Path, default=Path("artifacts/task1_profile.json")
    )
    parser.add_argument(
        "--dataset",
        action="append",
        help="Profile only this dataset name; repeat for multiple datasets.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    run(
        config_path=args.config,
        data_dir=args.data_dir,
        output_path=args.output,
        selected_datasets=set(args.dataset) if args.dataset else None,
    )


if __name__ == "__main__":
    main()
