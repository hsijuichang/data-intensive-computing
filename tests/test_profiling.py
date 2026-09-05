from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import duckdb

from data_intensive_computing.profiling import DatasetProfiler
from data_intensive_computing.profiling import standardize_column_name


class ColumnNamingTests(unittest.TestCase):
    def test_standardizes_mixed_source_names(self) -> None:
        cases = {
            "Date Local": "date_local",
            "VendorID": "vendor_id",
            "PULocationID": "pu_location_id",
            "service_zone": "service_zone",
            "Units of Measure": "units_of_measure",
        }

        for source, expected in cases.items():
            with self.subTest(source=source):
                self.assertEqual(standardize_column_name(source), expected)


class DatasetProfilerTests(unittest.TestCase):
    def test_profiles_keys_types_and_quality_rules(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory)
            (data_dir / "events.csv").write_text(
                "ID,Event Time,Category,Amount\n"
                "1,2024-01-01 00:00,A,10\n"
                "1,2024-01-01 00:00,A,10\n"
                "2,not-a-time,B,-1\n"
                ",,B,\n",
                encoding="utf-8",
            )
            config = {
                "name": "events",
                "file_pattern": "events.csv",
                "format": "csv",
                "entity": "An event.",
                "primary_key": ["ID"],
                "join_attributes": ["ID -> another.ID"],
                "temporal": [
                    {
                        "name": "event_timestamp",
                        "expression": (
                            'try_strptime("Event Time", '
                            "'%Y-%m-%d %H:%M')"
                        ),
                    }
                ],
                "categorical": ["Category"],
                "numeric": [
                    {
                        "name": "amount",
                        "expression": '"Amount"',
                        "condition": "value >= 0",
                    }
                ],
                "custom_checks": [],
                "growth": "Append-only.",
            }

            profile = DatasetProfiler(duckdb.connect(), data_dir).profile(config)

        self.assertEqual(profile["row_count"], 4)
        self.assertEqual(profile["key_quality"]["missing_key_rows"], 1)
        self.assertEqual(profile["key_quality"]["duplicate_key_rows"], 1)
        self.assertEqual(
            profile["temporal_attributes"][0]["invalid_count"], 2
        )
        self.assertEqual(profile["categorical_attributes"][0]["distinct_count"], 2)
        self.assertEqual(profile["numeric_quality"][0]["invalid_count"], 1)


if __name__ == "__main__":
    unittest.main()
