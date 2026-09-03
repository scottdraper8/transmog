"""Tests that batch_size does not change flatten() results."""

from typing import Any

import pytest

import transmog as tm
from transmog.config import TransmogConfig

from ..conftest import read_csv_rows


class TestBatchProcessing:
    """Test that batching is an implementation detail of flatten()."""

    @pytest.fixture
    def sample_records(self) -> list[dict[str, Any]]:
        """Sample records for batch processing tests."""
        return [
            {"id": 1, "name": "Alice", "age": 30, "city": "New York"},
            {"id": 2, "name": "Bob", "age": 25, "city": "Los Angeles"},
            {"id": 3, "name": "Charlie", "age": 35, "city": "Chicago"},
            {"id": 4, "name": "Diana", "age": 28, "city": "Houston"},
            {"id": 5, "name": "Eve", "age": 32, "city": "Phoenix"},
        ]

    def test_batch_processing_preserves_rows(self, sample_records):
        """Every input row is present with original field values."""
        result = tm.flatten(
            sample_records, name="users", config=TransmogConfig(batch_size=3)
        )

        assert len(result.main) == len(sample_records)
        for source, processed in zip(sample_records, result.main, strict=True):
            assert processed["name"] == source["name"]
            assert processed["age"] == source["age"]
            assert processed["city"] == source["city"]

    @pytest.mark.parametrize("batch_size", [1, 2, 5, 100])
    def test_batch_size_does_not_affect_output(self, sample_records, batch_size):
        """Output rows are identical regardless of batch size."""
        baseline = tm.flatten(
            sample_records, name="users", config=TransmogConfig(batch_size=5)
        )
        result = tm.flatten(
            sample_records, name="users", config=TransmogConfig(batch_size=batch_size)
        )

        assert len(result.main) == len(baseline.main)
        metadata = {"_id", "_parent_id", "_timestamp"}
        for left, right in zip(baseline.main, result.main, strict=True):
            for key, value in left.items():
                if key in metadata:
                    continue
                assert right[key] == value

    def test_batch_processing_with_arrays(self):
        """Nested arrays are extracted even when batch_size is 1."""
        records = [
            {
                "id": 1,
                "company": "TechCorp",
                "employees": [
                    {"name": "Alice", "role": "Engineer"},
                    {"name": "Bob", "role": "Designer"},
                ],
            },
            {
                "id": 2,
                "company": "DataCorp",
                "employees": [
                    {"name": "Charlie", "role": "Analyst"},
                ],
            },
        ]
        result = tm.flatten(
            records, name="companies", config=TransmogConfig(batch_size=1)
        )

        assert len(result.main) == 2
        employee_tables = [name for name in result.tables if "employees" in name]
        assert employee_tables
        employees = result.tables[employee_tables[0]]
        assert len(employees) == 3
        assert {row["name"] for row in employees} == {"Alice", "Bob", "Charlie"}

    def test_batch_processing_writes_csv(self, sample_records, tmp_path):
        """Saving batched results writes the same rows to CSV."""
        result = tm.flatten(
            sample_records, name="users", config=TransmogConfig(batch_size=2)
        )
        saved = result.save(str(tmp_path / "users.csv"), output_format="csv")
        paths = list(saved.values()) if isinstance(saved, dict) else saved
        rows = read_csv_rows(paths[0])
        assert [row["name"] for row in rows] == [
            "Alice",
            "Bob",
            "Charlie",
            "Diana",
            "Eve",
        ]
