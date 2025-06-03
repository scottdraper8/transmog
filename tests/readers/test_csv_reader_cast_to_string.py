"""
Tests for CSV reader cast_to_string functionality.

This module tests that the CSV reader correctly handles the cast_to_string parameter
with both PyArrow and native CSV implementations.
"""

from unittest.mock import patch

import pytest

from transmog.io.readers.csv import CSVReader


class TestCsvReaderCastToString:
    """Test cast_to_string functionality for CSV reader."""

    @pytest.fixture
    def mixed_type_csv_content(self):
        """CSV content with mixed data types including problematic dates."""
        return """name,date,age,salary,active,ratio,notes
John Doe,2025-03-04,30,50000.50,true,0.85,Regular employee
Jane Smith,2025-03-05,25,45000,false,0.92,Part-time
Bob Wilson,invalid-date,35,60000.75,1,0.78,"Has, comma in notes"
Alice Brown,2025-12-31,28,55000,0,0.88,
Charlie Davis,,40,65000.25,true,0.95,NULL
"""

    @pytest.fixture
    def mixed_type_csv_file(self, tmp_path, mixed_type_csv_content):
        """Create a CSV file with mixed data types."""
        file_path = tmp_path / "mixed_types.csv"
        with open(file_path, "w") as f:
            f.write(mixed_type_csv_content)
        return file_path

    def test_cast_to_string_true_with_pyarrow(self, mixed_type_csv_file):
        """Test that cast_to_string=True forces all values to strings with PyArrow."""
        reader = CSVReader(cast_to_string=True, infer_types=False)
        records = reader.read_all(mixed_type_csv_file)

        assert len(records) == 5

        # Check first record - all values should be strings
        first_record = records[0]
        for key, value in first_record.items():
            if value is not None:  # None values stay None
                assert isinstance(value, str), (
                    f"Field '{key}' should be string, got {type(value)}: {value}"
                )

        # Verify specific values
        assert first_record["name"] == "John Doe"
        assert (
            first_record["date"] == "2025-03-04"
        )  # Should remain as string, not parsed as date
        assert first_record["age"] == "30"  # Should be string, not int
        assert first_record["salary"] == "50000.50"  # Should be string, not float
        assert first_record["active"] == "true"  # Should be string, not boolean
        assert first_record["ratio"] == "0.85"  # Should be string, not float

    def test_cast_to_string_true_with_native_csv(self, mixed_type_csv_file):
        """Test that cast_to_string=True works with native CSV implementation."""
        # Force use of native CSV by temporarily disabling PyArrow
        with patch("transmog.io.readers.csv.PYARROW_AVAILABLE", False):
            reader = CSVReader(cast_to_string=True, infer_types=False)
            records = reader.read_all(mixed_type_csv_file)

        assert len(records) == 5

        # Check first record - all values should be strings
        first_record = records[0]
        for key, value in first_record.items():
            if value is not None:  # None values stay None
                assert isinstance(value, str), (
                    f"Field '{key}' should be string, got {type(value)}: {value}"
                )

        # Verify specific values match PyArrow behavior
        assert first_record["name"] == "John Doe"
        assert first_record["date"] == "2025-03-04"
        assert first_record["age"] == "30"
        assert first_record["salary"] == "50000.50"
        assert first_record["active"] == "true"
        assert first_record["ratio"] == "0.85"

    def test_cast_to_string_false_with_type_inference(self, mixed_type_csv_file):
        """Test that cast_to_string=False allows type inference."""
        reader = CSVReader(cast_to_string=False, infer_types=True)
        records = reader.read_all(mixed_type_csv_file)

        assert len(records) == 5

        first_record = records[0]

        # With type inference, numeric values should be converted
        # Note: Actual types may vary between PyArrow and native implementations
        assert isinstance(first_record["name"], str)  # Name should stay string
        assert isinstance(
            first_record["date"], str
        )  # Date should be string (since we're not parsing dates)

        # Age might be int or string depending on implementation
        assert isinstance(first_record["age"], (int, str))
        if isinstance(first_record["age"], int):
            assert first_record["age"] == 30

        # Salary might be float or string
        assert isinstance(first_record["salary"], (float, str))
        if isinstance(first_record["salary"], float):
            assert abs(first_record["salary"] - 50000.5) < 0.01

    def test_cast_to_string_handles_problematic_dates(self, mixed_type_csv_file):
        """Test that cast_to_string=True prevents date parsing errors."""
        reader = CSVReader(cast_to_string=True)
        records = reader.read_all(mixed_type_csv_file)

        # Should successfully read all records without date parsing errors
        assert len(records) == 5

        # Check the record with invalid date
        bob_record = next(r for r in records if r["name"] == "Bob Wilson")
        assert bob_record["date"] == "invalid-date"  # Should be preserved as string
        assert isinstance(bob_record["date"], str)

    def test_cast_to_string_with_null_values(self, mixed_type_csv_file):
        """Test cast_to_string behavior with null values."""
        reader = CSVReader(cast_to_string=True, null_values=["", "NULL", "null"])
        records = reader.read_all(mixed_type_csv_file)

        # Find record with empty date
        charlie_record = next(r for r in records if r["name"] == "Charlie Davis")

        # Empty date should be None (null), not string
        assert charlie_record["date"] is None

        # Find record with NULL in notes
        alice_record = next(r for r in records if r["name"] == "Alice Brown")
        assert alice_record["notes"] is None or alice_record["notes"] == ""

    def test_cast_to_string_consistency_between_implementations(
        self, mixed_type_csv_file
    ):
        """Test that PyArrow and native implementations produce consistent results with cast_to_string."""
        # Test with PyArrow
        reader_pyarrow = CSVReader(cast_to_string=True)
        records_pyarrow = reader_pyarrow.read_all(mixed_type_csv_file)

        # Test with native CSV (force fallback)
        with patch("transmog.io.readers.csv.PYARROW_AVAILABLE", False):
            reader_native = CSVReader(cast_to_string=True)
            records_native = reader_native.read_all(mixed_type_csv_file)

        # Both should have same number of records
        assert len(records_pyarrow) == len(records_native)

        # Compare each record
        for i, (pyarrow_record, native_record) in enumerate(
            zip(records_pyarrow, records_native)
        ):
            # Should have same keys
            assert set(pyarrow_record.keys()) == set(native_record.keys())

            # Compare each field value and type
            for key in pyarrow_record.keys():
                pyarrow_value = pyarrow_record[key]
                native_value = native_record[key]

                # Both should be same type (both string or both None)
                assert isinstance(pyarrow_value, type(native_value)), (
                    f"Record {i}, field '{key}': PyArrow type {type(pyarrow_value)} != Native type {type(native_value)}"
                )

                # And same value
                assert pyarrow_value == native_value, (
                    f"Record {i}, field '{key}': PyArrow value '{pyarrow_value}' != Native value '{native_value}'"
                )

    def test_chunked_reading_with_cast_to_string(self, mixed_type_csv_file):
        """Test that chunked reading respects cast_to_string parameter."""
        reader = CSVReader(cast_to_string=True)
        chunks = list(reader.read_in_chunks(mixed_type_csv_file, chunk_size=2))

        # Should have 3 chunks (2 + 2 + 1 records)
        assert len(chunks) == 3
        assert len(chunks[0]) == 2
        assert len(chunks[1]) == 2
        assert len(chunks[2]) == 1

        # All values in all chunks should be strings or None
        for chunk in chunks:
            for record in chunk:
                for key, value in record.items():
                    if value is not None:
                        assert isinstance(value, str), (
                            f"Field '{key}' should be string, got {type(value)}: {value}"
                        )

    def test_edge_case_all_numeric_with_cast_to_string(self, tmp_path):
        """Test cast_to_string with purely numeric data."""
        content = """id,score,rating
1,95.5,4.8
2,87.2,4.2
3,92.0,4.9
"""
        file_path = tmp_path / "numeric.csv"
        with open(file_path, "w") as f:
            f.write(content)

        reader = CSVReader(cast_to_string=True)
        records = reader.read_all(file_path)

        assert len(records) == 3

        # All numeric values should be strings
        for record in records:
            assert isinstance(record["id"], str)
            assert isinstance(record["score"], str)
            assert isinstance(record["rating"], str)

        # Verify specific values
        assert records[0]["id"] == "1"
        assert records[0]["score"] == "95.5"
        assert records[0]["rating"] == "4.8"
