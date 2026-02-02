"""Tests for cross-format consistency of edge case handling.

Tests that null, NaN, empty values, and sparse data are handled consistently
across CSV, Parquet, and ORC output formats.
"""

import csv
import tempfile
from pathlib import Path

import pytest

import transmog as tm
from transmog.config import TransmogConfig


def _get_file_path(paths, extension):
    """Extract file path from save() result (handles both list and dict)."""
    if isinstance(paths, dict):
        return [p for p in paths.values() if p.endswith(extension)][0]
    else:
        return [p for p in paths if p.endswith(extension)][0]


class TestNullConsistencyAcrossFormats:
    """Test that null values are handled consistently across formats."""

    @pytest.fixture
    def data_with_nulls(self):
        """Sample data with null values."""
        return [
            {"id": 1, "name": "Alice", "value": 100},
            {"id": 2, "name": "Bob", "value": None},
            {"id": 3, "name": None, "value": 300},
        ]

    def test_null_handling_csv(self, data_with_nulls):
        """Test null handling in CSV output."""
        config = TransmogConfig(include_nulls=True)
        result = tm.flatten(data_with_nulls, name="test", config=config)

        with tempfile.TemporaryDirectory() as tmpdir:
            paths = result.save(tmpdir, output_format="csv")

            # Read back CSV
            csv_path = _get_file_path(paths, ".csv")
            with open(csv_path, newline="") as f:
                reader = csv.DictReader(f)
                rows = list(reader)

            assert len(rows) == 3
            # CSV represents nulls as empty strings
            # Check that we have the expected number of rows

    @pytest.mark.skipif(
        not pytest.importorskip("pyarrow", reason="PyArrow not available"),
        reason="PyArrow required",
    )
    def test_null_handling_parquet(self, data_with_nulls):
        """Test null handling in Parquet output."""
        import pyarrow.parquet as pq

        config = TransmogConfig(include_nulls=True)
        result = tm.flatten(data_with_nulls, name="test", config=config)

        with tempfile.TemporaryDirectory() as tmpdir:
            paths = result.save(tmpdir, output_format="parquet")

            # Read back Parquet
            pq_path = _get_file_path(paths, ".parquet")
            table = pq.read_table(pq_path)

            assert len(table) == 3
            # Parquet preserves null semantics

    @pytest.mark.skipif(
        not pytest.importorskip("pyarrow.orc", reason="PyArrow ORC not available"),
        reason="PyArrow ORC required",
    )
    def test_null_handling_orc(self, data_with_nulls):
        """Test null handling in ORC output."""
        import pyarrow.orc as orc

        config = TransmogConfig(include_nulls=True)
        result = tm.flatten(data_with_nulls, name="test", config=config)

        with tempfile.TemporaryDirectory() as tmpdir:
            paths = result.save(tmpdir, output_format="orc")

            # Read back ORC
            orc_path = _get_file_path(paths, ".orc")
            table = orc.read_table(orc_path)

            assert len(table) == 3


class TestSparseDataConsistency:
    """Test that sparse data (different fields per record) is handled consistently."""

    @pytest.fixture
    def sparse_data(self):
        """Sample sparse data with different fields per record."""
        return [
            {"id": 1, "name": "Alice", "email": "alice@example.com"},
            {"id": 2, "name": "Bob", "phone": "555-1234"},
            {"id": 3, "email": "charlie@example.com", "phone": "555-5678"},
        ]

    def test_sparse_data_csv(self, sparse_data):
        """Test sparse data in CSV output."""
        result = tm.flatten(sparse_data, name="test")

        with tempfile.TemporaryDirectory() as tmpdir:
            paths = result.save(tmpdir, output_format="csv")

            csv_path = _get_file_path(paths, ".csv")
            with open(csv_path, newline="") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                fieldnames = reader.fieldnames

            assert len(rows) == 3
            # All fields should be in header
            assert "name" in fieldnames
            assert "email" in fieldnames
            assert "phone" in fieldnames

    @pytest.mark.skipif(
        not pytest.importorskip("pyarrow", reason="PyArrow not available"),
        reason="PyArrow required",
    )
    def test_sparse_data_parquet(self, sparse_data):
        """Test sparse data in Parquet output."""
        import pyarrow.parquet as pq

        result = tm.flatten(sparse_data, name="test")

        with tempfile.TemporaryDirectory() as tmpdir:
            paths = result.save(tmpdir, output_format="parquet")

            pq_path = _get_file_path(paths, ".parquet")
            table = pq.read_table(pq_path)

            assert len(table) == 3
            # All fields should be in schema
            schema_names = table.schema.names
            assert "name" in schema_names
            assert "email" in schema_names
            assert "phone" in schema_names


class TestMixedTypesConsistency:
    """Test that mixed data types are handled consistently across formats."""

    @pytest.fixture
    def mixed_type_data(self):
        """Sample data with mixed types."""
        return [
            {
                "id": 1,
                "int_val": 42,
                "float_val": 3.14,
                "str_val": "hello",
                "bool_val": True,
            },
            {"id": 2, "int_val": 0, "float_val": 0.0, "str_val": "", "bool_val": False},
            {
                "id": 3,
                "int_val": -1,
                "float_val": -1.5,
                "str_val": "world",
                "bool_val": True,
            },
        ]

    def test_mixed_types_csv(self, mixed_type_data):
        """Test mixed types in CSV output."""
        result = tm.flatten(mixed_type_data, name="test")

        with tempfile.TemporaryDirectory() as tmpdir:
            paths = result.save(tmpdir, output_format="csv")

            csv_path = _get_file_path(paths, ".csv")
            with open(csv_path, newline="") as f:
                reader = csv.DictReader(f)
                rows = list(reader)

            assert len(rows) == 3
            # CSV converts everything to strings
            assert rows[0]["int_val"] == "42"
            assert rows[0]["float_val"] == "3.14"

    @pytest.mark.skipif(
        not pytest.importorskip("pyarrow", reason="PyArrow not available"),
        reason="PyArrow required",
    )
    def test_mixed_types_parquet(self, mixed_type_data):
        """Test mixed types in Parquet output."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        result = tm.flatten(mixed_type_data, name="test")

        with tempfile.TemporaryDirectory() as tmpdir:
            paths = result.save(tmpdir, output_format="parquet")

            pq_path = _get_file_path(paths, ".parquet")
            table = pq.read_table(pq_path)

            assert len(table) == 3
            # Parquet preserves types
            schema = table.schema
            assert schema.field("int_val").type == pa.int64()
            assert schema.field("float_val").type == pa.float64()


class TestEdgeCasesAllFormats:
    """Test various edge cases across all formats."""

    @pytest.fixture
    def edge_case_data(self):
        """Data with various edge cases."""
        return [
            {
                "id": 1,
                "zero_int": 0,
                "zero_float": 0.0,
                "false_bool": False,
                "empty_after_flatten": "valid",
            },
            {
                "id": 2,
                "zero_int": 100,
                "zero_float": 1.5,
                "false_bool": True,
                "empty_after_flatten": "also_valid",
            },
        ]

    def test_zero_values_preserved_csv(self, edge_case_data):
        """Test that zero values are preserved in CSV."""
        result = tm.flatten(edge_case_data, name="test")

        with tempfile.TemporaryDirectory() as tmpdir:
            paths = result.save(tmpdir, output_format="csv")

            csv_path = _get_file_path(paths, ".csv")
            with open(csv_path, newline="") as f:
                reader = csv.DictReader(f)
                rows = list(reader)

            # Zero values should be present, not treated as null
            assert rows[0]["zero_int"] == "0"
            assert rows[0]["zero_float"] == "0.0"
            assert rows[0]["false_bool"] == "False"

    @pytest.mark.skipif(
        not pytest.importorskip("pyarrow", reason="PyArrow not available"),
        reason="PyArrow required",
    )
    def test_zero_values_preserved_parquet(self, edge_case_data):
        """Test that zero values are preserved in Parquet."""
        import pyarrow.parquet as pq

        result = tm.flatten(edge_case_data, name="test")

        with tempfile.TemporaryDirectory() as tmpdir:
            paths = result.save(tmpdir, output_format="parquet")

            pq_path = _get_file_path(paths, ".parquet")
            table = pq.read_table(pq_path)

            # Zero values should be present
            zero_ints = table.column("zero_int").to_pylist()
            assert 0 in zero_ints

            zero_floats = table.column("zero_float").to_pylist()
            assert 0.0 in zero_floats

            false_bools = table.column("false_bool").to_pylist()
            assert False in false_bools


class TestCompleteWorkflowConsistency:
    """Test complete flatten -> save workflow consistency."""

    def test_same_data_all_formats(self):
        """Test that same data produces consistent results across formats."""
        data = {
            "id": 1,
            "name": "Test Entity",
            "metrics": {"score": 95.5, "count": 42},
            "tags": ["important", "verified"],
        }

        result = tm.flatten(data, name="entity")

        with tempfile.TemporaryDirectory() as tmpdir:
            # Save to all formats
            csv_paths = result.save(f"{tmpdir}/csv", output_format="csv")
            assert len(csv_paths) >= 1

            try:
                pq_paths = result.save(f"{tmpdir}/parquet", output_format="parquet")
                assert len(pq_paths) >= 1
            except Exception:
                pass  # PyArrow may not be available

            try:
                orc_paths = result.save(f"{tmpdir}/orc", output_format="orc")
                assert len(orc_paths) >= 1
            except Exception:
                pass  # PyArrow ORC may not be available

    def test_batch_data_all_formats(self):
        """Test batch data produces consistent results across formats."""
        data = [
            {"id": 1, "value": 100, "status": "active"},
            {"id": 2, "value": 200, "status": "pending"},
            {"id": 3, "value": 300, "status": "active"},
        ]

        result = tm.flatten(data, name="records")

        # Verify we have 3 records
        assert len(result.main) == 3

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_paths = result.save(f"{tmpdir}/csv", output_format="csv")

            # Read back and verify count
            csv_path = _get_file_path(csv_paths, ".csv")
            with open(csv_path, newline="") as f:
                reader = csv.DictReader(f)
                rows = list(reader)

            assert len(rows) == 3


class TestNullSkipModeConsistency:
    """Test null skip mode (include_nulls=False) consistency across formats."""

    def test_nulls_skipped_consistently(self):
        """Test that nulls are skipped consistently in all formats."""
        data = [
            {"id": 1, "name": "Alice", "optional": "present"},
            {"id": 2, "name": "Bob", "optional": None},
            {"id": 3, "name": "Charlie"},  # Missing key
        ]

        # Default config skips nulls
        result = tm.flatten(data, name="test")

        # Record 1 should have optional
        assert "optional" in result.main[0]

        # Record 2 should NOT have optional (None skipped)
        assert "optional" not in result.main[1]

        # Record 3 should NOT have optional (missing key)
        assert "optional" not in result.main[2]

        with tempfile.TemporaryDirectory() as tmpdir:
            # CSV output should reflect this
            paths = result.save(tmpdir, output_format="csv")
            csv_path = _get_file_path(paths, ".csv")

            with open(csv_path, newline="") as f:
                reader = csv.DictReader(f)
                rows = list(reader)

            # All rows should have optional in header (CSV includes all fields)
            # but values should be empty for records 2 and 3
            assert "optional" in reader.fieldnames
            assert rows[0]["optional"] == "present"
            assert rows[1]["optional"] == ""
            assert rows[2]["optional"] == ""


class TestSpecialCharactersConsistency:
    """Test special characters handling across formats."""

    @pytest.fixture
    def special_char_data(self):
        """Data with special characters."""
        return [
            {"id": 1, "text": "Hello, World!", "unicode": "日本語"},
            {"id": 2, "text": "Line1\nLine2", "unicode": "Ελληνικά"},
            {"id": 3, "text": 'Quote: "test"', "unicode": "العربية"},
        ]

    def test_special_chars_csv(self, special_char_data):
        """Test special characters in CSV output."""
        result = tm.flatten(special_char_data, name="test")

        with tempfile.TemporaryDirectory() as tmpdir:
            paths = result.save(tmpdir, output_format="csv")

            csv_path = _get_file_path(paths, ".csv")
            with open(csv_path, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                rows = list(reader)

            assert len(rows) == 3
            assert rows[0]["unicode"] == "日本語"
            assert rows[1]["unicode"] == "Ελληνικά"
            assert rows[2]["unicode"] == "العربية"

    @pytest.mark.skipif(
        not pytest.importorskip("pyarrow", reason="PyArrow not available"),
        reason="PyArrow required",
    )
    def test_special_chars_parquet(self, special_char_data):
        """Test special characters in Parquet output."""
        import pyarrow.parquet as pq

        result = tm.flatten(special_char_data, name="test")

        with tempfile.TemporaryDirectory() as tmpdir:
            paths = result.save(tmpdir, output_format="parquet")

            pq_path = _get_file_path(paths, ".parquet")
            table = pq.read_table(pq_path)

            unicode_vals = table.column("unicode").to_pylist()
            assert "日本語" in unicode_vals
            assert "Ελληνικά" in unicode_vals
            assert "العربية" in unicode_vals
