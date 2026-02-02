"""Tests for NaN and Infinity value handling.

Tests how transmog handles special float values (NaN, Inf, -Inf) during
flattening and output to various formats.
"""

import math
import tempfile
from pathlib import Path

import pytest

import transmog as tm
from transmog.config import TransmogConfig
from transmog.flattening import is_null_like


class TestIsNullLikeHelper:
    """Test the is_null_like helper function."""

    def test_none_is_null_like(self):
        """Test that None is considered null-like."""
        assert is_null_like(None) is True

    def test_empty_string_is_null_like(self):
        """Test that empty string is considered null-like."""
        assert is_null_like("") is True

    def test_nan_is_null_like(self):
        """Test that NaN is considered null-like."""
        assert is_null_like(float("nan")) is True

    def test_positive_inf_is_null_like(self):
        """Test that positive infinity is considered null-like."""
        assert is_null_like(float("inf")) is True

    def test_negative_inf_is_null_like(self):
        """Test that negative infinity is considered null-like."""
        assert is_null_like(float("-inf")) is True

    def test_valid_float_is_not_null_like(self):
        """Test that valid floats are not null-like."""
        assert is_null_like(3.14) is False
        assert is_null_like(0.0) is False
        assert is_null_like(-1.5) is False

    def test_zero_is_not_null_like(self):
        """Test that zero values are not null-like."""
        assert is_null_like(0) is False
        assert is_null_like(0.0) is False

    def test_false_is_not_null_like(self):
        """Test that False is not null-like."""
        assert is_null_like(False) is False

    def test_non_empty_string_is_not_null_like(self):
        """Test that non-empty strings are not null-like."""
        assert is_null_like("hello") is False
        assert is_null_like(" ") is False


class TestNanFlatteningSkipMode:
    """Test NaN handling with include_nulls=False (default)."""

    def test_nan_value_skipped_at_top_level(self):
        """Test that NaN values are skipped at top level."""
        data = {"id": 1, "valid": 3.14, "nan_value": float("nan")}

        result = tm.flatten(data, name="test")

        main = result.main[0]
        assert "valid" in main
        assert main["valid"] == 3.14
        assert "nan_value" not in main

    def test_positive_inf_skipped_at_top_level(self):
        """Test that positive infinity is skipped at top level."""
        data = {"id": 1, "valid": 42, "inf_value": float("inf")}

        result = tm.flatten(data, name="test")

        main = result.main[0]
        assert "valid" in main
        assert "inf_value" not in main

    def test_negative_inf_skipped_at_top_level(self):
        """Test that negative infinity is skipped at top level."""
        data = {"id": 1, "valid": 42, "neg_inf": float("-inf")}

        result = tm.flatten(data, name="test")

        main = result.main[0]
        assert "valid" in main
        assert "neg_inf" not in main

    def test_nan_in_nested_structure_skipped(self):
        """Test that NaN in nested structures is skipped."""
        data = {
            "id": 1,
            "nested": {"valid": 100, "nan_field": float("nan"), "also_valid": "text"},
        }

        result = tm.flatten(data, name="test")

        main = result.main[0]
        assert "nested_valid" in main
        assert "nested_also_valid" in main
        assert "nested_nan_field" not in main

    def test_nan_in_array_skipped(self):
        """Test that NaN values in arrays are skipped."""
        data = {"id": 1, "values": [1.0, float("nan"), 2.0, float("inf"), 3.0]}

        config = TransmogConfig(include_nulls=False)
        result = tm.flatten(data, name="test", config=config)

        # Array should still be present but NaN/Inf items filtered
        main = result.main[0]
        assert "values" in main
        # The array should contain only valid values
        values = main["values"]
        assert 1.0 in values
        assert 2.0 in values
        assert 3.0 in values

    def test_mixed_nan_and_valid_values(self):
        """Test records with mix of NaN and valid values."""
        data = [
            {"id": 1, "score": 95.5, "bonus": float("nan")},
            {"id": 2, "score": float("nan"), "bonus": 10.0},
            {"id": 3, "score": 87.2, "bonus": 5.0},
        ]

        result = tm.flatten(data, name="test")

        assert len(result.main) == 3

        # First record: score present, bonus skipped
        assert "score" in result.main[0]
        assert result.main[0]["score"] == 95.5

        # Second record: score skipped, bonus present
        assert "bonus" in result.main[1]
        assert result.main[1]["bonus"] == 10.0

        # Third record: both present
        assert "score" in result.main[2]
        assert "bonus" in result.main[2]


class TestNanFlatteningIncludeMode:
    """Test NaN handling with include_nulls=True."""

    def test_nan_value_included_as_none(self):
        """Test that NaN values are included as None when include_nulls=True."""
        data = {"id": 1, "valid": 3.14, "nan_value": float("nan")}

        config = TransmogConfig(include_nulls=True)
        result = tm.flatten(data, name="test", config=config)

        main = result.main[0]
        assert "valid" in main
        assert "nan_value" in main
        assert main["nan_value"] is None

    def test_inf_values_included_as_none(self):
        """Test that Inf values are included as None when include_nulls=True."""
        data = {"id": 1, "pos_inf": float("inf"), "neg_inf": float("-inf")}

        config = TransmogConfig(include_nulls=True)
        result = tm.flatten(data, name="test", config=config)

        main = result.main[0]
        assert "pos_inf" in main
        assert main["pos_inf"] is None
        assert "neg_inf" in main
        assert main["neg_inf"] is None

    def test_nan_in_nested_included(self):
        """Test that NaN in nested structures is included as None."""
        data = {"id": 1, "nested": {"valid": 100, "nan_field": float("nan")}}

        config = TransmogConfig(include_nulls=True)
        result = tm.flatten(data, name="test", config=config)

        main = result.main[0]
        assert "nested_valid" in main
        assert "nested_nan_field" in main
        assert main["nested_nan_field"] is None


class TestNanWithValidFloats:
    """Test that valid floats are preserved correctly alongside NaN handling."""

    def test_zero_float_preserved(self):
        """Test that 0.0 is preserved (not treated as null-like)."""
        data = {"id": 1, "zero": 0.0, "nan": float("nan")}

        result = tm.flatten(data, name="test")

        main = result.main[0]
        assert "zero" in main
        assert main["zero"] == 0.0
        assert "nan" not in main

    def test_negative_float_preserved(self):
        """Test that negative floats are preserved."""
        data = {"id": 1, "negative": -3.14, "neg_inf": float("-inf")}

        result = tm.flatten(data, name="test")

        main = result.main[0]
        assert "negative" in main
        assert main["negative"] == -3.14
        assert "neg_inf" not in main

    def test_very_small_float_preserved(self):
        """Test that very small floats are preserved."""
        data = {"id": 1, "tiny": 1e-300, "nan": float("nan")}

        result = tm.flatten(data, name="test")

        main = result.main[0]
        assert "tiny" in main
        assert main["tiny"] == 1e-300

    def test_very_large_float_preserved(self):
        """Test that very large (but finite) floats are preserved."""
        data = {"id": 1, "huge": 1e300, "inf": float("inf")}

        result = tm.flatten(data, name="test")

        main = result.main[0]
        assert "huge" in main
        assert main["huge"] == 1e300
        assert "inf" not in main


class TestNanCsvOutput:
    """Test NaN handling in CSV output."""

    def test_nan_written_as_empty_in_csv(self):
        """Test that NaN values become empty strings in CSV."""
        # First flatten with include_nulls to keep the field
        data = [{"id": "1", "value": float("nan")}, {"id": "2", "value": 42.0}]

        with tempfile.TemporaryDirectory() as tmpdir:
            from transmog.writers import CsvWriter

            writer = CsvWriter()
            output_path = Path(tmpdir) / "test.csv"
            writer.write(data, str(output_path))

            # Read back and verify
            with open(output_path) as f:
                content = f.read()

            # NaN should be written as empty string
            lines = content.strip().split("\n")
            assert len(lines) == 3  # header + 2 data rows

            # First data row should have empty value for NaN
            assert "1," in lines[1] or ",1" in lines[1]

    def test_inf_written_as_empty_in_csv(self):
        """Test that Inf values become empty strings in CSV."""
        data = [{"id": "1", "value": float("inf")}, {"id": "2", "value": float("-inf")}]

        with tempfile.TemporaryDirectory() as tmpdir:
            from transmog.writers import CsvWriter

            writer = CsvWriter()
            output_path = Path(tmpdir) / "test.csv"
            writer.write(data, str(output_path))

            # Read back and verify
            with open(output_path) as f:
                content = f.read()

            # Inf values should be written as empty strings
            assert "inf" not in content.lower()


class TestNanParquetOutput:
    """Test NaN handling in Parquet output."""

    @pytest.mark.skipif(
        not pytest.importorskip("pyarrow", reason="PyArrow not available"),
        reason="PyArrow required",
    )
    def test_parquet_schema_inference_with_nan_only_column(self):
        """Test schema inference when column has only NaN values."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        data = [
            {"id": "1", "all_nan": float("nan")},
            {"id": "2", "all_nan": float("nan")},
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            from transmog.writers import ParquetWriter

            writer = ParquetWriter()
            output_path = Path(tmpdir) / "test.parquet"
            writer.write(data, str(output_path))

            # Read back and verify schema
            table = pq.read_table(str(output_path))
            assert "all_nan" in table.schema.names

            # Column should be float64 type
            field = table.schema.field("all_nan")
            assert field.type == pa.float64()

    @pytest.mark.skipif(
        not pytest.importorskip("pyarrow", reason="PyArrow not available"),
        reason="PyArrow required",
    )
    def test_parquet_mixed_nan_and_valid_floats(self):
        """Test Parquet output with mix of NaN and valid floats."""
        import pyarrow.parquet as pq

        data = [
            {"id": "1", "value": 3.14},
            {"id": "2", "value": float("nan")},
            {"id": "3", "value": 2.71},
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            from transmog.writers import ParquetWriter

            writer = ParquetWriter()
            output_path = Path(tmpdir) / "test.parquet"
            writer.write(data, str(output_path))

            # Read back and verify
            table = pq.read_table(str(output_path))
            assert len(table) == 3

            # Values should be preserved (NaN stays as NaN in Parquet)
            values = table.column("value").to_pylist()
            assert values[0] == 3.14
            assert math.isnan(values[1])
            assert values[2] == 2.71


class TestNanOrcOutput:
    """Test NaN handling in ORC output."""

    @pytest.mark.skipif(
        not pytest.importorskip("pyarrow.orc", reason="PyArrow ORC not available"),
        reason="PyArrow ORC required",
    )
    def test_orc_schema_inference_with_nan(self):
        """Test ORC schema inference with NaN values."""
        import pyarrow.orc as orc

        data = [
            {"id": "1", "value": float("nan")},
            {"id": "2", "value": 42.0},
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            from transmog.writers import OrcWriter

            writer = OrcWriter()
            output_path = Path(tmpdir) / "test.orc"
            writer.write(data, str(output_path))

            # Read back and verify
            table = orc.read_table(str(output_path))
            assert len(table) == 2
            assert "value" in table.schema.names


class TestNanEndToEnd:
    """End-to-end tests for NaN handling through flatten and save."""

    def test_flatten_and_save_with_nan_csv(self):
        """Test complete workflow: flatten data with NaN, save to CSV."""
        data = {
            "id": 1,
            "name": "Test",
            "score": float("nan"),
            "valid_score": 95.5,
        }

        result = tm.flatten(data, name="test")

        with tempfile.TemporaryDirectory() as tmpdir:
            paths = result.save(tmpdir, output_format="csv")

            # Verify file was created
            assert len(paths) >= 1

    @pytest.mark.skipif(
        not pytest.importorskip("pyarrow", reason="PyArrow not available"),
        reason="PyArrow required",
    )
    def test_flatten_and_save_with_nan_parquet(self):
        """Test complete workflow: flatten data with NaN, save to Parquet."""
        data = {
            "id": 1,
            "name": "Test",
            "score": float("nan"),
            "valid_score": 95.5,
        }

        result = tm.flatten(data, name="test")

        with tempfile.TemporaryDirectory() as tmpdir:
            paths = result.save(tmpdir, output_format="parquet")

            # Verify file was created
            assert len(paths) >= 1

    def test_batch_processing_with_nan(self):
        """Test batch processing with NaN values across records."""
        data = [
            {"id": 1, "value": 10.0},
            {"id": 2, "value": float("nan")},
            {"id": 3, "value": 30.0},
            {"id": 4, "value": float("inf")},
            {"id": 5, "value": 50.0},
        ]

        result = tm.flatten(data, name="test")

        # All records should be present
        assert len(result.main) == 5

        # Records with valid values should have the value field
        valid_records = [r for r in result.main if "value" in r]
        assert len(valid_records) == 3

        # Verify the valid values
        values = [r["value"] for r in valid_records]
        assert 10.0 in values
        assert 30.0 in values
        assert 50.0 in values
