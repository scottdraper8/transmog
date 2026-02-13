"""Tests for PyArrow base writer converter functions and caching."""

import pytest

from transmog.writers.arrow_base import PYARROW_AVAILABLE

pytestmark = pytest.mark.skipif(not PYARROW_AVAILABLE, reason="PyArrow not available")


class TestConverterFunctions:
    """Test module-level converter functions."""

    def test_convert_bool_normal(self):
        """Test bool converter with standard inputs."""
        from transmog.writers.arrow_base import _convert_bool

        assert _convert_bool(True) is True
        assert _convert_bool(False) is False
        assert _convert_bool(1) is True
        assert _convert_bool(0) is False

    def test_convert_int_normal(self):
        """Test int converter with standard inputs."""
        from transmog.writers.arrow_base import _convert_int

        assert _convert_int(42) == 42
        assert _convert_int(3.9) == 3
        assert _convert_int("7") == 7

    def test_convert_int_error(self):
        """Test int converter returns None on unconvertible input."""
        from transmog.writers.arrow_base import _convert_int

        assert _convert_int("not_int") is None

    def test_convert_float_normal(self):
        """Test float converter with standard inputs."""
        from transmog.writers.arrow_base import _convert_float

        assert _convert_float(3.14) == 3.14
        assert _convert_float(42) == 42.0
        assert _convert_float("2.5") == 2.5

    def test_convert_float_error(self):
        """Test float converter returns None on unconvertible input."""
        from transmog.writers.arrow_base import _convert_float

        assert _convert_float("not_float") is None

    def test_convert_str_normal(self):
        """Test str converter with standard inputs."""
        from transmog.writers.arrow_base import _convert_str

        assert _convert_str(42) == "42"
        assert _convert_str(3.14) == "3.14"
        assert _convert_str(True) == "True"
        assert _convert_str("hello") == "hello"


class TestTypeConvertersMapping:
    """Test lazy type-to-converter mapping."""

    def test_get_type_converters_returns_expected_types(self):
        """Test mapping contains bool, int64, and float64 entries."""
        import pyarrow as pa

        from transmog.writers.arrow_base import (
            _convert_bool,
            _convert_float,
            _convert_int,
            _get_type_converters,
        )

        converters = _get_type_converters()
        assert converters[pa.bool_()] is _convert_bool
        assert converters[pa.int64()] is _convert_int
        assert converters[pa.float64()] is _convert_float

    def test_get_type_converters_is_stable(self):
        """Test repeated calls return the same dict object."""
        from transmog.writers.arrow_base import _get_type_converters

        first = _get_type_converters()
        second = _get_type_converters()
        assert first is second


class TestCreateSchemaConverters:
    """Test that _create_schema returns correct converters."""

    def test_mixed_types(self):
        """Test schema and converters for bool, int, float, and string fields."""
        import pyarrow as pa

        from transmog.writers.arrow_base import (
            _convert_bool,
            _convert_float,
            _convert_int,
            _convert_str,
        )
        from transmog.writers.parquet import ParquetStreamingWriter

        writer = ParquetStreamingWriter.__new__(ParquetStreamingWriter)
        records = [
            {"flag": True, "count": 5, "score": 3.14, "name": "Alice"},
        ]
        schema, converters = writer._create_schema(records)

        assert schema.field("flag").type == pa.bool_()
        assert schema.field("count").type == pa.int64()
        assert schema.field("score").type == pa.float64()
        assert schema.field("name").type == pa.string()

        assert converters["flag"] is _convert_bool
        assert converters["count"] is _convert_int
        assert converters["score"] is _convert_float
        assert converters["name"] is _convert_str

    def test_empty_records(self):
        """Test empty records produce empty schema and converters."""
        from transmog.writers.parquet import ParquetStreamingWriter

        writer = ParquetStreamingWriter.__new__(ParquetStreamingWriter)
        schema, converters = writer._create_schema([])
        assert len(schema) == 0
        assert converters == {}

    def test_stringify_mode(self):
        """Test stringify mode assigns str converter to all fields."""
        from transmog.writers.arrow_base import _convert_str
        from transmog.writers.parquet import ParquetStreamingWriter

        writer = ParquetStreamingWriter.__new__(ParquetStreamingWriter)
        records = [{"count": 5, "score": 3.14}]
        schema, converters = writer._create_schema(records, stringify_mode=True)

        assert all(converters[k] is _convert_str for k in converters)


class TestConverterCaching:
    """Test that converters are cached across batches and cleared on close."""

    def test_converters_cached_across_batches(self, tmp_path):
        """Test second batch reuses the same converters dict object."""
        from transmog.writers.parquet import ParquetStreamingWriter

        with ParquetStreamingWriter(
            destination=str(tmp_path), entity_name="test", row_group_size=2
        ) as writer:
            writer.write_main_records(
                [
                    {"id": 1, "name": "Alice"},
                    {"id": 2, "name": "Bob"},
                ]
            )
            assert "main" in writer.converters
            first_converters = writer.converters["main"]

            writer.write_main_records(
                [
                    {"id": 3, "name": "Charlie"},
                    {"id": 4, "name": "Dave"},
                ]
            )
            assert writer.converters["main"] is first_converters

    def test_converters_cleared_on_close(self, tmp_path):
        """Test close() clears the converters dict."""
        from transmog.writers.parquet import ParquetStreamingWriter

        writer = ParquetStreamingWriter(destination=str(tmp_path), entity_name="test")
        writer.write_main_records([{"id": 1, "name": "Alice"}])
        writer.close()

        assert writer.converters == {}


class TestColumnBufferReuse:
    """Test that column buffers and record buffers are reused across batches."""

    def test_column_buffers_reused_across_batches(self, tmp_path):
        """Test inner list objects are the same Python objects on second batch."""
        from transmog.writers.parquet import ParquetStreamingWriter

        with ParquetStreamingWriter(
            destination=str(tmp_path), entity_name="test", row_group_size=2
        ) as writer:
            writer.write_main_records(
                [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
            )
            assert "main" in writer._column_buffers
            first_lists = {k: v for k, v in writer._column_buffers["main"].items()}

            writer.write_main_records(
                [{"id": 3, "name": "Charlie"}, {"id": 4, "name": "Dave"}]
            )
            for key, first_list in first_lists.items():
                assert writer._column_buffers["main"][key] is first_list

    def test_column_buffers_cleared_on_close(self, tmp_path):
        """Test close() clears the _column_buffers dict."""
        from transmog.writers.parquet import ParquetStreamingWriter

        writer = ParquetStreamingWriter(destination=str(tmp_path), entity_name="test")
        writer.write_main_records([{"id": 1, "name": "Alice"}])
        writer.close()

        assert writer._column_buffers == {}

    def test_record_buffer_reused_across_batches(self, tmp_path):
        """Test same list object persists after flush (clear instead of reassign)."""
        from transmog.writers.parquet import ParquetStreamingWriter

        with ParquetStreamingWriter(
            destination=str(tmp_path), entity_name="test", row_group_size=2
        ) as writer:
            writer.write_main_records([{"id": 1, "name": "Alice"}])
            buffer_ref = writer.buffers["main"]

            writer.write_main_records(
                [{"id": 2, "name": "Bob"}, {"id": 3, "name": "Charlie"}]
            )
            assert writer.buffers["main"] is buffer_ref


class TestPyArrowWriterExceptionHandling:
    """Test narrowed exception handling in PyArrowWriter.write()."""

    def test_memory_error_propagates(self):
        """Test that MemoryError is not caught by the writer."""
        from unittest.mock import patch

        from transmog.writers.parquet import ParquetWriter

        data = [{"id": 1, "name": "Alice"}]
        writer = ParquetWriter()

        with patch("transmog.writers.arrow_base.pa.table", side_effect=MemoryError()):
            with pytest.raises(MemoryError):
                writer.write(data, "/tmp/test.parquet")

    def test_oserror_wrapped_in_output_error(self, tmp_path):
        """Test that OSError is wrapped in OutputError."""
        from unittest.mock import patch

        from transmog.exceptions import OutputError
        from transmog.writers.parquet import ParquetWriter

        data = [{"id": 1, "name": "Alice"}]
        writer = ParquetWriter()

        with patch(
            "transmog.writers.parquet.pq.write_table",
            side_effect=OSError("disk full"),
        ):
            with pytest.raises(OutputError, match="Failed to write Parquet file"):
                writer.write(data, str(tmp_path / "test.parquet"))

    def test_output_error_propagates_unwrapped(self):
        """Test that OutputError from text-mode stream is not double-wrapped."""
        import io

        from transmog.exceptions import OutputError
        from transmog.writers.parquet import ParquetWriter

        data = [{"id": 1, "name": "Alice"}]
        writer = ParquetWriter()
        text_stream = io.StringIO()
        text_stream.mode = "w"  # type: ignore[attr-defined]

        with pytest.raises(OutputError, match="requires binary streams"):
            writer.write(data, text_stream)
