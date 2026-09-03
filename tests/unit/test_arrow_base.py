"""Tests for PyArrow base writer converter functions and caching."""

from unittest.mock import patch

import pytest


class TestConverterFunctions:
    """Test module-level converter error handling."""

    def test_convert_int_error(self):
        """Test int converter returns None on unconvertible input."""
        from transmog.writers.arrow_base import _convert_int

        assert _convert_int("not_int") is None

    def test_convert_float_error(self):
        """Test float converter returns None on unconvertible input."""
        from transmog.writers.arrow_base import _convert_float

        assert _convert_float("not_float") is None


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


class TestCreateSchemaConverters:
    """Test that _create_schema returns correct converters."""

    def test_mixed_types(self, tmp_path):
        """Test schema and converters for bool, int, float, and string fields."""
        import pyarrow as pa

        from transmog.writers.arrow_base import (
            _convert_bool,
            _convert_float,
            _convert_int,
            _convert_str,
        )
        from transmog.writers.parquet import ParquetStreamingWriter

        writer = ParquetStreamingWriter(destination=str(tmp_path), entity_name="test")
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

    def test_empty_records(self, tmp_path):
        """Test empty records produce empty schema and converters."""
        from transmog.writers.parquet import ParquetStreamingWriter

        writer = ParquetStreamingWriter(destination=str(tmp_path), entity_name="test")
        schema, converters = writer._create_schema([])
        assert len(schema) == 0
        assert converters == {}

    def test_stringify_mode(self, tmp_path):
        """Test stringify mode assigns str converter to all fields."""
        from transmog.writers.arrow_base import _convert_str
        from transmog.writers.parquet import ParquetStreamingWriter

        writer = ParquetStreamingWriter(destination=str(tmp_path), entity_name="test")
        records = [{"count": 5, "score": 3.14}]
        schema, converters = writer._create_schema(records, stringify_mode=True)

        assert all(converters[k] is _convert_str for k in converters)


class TestPartFileOutput:
    """Test that streaming writers produce part files."""

    def test_produces_part_files(self, tmp_path):
        """Test that multiple batches produce numbered part files."""
        from transmog.writers.parquet import ParquetStreamingWriter

        with ParquetStreamingWriter(
            destination=str(tmp_path),
            entity_name="test",
            batch_size=2,
            consolidate=False,
        ) as writer:
            writer.write_main_records(
                [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
            )
            writer.write_main_records(
                [{"id": 3, "name": "Charlie"}, {"id": 4, "name": "Dave"}]
            )

        import pyarrow.parquet as pq

        part_0 = tmp_path / "test_part_0000.parquet"
        part_1 = tmp_path / "test_part_0001.parquet"
        assert part_0.exists()
        assert part_1.exists()
        assert pq.read_table(str(part_0)).column("name").to_pylist() == [
            "Alice",
            "Bob",
        ]
        assert pq.read_table(str(part_1)).column("name").to_pylist() == [
            "Charlie",
            "Dave",
        ]


class TestPyArrowWriterExceptionHandling:
    """Test narrowed exception handling in PyArrowWriter.write()."""

    def test_memory_error_propagates(self):
        """Test that MemoryError is not caught by the writer."""
        from transmog.writers.parquet import ParquetWriter

        data = [{"id": 1, "name": "Alice"}]
        writer = ParquetWriter()

        with patch("transmog.writers.arrow_base.pa.table", side_effect=MemoryError()):
            with pytest.raises(MemoryError):
                writer.write(data, "/tmp/test.parquet")

    def test_oserror_wrapped_in_output_error(self, tmp_path):
        """Test that OSError is wrapped in OutputError."""
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


class TestArrowStreamingWriterExceptionCleanup:
    """Test on-disk behavior of PyArrowStreamingWriter on exceptions."""

    def test_close_flush_failure_leaves_no_consolidated_file(self, tmp_path):
        """Failed close does not produce a consolidated output file."""
        from transmog.writers.parquet import ParquetStreamingWriter

        def always_fail(_self, _writer_obj, _table):
            raise OSError("flush failure during close")

        writer = ParquetStreamingWriter(
            destination=str(tmp_path), entity_name="test", batch_size=100
        )
        writer.write_main_records(
            [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        )

        with patch.object(
            ParquetStreamingWriter, "_write_to_format_writer", always_fail
        ):
            with pytest.raises(OSError, match="flush failure during close"):
                writer.close()

        assert not (tmp_path / "test.parquet").exists()

    def test_partial_file_on_disk_after_exception(self, tmp_path):
        """First batch's part file survives on disk after second batch fails."""
        import pyarrow.parquet as pq

        from transmog.writers.parquet import ParquetStreamingWriter

        def always_fail(_self, _writer_obj, _table):
            raise OSError("simulated write failure")

        with pytest.raises(OSError, match="simulated write failure"):
            with ParquetStreamingWriter(
                destination=str(tmp_path),
                entity_name="test",
                batch_size=2,
                consolidate=False,
            ) as writer:
                writer.write_main_records(
                    [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
                )
                with patch.object(
                    ParquetStreamingWriter, "_write_to_format_writer", always_fail
                ):
                    writer.write_main_records(
                        [{"id": 3, "name": "Charlie"}, {"id": 4, "name": "Dave"}]
                    )

        part_file = tmp_path / "test_part_0000.parquet"
        assert part_file.exists()
        table = pq.read_table(str(part_file))
        assert table.column("name").to_pylist() == ["Alice", "Bob"]


class TestSchemaLog:
    """Test schema log generation for part files."""

    def test_schema_log_written(self, tmp_path):
        """Test that _schema_log.json is written at close time."""
        import json

        from transmog.writers.parquet import ParquetStreamingWriter

        with ParquetStreamingWriter(
            destination=str(tmp_path),
            entity_name="test",
            batch_size=2,
            consolidate=False,
        ) as writer:
            writer.write_main_records(
                [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
            )

        log_path = tmp_path / "_schema_log.json"
        assert log_path.exists()
        log = json.loads(log_path.read_text())
        assert "tables" in log
        assert "main" in log["tables"]
        assert len(log["tables"]["main"]["parts"]) == 1
        assert log["tables"]["main"]["parts"][0]["deviations"] is None

    def test_schema_deviations_tracked(self, tmp_path):
        """Test that schema deviations are recorded in the log."""
        import json
        import warnings

        from transmog.writers.parquet import ParquetStreamingWriter

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            with ParquetStreamingWriter(
                destination=str(tmp_path),
                entity_name="test",
                batch_size=2,
                consolidate=False,
            ) as writer:
                writer.write_main_records(
                    [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
                )
                writer.write_main_records(
                    [
                        {"id": 3, "name": "Charlie", "extra": "val"},
                        {"id": 4, "name": "Dave", "extra": "val2"},
                    ]
                )

        log_path = tmp_path / "_schema_log.json"
        log = json.loads(log_path.read_text())
        parts = log["tables"]["main"]["parts"]
        assert len(parts) == 2
        assert parts[0]["deviations"] is None
        assert parts[1]["deviations"] is not None
        assert "extra" in parts[1]["deviations"]["structural"]["added"]

    def test_coercion_unifies_parquet_schemas(self, tmp_path):
        """Test that coerce_schema rewrites minority part files."""
        import json
        import warnings

        import pyarrow.parquet as pq

        from transmog.writers.parquet import ParquetStreamingWriter

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            with ParquetStreamingWriter(
                destination=str(tmp_path),
                entity_name="test",
                batch_size=2,
                coerce_schema=True,
                consolidate=False,
            ) as writer:
                writer.write_main_records(
                    [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
                )
                writer.write_main_records(
                    [
                        {"id": 3, "name": "Charlie", "extra": "val"},
                        {"id": 4, "name": "Dave", "extra": "val2"},
                    ]
                )

        # Both part files should have the unified schema after coercion
        part0 = pq.read_table(str(tmp_path / "test_part_0000.parquet"))
        part1 = pq.read_table(str(tmp_path / "test_part_0001.parquet"))

        assert "extra" in part0.column_names
        assert "extra" in part1.column_names
        assert part0.num_rows == 2
        assert part1.num_rows == 2
        # Coerced column should be null-filled in part 0
        assert part0.column("extra").null_count == 2

        log = json.loads((tmp_path / "_schema_log.json").read_text())
        assert "coerced_to" in log["tables"]["main"]["parts"][0]
