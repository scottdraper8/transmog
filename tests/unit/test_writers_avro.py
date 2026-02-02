"""
Tests for Avro writer in Transmog.

Tests Avro file writing functionality, schema inference, and edge cases.
"""

import math
import sys
import tempfile
from pathlib import Path

import pytest

from transmog.exceptions import OutputError
from transmog.writers import AvroStreamingWriter, AvroWriter


class TestAvroWriter:
    """Test the AvroWriter class."""

    def test_avro_writer_basic(self):
        """Test basic Avro writing functionality."""
        data = [
            {"id": "1", "name": "Alice", "age": "25"},
            {"id": "2", "name": "Bob", "age": "30"},
            {"id": "3", "name": "Charlie", "age": "35"},
        ]

        with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
            output_file = f.name

        try:
            writer = AvroWriter()
            writer.write(data, output_file)

            assert Path(output_file).exists()
            assert Path(output_file).stat().st_size > 0

            # Verify by reading back
            import fastavro

            with open(output_file, "rb") as f:
                reader = fastavro.reader(f)
                records = list(reader)
                assert len(records) == 3
                assert records[0]["id"] == "1"
                assert records[0]["name"] == "Alice"

        finally:
            Path(output_file).unlink(missing_ok=True)

    def test_avro_writer_empty_data(self):
        """Test writing empty data to Avro."""
        data = []

        with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
            output_file = f.name

        try:
            writer = AvroWriter()
            result = writer.write(data, output_file)

            # Empty data returns destination without writing
            assert result == output_file

        finally:
            Path(output_file).unlink(missing_ok=True)

    def test_avro_writer_mixed_types(self):
        """Test writing mixed data types to Avro."""
        data = [
            {"id": 1, "name": "Alice", "score": 95.5, "active": True},
            {"id": 2, "name": "Bob", "score": 87.2, "active": False},
            {"id": 3, "name": "Charlie", "score": 92.0, "active": True},
        ]

        with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
            output_file = f.name

        try:
            writer = AvroWriter()
            writer.write(data, output_file)

            assert Path(output_file).exists()
            assert Path(output_file).stat().st_size > 0

            # Verify types are preserved
            import fastavro

            with open(output_file, "rb") as f:
                reader = fastavro.reader(f)
                records = list(reader)
                assert records[0]["id"] == 1
                assert records[0]["score"] == 95.5
                assert records[0]["active"] is True

        finally:
            Path(output_file).unlink(missing_ok=True)

    def test_avro_writer_unicode_data(self):
        """Test writing Unicode data to Avro."""
        data = [
            {"id": "1", "name": "José", "city": "São Paulo"},
            {"id": "2", "name": "François", "city": "Montréal"},
            {"id": "3", "name": "张三", "city": "北京"},
        ]

        with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
            output_file = f.name

        try:
            writer = AvroWriter()
            writer.write(data, output_file)

            assert Path(output_file).exists()
            assert Path(output_file).stat().st_size > 0

            # Verify Unicode is preserved
            import fastavro

            with open(output_file, "rb") as f:
                reader = fastavro.reader(f)
                records = list(reader)
                assert records[0]["name"] == "José"
                assert records[2]["name"] == "张三"

        finally:
            Path(output_file).unlink(missing_ok=True)

    def test_avro_writer_large_dataset(self):
        """Test writing large dataset to Avro."""
        data = [
            {
                "id": i,
                "name": f"User_{i}",
                "email": f"user{i}@example.com",
                "score": i * 10.5,
                "active": i % 2 == 0,
            }
            for i in range(1000)
        ]

        with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
            output_file = f.name

        try:
            writer = AvroWriter()
            writer.write(data, output_file)

            assert Path(output_file).exists()
            file_size = Path(output_file).stat().st_size
            assert file_size > 1000

            # Verify record count
            import fastavro

            with open(output_file, "rb") as f:
                reader = fastavro.reader(f)
                records = list(reader)
                assert len(records) == 1000

        finally:
            Path(output_file).unlink(missing_ok=True)

    def test_avro_writer_sparse_data(self):
        """Test writing sparse data (missing fields) to Avro."""
        data = [
            {"id": "1", "name": "Alice", "email": "alice@example.com"},
            {"id": "2", "name": "Bob"},
            {"id": "3", "email": "charlie@example.com"},
            {"id": "4", "name": "Diana", "email": "diana@example.com", "phone": "123"},
        ]

        with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
            output_file = f.name

        try:
            writer = AvroWriter()
            writer.write(data, output_file)

            assert Path(output_file).exists()
            assert Path(output_file).stat().st_size > 0

            # Verify nulls are handled
            import fastavro

            with open(output_file, "rb") as f:
                reader = fastavro.reader(f)
                records = list(reader)
                assert records[1]["email"] is None
                assert records[2]["name"] is None

        finally:
            Path(output_file).unlink(missing_ok=True)

    def test_avro_writer_null_values(self):
        """Test writing null values to Avro."""
        data = [
            {"id": "1", "name": "Alice", "optional_field": "value1"},
            {"id": "2", "name": "Bob", "optional_field": None},
            {"id": "3", "name": "Charlie", "optional_field": ""},
        ]

        with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
            output_file = f.name

        try:
            writer = AvroWriter()
            writer.write(data, output_file)

            assert Path(output_file).exists()

            import fastavro

            with open(output_file, "rb") as f:
                reader = fastavro.reader(f)
                records = list(reader)
                assert records[0]["optional_field"] == "value1"
                assert records[1]["optional_field"] is None
                assert records[2]["optional_field"] == ""

        finally:
            Path(output_file).unlink(missing_ok=True)

    def test_avro_writer_nan_inf_values(self):
        """Test writing NaN and Infinity values to Avro."""
        data = [
            {"id": "1", "value": 1.5},
            {"id": "2", "value": float("nan")},
            {"id": "3", "value": float("inf")},
            {"id": "4", "value": float("-inf")},
        ]

        with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
            output_file = f.name

        try:
            writer = AvroWriter()
            writer.write(data, output_file)

            assert Path(output_file).exists()

            import fastavro

            with open(output_file, "rb") as f:
                reader = fastavro.reader(f)
                records = list(reader)
                assert records[0]["value"] == 1.5
                # NaN and Inf should be converted to None
                assert records[1]["value"] is None
                assert records[2]["value"] is None
                assert records[3]["value"] is None

        finally:
            Path(output_file).unlink(missing_ok=True)


class TestAvroWriterOptions:
    """Test AvroWriter with various options."""

    def test_avro_writer_compression_codecs(self):
        """Test Avro writer with different compression codecs."""
        data = [
            {"id": "1", "name": "Alice", "data": "x" * 100},
            {"id": "2", "name": "Bob", "data": "y" * 100},
            {"id": "3", "name": "Charlie", "data": "z" * 100},
        ]

        # Test codecs supported with cramjam dependency
        # snappy, bzip2, and xz are provided by cramjam
        # zstandard and lz4 require additional Python packages
        codecs = ["null", "deflate", "snappy", "bzip2", "xz"]

        for codec in codecs:
            with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
                output_file = f.name

            try:
                writer = AvroWriter(codec=codec)
                writer.write(data, output_file)

                assert Path(output_file).exists()
                assert Path(output_file).stat().st_size > 0

            except (ValueError, ImportError):
                # Some codecs might not be available
                pass
            finally:
                Path(output_file).unlink(missing_ok=True)

    def test_avro_writer_invalid_codec(self):
        """Test Avro writer with invalid codec."""
        data = [{"id": "1", "name": "Alice"}]

        with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
            output_file = f.name

        try:
            writer = AvroWriter(codec="invalid_codec")
            with pytest.raises(OutputError):
                writer.write(data, output_file)
        finally:
            Path(output_file).unlink(missing_ok=True)


class TestAvroWriterSchemaInference:
    """Test Avro schema inference functionality."""

    def test_schema_inference_string_fields(self):
        """Test schema inference for string fields."""
        data = [
            {"name": "Alice", "city": "NYC"},
            {"name": "Bob", "city": "LA"},
        ]

        with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
            output_file = f.name

        try:
            writer = AvroWriter()
            writer.write(data, output_file)

            import fastavro

            with open(output_file, "rb") as f:
                reader = fastavro.reader(f)
                schema = reader.writer_schema
                field_types = {f["name"]: f["type"] for f in schema["fields"]}
                assert field_types["name"] == "string"
                assert field_types["city"] == "string"

        finally:
            Path(output_file).unlink(missing_ok=True)

    def test_schema_inference_numeric_fields(self):
        """Test schema inference for numeric fields."""
        data = [
            {"int_val": 42, "float_val": 3.14},
            {"int_val": 100, "float_val": 2.71},
        ]

        with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
            output_file = f.name

        try:
            writer = AvroWriter()
            writer.write(data, output_file)

            import fastavro

            with open(output_file, "rb") as f:
                reader = fastavro.reader(f)
                schema = reader.writer_schema
                field_types = {f["name"]: f["type"] for f in schema["fields"]}
                assert field_types["int_val"] == "long"
                assert field_types["float_val"] == "double"

        finally:
            Path(output_file).unlink(missing_ok=True)

    def test_schema_inference_boolean_fields(self):
        """Test schema inference for boolean fields."""
        data = [
            {"active": True, "verified": False},
            {"active": False, "verified": True},
        ]

        with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
            output_file = f.name

        try:
            writer = AvroWriter()
            writer.write(data, output_file)

            import fastavro

            with open(output_file, "rb") as f:
                reader = fastavro.reader(f)
                schema = reader.writer_schema
                field_types = {f["name"]: f["type"] for f in schema["fields"]}
                assert field_types["active"] == "boolean"
                assert field_types["verified"] == "boolean"

        finally:
            Path(output_file).unlink(missing_ok=True)

    def test_schema_inference_nullable_fields(self):
        """Test schema inference for nullable fields."""
        data = [
            {"name": "Alice", "email": "alice@example.com"},
            {"name": "Bob", "email": None},
        ]

        with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
            output_file = f.name

        try:
            writer = AvroWriter()
            writer.write(data, output_file)

            import fastavro

            with open(output_file, "rb") as f:
                reader = fastavro.reader(f)
                schema = reader.writer_schema
                field_types = {f["name"]: f["type"] for f in schema["fields"]}
                # Nullable fields should be union types
                assert isinstance(field_types["email"], list)
                assert "null" in field_types["email"]

        finally:
            Path(output_file).unlink(missing_ok=True)


class TestAvroStreamingWriter:
    """Test the AvroStreamingWriter class."""

    def test_streaming_writer_basic(self):
        """Test basic streaming writer functionality."""
        with tempfile.TemporaryDirectory() as temp_dir:
            writer = AvroStreamingWriter(
                destination=temp_dir,
                entity_name="test_entity",
            )

            records = [
                {"id": "1", "name": "Alice"},
                {"id": "2", "name": "Bob"},
            ]

            writer.write_main_records(records)
            writer.close()

            output_file = Path(temp_dir) / "test_entity.avro"
            assert output_file.exists()

            import fastavro

            with open(output_file, "rb") as f:
                reader = fastavro.reader(f)
                read_records = list(reader)
                assert len(read_records) == 2

    def test_streaming_writer_child_tables(self):
        """Test streaming writer with child tables."""
        with tempfile.TemporaryDirectory() as temp_dir:
            writer = AvroStreamingWriter(
                destination=temp_dir,
                entity_name="parent",
            )

            main_records = [{"id": "1", "name": "Parent1"}]
            child_records = [
                {"id": "c1", "parent_id": "1", "value": "child1"},
                {"id": "c2", "parent_id": "1", "value": "child2"},
            ]

            writer.write_main_records(main_records)
            writer.write_child_records("parent_children", child_records)
            writer.close()

            main_file = Path(temp_dir) / "parent.avro"
            child_file = Path(temp_dir) / "parent_children.avro"

            assert main_file.exists()
            assert child_file.exists()

            import fastavro

            with open(child_file, "rb") as f:
                reader = fastavro.reader(f)
                read_records = list(reader)
                assert len(read_records) == 2

    def test_streaming_writer_schema_drift_detection(self):
        """Test that schema drift is detected and raises error."""
        with tempfile.TemporaryDirectory() as temp_dir:
            writer = AvroStreamingWriter(
                destination=temp_dir,
                entity_name="test",
            )

            # First batch establishes schema
            records1 = [{"id": "1", "name": "Alice"}]
            writer.write_main_records(records1)

            # Second batch with new field should raise error
            records2 = [{"id": "2", "name": "Bob", "new_field": "value"}]

            with pytest.raises(OutputError) as exc_info:
                writer.write_main_records(records2)

            assert "schema changed" in str(exc_info.value).lower()
            writer.close()

    def test_streaming_writer_context_manager(self):
        """Test streaming writer as context manager."""
        with tempfile.TemporaryDirectory() as temp_dir:
            with AvroStreamingWriter(
                destination=temp_dir,
                entity_name="test",
            ) as writer:
                writer.write_main_records([{"id": "1", "name": "Alice"}])

            output_file = Path(temp_dir) / "test.avro"
            assert output_file.exists()

    def test_streaming_writer_single_file(self):
        """Test streaming writer to single file."""
        with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
            output_file = f.name

        try:
            writer = AvroStreamingWriter(
                destination=output_file,
                entity_name="test",
            )

            writer.write_main_records([{"id": "1", "name": "Alice"}])
            writer.close()

            assert Path(output_file).exists()

            import fastavro

            with open(output_file, "rb") as f:
                reader = fastavro.reader(f)
                records = list(reader)
                assert len(records) == 1

        finally:
            Path(output_file).unlink(missing_ok=True)


class TestAvroWriterErrorHandling:
    """Test AvroWriter error handling."""

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="Windows allows creation of paths like /nonexistent/directory",
    )
    def test_avro_writer_invalid_path(self):
        """Test writing to invalid path."""
        data = [{"id": "1", "name": "Alice"}]
        invalid_path = "/nonexistent/directory/file.avro"

        writer = AvroWriter()
        with pytest.raises(OutputError):
            writer.write(data, invalid_path)

    def test_avro_writer_text_stream_error(self):
        """Test that text streams raise appropriate error."""
        data = [{"id": "1", "name": "Alice"}]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".avro", delete=False) as f:
            try:
                writer = AvroWriter()
                with pytest.raises(OutputError) as exc_info:
                    writer.write(data, f)
                assert "binary" in str(exc_info.value).lower()
            finally:
                Path(f.name).unlink(missing_ok=True)


class TestAvroWriterIntegration:
    """Test AvroWriter integration with other components."""

    def test_avro_writer_with_transmog_result(self):
        """Test AvroWriter with transmog flatten result."""
        import transmog as tm

        test_data = {
            "id": 1,
            "name": "Test Company",
            "employees": [
                {"id": 1, "name": "Alice", "role": "Developer"},
                {"id": 2, "name": "Bob", "role": "Designer"},
            ],
        }

        result = tm.flatten(test_data, name="company")

        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "output"

            paths = result.save(str(output_path), output_format="avro")

            if isinstance(paths, dict):
                for path in paths.values():
                    assert Path(path).exists()
                    assert Path(path).suffix == ".avro"
            else:
                for path in paths:
                    assert Path(path).exists()
                    assert Path(path).suffix == ".avro"

    def test_avro_flatten_stream(self):
        """Test flatten_stream with Avro output."""
        import transmog as tm

        test_data = [
            {"id": 1, "name": "Alice", "score": 95.5},
            {"id": 2, "name": "Bob", "score": 87.2},
            {"id": 3, "name": "Charlie", "score": 92.0},
        ]

        with tempfile.TemporaryDirectory() as temp_dir:
            tm.flatten_stream(
                test_data,
                temp_dir,
                name="users",
                output_format="avro",
                codec="snappy",
            )

            output_file = Path(temp_dir) / "users.avro"
            assert output_file.exists()

            import fastavro

            with open(output_file, "rb") as f:
                reader = fastavro.reader(f)
                records = list(reader)
                assert len(records) == 3

    def test_avro_writer_performance(self):
        """Test AvroWriter performance with medium dataset."""
        import time

        data = [
            {
                "id": i,
                "name": f"Record_{i}",
                "category": f"Category_{i % 10}",
                "value": i * 1.5,
                "timestamp": f"2023-01-{(i % 28) + 1:02d}T10:00:00Z",
                "active": i % 3 == 0,
            }
            for i in range(5000)
        ]

        with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
            output_file = f.name

        try:
            start_time = time.time()

            writer = AvroWriter()
            writer.write(data, output_file)

            duration = time.time() - start_time

            assert Path(output_file).exists()
            assert Path(output_file).stat().st_size > 0

            # Performance should be reasonable
            assert duration < 5.0, f"Writing took too long: {duration:.2f} seconds"

        finally:
            Path(output_file).unlink(missing_ok=True)


class TestAvroWriterEdgeCases:
    """Test edge cases for AvroWriter."""

    def test_avro_writer_very_long_field_names(self):
        """Test writing data with very long field names."""
        long_field_name = "a" * 500
        data = [
            {"id": "1", long_field_name: "value1"},
            {"id": "2", long_field_name: "value2"},
        ]

        with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
            output_file = f.name

        try:
            writer = AvroWriter()
            writer.write(data, output_file)

            assert Path(output_file).exists()
            assert Path(output_file).stat().st_size > 0

        finally:
            Path(output_file).unlink(missing_ok=True)

    def test_avro_writer_many_columns(self):
        """Test writing data with many columns."""
        data = [{f"col_{i}": f"value_{i}_{j}" for i in range(100)} for j in range(10)]

        with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
            output_file = f.name

        try:
            writer = AvroWriter()
            writer.write(data, output_file)

            assert Path(output_file).exists()
            assert Path(output_file).stat().st_size > 0

        finally:
            Path(output_file).unlink(missing_ok=True)

    def test_avro_writer_special_characters_in_data(self):
        """Test writing data with special characters."""
        data = [
            {"id": "1", "text": "Line 1\nLine 2\tTabbed"},
            {"id": "2", "text": 'Quote: "Hello"'},
            {"id": "3", "text": "Comma, semicolon; pipe|"},
            {"id": "4", "text": "Unicode: 🚀 emoji"},
        ]

        with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
            output_file = f.name

        try:
            writer = AvroWriter()
            writer.write(data, output_file)

            assert Path(output_file).exists()

            import fastavro

            with open(output_file, "rb") as f:
                reader = fastavro.reader(f)
                records = list(reader)
                assert records[3]["text"] == "Unicode: 🚀 emoji"

        finally:
            Path(output_file).unlink(missing_ok=True)

    def test_avro_writer_bytes_field(self):
        """Test writing bytes data to Avro."""
        data = [
            {"id": "1", "data": b"binary data"},
            {"id": "2", "data": b"\x00\x01\x02\x03"},
        ]

        with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
            output_file = f.name

        try:
            writer = AvroWriter()
            writer.write(data, output_file)

            assert Path(output_file).exists()

            import fastavro

            with open(output_file, "rb") as f:
                reader = fastavro.reader(f)
                records = list(reader)
                assert records[0]["data"] == b"binary data"

        finally:
            Path(output_file).unlink(missing_ok=True)

    def test_avro_writer_single_record(self):
        """Test writing a single record."""
        data = [{"id": "1", "name": "Alice"}]

        with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
            output_file = f.name

        try:
            writer = AvroWriter()
            writer.write(data, output_file)

            assert Path(output_file).exists()

            import fastavro

            with open(output_file, "rb") as f:
                reader = fastavro.reader(f)
                records = list(reader)
                assert len(records) == 1

        finally:
            Path(output_file).unlink(missing_ok=True)
