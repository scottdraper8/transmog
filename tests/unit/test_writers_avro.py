"""
Tests for Avro writer in Transmog.

Tests Avro file writing functionality, schema inference, and edge cases.
"""

import io
import sys
import tempfile
import warnings
from pathlib import Path
from unittest.mock import patch

import pytest

from transmog.exceptions import ConfigurationError, OutputError
from transmog.writers.avro import AVRO_AVAILABLE

if AVRO_AVAILABLE:
    from transmog.writers import AvroStreamingWriter, AvroWriter

# ---- Helper Functions ----


def read_avro_records(file_path):
    """Read records from an Avro file.

    Args:
        file_path: Path to Avro file

    Returns:
        List of records from the file
    """
    import fastavro

    with open(file_path, "rb") as f:
        reader = fastavro.reader(f)
        return list(reader)


def read_avro_schema(file_path):
    """Read schema from an Avro file.

    Args:
        file_path: Path to Avro file

    Returns:
        Schema dictionary from the file
    """
    import fastavro

    with open(file_path, "rb") as f:
        reader = fastavro.reader(f)
        return reader.writer_schema


def read_all_avro_records(directory, entity_name):
    """Read records from all Avro files for an entity in a directory.

    Checks for a consolidated file first, then falls back to part files.

    Args:
        directory: Path to directory containing Avro files
        entity_name: Entity name prefix

    Returns:
        List of all records across all files, in file order
    """
    directory = Path(directory)
    consolidated = directory / f"{entity_name}.avro"
    if consolidated.exists():
        return read_avro_records(str(consolidated))
    part_files = sorted(directory.glob(f"{entity_name}_part_*.avro"))
    all_records = []
    for part_file in part_files:
        all_records.extend(read_avro_records(str(part_file)))
    return all_records


# ---- Fixtures ----


@pytest.fixture
def avro_temp_file(tmp_path):
    """Create a temporary Avro file path.

    Returns path to a temporary .avro file that will be cleaned up after test.
    """
    return tmp_path / "test.avro"


@pytest.fixture
def avro_temp_dir(tmp_path):
    """Create a temporary directory for Avro output.

    Returns path to a temporary directory that will be cleaned up after test.
    """
    output_dir = tmp_path / "avro_output"
    output_dir.mkdir()
    return output_dir


@pytest.mark.skipif(not AVRO_AVAILABLE, reason="fastavro not available")
class TestAvroWriter:
    """Test the AvroWriter class."""

    def test_avro_writer_basic(self, avro_temp_file):
        """Test basic Avro writing functionality."""
        data = [
            {"id": "1", "name": "Alice", "age": "25"},
            {"id": "2", "name": "Bob", "age": "30"},
            {"id": "3", "name": "Charlie", "age": "35"},
        ]

        writer = AvroWriter()
        writer.write(data, str(avro_temp_file))

        assert avro_temp_file.exists()
        assert avro_temp_file.stat().st_size > 0

        # Verify by reading back
        records = read_avro_records(str(avro_temp_file))
        assert len(records) == 3
        assert records[0]["id"] == "1"
        assert records[0]["name"] == "Alice"

    def test_avro_writer_empty_data(self, avro_temp_file):
        """Test writing empty data to Avro.

        Empty data should return the destination path without creating a file
        or creating an empty file, depending on implementation.
        """
        data = []

        writer = AvroWriter()
        result = writer.write(data, str(avro_temp_file))

        # Empty data returns destination path
        assert result == str(avro_temp_file)

        # Verify no file was created or file is empty if created
        if avro_temp_file.exists():
            assert avro_temp_file.stat().st_size == 0

    def test_avro_writer_mixed_types(self, avro_temp_file):
        """Test writing mixed data types to Avro."""
        data = [
            {"id": 1, "name": "Alice", "score": 95.5, "active": True},
            {"id": 2, "name": "Bob", "score": 87.2, "active": False},
            {"id": 3, "name": "Charlie", "score": 92.0, "active": True},
        ]

        writer = AvroWriter()
        writer.write(data, str(avro_temp_file))

        assert avro_temp_file.exists()
        assert avro_temp_file.stat().st_size > 0

        # Verify types are preserved
        records = read_avro_records(str(avro_temp_file))
        assert records[0]["id"] == 1
        assert records[0]["score"] == 95.5
        assert records[0]["active"] is True

    def test_avro_writer_unicode_data(self, avro_temp_file):
        """Test writing Unicode data to Avro."""
        data = [
            {"id": "1", "name": "Jose", "city": "Sao Paulo"},
            {"id": "2", "name": "Francois", "city": "Montreal"},
            {"id": "3", "name": "\u5f20\u4e09", "city": "\u5317\u4eac"},
        ]

        writer = AvroWriter()
        writer.write(data, str(avro_temp_file))

        assert avro_temp_file.exists()
        assert avro_temp_file.stat().st_size > 0

        # Verify Unicode is preserved
        records = read_avro_records(str(avro_temp_file))
        assert records[0]["name"] == "Jose"
        assert records[2]["name"] == "\u5f20\u4e09"

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

    def test_avro_writer_null_values(self, avro_temp_file):
        """Test writing null values to Avro."""
        data = [
            {"id": "1", "name": "Alice", "optional_field": "value1"},
            {"id": "2", "name": "Bob", "optional_field": None},
            {"id": "3", "name": "Charlie", "optional_field": ""},
        ]

        writer = AvroWriter()
        writer.write(data, str(avro_temp_file))

        assert avro_temp_file.exists()

        records = read_avro_records(str(avro_temp_file))
        assert records[0]["optional_field"] == "value1"
        assert records[1]["optional_field"] is None
        assert records[2]["optional_field"] == ""

    def test_avro_writer_nan_inf_values(self, avro_temp_file):
        """Test writing NaN and Infinity values to Avro.

        NaN and Infinity should be converted to None for Avro compatibility.
        """
        data = [
            {"id": "1", "value": 1.5},
            {"id": "2", "value": float("nan")},
            {"id": "3", "value": float("inf")},
            {"id": "4", "value": float("-inf")},
        ]

        writer = AvroWriter()
        writer.write(data, str(avro_temp_file))

        assert avro_temp_file.exists()

        records = read_avro_records(str(avro_temp_file))
        assert records[0]["value"] == 1.5
        # NaN and Inf should be converted to None
        assert records[1]["value"] is None
        assert records[2]["value"] is None
        assert records[3]["value"] is None

    def test_avro_writer_nan_inf_only_preserves_float_type(self, avro_temp_file):
        """Test that fields with only NaN/Inf values are typed as double.

        When a field contains only NaN/Inf values (which normalize to None),
        the schema should still infer the field as ['null', 'double'] not ['null', 'string'].
        This ensures consistency with Parquet/ORC writers.
        """
        data = [
            {"id": 1, "value": float("nan")},
            {"id": 2, "value": float("inf")},
            {"id": 3, "value": float("-inf")},
        ]

        writer = AvroWriter()
        writer.write(data, str(avro_temp_file))

        assert avro_temp_file.exists()

        schema = read_avro_schema(str(avro_temp_file))
        field_types = {f["name"]: f["type"] for f in schema["fields"]}

        # Field should be typed as double union, not string
        assert field_types["value"] == ["null", "double"]

        records = read_avro_records(str(avro_temp_file))
        assert all(r["value"] is None for r in records)


@pytest.mark.skipif(not AVRO_AVAILABLE, reason="fastavro not available")
class TestAvroWriterOptions:
    """Test AvroWriter with various options."""

    @pytest.mark.parametrize(
        "codec",
        ["null", "deflate", "snappy", "bzip2", "xz"],
        ids=["no_compression", "deflate", "snappy", "bzip2", "xz"],
    )
    def test_avro_writer_compression_codecs(self, codec, avro_temp_file):
        """Test Avro writer with different compression codecs.

        Tests codecs supported with cramjam dependency.
        zstandard and lz4 require additional Python packages.
        """
        data = [
            {"id": "1", "name": "Alice", "data": "x" * 100},
            {"id": "2", "name": "Bob", "data": "y" * 100},
            {"id": "3", "name": "Charlie", "data": "z" * 100},
        ]

        writer = AvroWriter(compression=codec)
        writer.write(data, str(avro_temp_file))

        assert avro_temp_file.exists()
        assert avro_temp_file.stat().st_size > 0

        # Verify data can be read back
        records = read_avro_records(str(avro_temp_file))
        assert len(records) == 3
        assert records[0]["name"] == "Alice"

    def test_avro_writer_invalid_codec(self, avro_temp_file):
        """Test Avro writer with invalid codec raises appropriate error."""
        data = [{"id": "1", "name": "Alice"}]

        writer = AvroWriter(compression="invalid_codec")
        with pytest.raises(OutputError) as exc_info:
            writer.write(data, str(avro_temp_file))

        # Verify error message mentions the invalid codec
        assert (
            "invalid_codec" in str(exc_info.value).lower()
            or "codec" in str(exc_info.value).lower()
        )

    def test_avro_writer_sync_interval(self, avro_temp_file):
        """Test Avro writer with custom sync_interval parameter."""
        data = [{"id": str(i), "data": "x" * 1000} for i in range(100)]

        # Test with default sync_interval
        writer_default = AvroWriter()
        writer_default.write(data, str(avro_temp_file))
        default_size = avro_temp_file.stat().st_size

        # Test with larger sync_interval (should result in slightly smaller file)
        avro_temp_file.unlink()
        writer_large = AvroWriter(sync_interval=64000)
        writer_large.write(data, str(avro_temp_file))
        large_interval_size = avro_temp_file.stat().st_size

        # Test sync_interval passed via write options
        avro_temp_file.unlink()
        writer_option = AvroWriter()
        writer_option.write(data, str(avro_temp_file), sync_interval=64000)
        option_size = avro_temp_file.stat().st_size

        # Verify all files are readable
        records = read_avro_records(str(avro_temp_file))
        assert len(records) == 100

        # File sizes should be comparable (sync markers make small difference)
        # Larger sync_interval typically means fewer markers, slightly smaller file
        assert large_interval_size <= default_size + 100  # Allow small variance
        assert option_size == large_interval_size  # Both use same sync_interval


@pytest.mark.skipif(not AVRO_AVAILABLE, reason="fastavro not available")
class TestAvroWriterSchemaInference:
    """Test Avro schema inference functionality."""

    @pytest.mark.parametrize(
        "data,field_name,expected_type",
        [
            ([{"name": "Alice"}, {"name": "Bob"}], "name", "string"),
            ([{"int_val": 42}, {"int_val": 100}], "int_val", "long"),
            ([{"float_val": 3.14}, {"float_val": 2.71}], "float_val", "double"),
            ([{"active": True}, {"active": False}], "active", "boolean"),
        ],
        ids=["string_fields", "numeric_int", "numeric_float", "boolean_fields"],
    )
    def test_schema_inference_basic_types(
        self, data, field_name, expected_type, avro_temp_file
    ):
        """Test schema inference for basic data types."""
        writer = AvroWriter()
        writer.write(data, str(avro_temp_file))

        schema = read_avro_schema(str(avro_temp_file))
        field_types = {f["name"]: f["type"] for f in schema["fields"]}
        assert field_types[field_name] == expected_type

    def test_schema_inference_nullable_fields(self, avro_temp_file):
        """Test schema inference for nullable fields."""
        data = [
            {"name": "Alice", "email": "alice@example.com"},
            {"name": "Bob", "email": None},
        ]

        writer = AvroWriter()
        writer.write(data, str(avro_temp_file))

        schema = read_avro_schema(str(avro_temp_file))
        field_types = {f["name"]: f["type"] for f in schema["fields"]}
        # Nullable fields should be union types
        assert isinstance(field_types["email"], list)
        assert "null" in field_types["email"]

    def test_union_type_coercion_preserves_data(self, avro_temp_file):
        """Test that union type coercion doesn't cause silent data loss.

        When a field has mixed types (e.g., ["null", "long", "string"]),
        the coercion logic should try each type in the union and preserve
        the original value if it matches any type, rather than silently
        converting to null when it doesn't match the first non-null type.
        """
        data = [
            {"id": 1, "value": 42},  # Integer
            {"id": 2, "value": "hello"},  # String that cannot be coerced to numeric
            {"id": 3, "value": "3.14"},  # Numeric string
            {"id": 4, "value": None},  # Null
        ]

        writer = AvroWriter()
        writer.write(data, str(avro_temp_file))

        # Read back and verify no data was lost
        records = read_avro_records(str(avro_temp_file))
        assert len(records) == 4

        # Verify the integer value was preserved
        assert records[0]["value"] == 42

        # Critical test: string value should NOT be converted to null
        # This is the key test that would fail before the fix
        assert records[1]["value"] == "hello"
        assert records[1]["value"] is not None

        # Numeric string gets coerced to the first matching type in union
        # The schema will be ['null', 'long', 'string'] since all values can be strings
        # "3.14" will match long as 3 or string as "3.14"
        assert records[2]["value"] is not None

        # Null should remain null
        assert records[3]["value"] is None


@pytest.mark.skipif(not AVRO_AVAILABLE, reason="fastavro not available")
class TestAvroStreamingWriter:
    """Test the AvroStreamingWriter class."""

    def test_streaming_writer_basic(self, avro_temp_dir):
        """Test basic streaming writer produces a consolidated file."""
        writer = AvroStreamingWriter(
            destination=str(avro_temp_dir),
            entity_name="test_entity",
        )

        records = [
            {"id": "1", "name": "Alice"},
            {"id": "2", "name": "Bob"},
        ]

        writer.write_main_records(records)
        paths = writer.close()

        output_file = avro_temp_dir / "test_entity.avro"
        assert output_file.exists()
        assert len(paths) == 1

        read_records = read_avro_records(str(output_file))
        assert len(read_records) == 2

    def test_streaming_writer_child_tables(self, avro_temp_dir):
        """Test streaming writer with child tables produces consolidated files."""
        writer = AvroStreamingWriter(
            destination=str(avro_temp_dir),
            entity_name="parent",
        )

        main_records = [{"id": "1", "name": "Parent1"}]
        child_records = [
            {"id": "c1", "parent_id": "1", "value": "child1"},
            {"id": "c2", "parent_id": "1", "value": "child2"},
        ]

        writer.write_main_records(main_records)
        writer.write_child_records("parent_children", child_records)
        paths = writer.close()

        main_file = avro_temp_dir / "parent.avro"
        child_file = avro_temp_dir / "parent_children.avro"

        assert main_file.exists()
        assert child_file.exists()
        assert len(paths) == 2

        read_records = read_avro_records(str(child_file))
        assert len(read_records) == 2

    def test_streaming_writer_context_manager(self, avro_temp_dir):
        """Test streaming writer as context manager produces a consolidated file."""
        with AvroStreamingWriter(
            destination=str(avro_temp_dir),
            entity_name="test",
        ) as writer:
            writer.write_main_records([{"id": "1", "name": "Alice"}])

        output_file = avro_temp_dir / "test.avro"
        assert output_file.exists()

    def test_streaming_writer_file_object_raises_configuration_error(self):
        """File-like object destinations raise ConfigurationError."""
        buf = io.BytesIO()
        with pytest.raises(ConfigurationError, match="directory path"):
            AvroStreamingWriter(destination=buf, entity_name="test")

    def test_streaming_writer_multiple_batches(self, avro_temp_dir):
        """Test streaming writer with multiple batches produces multiple part files."""
        # Use batch_size=2 to force flushing after each write_main_records call
        writer = AvroStreamingWriter(
            destination=str(avro_temp_dir),
            entity_name="test_batches",
            batch_size=2,
            consolidate=False,
        )

        # Write first batch (2 records = batch_size, triggers flush)
        batch1 = [
            {"id": "1", "name": "Alice", "value": 100},
            {"id": "2", "name": "Bob", "value": 200},
        ]
        writer.write_main_records(batch1)

        # Write second batch (2 records = batch_size, triggers flush)
        batch2 = [
            {"id": "3", "name": "Charlie", "value": 300},
            {"id": "4", "name": "Diana", "value": 400},
        ]
        writer.write_main_records(batch2)

        # Write third batch (1 record < batch_size, flushed at close)
        batch3 = [
            {"id": "5", "name": "Eve", "value": 500},
        ]
        writer.write_main_records(batch3)

        paths = writer.close()

        # Expect 3 part files: two from batch_size flushes, one from close()
        assert len(paths) == 3
        for i in range(3):
            part_file = avro_temp_dir / f"test_batches_part_{i:04d}.avro"
            assert part_file.exists()

        # Read all records from all part files
        all_records = read_all_avro_records(avro_temp_dir, "test_batches")
        assert len(all_records) == 5, f"Expected 5 records but got {len(all_records)}"

        ids = {r["id"] for r in all_records}
        assert ids == {"1", "2", "3", "4", "5"}

        # Verify data integrity
        records_by_id = {r["id"]: r for r in all_records}
        assert records_by_id["1"]["name"] == "Alice"
        assert records_by_id["1"]["value"] == 100
        assert records_by_id["5"]["name"] == "Eve"
        assert records_by_id["5"]["value"] == 500

    def test_streaming_writer_multiple_batches_with_child_tables(self, avro_temp_dir):
        """Test streaming writer with multiple batches including child tables."""
        writer = AvroStreamingWriter(
            destination=str(avro_temp_dir),
            entity_name="test_parent",
            batch_size=2,
            consolidate=False,
        )

        # Write first batch of main records (triggers flush at batch_size=2)
        main_batch1 = [
            {"id": "1", "name": "Parent1"},
            {"id": "2", "name": "Parent2"},
        ]
        writer.write_main_records(main_batch1)

        # Write first batch of child records (triggers flush at batch_size=2)
        child_batch1 = [
            {"id": "c1", "parent_id": "1", "value": "child1"},
            {"id": "c2", "parent_id": "1", "value": "child2"},
        ]
        writer.write_child_records("children", child_batch1)

        # Write second batch of main records (flushed at close)
        main_batch2 = [
            {"id": "3", "name": "Parent3"},
        ]
        writer.write_main_records(main_batch2)

        # Write second batch of child records (flushed at close)
        child_batch2 = [
            {"id": "c3", "parent_id": "2", "value": "child3"},
            {"id": "c4", "parent_id": "3", "value": "child4"},
        ]
        writer.write_child_records("children", child_batch2)

        paths = writer.close()

        # Verify main part files
        main_records = read_all_avro_records(avro_temp_dir, "test_parent")
        assert len(main_records) == 3
        assert {r["id"] for r in main_records} == {"1", "2", "3"}

        # Verify child part files
        child_records = read_all_avro_records(avro_temp_dir, "children")
        assert len(child_records) == 4
        assert {r["id"] for r in child_records} == {"c1", "c2", "c3", "c4"}


@pytest.mark.skipif(not AVRO_AVAILABLE, reason="fastavro not available")
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

    def test_avro_writer_memory_error_propagates(self):
        """Test that MemoryError is not caught by the writer."""
        from unittest.mock import patch

        data = [{"id": "1", "name": "Alice"}]
        writer = AvroWriter()

        with patch("builtins.open", side_effect=MemoryError("out of memory")):
            with pytest.raises(MemoryError):
                writer.write(data, "/tmp/test.avro")

    def test_avro_writer_oserror_wrapped_in_output_error(self):
        """Test that OSError is wrapped in OutputError."""
        from unittest.mock import patch

        data = [{"id": "1", "name": "Alice"}]
        writer = AvroWriter()

        with patch("builtins.open", side_effect=OSError("disk full")):
            with pytest.raises(OutputError, match="Failed to write Avro file"):
                writer.write(data, "/tmp/test.avro")


@pytest.mark.skipif(not AVRO_AVAILABLE, reason="fastavro not available")
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
        """Test flatten_stream with Avro output produces a consolidated file."""
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
                compression="snappy",
            )

            consolidated = Path(temp_dir) / "users.avro"
            assert consolidated.exists()

            all_records = read_all_avro_records(temp_dir, "users")
            assert len(all_records) == 3

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


@pytest.mark.skipif(not AVRO_AVAILABLE, reason="fastavro not available")
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
            {"id": "4", "text": "Unicode: \U0001f680 emoji"},
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
                assert records[3]["text"] == "Unicode: \U0001f680 emoji"

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


@pytest.mark.skipif(not AVRO_AVAILABLE, reason="fastavro not available")
class TestAvroStreamingWriterExceptionCleanup:
    """Test resource cleanup behavior of AvroStreamingWriter on exceptions."""

    def test_context_manager_cleans_up_on_write_exception(self, tmp_path):
        """Context manager clears internal state when _write_buffer raises."""
        avro_dir = tmp_path / "avro_out"
        avro_dir.mkdir()

        def always_fail(*_args, **_kwargs):
            raise OSError("simulated avro write failure")

        with pytest.raises(OSError, match="simulated avro write failure"):
            with AvroStreamingWriter(
                destination=str(avro_dir),
                entity_name="test",
                batch_size=2,
                consolidate=False,
            ) as writer:
                writer.write_main_records(
                    [{"id": "1", "name": "Alice"}, {"id": "2", "name": "Bob"}]
                )
                with patch("transmog.writers.avro.avro_writer", always_fail):
                    # This batch triggers flush (buffer has 2 records from above
                    # that already flushed, plus these 2 new ones)
                    writer.write_main_records(
                        [
                            {"id": "3", "name": "Charlie"},
                            {"id": "4", "name": "Dave"},
                        ]
                    )

        assert writer._closed is True
        assert writer.buffers == {}
        assert writer.part_counts == {}

    def test_no_context_manager_retains_metadata_on_exception(self, tmp_path):
        """Without context manager, metadata is retained after exception."""
        avro_dir = tmp_path / "avro_out"
        avro_dir.mkdir()

        def always_fail(*_args, **_kwargs):
            raise OSError("simulated avro write failure")

        writer = AvroStreamingWriter(
            destination=str(avro_dir),
            entity_name="test",
            batch_size=2,
            consolidate=False,
        )
        writer.write_main_records(
            [{"id": "1", "name": "Alice"}, {"id": "2", "name": "Bob"}]
        )

        with patch("transmog.writers.avro.avro_writer", always_fail):
            with pytest.raises(OSError, match="simulated avro write failure"):
                writer.write_main_records(
                    [
                        {"id": "3", "name": "Charlie"},
                        {"id": "4", "name": "Dave"},
                    ]
                )

        assert not getattr(writer, "_closed", False)
        # Writer should still have some state from the successful first batch
        assert writer.all_part_paths != []

        writer.close()

    def test_first_batch_part_file_survives_second_batch_failure(self, tmp_path):
        """First batch's part file persists on disk after second batch fails."""
        avro_dir = tmp_path / "avro_out"
        avro_dir.mkdir()

        def always_fail(*_args, **_kwargs):
            raise OSError("simulated avro write failure")

        with pytest.raises(OSError, match="simulated avro write failure"):
            with AvroStreamingWriter(
                destination=str(avro_dir),
                entity_name="test",
                batch_size=2,
                consolidate=False,
            ) as writer:
                writer.write_main_records(
                    [{"id": "1", "name": "Alice"}, {"id": "2", "name": "Bob"}]
                )
                with patch("transmog.writers.avro.avro_writer", always_fail):
                    writer.write_main_records(
                        [
                            {"id": "3", "name": "Charlie"},
                            {"id": "4", "name": "Dave"},
                        ]
                    )

        # The first batch's part file should still exist
        part_file = avro_dir / "test_part_0000.avro"
        assert part_file.exists()

        records = read_avro_records(str(part_file))
        assert len(records) == 2
        assert records[0]["name"] == "Alice"


@pytest.mark.skipif(not AVRO_AVAILABLE, reason="fastavro not available")
class TestAvroCoercion:
    """Test schema coercion for Avro part files."""

    def test_coercion_unifies_avro_schemas(self, tmp_path):
        """Test that coerce_schema rewrites minority Avro part files."""
        import json

        avro_dir = tmp_path / "avro_out"
        avro_dir.mkdir()

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            with AvroStreamingWriter(
                destination=str(avro_dir),
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

        # Part 0 should have been rewritten with the "extra" field
        part0_records = read_avro_records(str(avro_dir / "test_part_0000.avro"))
        part1_records = read_avro_records(str(avro_dir / "test_part_0001.avro"))

        assert len(part0_records) == 2
        assert len(part1_records) == 2
        assert "extra" in part0_records[0]
        assert "extra" in part1_records[0]
        # Coerced field should be null in part 0
        assert part0_records[0]["extra"] is None
        assert part1_records[0]["extra"] == "val"

        log = json.loads((avro_dir / "_schema_log.json").read_text())
        assert "coerced_to" in log["tables"]["main"]["parts"][0]
