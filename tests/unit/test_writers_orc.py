"""Tests for ORC writer."""

import io
import os
import tempfile

import pytest

from transmog.exceptions import MissingDependencyError, OutputError
from transmog.writers.orc import ORC_AVAILABLE

if ORC_AVAILABLE:
    import pyarrow as pa
    import pyarrow.orc as orc

    from transmog.writers.orc import OrcStreamingWriter, OrcWriter


@pytest.mark.skipif(not ORC_AVAILABLE, reason="PyArrow not available")
class TestOrcWriter:
    """Test ORC writer functionality."""

    def test_write_simple_data(self, tmp_path):
        """Test writing simple data to ORC file."""
        data = [
            {"id": 1, "name": "Alice", "value": 10.5},
            {"id": 2, "name": "Bob", "value": 20.3},
        ]

        writer = OrcWriter()
        output_path = tmp_path / "output.orc"
        result = writer.write(data, str(output_path))

        assert result == str(output_path)
        assert output_path.exists()

        # Read back and verify
        table = orc.read_table(str(output_path))
        assert len(table) == 2
        assert set(table.column_names) == {"id", "name", "value"}

    def test_write_empty_data(self, tmp_path):
        """Test writing empty data."""
        writer = OrcWriter()
        output_path = tmp_path / "empty.orc"
        result = writer.write([], str(output_path))

        assert result == str(output_path)

    def test_write_with_missing_fields(self, tmp_path):
        """Test writing data with missing fields."""
        data = [
            {"id": 1, "name": "Alice", "value": 10.5},
            {"id": 2, "name": "Bob"},
            {"id": 3, "value": 30.5},
        ]

        writer = OrcWriter()
        output_path = tmp_path / "output.orc"
        writer.write(data, str(output_path))

        table = orc.read_table(str(output_path))
        assert len(table) == 3

    def test_write_with_compression(self, tmp_path):
        """Test writing with different compression."""
        data = [{"id": i, "value": i * 10} for i in range(100)]

        writer = OrcWriter(compression="snappy")
        output_path = tmp_path / "compressed.orc"
        writer.write(data, str(output_path))

        assert output_path.exists()
        table = orc.read_table(str(output_path))
        assert len(table) == 100

    def test_write_to_binary_stream(self):
        """Test writing to binary stream."""
        data = [{"id": 1, "name": "Alice"}]

        writer = OrcWriter()
        buffer = io.BytesIO()
        result = writer.write(data, buffer)

        assert result is buffer
        assert len(buffer.getvalue()) > 0

    def test_write_to_text_stream_raises_error(self):
        """Test that writing to text stream raises error."""
        data = [{"id": 1, "name": "Alice"}]

        writer = OrcWriter()
        buffer = io.StringIO()

        with pytest.raises(OutputError, match="Failed to write ORC file"):
            writer.write(data, buffer)


@pytest.mark.skipif(not ORC_AVAILABLE, reason="PyArrow not available")
class TestOrcStreamingWriter:
    """Test ORC streaming writer functionality."""

    def test_write_main_records(self, tmp_path):
        """Test writing main records."""
        records = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]

        with OrcStreamingWriter(
            destination=str(tmp_path), entity_name="users"
        ) as writer:
            writer.write_main_records(records)

        output_path = tmp_path / "users.orc"
        assert output_path.exists()

        table = orc.read_table(str(output_path))
        assert len(table) == 2

    def test_write_child_records(self, tmp_path):
        """Test writing child records."""
        main_records = [{"id": 1, "name": "Alice"}]
        child_records = [
            {"user_id": 1, "order_id": 101},
            {"user_id": 1, "order_id": 102},
        ]

        with OrcStreamingWriter(
            destination=str(tmp_path), entity_name="users"
        ) as writer:
            writer.write_main_records(main_records)
            writer.write_child_records("orders", child_records)

        assert (tmp_path / "users.orc").exists()
        assert (tmp_path / "orders.orc").exists()

        orders_table = orc.read_table(str(tmp_path / "orders.orc"))
        assert len(orders_table) == 2

    def test_buffering(self, tmp_path):
        """Test that buffering works correctly."""
        with OrcStreamingWriter(
            destination=str(tmp_path), entity_name="data", batch_size=10
        ) as writer:
            for i in range(25):
                writer.write_main_records([{"id": i, "value": i * 2}])

        output_path = tmp_path / "data.orc"
        assert output_path.exists()

        table = orc.read_table(str(output_path))
        assert len(table) == 25

    def test_multiple_tables(self, tmp_path):
        """Test writing multiple child tables."""
        with OrcStreamingWriter(
            destination=str(tmp_path), entity_name="users"
        ) as writer:
            writer.write_main_records([{"id": 1, "name": "Alice"}])
            writer.write_child_records("orders", [{"user_id": 1, "order_id": 101}])
            writer.write_child_records("addresses", [{"user_id": 1, "city": "NYC"}])

        assert (tmp_path / "users.orc").exists()
        assert (tmp_path / "orders.orc").exists()
        assert (tmp_path / "addresses.orc").exists()

    def test_safe_table_names(self, tmp_path):
        """Test that table names are sanitized properly."""
        with OrcStreamingWriter(
            destination=str(tmp_path), entity_name="users"
        ) as writer:
            writer.write_child_records("user.addresses", [{"id": 1}])

        assert (tmp_path / "user_addresses.orc").exists()

    def test_close_idempotent(self, tmp_path):
        """Test that close can be called multiple times."""
        writer = OrcStreamingWriter(destination=str(tmp_path), entity_name="data")
        writer.write_main_records([{"id": 1}])
        writer.close()
        writer.close()  # Should not raise

        assert (tmp_path / "data.orc").exists()


def test_orc_not_available():
    """Test that appropriate error is raised when PyArrow is not available."""
    if ORC_AVAILABLE:
        pytest.skip("PyArrow is available")

    with pytest.raises(MissingDependencyError):
        from transmog.writers.orc import OrcWriter

        writer = OrcWriter()
        writer.write([{"id": 1}], "output.orc")
