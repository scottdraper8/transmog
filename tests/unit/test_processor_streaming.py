"""
Unit tests for the processor's streaming functionality.

These tests cover the streaming methods in the Processor class, including:
- stream_process()
- stream_process_file()
- stream_process_csv()
"""

import os
import json
import csv
import io
import tempfile
import pytest
from typing import Dict, List, Any

from transmog import Processor, TransmogConfig
from transmog.io.writer_interface import StreamingWriter
from transmog.io.writers.json import JsonStreamingWriter
from transmog.io.writers.csv import CsvStreamingWriter
from transmog.error import FileError


class TestProcessorStreaming:
    """Tests for the streaming functionality in the Processor class."""

    def test_stream_process_dict(self):
        """Test stream_process with a dictionary input."""
        # Prepare test data
        data = {
            "id": 123,
            "name": "Test Entity",
            "items": [{"id": 1, "name": "Item 1"}, {"id": 2, "name": "Item 2"}],
        }

        # Create in-memory buffer for output
        buffer = io.StringIO()

        # Create processor and process data
        processor = Processor()
        processor.stream_process(
            data=data,
            entity_name="test",
            output_format="json",
            output_destination=buffer,
        )

        # Check output
        buffer.seek(0)
        result = json.loads(buffer.getvalue())

        # In the current implementation, single dictionaries are output as a list with one item
        assert isinstance(result, list)
        assert len(result) == 1
        record = result[0]

        # Verify main record was processed correctly
        assert "id" in record
        assert record["id"] == "123"
        assert record["name"] == "Test Entity"
        assert "__extract_id" in record
        assert "__extract_datetime" in record

    def test_stream_process_list(self):
        """Test stream_process with a list input."""
        # Prepare test data
        data = [
            {"id": 1, "name": "Record 1"},
            {"id": 2, "name": "Record 2"},
            {"id": 3, "name": "Record 3"},
        ]

        # Create temporary directory for output files
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create processor and process data
            processor = Processor()
            processor.stream_process(
                data=data,
                entity_name="records",
                output_format="json",
                output_destination=temp_dir,
            )

            # Check if output file was created
            output_file = os.path.join(temp_dir, "records.json")
            assert os.path.exists(output_file)

            # Load and verify content
            with open(output_file, "r") as f:
                result = json.load(f)

                # Verify correct number of records
                assert len(result) == 3

                # Verify all records have IDs and content
                for i, record in enumerate(result):
                    assert "__extract_id" in record
                    assert record["id"] == str(i + 1)
                    assert record["name"] == f"Record {i + 1}"

    def test_stream_process_json_string(self):
        """Test stream_process with a JSON string input."""
        # Prepare test data as JSON string
        data = json.dumps(
            [{"id": 1, "name": "Record 1"}, {"id": 2, "name": "Record 2"}]
        )

        # Create in-memory buffer for output
        buffer = io.StringIO()

        # Create processor and process data
        processor = Processor()
        processor.stream_process(
            data=data,
            entity_name="records",
            output_format="json",
            output_destination=buffer,
        )

        # Check output
        buffer.seek(0)
        result = json.loads(buffer.getvalue())

        # Verify correct number of records
        assert len(result) == 2

        # Verify all records have expected content
        assert result[0]["id"] == "1"
        assert result[1]["id"] == "2"

    def test_stream_process_with_child_tables(self):
        """Test stream_process with nested data that produces child tables."""
        # Prepare test data with nested arrays
        data = {
            "id": "parent",
            "name": "Parent Record",
            "children": [
                {"id": "child1", "name": "Child 1"},
                {"id": "child2", "name": "Child 2"},
            ],
            "tags": [
                {"tag": "important", "priority": 1},
                {"tag": "urgent", "priority": 2},
            ],
        }

        # Create temporary directory for output files
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create processor and process data with array processing enabled
            config = TransmogConfig.default().with_processing(visit_arrays=True)
            processor = Processor(config=config)

            processor.stream_process(
                data=data,
                entity_name="parent",
                output_format="json",
                output_destination=temp_dir,
            )

            # Check if main and child output files were created
            main_file = os.path.join(temp_dir, "parent.json")
            children_file = os.path.join(temp_dir, "parent_children.json")
            tags_file = os.path.join(temp_dir, "parent_tags.json")

            assert os.path.exists(main_file)
            assert os.path.exists(children_file)
            assert os.path.exists(tags_file)

            # Load and verify content
            with open(main_file, "r") as f:
                main_records = json.load(f)
                # Main output is a list with one item
                assert isinstance(main_records, list)
                assert len(main_records) == 1
                main_record = main_records[0]
                assert main_record["id"] == "parent"
                assert main_record["name"] == "Parent Record"
                assert "__extract_id" in main_record

            # Check child tables
            with open(children_file, "r") as f:
                children = json.load(f)
                assert len(children) == 2  # Two child records
                child_ids = [c["id"] for c in children]
                assert "child1" in child_ids
                assert "child2" in child_ids
                # Verify parent-child relationship
                for child in children:
                    assert "__parent_extract_id" in child
                    assert "__extract_id" in child
                    assert "__extract_datetime" in child

            # Check tags table
            with open(tags_file, "r") as f:
                tags = json.load(f)
                assert len(tags) == 2  # Two tag records
                tag_values = [t["tag"] for t in tags]
                assert "important" in tag_values
                assert "urgent" in tag_values
                # Verify parent-child relationship
                for tag in tags:
                    assert "__parent_extract_id" in tag
                    assert "__extract_id" in tag

    def test_stream_process_file_json(self):
        """Test stream_process_file with a JSON file."""
        # Create test data
        data = [
            {"id": 1, "name": "Record 1", "value": 100},
            {"id": 2, "name": "Record 2", "value": 200},
            {"id": 3, "name": "Record 3", "value": 300},
        ]

        # Create a temporary JSON input file
        with tempfile.NamedTemporaryFile(
            mode="w+", suffix=".json", delete=False
        ) as input_file:
            json.dump(data, input_file)
            input_path = input_file.name

        try:
            # Create a temporary directory for output
            with tempfile.TemporaryDirectory() as temp_dir:
                # Process the file
                processor = Processor()

                processor.stream_process_file(
                    file_path=input_path,
                    entity_name="records",
                    output_format="json",
                    output_destination=temp_dir,
                )

                # Verify output file exists
                output_file = os.path.join(temp_dir, "records.json")
                assert os.path.exists(output_file)

                # Verify content
                with open(output_file, "r") as f:
                    result = json.load(f)
                    assert len(result) == 3

                    # Check processed data
                    for i, record in enumerate(result):
                        assert record["id"] == str(i + 1)
                        assert record["value"] == str((i + 1) * 100)
                        assert "__extract_id" in record
        finally:
            # Clean up input file
            os.unlink(input_path)

    def test_stream_process_file_jsonl(self):
        """Test stream_process_file with a JSONL file."""
        # Create a temporary JSONL input file
        with tempfile.NamedTemporaryFile(
            mode="w+", suffix=".jsonl", delete=False
        ) as input_file:
            # Write JSONL content
            input_file.write('{"id": 1, "name": "Record 1"}\n')
            input_file.write('{"id": 2, "name": "Record 2"}\n')
            input_file.write('{"id": 3, "name": "Record 3"}\n')
            input_path = input_file.name

        try:
            # Create a temporary directory for output
            with tempfile.TemporaryDirectory() as temp_dir:
                # Process the file
                processor = Processor()

                processor.stream_process_file(
                    file_path=input_path,
                    entity_name="records",
                    output_format="json",
                    output_destination=temp_dir,
                )

                # Verify output file exists
                output_file = os.path.join(temp_dir, "records.json")
                assert os.path.exists(output_file)

                # Verify content
                with open(output_file, "r") as f:
                    result = json.load(f)
                    assert len(result) == 3

                    # Check processed data
                    for i, record in enumerate(result):
                        assert record["id"] == str(i + 1)
                        assert record["name"] == f"Record {i + 1}"
                        assert "__extract_id" in record
        finally:
            # Clean up input file
            os.unlink(input_path)

    def test_stream_process_csv(self):
        """Test stream_process_csv method."""
        # Create a temporary CSV input file
        with tempfile.NamedTemporaryFile(
            mode="w+", suffix=".csv", delete=False, newline=""
        ) as input_file:
            writer = csv.writer(input_file)
            writer.writerow(["id", "name", "value"])
            writer.writerow([1, "Record 1", 100])
            writer.writerow([2, "Record 2", 200])
            writer.writerow([3, "Record 3", 300])
            input_path = input_file.name

        try:
            # Create a temporary directory for output
            with tempfile.TemporaryDirectory() as temp_dir:
                # Process the CSV file
                processor = Processor()

                processor.stream_process_csv(
                    file_path=input_path,
                    entity_name="records",
                    output_format="json",
                    output_destination=temp_dir,
                    has_header=True,
                    delimiter=",",
                )

                # Verify output file exists
                output_file = os.path.join(temp_dir, "records.json")
                assert os.path.exists(output_file)

                # Verify content
                with open(output_file, "r") as f:
                    result = json.load(f)
                    assert len(result) == 3

                    # Check processed data
                    for i, record in enumerate(result):
                        assert record["id"] == str(i + 1)
                        assert record["name"] == f"Record {i + 1}"
                        assert record["value"] == str((i + 1) * 100)
                        assert "__extract_id" in record
        finally:
            # Clean up input file
            os.unlink(input_path)

    def test_stream_process_csv_options(self):
        """Test stream_process_csv with different options."""
        # Create a temporary CSV input file with different delimiter
        with tempfile.NamedTemporaryFile(
            mode="w+", suffix=".csv", delete=False, newline=""
        ) as input_file:
            # Use tab delimiter and include a missing value
            input_file.write("id\tname\tvalue\n")
            input_file.write("1\tRecord 1\t100\n")
            input_file.write("2\tRecord 2\t\n")  # Missing value
            input_file.write("3\tRecord 3\t300\n")
            input_path = input_file.name

        try:
            # Create a temporary directory for output
            with tempfile.TemporaryDirectory() as temp_dir:
                # Process the CSV file with custom options
                processor = Processor()

                processor.stream_process_csv(
                    file_path=input_path,
                    entity_name="records",
                    output_format="json",
                    output_destination=temp_dir,
                    has_header=True,
                    delimiter="\t",
                    null_values=[""],  # Treat empty fields as null
                    infer_types=True,
                )

                # Verify output file exists
                output_file = os.path.join(temp_dir, "records.json")
                assert os.path.exists(output_file)

                # Verify content
                with open(output_file, "r") as f:
                    result = json.load(f)
                    assert len(result) == 3

                    # Check second record with missing value
                    assert result[1]["id"] == "2"
                    assert result[1]["name"] == "Record 2"
                    assert "value" not in result[1] or result[1]["value"] is None
        finally:
            # Clean up input file
            os.unlink(input_path)

    def test_stream_process_error_handling(self):
        """Test error handling in stream_process methods."""
        # Test with non-existent file
        with pytest.raises(FileError):
            processor = Processor()
            processor.stream_process_file(
                file_path="non_existent_file.json",
                entity_name="test",
                output_format="json",
                output_destination="output_dir",
            )

        # Test with invalid JSON string
        with pytest.raises(Exception):  # Could be ParsingError or another error
            processor = Processor()
            processor.stream_process(
                data="{invalid_json",
                entity_name="test",
                output_format="json",
                output_destination="output_dir",
            )

        # Test with missing output format
        with pytest.raises(ValueError):
            processor = Processor()
            processor.stream_process(
                data={"id": 1},
                entity_name="test",
                output_format="",  # Empty format
                output_destination="output_dir",
            )

        # Test with unsupported output format
        with pytest.raises(Exception):
            processor = Processor()
            processor.stream_process(
                data={"id": 1},
                entity_name="test",
                output_format="unsupported_format",
                output_destination="output_dir",
            )

    def test_stream_process_format_options(self):
        """Test stream_process with format-specific options."""
        # Prepare test data
        data = [{"id": 1, "name": "Record 1"}, {"id": 2, "name": "Record 2"}]

        # Test with JSON format options
        buffer = io.StringIO()

        # Create processor and process with pretty-printed JSON
        processor = Processor()
        processor.stream_process(
            data=data,
            entity_name="records",
            output_format="json",
            output_destination=buffer,
            indent=2,  # JSON format option - implementation uses 2-space indentation
            sort_keys=True,  # JSON format option
        )

        # Check output is pretty-printed
        buffer.seek(0)
        output = buffer.getvalue()

        # Verify indentation is present
        assert "  " in output  # Check for 2-space indentation

        # Verify the output can be parsed
        parsed_output = json.loads(output)
        assert len(parsed_output) == 2

        # Check that records have the expected fields
        for record in parsed_output:
            assert "id" in record
            assert "name" in record
            assert "__extract_id" in record
            assert "__extract_datetime" in record

    def test_stream_process_large_dataset(self):
        """Test stream_process with a large dataset to verify chunking works."""
        # Create a large dataset (1000 records)
        large_data = [
            {"id": i, "name": f"Record {i}", "value": i * 10} for i in range(1, 1001)
        ]

        # Create temporary directory for output
        with tempfile.TemporaryDirectory() as temp_dir:
            # Process with a small batch size to force multiple chunks
            config = TransmogConfig.default().with_processing(batch_size=10)
            processor = Processor(config=config)

            processor.stream_process(
                data=large_data,
                entity_name="large",
                output_format="json",
                output_destination=temp_dir,
            )

            # Verify output file exists
            output_file = os.path.join(temp_dir, "large.json")
            assert os.path.exists(output_file)

            # Verify all records were processed
            with open(output_file, "r") as f:
                result = json.load(f)
                assert len(result) == 1000

                # Check first and last record
                assert result[0]["id"] == "1"
                assert result[-1]["id"] == "1000"
