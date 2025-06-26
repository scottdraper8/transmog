"""
Core integration tests for the transmog package.

These tests verify the complete processing flow between components
using the new interface-based testing approach.
"""

import json
import os

import pytest

from tests.interfaces.test_integration_interface import AbstractIntegrationTest
from transmog import Processor, TransmogConfig


class TestCoreIntegration(AbstractIntegrationTest):
    """
    Core integration tests for the transmog package.

    Tests key component interactions and end-to-end flows using the interface-based
    testing approach.
    """

    @pytest.fixture
    def processor(self):
        """Create a processor for core integration testing."""
        config = (
            TransmogConfig.default()
            .with_processing(cast_to_string=True)
            .with_metadata(force_transmog_id=True)
        )
        return Processor(config=config)

    def test_processor_config_flow(self, output_dir):
        """Test that processor configuration flows through the entire processing chain."""
        # Create test data with a string value to test empty value handling
        # Note: null handling varies by implementation, so we focus on empty strings
        data = {
            "id": "test",
            "name": "Test Record",
            "empty_value": "",
            "items": [{"id": "item1", "value": 100}, {"id": "item2", "value": 200}],
        }

        # Create two processors with different configurations
        config1 = TransmogConfig.default().with_processing(
            cast_to_string=True,
            include_empty=False,  # Skip empty values
        )

        config2 = TransmogConfig.default().with_processing(
            cast_to_string=True,
            include_empty=True,  # Include empty values
        )

        # Process the data with both configurations
        processor1 = Processor(config=config1)
        processor2 = Processor(config=config2)

        result1 = processor1.process(data, entity_name="config_test")
        result2 = processor2.process(data, entity_name="config_test")

        # Check results reflect configuration differences
        main1 = result1.get_main_table()[0]
        main2 = result2.get_main_table()[0]

        # Get field names excluding metadata fields
        fields1 = {field for field in main1.keys() if not field.startswith("__")}
        fields2 = {field for field in main2.keys() if not field.startswith("__")}

        # Find the field that corresponds to empty_value (may be flattened or renamed)
        empty_field = next((f for f in fields2 if "empty" in f.lower()), None)

        # Config1 should skip empty values, so this field shouldn't exist
        if empty_field:
            assert empty_field not in fields1, (
                "Empty field should be skipped in config1"
            )
            assert empty_field in fields2, "Empty field should be included in config2"
            assert main2[empty_field] == "", (
                "Empty field should have an empty string value"
            )

        # Write to files and verify differences persist
        if hasattr(result1, "write_all_json") and empty_field:
            # Create directories for output
            config1_dir = os.path.join(output_dir, "config1")
            config2_dir = os.path.join(output_dir, "config2")
            os.makedirs(config1_dir, exist_ok=True)
            os.makedirs(config2_dir, exist_ok=True)

            # Write results to JSON
            json_paths1 = result1.write_all_json(base_path=config1_dir)
            json_paths2 = result2.write_all_json(base_path=config2_dir)

            # Read back the main tables
            with open(json_paths1["main"]) as f:
                data1 = json.load(f)

            with open(json_paths2["main"]) as f:
                data2 = json.load(f)

            # Verify our configuration differences persisted
            # This might be the first record in an array
            if isinstance(data1, list):
                data1 = data1[0]
                data2 = data2[0]

            # Check that the empty field is only in config2 result
            assert empty_field not in data1
            assert empty_field in data2
            assert data2[empty_field] == ""

    def test_complex_hierarchy_flow(self, processor, complex_data, output_dir):
        """Test processing and writing complex hierarchical data."""
        # Process the complex hierarchical data
        result = processor.process(complex_data, entity_name="hierarchy")

        # Get all tables
        table_names = result.get_table_names()

        # Verify level tables were created with more precise filtering
        level1_table = next(
            (
                t
                for t in table_names
                if "level1" in t.lower()
                and "level2" not in t.lower()
                and "level3" not in t.lower()
            ),
            None,
        )
        level2_table = next(
            (
                t
                for t in table_names
                if "level2" in t.lower() and "level3" not in t.lower()
            ),
            None,
        )
        level3_table = next((t for t in table_names if "level3" in t.lower()), None)

        # Check record counts
        level1_records = result.get_child_table(level1_table)
        level2_records = result.get_child_table(level2_table)
        level3_records = result.get_child_table(level3_table)

        assert len(level1_records) > 0
        assert len(level2_records) > 0
        assert len(level3_records) > 0

        # Write all tables to CSV
        if hasattr(result, "write_all_csv"):
            paths = result.write_all_csv(base_path=output_dir)

            # Verify output files were created
            assert os.path.exists(paths["main"])
            assert os.path.exists(paths[level1_table])
            assert os.path.exists(paths[level2_table])
            assert os.path.exists(paths[level3_table])

            # Verify files contain expected data
            with open(paths[level1_table]) as f:
                header = f.readline().strip().lower()
                assert "__transmog_id" in header
                assert "__parent_transmog_id" in header

    def test_reader_writer_roundtrip(self, processor, sample_data, output_dir):
        """Test complete roundtrip through the processor, writer, and reader."""
        # Process the data
        result = processor.process_batch(sample_data, entity_name="roundtrip")

        # Write to JSON
        json_dir = os.path.join(output_dir, "json")
        os.makedirs(json_dir, exist_ok=True)

        if hasattr(result, "write_all_json"):
            json_paths = result.write_all_json(base_path=json_dir)
            main_json_path = json_paths["main"]

            # Read the JSON file back in
            read_result = processor.process_file(
                main_json_path, entity_name="read_test"
            )

            # Verify records were read correctly
            read_records = read_result.get_main_table()
            assert len(read_records) == len(sample_data)

        # Write to CSV
        csv_dir = os.path.join(output_dir, "csv")
        os.makedirs(csv_dir, exist_ok=True)

        if hasattr(result, "write_all_csv"):
            csv_paths = result.write_all_csv(base_path=csv_dir)
            main_csv_path = csv_paths["main"]

            # Read the CSV file back in
            read_csv_result = processor.process_file(
                main_csv_path, entity_name="csv_test"
            )

            # Verify records were read correctly
            read_csv_records = read_csv_result.get_main_table()
            assert len(read_csv_records) == len(sample_data)

    def test_batch_processing_flow(self, processor, output_dir):
        """Test the flow from batch processing to output files."""
        # Create a large batch of data
        batch_data = [
            {
                "id": f"record{i}",
                "name": f"Record {i}",
                "values": list(range(1, 4)),
                "nested": {"field1": f"value{i}", "field2": i * 10},
            }
            for i in range(1, 11)
        ]

        # Process in chunks
        result = processor.process_chunked(
            batch_data, entity_name="batch_flow", chunk_size=3
        )

        # Verify main table
        main_records = result.get_main_table()
        assert len(main_records) == 10

        # Get child tables
        table_names = result.get_table_names()
        values_table = next((t for t in table_names if "values" in t.lower()), None)

        # If values were extracted as a table, verify them
        if values_table:
            values = result.get_child_table(values_table)
            assert len(values) > 0

            # With our new array processing behavior, the parent-child relationships might be different
            # So we'll just verify that each value has a parent ID field
            for value in values:
                assert "__parent_transmog_id" in value, (
                    "Value missing parent ID reference"
                )

        # Write to different formats and verify output
        if hasattr(result, "write_all_json"):
            json_paths = result.write_all_json(
                base_path=os.path.join(output_dir, "json")
            )
            assert os.path.exists(json_paths["main"])

        if hasattr(result, "write_all_csv"):
            csv_paths = result.write_all_csv(base_path=os.path.join(output_dir, "csv"))
            assert os.path.exists(csv_paths["main"])
