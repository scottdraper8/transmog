"""
End-to-end integration tests for transmog.

This module tests the complete processing flow from raw data to output files.
"""

import json
import os

from tests.interfaces.test_integration_interface import AbstractIntegrationTest
from transmog import Processor, TransmogConfig


class TestEndToEnd(AbstractIntegrationTest):
    """
    End-to-end integration tests for the transmog package.

    These tests verify the complete processing flow from raw data to output files,
    focusing on component interaction rather than implementation details.
    """

    def test_json_to_csv_flow(self, processor, sample_data, output_dir):
        """Test complete flow from JSON data to CSV output."""
        # Process the data
        result = processor.process_batch(sample_data, entity_name="json_to_csv")

        # Write to CSV
        if hasattr(result, "write_all_csv"):
            paths = result.write_all_csv(base_path=output_dir)

            # Check if files were created
            for _table_name, path in paths.items():
                assert os.path.exists(path), f"Output file not found: {path}"
                assert os.path.getsize(path) > 0, f"Output file is empty: {path}"

                # Check if the file is a valid CSV
                with open(path) as f:
                    header = f.readline().strip()
                    assert "," in header, f"File does not appear to be CSV: {path}"
                    assert len(header) > 0, f"File has no header: {path}"

    def test_file_processing_flow(self, sample_data, output_dir):
        """Test processing flow from file input to file output."""
        # Create a JSON input file
        input_file = os.path.join(output_dir, "input.json")
        with open(input_file, "w") as f:
            json.dump(sample_data, f)

        # Create processor with different configuration
        config = (
            TransmogConfig.default()
            .with_processing(cast_to_string=True, include_empty=True)
            .with_naming(separator=".")
        )
        processor = Processor(config=config)

        # Process the file
        result = processor.process_file(input_file, entity_name="file_flow")

        # Write to JSON output
        if hasattr(result, "write_all_json"):
            paths = result.write_all_json(base_path=output_dir)

            # Verify JSON output files
            for _table_name, path in paths.items():
                assert os.path.exists(path), f"Output file not found: {path}"

                # Read back the file to verify it's valid JSON
                with open(path) as f:
                    loaded_data = json.load(f)
                    assert isinstance(loaded_data, list), (
                        f"Output not a JSON array: {path}"
                    )
                    assert len(loaded_data) > 0, f"Output file is empty: {path}"

    def test_multi_level_hierarchy_processing(
        self, processor, complex_data, output_dir
    ):
        """Test processing of multi-level hierarchical data."""
        # Additional test beyond what's in the AbstractIntegrationTest

        # Process the data
        result = processor.process(complex_data, entity_name="multi_level")

        # Write results to JSON for inspection
        if hasattr(result, "write_all_json"):
            paths = result.write_all_json(base_path=output_dir)

            # Get all table paths
            all_tables = list(paths.keys())

            # Debug
            print(f"All tables in multi-level hierarchy test: {all_tables}")

            # Read back the first level table with more precise filtering
            level1_table = next(
                (
                    t
                    for t in all_tables
                    if "level1" in t and "level2" not in t and "level3" not in t
                ),
                None,
            )
            with open(paths[level1_table]) as f:
                level1_records = json.load(f)

            # Verify level1 records count
            assert len(level1_records) == 2, "Should have 2 level1 records"

            # Collect all level2 records across potentially multiple tables
            level2_tables = [
                t for t in all_tables if "level2" in t and "level3" not in t
            ]

            level2_records = []
            for table in level2_tables:
                with open(paths[table]) as f:
                    level2_records.extend(json.load(f))

            # Verify total level2 records count
            assert len(level2_records) == 4, "Should have 4 level2 records"

            # Collect all level3 records
            level3_tables = [t for t in all_tables if "level3" in t]
            level3_records = []
            for table in level3_tables:
                with open(paths[table]) as f:
                    level3_records.extend(json.load(f))

            # Verify level3 records count
            assert len(level3_records) == 8, "Should have 8 level3 records"

    def test_config_propagation(self, output_dir):
        """Test that configuration propagates through the entire processing chain."""
        # Create test data
        data = {
            "id": "config_test",
            "empty_value": "",
            "null_value": None,
            "nested": {"field1": "value1", "empty": ""},
            "items": [{"id": "item1", "value": 1}, {"id": "item2", "value": 2}],
        }

        # Create a processor with specific configuration
        config1 = TransmogConfig.default().with_processing(
            cast_to_string=False, skip_null=False, include_empty=True
        )
        processor1 = Processor(config=config1)

        # Create another processor with different config
        config2 = TransmogConfig.default().with_processing(
            cast_to_string=True, skip_null=True, include_empty=False
        )
        processor2 = Processor(config=config2)

        # Process with both processors
        result1 = processor1.process(data, entity_name="config1")
        result2 = processor2.process(data, entity_name="config2")

        # Verify config differences propagated to results
        main1 = result1.get_main_table()[0]
        main2 = result2.get_main_table()[0]

        # Check null handling
        assert "null_value" in main1, "Processor1 should keep null values"
        assert "null_value" not in main2, "Processor2 should skip null values"

        # Check empty value handling
        assert "empty_value" in main1, "Processor1 should keep empty values"
        assert "empty_value" not in main2, "Processor2 should skip empty values"

        # Check type casting
        assert isinstance(main1["id"], str), "ID should be a string"

        # Write to output and check consistency
        if hasattr(result1, "write_all_json"):
            paths1 = result1.write_all_json(
                base_path=os.path.join(output_dir, "config1")
            )
            paths2 = result2.write_all_json(
                base_path=os.path.join(output_dir, "config2")
            )

            # Read back and verify config differences persisted
            with open(paths1["main"]) as f:
                data1 = json.load(f)[0]
            with open(paths2["main"]) as f:
                data2 = json.load(f)[0]

            assert "null_value" in data1
            assert "null_value" not in data2
