"""
Unit tests for the processor module.
"""

import os
import json
import tempfile
import pytest
from transmog import Processor, ProcessingResult, TransmogConfig
from transmog.config import ProcessingMode


class TestProcessor:
    """Tests for the Processor class."""

    def test_process_simple_data(self, processor, simple_data):
        """Test processing simple data."""
        result = processor.process(simple_data, entity_name="test")

        # Verify the result is a ProcessingResult
        assert isinstance(result, ProcessingResult)

        # Verify main records
        main_records = result.get_main_table()
        assert len(main_records) == 1
        assert main_records[0]["id"] == "123"
        assert main_records[0]["name"] == "Test Entity"

        # Check for address fields - since we need to look at the full field names in the table
        # Let's print the keys to debug
        print("Main record keys:", main_records[0].keys())

        # Check for address fields with the correct field name
        assert "addr_street" in main_records[0]
        assert "addr_city" in main_records[0]
        assert "addr_zip" in main_records[0]

    def test_process_complex_data(self, processor, complex_data):
        """Test processing complex data with multiple nesting levels."""
        result = processor.process(complex_data, entity_name="complex")

        # Verify table structure
        table_names = result.get_table_names()
        print(f"Generated table names: {table_names}")

        # Check the main table contains the right number of records - should be 1 record
        assert len(result.get_main_table()) == 1

        # Verify child tables for each nested array
        assert "complex_items" in result.get_table_names()
        items = result.get_child_table("complex_items")
        assert len(items) > 0  # Should have extracted items

        # Verify we have 5 items as expected
        assert len(items) == 5

        # Each item should have a parent reference
        for item in items:
            assert "__parent_extract_id" in item

    def test_process_batch(self, processor, batch_data):
        """Test processing a batch of records."""
        result = processor.process_batch(batch_data, entity_name="batch")

        # Verify main table
        main_records = result.get_main_table()
        assert len(main_records) == len(batch_data)

        # Verify records were processed - don't assume specific string format
        assert len(main_records) > 0
        for record in main_records:
            assert "id" in record
            assert record["id"] is not None

    def test_process_file(self, processor, json_file):
        """Test processing a file."""
        result = processor.process_file(json_file, entity_name="file_test")

        # Verify main records
        main_records = result.get_main_table()
        assert len(main_records) == 10  # 10 records in the file

        # Verify each record has expected fields
        for record in main_records:
            assert "__extract_id" in record
            assert "id" in record
            assert "name" in record
            assert "active" in record

    def test_process_chunked(self, processor, complex_batch):
        """Test chunked processing."""
        # Create a processor with visit_arrays=True
        config = (
            TransmogConfig.default()
            .with_processing(cast_to_string=True, visit_arrays=True)
            .with_naming(separator="_", abbreviate_field_names=False)
        )
        processor_with_arrays = Processor(config=config)

        result = processor_with_arrays.process_chunked(
            complex_batch,
            entity_name="chunked",
            chunk_size=2,  # Process 2 records at a time
        )

        # Verify main table
        main_records = result.get_main_table()
        assert len(main_records) == len(complex_batch)

        # Check that items field is present in the records
        for record in main_records:
            assert "items" in record or "items_0_id" in record

    def test_write_parquet(self, processor, simple_data, test_output_dir):
        """Test writing results to Parquet."""
        result = processor.process(simple_data, entity_name="parquet_test")

        # Write to Parquet
        output_paths = result.write_all_parquet(base_path=test_output_dir)

        # Verify files were created
        main_path = output_paths["main"]
        assert os.path.exists(main_path)

        # Check child table files
        for table_name, path in output_paths.items():
            if table_name != "main":
                assert os.path.exists(path)

    def test_processor_options(self, simple_data):
        """Test processor with different options."""
        # Test with skip_null=False
        config1 = (
            TransmogConfig.default()
            .with_processing(cast_to_string=True, skip_null=False)
            .with_naming(abbreviate_field_names=False)
        )
        processor1 = Processor(config=config1)
        result1 = processor1.process({"id": 1, "name": None}, entity_name="test")
        assert "name" in result1.get_main_table()[0]

        # Test with include_empty=True
        config2 = (
            TransmogConfig.default()
            .with_processing(cast_to_string=True, include_empty=True)
            .with_naming(abbreviate_field_names=False)
        )
        processor2 = Processor(config=config2)
        result2 = processor2.process({"id": 1, "name": ""}, entity_name="test")
        assert result2.get_main_table()[0]["name"] == ""

        # Test with custom separator
        config3 = TransmogConfig.default().with_naming(
            separator=".", abbreviate_field_names=False
        )
        processor3 = Processor(config=config3)
        result3 = processor3.process(
            {"id": 1, "address": {"city": "Test"}}, entity_name="test"
        )
        assert "address.city" in result3.get_main_table()[0]

    def test_combine_results(self, processor, batch_data):
        """Test combining multiple processing results."""
        # Split batch into two parts
        batch1 = batch_data[:5]
        batch2 = batch_data[5:]

        # Process each batch
        result1 = processor.process_batch(batch1, entity_name="combined")
        result2 = processor.process_batch(batch2, entity_name="combined")

        # Combine results
        combined = ProcessingResult.combine_results([result1, result2])

        # Verify combined results
        assert len(combined.get_main_table()) == len(batch_data)
        assert combined.entity_name == "combined"

    def test_optimize_for_memory(self, complex_batch):
        """Test optimize_for_memory option."""
        config = (
            TransmogConfig.default()
            .with_processing(
                cast_to_string=True,
                batch_size=2,
                processing_mode=ProcessingMode.LOW_MEMORY,
                visit_arrays=True,
            )
            .with_naming(separator="_")
        )
        processor = Processor(config=config)

        result = processor.process_chunked(complex_batch, entity_name="memory_test")

        # Verify results
        assert len(result.get_main_table()) == len(complex_batch)

        # Check that items field is present in the records
        main_records = result.get_main_table()
        for record in main_records:
            assert "items" in record or any("items_" in key for key in record.keys())

    def test_configuration_propagation(self):
        """Test configuration propagation."""
        # Create a custom configuration
        config = (
            TransmogConfig.default()
            .with_naming(separator="|", abbreviate_field_names=False)
            .with_processing(cast_to_string=False, skip_null=False)
            .with_metadata(id_field="__custom_id", parent_field="__custom_parent")
        )
        processor = Processor(config=config)

        # Verify configuration is correctly set
        assert processor.config.naming.separator == "|"
        assert processor.config.processing.cast_to_string is False
        assert processor.config.processing.skip_null is False
        assert processor.config.metadata.id_field == "__custom_id"
        assert processor.config.metadata.parent_field == "__custom_parent"

        # Test with nested data
        result = processor.process(
            {"id": 123, "nested": {"field": "value"}}, entity_name="config_test"
        )

        # Check that separator was used in field names
        main_table = result.get_main_table()
        assert "nested|field" in main_table[0]

    def test_unified_processing_modes(self, complex_batch):
        """Test different processing modes."""
        # Test standard mode
        standard_config = TransmogConfig.default().with_processing(
            cast_to_string=True,
            visit_arrays=True,
            processing_mode=ProcessingMode.STANDARD,
        )
        standard_processor = Processor(config=standard_config)
        standard_result = standard_processor.process(
            complex_batch[0], entity_name="standard"
        )

        # Test high performance mode
        perf_config = TransmogConfig.default().with_processing(
            cast_to_string=True,
            visit_arrays=True,
            processing_mode=ProcessingMode.HIGH_PERFORMANCE,
            batch_size=1000,
        )
        perf_processor = Processor(config=perf_config)
        perf_result = perf_processor.process_chunked(
            complex_batch, entity_name="performance"
        )

        # Test low memory mode
        memory_config = TransmogConfig.default().with_processing(
            cast_to_string=True,
            visit_arrays=True,
            processing_mode=ProcessingMode.LOW_MEMORY,
            batch_size=2,
        )
        memory_processor = Processor(config=memory_config)
        memory_result = memory_processor.process_chunked(
            complex_batch, entity_name="memory"
        )

        # All modes should produce results
        assert len(standard_result.get_main_table()) > 0
        assert len(perf_result.get_main_table()) == len(complex_batch)
        assert len(memory_result.get_main_table()) == len(complex_batch)

    def test_data_iterators(self, processor, simple_data, tmp_path):
        """Test different data iterator sources."""
        # Test with dict
        dict_result = processor.process(simple_data, entity_name="dict_test")
        assert len(dict_result.get_main_table()) == 1

        # Test with list of dicts
        list_data = [simple_data, simple_data]
        list_result = processor.process(list_data, entity_name="list_test")
        assert len(list_result.get_main_table()) == 2

        # Test with JSON string - use process_chunked which accepts input_format
        json_str = json.dumps(simple_data)
        str_result = processor.process_chunked(
            json_str, entity_name="str_test", input_format="json"
        )
        assert len(str_result.get_main_table()) == 1

        # Test with file
        file_path = os.path.join(tmp_path, "test.json")
        with open(file_path, "w") as f:
            json.dump(simple_data, f)
        file_result = processor.process_file(file_path, entity_name="file_test")
        assert len(file_result.get_main_table()) == 1


@pytest.fixture
def processor():
    """Create a processor for testing."""
    config = TransmogConfig.default().with_processing(cast_to_string=True)
    return Processor(config=config)


@pytest.fixture
def simple_data():
    """Create simple test data."""
    return {
        "id": "123",
        "name": "Test Entity",
        "active": True,
        "address": {
            "street": "123 Main St",
            "city": "Test City",
            "zip": "12345",
        },
        "contacts": [
            {"type": "primary", "name": "John Doe", "phone": "123-456-7890"},
            {"type": "secondary", "name": "Jane Smith", "phone": "098-765-4321"},
        ],
    }


@pytest.fixture
def complex_data():
    """Create complex test data."""
    return {
        "id": "root",
        "name": "Root Entity",
        "items": [
            {"id": "item1", "value": 100},
            {"id": "item2", "value": 200},
            {"id": "item3", "value": 300},
            {"id": "item4", "value": 400},
            {"id": "item5", "value": 500},
        ],
    }


@pytest.fixture
def complex_batch():
    """Create a batch of complex test data."""
    batch = []
    for i in range(10):
        batch.append(
            {
                "id": f"record{i}",
                "name": f"Record {i}",
                "items": [
                    {"id": f"item{i}_1", "value": i * 100 + 1},
                    {"id": f"item{i}_2", "value": i * 100 + 2},
                ],
            }
        )
    return batch


@pytest.fixture
def batch_data():
    """Create a batch of simple test data."""
    return [{"id": str(i), "name": f"Record {i}"} for i in range(10)]


@pytest.fixture
def json_file():
    """Create a temporary JSON file."""
    data = [
        {"id": str(i), "name": f"Record {i}", "active": i % 2 == 0} for i in range(10)
    ]
    with tempfile.NamedTemporaryFile(delete=False, mode="w", suffix=".json") as f:
        json.dump(data, f)
        return f.name


@pytest.fixture
def test_output_dir():
    """Create a temporary directory for test output."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir
