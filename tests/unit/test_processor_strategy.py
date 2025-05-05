"""
Unit tests for the processor strategy pattern.
"""

import os
import json
import tempfile
import pytest
from transmog import Processor, ProcessingResult, TransmogConfig
from transmog.process import (
    ProcessingStrategy,
    InMemoryStrategy,
    FileStrategy,
    BatchStrategy,
    ChunkedStrategy,
    CSVStrategy,
)


class TestProcessorStrategy:
    """Tests for the processor strategy pattern."""

    def test_in_memory_strategy(self, processor, simple_data):
        """Test processing using InMemoryStrategy."""
        # Create strategy directly
        strategy = InMemoryStrategy(processor.config)

        # Process using the strategy
        result = strategy.process(simple_data, entity_name="test")

        # Verify the result
        assert isinstance(result, ProcessingResult)

        # Check main table
        main_records = result.get_main_table()
        assert len(main_records) == 1
        assert main_records[0]["id"] == "123"
        assert main_records[0]["name"] == "Test Entity"
        assert "addr_street" in main_records[0]

    def test_file_strategy(self, processor, json_file):
        """Test processing using FileStrategy."""
        # Create strategy directly
        strategy = FileStrategy(processor.config)

        # Process using the strategy
        result = strategy.process(json_file, entity_name="file_test")

        # Verify the result
        assert isinstance(result, ProcessingResult)

        # Check main table
        main_records = result.get_main_table()
        assert len(main_records) == 10  # 10 records in the file

        # Verify records have been processed
        for record in main_records:
            assert "__extract_id" in record
            assert "id" in record

    def test_batch_strategy(self, processor, batch_data):
        """Test processing using BatchStrategy."""
        # Create strategy directly
        strategy = BatchStrategy(processor.config)

        # Process using the strategy
        result = strategy.process(batch_data, entity_name="batch_test")

        # Verify the result
        assert isinstance(result, ProcessingResult)

        # Check main table
        main_records = result.get_main_table()
        assert len(main_records) == len(batch_data)

    def test_chunked_strategy(self, processor, complex_batch):
        """Test processing using ChunkedStrategy."""
        # Create strategy directly
        strategy = ChunkedStrategy(processor.config)

        # Process using the strategy
        result = strategy.process(
            complex_batch,
            entity_name="chunked_test",
            chunk_size=2,  # Process 2 records at a time
        )

        # Verify the result
        assert isinstance(result, ProcessingResult)

        # Check main table
        main_records = result.get_main_table()
        assert len(main_records) == len(complex_batch)

    def test_csv_strategy(self, processor, tmp_path):
        """Test processing using CSVStrategy."""
        # Create a sample CSV file
        csv_file = tmp_path / "test.csv"
        with open(csv_file, "w") as f:
            f.write("id,name,age\n")
            f.write("1,Alice,30\n")
            f.write("2,Bob,25\n")
            f.write("3,Charlie,35\n")

        # Create a processor with cast_to_string=False
        from transmog import TransmogConfig

        config = TransmogConfig.default().with_processing(cast_to_string=False)
        no_cast_processor = Processor(config=config)

        # Create strategy with the new configuration
        strategy = CSVStrategy(no_cast_processor.config)

        # Process using the strategy
        result = strategy.process(
            str(csv_file), entity_name="csv_test", infer_types=True
        )

        # Verify the result
        assert isinstance(result, ProcessingResult)

        # Check main table
        main_records = result.get_main_table()
        assert len(main_records) == 3

        # Verify records have been processed correctly
        assert main_records[0]["id"] == 1
        assert main_records[0]["name"] == "Alice"
        assert main_records[0]["age"] == 30

    def test_processor_uses_correct_strategy(
        self, processor, simple_data, json_file, batch_data, tmp_path
    ):
        """Test that the processor selects the correct strategy based on input."""
        # Test with dict - should use InMemoryStrategy
        result1 = processor.process(simple_data, entity_name="test")
        assert isinstance(result1, ProcessingResult)
        assert len(result1.get_main_table()) == 1

        # Test with file path - should use FileStrategy
        result2 = processor.process_file(json_file, entity_name="file_test")
        assert isinstance(result2, ProcessingResult)
        assert len(result2.get_main_table()) == 10

        # Test with batch - should use BatchStrategy
        result3 = processor.process_batch(batch_data, entity_name="batch_test")
        assert isinstance(result3, ProcessingResult)
        assert len(result3.get_main_table()) == len(batch_data)

        # Test with chunked processing - should use ChunkedStrategy
        result4 = processor.process_chunked(
            batch_data, entity_name="chunked_test", chunk_size=2
        )
        assert isinstance(result4, ProcessingResult)
        assert len(result4.get_main_table()) == len(batch_data)

        # Test with CSV file - should use CSVStrategy
        csv_file = tmp_path / "test.csv"
        with open(csv_file, "w") as f:
            f.write("id,name,age\n")
            f.write("1,Alice,30\n")
            f.write("2,Bob,25\n")

        # Create a processor with cast_to_string=False
        from transmog import TransmogConfig

        config = TransmogConfig.default().with_processing(cast_to_string=False)
        no_cast_processor = Processor(config=config)

        result5 = no_cast_processor.process_csv(str(csv_file), entity_name="csv_test")
        assert isinstance(result5, ProcessingResult)
        assert len(result5.get_main_table()) == 2


@pytest.fixture
def processor():
    """Create a processor with default configuration."""
    return Processor()


@pytest.fixture
def simple_data():
    """Create simple test data."""
    return {
        "id": 123,
        "name": "Test Entity",
        "addr": {"street": "123 Main St", "city": "Testville", "zip": "12345"},
    }


@pytest.fixture
def complex_batch():
    """Create a batch of complex test data."""
    return [
        {
            "id": i,
            "name": f"Entity {i}",
            "items": [{"id": f"{i}01", "value": "A"}, {"id": f"{i}02", "value": "B"}],
        }
        for i in range(10)
    ]


@pytest.fixture
def batch_data():
    """Create a batch of simple test data."""
    return [{"id": i, "name": f"Record {i}"} for i in range(10)]


@pytest.fixture
def json_file():
    """Create a temporary JSON file with test data."""
    data = [{"id": i, "name": f"Record {i}", "active": i % 2 == 0} for i in range(10)]

    with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as f:
        json.dump(data, f)
        file_path = f.name

    yield file_path

    # Clean up after test
    if os.path.exists(file_path):
        os.unlink(file_path)
