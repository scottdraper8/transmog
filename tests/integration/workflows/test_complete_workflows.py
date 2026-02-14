"""
Integration tests for Transmog.

Tests complete workflows and real-world scenarios.
"""

import json
import tempfile
from pathlib import Path

import pytest

import transmog as tm
from transmog.writers.avro import AVRO_AVAILABLE

from ...conftest import assert_files_created, count_files_in_dir


class TestEndToEndWorkflows:
    """Test complete end-to-end workflows."""

    @pytest.mark.skipif(not AVRO_AVAILABLE, reason="fastavro not available")
    def test_json_to_multiple_formats(self, complex_nested_data, output_dir):
        """Test processing JSON and outputting to multiple formats."""
        # Step 1: Flatten the data
        result = tm.flatten(complex_nested_data, name="organization")

        # Verify flattening worked
        assert len(result.main) == 1
        assert len(result.tables) > 0

        # Step 2: Save to CSV
        csv_paths = result.save(str(output_dir / "csv_output"), output_format="csv")
        if isinstance(csv_paths, dict):
            assert_files_created(list(csv_paths.values()))
        else:
            assert_files_created(csv_paths)

        # Step 3: Save to Parquet
        parquet_paths = result.save(
            str(output_dir / "parquet_output"), output_format="parquet"
        )
        if isinstance(parquet_paths, dict):
            assert_files_created(list(parquet_paths.values()))
        else:
            assert_files_created(parquet_paths)

        # Step 4: Save to Avro
        avro_paths = result.save(str(output_dir / "avro_output"), output_format="avro")
        if isinstance(avro_paths, dict):
            assert_files_created(list(avro_paths.values()))
        else:
            assert_files_created(avro_paths)

        # Verify all formats created files in their respective subdirectories
        assert count_files_in_dir(output_dir / "csv_output", "*.csv") > 0
        assert count_files_in_dir(output_dir / "parquet_output", "*.parquet") > 0
        assert count_files_in_dir(output_dir / "avro_output", "*.avro") > 0

    def test_file_to_file_processing(self, large_json_file, output_dir):
        """Test processing from file to file."""
        # Step 1: Process file directly
        result = tm.flatten(large_json_file, name="large_dataset")

        # Verify processing
        assert len(result.main) == 1000  # 1000 records from fixture

        # Step 2: Save processed results
        output_paths = result.save(str(output_dir / "processed"))
        if isinstance(output_paths, dict):
            assert_files_created(list(output_paths.values()))
        else:
            assert_files_created(output_paths)

    def test_streaming_large_dataset(self, output_dir):
        """Test streaming processing of large dataset."""
        # Create large dataset
        large_data = [
            {
                "id": i,
                "name": f"User {i}",
                "profile": {
                    "age": 20 + (i % 50),
                    "city": f"City {i % 10}",
                    "preferences": [f"pref{j}" for j in range(i % 5)],
                },
            }
            for i in range(1, 101)  # 100 users
        ]

        # Stream process to CSV
        config = tm.TransmogConfig(batch_size=20)
        result = tm.flatten_stream(
            large_data,
            output_path=str(output_dir / "streaming_csv"),
            name="users",
            output_format="csv",
            config=config,
        )

        assert result is None  # Streaming returns None

        # Verify files were created and contain data
        csv_files = list((output_dir / "streaming_csv").glob("**/*.csv"))
        assert len(csv_files) > 0

        # Verify content by reading back the main CSV
        import csv

        main_csv = [
            f for f in csv_files if "users" in f.name and "profile" not in f.name
        ]
        if main_csv:
            with open(main_csv[0], newline="") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            # Should have processed all 100 records
            assert len(rows) == 100
            # Verify first and last records
            assert rows[0]["name"] == "User 1"
            assert rows[-1]["name"] == "User 100"

    def test_deterministic_id_consistency(self, array_data):
        """Test that deterministic IDs are consistent across runs."""
        config = tm.TransmogConfig(id_generation="hash", id_field="id")
        result1 = tm.flatten(array_data, name="test", config=config)
        result2 = tm.flatten(array_data, name="test", config=config)

        # Main table IDs should be identical
        assert result1.main[0]["id"] == result2.main[0]["id"]

        # Child table IDs should also be identical
        for table_name in result1.tables:
            if table_name in result2.tables:
                for i, record in enumerate(result1.tables[table_name]):
                    if "_id" in record and i < len(result2.tables[table_name]):
                        assert record["_id"] == result2.tables[table_name][i]["_id"]

        # Data content should be identical
        assert result1.main[0]["name"] == result2.main[0]["name"]


class TestRealWorldScenarios:
    """Test real-world data processing scenarios."""

    def test_ecommerce_order_processing(self, output_dir):
        """Test processing e-commerce order data."""
        ecommerce_data = [
            {
                "order_id": "ORD-001",
                "customer": {
                    "id": "CUST-001",
                    "name": "John Doe",
                    "email": "john@example.com",
                },
                "items": [
                    {
                        "sku": "ITEM-001",
                        "name": "Widget A",
                        "price": 29.99,
                        "quantity": 2,
                        "categories": ["Electronics", "Gadgets"],
                    },
                    {
                        "sku": "ITEM-002",
                        "name": "Widget B",
                        "price": 19.99,
                        "quantity": 1,
                        "categories": ["Electronics"],
                    },
                ],
            }
        ]

        result = tm.flatten(ecommerce_data, name="orders")

        # Verify main orders table
        assert len(result.main) == 1
        assert result.main[0]["order_id"] == "ORD-001"

        # Verify child tables exist
        table_names = list(result.tables.keys())
        assert any("items" in name.lower() for name in table_names)

        # Save to CSV for analysis
        paths = result.save(str(output_dir / "ecommerce"), output_format="csv")
        if isinstance(paths, dict):
            assert_files_created(list(paths.values()))
        else:
            assert_files_created(paths)

    def test_social_media_posts_processing(self, output_dir):
        """Test processing social media posts data."""
        social_data = [
            {
                "post_id": "POST-001",
                "user": {
                    "id": "USER-001",
                    "username": "alice_dev",
                    "profile": {"name": "Alice Developer", "followers": 1500},
                },
                "content": "Just shipped a new feature! 🚀",
                "hashtags": ["coding", "programming", "tech"],
                "mentions": ["@bob_designer", "@charlie_pm"],
                "engagement": {"likes": 45, "comments": 12, "shares": 8},
            }
        ]

        config = tm.TransmogConfig(array_mode=tm.ArrayMode.SEPARATE)
        result = tm.flatten(social_data, name="posts", config=config)

        # Verify processing
        assert len(result.main) == 1
        assert len(result.tables) > 0

        # Check specific tables
        table_names = list(result.tables.keys())
        assert any("hashtags" in name.lower() for name in table_names)

        # Save results
        paths = result.save(str(output_dir / "social_media"))
        if isinstance(paths, dict):
            assert_files_created(list(paths.values()))
        else:
            assert_files_created(paths)


class TestPerformanceScenarios:
    """Test performance-related scenarios."""

    def test_memory_efficient_processing(self, output_dir):
        """Test batch processing with memory-efficient configuration."""
        # Create a dataset with complex structure
        large_dataset = []
        for i in range(50):  # Smaller for testing
            record = {
                "id": i,
                "data": {
                    "values": list(range(20)),  # 20 integers per record
                    "metadata": {
                        "created": f"2023-01-{(i % 28) + 1:02d}T00:00:00Z",
                        "tags": [f"tag{j}" for j in range(5)],  # 5 tags per record
                    },
                },
            }
            large_dataset.append(record)

        # Process with batch configuration
        config = tm.TransmogConfig(batch_size=10)
        result = tm.flatten(large_dataset, name="large_data", config=config)

        assert len(result.main) == 50
        # Verify data integrity
        assert result.main[0]["id"] == 0
        assert result.main[49]["id"] == 49

        # Stream process for comparison
        tm.flatten_stream(
            large_dataset,
            output_path=str(output_dir / "memory_efficient"),
            name="large_data",
            output_format="csv",
            batch_size=10,
        )

        # Verify output files exist and contain data
        csv_files = list((output_dir / "memory_efficient").glob("**/*.csv"))
        assert len(csv_files) > 0

    def test_high_throughput_streaming(self, output_dir):
        """Test high-throughput streaming processing."""

        # Generate data that simulates high-throughput scenario
        def generate_batch(batch_id, size=20):
            return [
                {
                    "batch_id": batch_id,
                    "record_id": f"{batch_id}-{i}",
                    "timestamp": f"2023-01-01T{i:02d}:00:00Z",
                    "payload": {
                        "data": f"payload_{batch_id}_{i}",
                        "metrics": [j * 0.1 for j in range(5)],
                    },
                }
                for i in range(size)
            ]

        # Process multiple batches
        for batch_id in range(3):
            batch_data = generate_batch(batch_id)

            tm.flatten_stream(
                batch_data,
                output_path=str(output_dir / f"batch_{batch_id}"),
                name="throughput_test",
                output_format="csv",
                batch_size=10,
            )

        # Verify all batches processed
        csv_files = list(output_dir.glob("**/*.csv"))
        assert len(csv_files) >= 3  # At least one file per batch
