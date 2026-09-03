"""End-to-end workflows across flatten, save, and streaming."""

import fastavro
import pyarrow.parquet as pq

import transmog as tm

from ...conftest import output_file_for, read_csv_rows


class TestEndToEndWorkflows:
    """Test complete end-to-end workflows."""

    def test_json_to_multiple_formats(self, complex_nested_data, output_dir):
        """The same flatten result round-trips to CSV, Parquet, and Avro."""
        result = tm.flatten(complex_nested_data, name="organization")

        assert len(result.main) == 1
        assert result.main[0]["name"] == "Complex Entity"
        assert any("departments" in name for name in result.tables)

        csv_paths = result.save(str(output_dir / "csv_output"), output_format="csv")
        parquet_paths = result.save(
            str(output_dir / "parquet_output"), output_format="parquet"
        )
        avro_paths = result.save(str(output_dir / "avro_output"), output_format="avro")

        csv_main = read_csv_rows(csv_paths["organization"])
        assert csv_main[0]["name"] == "Complex Entity"

        parquet_table = pq.read_table(parquet_paths["organization"])
        assert parquet_table.column("name").to_pylist() == ["Complex Entity"]

        with open(avro_paths["organization"], "rb") as handle:
            avro_records = list(fastavro.reader(handle))
        assert avro_records[0]["name"] == "Complex Entity"

        dept_key = next(name for name in result.tables if name.endswith("_departments"))
        csv_depts = read_csv_rows(csv_paths[dept_key])
        assert {row["name"] for row in csv_depts} == {"Engineering", "Sales"}

    def test_file_to_file_processing(self, large_json_file, output_dir):
        """A JSON file flattens and saves with all records intact."""
        result = tm.flatten(large_json_file, name="large_dataset")
        assert len(result.main) == 1000

        paths = result.save(str(output_dir / "processed"), output_format="csv")
        path_list = list(paths.values()) if isinstance(paths, dict) else paths
        rows = read_csv_rows(output_file_for(path_list, "large_dataset", ".csv"))
        assert len(rows) == 1000
        assert rows[0]["id"] == "1"
        assert rows[-1]["id"] == "1000"

    def test_streaming_large_dataset(self, output_dir):
        """Streaming writes one consolidated CSV with every record."""
        large_data = [
            {
                "id": i,
                "name": f"User {i}",
                "profile": {
                    "age": 20 + (i % 50),
                    "city": f"City {i % 10}",
                },
            }
            for i in range(1, 101)
        ]

        result = tm.flatten_stream(
            large_data,
            output_path=str(output_dir / "streaming_csv"),
            name="users",
            output_format="csv",
            config=tm.TransmogConfig(batch_size=20),
        )

        rows = read_csv_rows(output_file_for(result, "users", ".csv"))
        assert len(rows) == 100
        assert rows[0]["name"] == "User 1"
        assert rows[-1]["name"] == "User 100"
        assert rows[0]["profile_city"] == "City 1"

    def test_deterministic_id_consistency(self, array_data):
        """Hash IDs are identical across independent flatten runs."""
        config = tm.TransmogConfig(id_generation="hash", id_field="id")
        result1 = tm.flatten(array_data, name="test", config=config)
        result2 = tm.flatten(array_data, name="test", config=config)

        assert result1.main[0]["id"] == result2.main[0]["id"]
        assert result1.main[0]["name"] == result2.main[0]["name"] == "Company"
        for table_name, rows in result1.tables.items():
            for left, right in zip(rows, result2.tables[table_name], strict=True):
                for key, value in left.items():
                    if key == "_timestamp":
                        continue
                    assert right[key] == value


class TestRealWorldScenarios:
    """Test real-world data processing scenarios."""

    def test_ecommerce_order_processing(self, output_dir):
        """Order items are extracted and saved with parent linkage."""
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
                    },
                    {
                        "sku": "ITEM-002",
                        "name": "Widget B",
                        "price": 19.99,
                        "quantity": 1,
                    },
                ],
            }
        ]

        result = tm.flatten(ecommerce_data, name="orders")
        assert result.main[0]["order_id"] == "ORD-001"
        assert result.main[0]["customer_name"] == "John Doe"

        items_table = next(name for name in result.tables if name.endswith("_items"))
        items = result.tables[items_table]
        assert {row["sku"] for row in items} == {"ITEM-001", "ITEM-002"}
        parent_ids = {row["_parent_id"] for row in items}
        assert parent_ids == {result.main[0]["_id"]}

        paths = result.save(str(output_dir / "ecommerce"), output_format="csv")
        item_rows = read_csv_rows(paths[items_table])
        assert [row["name"] for row in item_rows] == ["Widget A", "Widget B"]
