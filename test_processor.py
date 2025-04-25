#!/usr/bin/env python3
"""
Test script to verify that the Processor class works correctly.
"""

from transmog import Processor

# Create a processor
processor = Processor()

# Sample data
data = {
    "id": "123",
    "name": "Sample Record",
    "details": {"type": "test", "created_at": "2023-01-01"},
    "items": [{"id": "item1", "value": 100}, {"id": "item2", "value": 200}],
}

# Process the data
result = processor.process(data, entity_name="test_entity")

# Print the main table
print("Main Table Results:")
for i, record in enumerate(result.main_table):
    print(f"\nRecord {i + 1}:")
    for key, value in record.items():
        print(f"  {key}: {value}")

# Print child tables
print("\nChild Tables:")
for table_name, records in result.child_tables.items():
    print(f"\n{table_name} Table:")
    for i, record in enumerate(records):
        print(f"  Record {i + 1}:")
        for key, value in record.items():
            print(f"    {key}: {value}")
