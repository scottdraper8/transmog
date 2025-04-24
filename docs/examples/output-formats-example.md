# Output Format Examples

This guide demonstrates the various output formats available in Transmog.

## Sample Data

We'll use this sample data for all examples:

```python
import transmog as tm

# Sample nested data
data = {
    "customer": {
        "id": 123,
        "name": "Acme Corp",
        "contact": {
            "email": "info@acme.example",
            "phone": "555-1234"
        },
        "orders": [
            {"id": "ORD-1", "amount": 99.99, "date": "2023-01-15"},
            {"id": "ORD-2", "amount": 149.50, "date": "2023-02-20"}
        ]
    },
    "metadata": {
        "source": "API",
        "timestamp": "2023-03-01T12:34:56Z"
    }
}

# Create a processor and process the data
processor = tm.Processor()
result = processor.process(data, entity_name="example")
```

## Native Data Structures

### Python Dictionaries

Getting data as Python dictionaries:

```python
# Get all tables as Python dictionaries
tables = result.to_dict()

# Access the main table
main_table = tables["main"]
print("Main table:", main_table)

# Access a child table (from the array extraction)
orders_table = tables["customer_orders"]
print("Orders table:", orders_table)
```

### JSON-Serializable Objects

Getting data as JSON-compatible objects:

```python
# Get all tables as JSON-serializable objects
json_objects = result.to_json_objects()

# Use with standard json module
import json
print(json.dumps(json_objects["main"], indent=2))
```

### PyArrow Tables

Getting data as PyArrow tables:

```python
# Get all tables as PyArrow Tables
# Requires PyArrow to be installed
pa_tables = result.to_pyarrow_tables()

# Access the main table
main_pa_table = pa_tables["main"]
print("Schema:", main_pa_table.schema)
print("Number of rows:", main_pa_table.num_rows)

# Access a column
if "customer_id" in main_pa_table.column_names:
    customer_ids = main_pa_table.column("customer_id")
    print("Customer IDs:", customer_ids.to_pylist())
```

## Bytes Serialization

### JSON Bytes

Getting data as JSON bytes:

```python
# Get all tables as JSON bytes
json_bytes = result.to_json_bytes(indent=2)

# Write bytes to a file
with open("output.json", "wb") as f:
    f.write(json_bytes["main"])

# Or use in memory
import io
buffer = io.BytesIO(json_bytes["main"])
text = buffer.getvalue().decode("utf-8")
print(text[:100] + "...")  # Show beginning of JSON
```

### CSV Bytes

Getting data as CSV bytes:

```python
# Get all tables as CSV bytes
csv_bytes = result.to_csv_bytes(include_header=True)

# Write bytes to a file
with open("output.csv", "wb") as f:
    f.write(csv_bytes["main"])

# Or use in memory
import io
buffer = io.BytesIO(csv_bytes["main"])
text = buffer.getvalue().decode("utf-8")
print(text[:100] + "...")  # Show beginning of CSV
```

### Parquet Bytes

Getting data as Parquet bytes:

```python
# Get all tables as Parquet bytes (requires PyArrow)
parquet_bytes = result.to_parquet_bytes(compression="snappy")

# Write bytes to a file
with open("output.parquet", "wb") as f:
    f.write(parquet_bytes["main"])

# Or use with PyArrow directly
import io
import pyarrow.parquet as pq
buffer = io.BytesIO(parquet_bytes["main"])
table = pq.read_table(buffer)
```

## File Output Methods

### Writing JSON Files

```python
# Write all tables to JSON files
json_paths = result.write_all_json(base_path="output/json")

# Check the file paths
for table_name, file_path in json_paths.items():
    print(f"Wrote {table_name} to {file_path}")
```

### Writing CSV Files

```python
# Write all tables to CSV files
csv_paths = result.write_all_csv(base_path="output/csv")

# Check the file paths
for table_name, file_path in csv_paths.items():
    print(f"Wrote {table_name} to {file_path}")
```

### Writing Parquet Files

```python
# Write all tables to Parquet files (requires PyArrow)
parquet_paths = result.write_all_parquet(
    base_path="output/parquet",
    compression="snappy"
)

# Check the file paths
for table_name, file_path in parquet_paths.items():
    print(f"Wrote {table_name} to {file_path}")
```

## Working with External Libraries

### Pandas Integration

Converting to pandas DataFrames:

```python
import pandas as pd

# Method 1: Via PyArrow Tables
pa_tables = result.to_pyarrow_tables()
main_df = pa_tables["main"].to_pandas()
print("DataFrame head:\n", main_df.head())

# Method 2: Via Python dictionaries
tables = result.to_dict()
main_df = pd.DataFrame(tables["main"])
print("DataFrame shape:", main_df.shape)
```

### SQLite Integration

Importing data into SQLite:

```python
import sqlite3
import pandas as pd

# Get data as pandas DataFrames
pa_tables = result.to_pyarrow_tables()
main_df = pa_tables["main"].to_pandas()
orders_df = pa_tables["customer_orders"].to_pandas()

# Create SQLite database
conn = sqlite3.connect(":memory:")

# Write DataFrames to SQLite tables
main_df.to_sql("main", conn, index=False)
orders_df.to_sql("orders", conn, index=False)

# Query the database
cursor = conn.cursor()
cursor.execute("SELECT * FROM main")
print("Main table records:", cursor.fetchall())

cursor.execute("SELECT COUNT(*) FROM orders")
print("Order count:", cursor.fetchone()[0])
```

### AWS S3 Integration

Uploading to S3:

```python
import boto3
import io

# Get data as bytes
parquet_bytes = result.to_parquet_bytes()

# Initialize S3 client
s3 = boto3.client('s3')

# Upload to S3
for table_name, data in parquet_bytes.items():
    s3.upload_fileobj(
        io.BytesIO(data),
        'my-bucket',
        f'data/{table_name}.parquet'
    )
    print(f"Uploaded {table_name} to S3")
```

## Format Conversion

Converting between formats:

```python
# Process CSV data
csv_result = processor.process_csv("data.csv", entity_name="csv_example")

# Convert to Parquet
csv_result.write_all_parquet("output/parquet")

# Process JSON data
json_result = processor.process_file("data.json", entity_name="json_example")

# Convert to CSV
json_result.write_all_csv("output/csv")
```

## Memory Management

For large datasets, memory usage can be managed with chunk processing:

```python
# Process in chunks
processor = tm.Processor()
chunk_size = 1000

# Process large file in memory-efficient chunks
result = processor.process_chunked(
    "large_file.json",
    entity_name="large_example",
    chunk_size=chunk_size
)

# Data is accessible as one combined result
print(f"Processed {len(result.to_dict()['main'])} records")
```

## Advanced PyArrow Usage

Using PyArrow for additional processing:

```python
import pyarrow.compute as pc

# Get data as PyArrow Tables
pa_tables = result.to_pyarrow_tables()
main_table = pa_tables["main"]
orders_table = pa_tables["customer_orders"]

# Filtering data
if "customer_name" in main_table.column_names:
    name_filter = pc.match_substring(main_table["customer_name"], "Acme")
    filtered = main_table.filter(name_filter)
    print(f"Records matching 'Acme': {filtered.num_rows}")

# Aggregations
if "amount" in orders_table.column_names:
    total = pc.sum(orders_table["amount"]).as_py()
    average = pc.mean(orders_table["amount"]).as_py()
    print(f"Total amount: {total}, Average: {average}")
```

## Output Format Comparison

Here's a comparison of the different output formats:

| Output Format  | Use Case                              | Dependencies                 |
|----------------|---------------------------------------|------------------------------|
| Dict/JSON      | Simple Python processing, integration | None (standard library)      |
| PyArrow Tables | Data analytics, columnar processing   | PyArrow                      |
| CSV            | Excel, simple tabular data            | None (PyArrow recommended)   |
| Parquet        | Efficient storage, analytics          | PyArrow                      |

## Conditional Output Selection

Adapting outputs based on available dependencies:

```python
# Choose output format based on available libraries
output_path = "output/data"

try:
    # Try to use Parquet first
    result.write_all_parquet(f"{output_path}/parquet")
except ImportError:
    try:
        # Fall back to CSV
        result.write_all_csv(f"{output_path}/csv")
    except Exception:
        # Last resort JSON
        result.write_all_json(f"{output_path}/json")
``` 