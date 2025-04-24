# CSV Processing with Transmogrify

This guide demonstrates how to process CSV files using Transmogrify.

## Basic CSV Processing

To process a CSV file:

```python
import transmogrify as tm

# Initialize processor
processor = tm.Processor()

# Process a CSV file
result = processor.process_csv(
    file_path="data.csv",
    entity_name="customer_data"
)

# Access the data
records = result.to_dict()["main"]
print(f"Processed {len(records)} records")
```

## CSV Processing Options

The CSV processor offers several configuration options:

```python
result = processor.process_csv(
    file_path="data.csv",
    entity_name="customers",
    delimiter=",",               # Specify delimiter character
    has_header=True,             # File has header row
    null_values=["", "NULL", "NA"], # Values to treat as null
    sanitize_column_names=True,  # Clean up column names
    infer_types=True,            # Infer data types
    skip_rows=0,                 # Skip rows at beginning of file
    quote_char='"',              # Character for quoting fields
    encoding="utf-8",            # File encoding
    chunk_size=1000              # Process in chunks of 1000 rows
)
```

## Auto Type Inference

When `infer_types=True`, the processor attempts to convert string values to appropriate types:

```python
# Process with type inference enabled
result = processor.process_csv(
    file_path="data.csv",
    entity_name="sales",
    infer_types=True
)

# Access the typed data
sales = result.to_dict()["main"]
for record in sales:
    # Revenue is now a numeric value, not a string
    if float(record["revenue"]) > 1000:
        print(f"High value sale: {record}")
```

## Processing Large Files

For large CSV files, use chunk-based processing:

```python
# Process a large CSV file
result = processor.process_csv(
    file_path="large_dataset.csv",
    entity_name="transactions",
    chunk_size=5000  # Process 5000 rows at a time
)

# The result combines all chunks
transactions = result.to_dict()["main"]
print(f"Processed {len(transactions)} transactions")
```

## Converting to Other Formats

After processing a CSV file, you can convert to other formats:

```python
# Process CSV file
result = processor.process_csv("data.csv", entity_name="products")

# Convert to JSON
result.write_all_json("output/json")

# Convert to Parquet
result.write_all_parquet("output/parquet")
```

## Custom Null Value Handling

Define which values should be treated as null:

```python
result = processor.process_csv(
    file_path="data.csv",
    entity_name="survey_results",
    null_values=["", "NULL", "N/A", "none", "-"]
)
```

## Working with Malformed CSV

For handling less-than-perfect CSV files:

```python
from transmogrify.recovery import SkipAndLogRecovery

# Create processor with recovery strategy
processor = tm.Processor(
    recovery_strategy=SkipAndLogRecovery()
)

# Process potentially problematic CSV
try:
    result = processor.process_csv(
        file_path="malformed.csv",
        entity_name="data",
        delimiter=",",
        skip_rows=1  # Skip header or problematic first row
    )
    print("Processing completed with recovery")
except Exception as e:
    print(f"Processing failed: {e}")
```

## Custom Delimiter Detection

The CSV processor can automatically detect delimiters:

```python
# Process a tab-delimited file without specifying delimiter
result = processor.process_csv(
    file_path="data.tsv",
    entity_name="log_data"
    # delimiter will be auto-detected as tab
)
```

## Column Name Sanitization

When `sanitize_column_names=True`, column names are cleaned:

```python
# With sanitize_column_names=True (default)
result = processor.process_csv(
    file_path="data.csv",
    entity_name="customers"
)
# "First Name" becomes "first_name"
# "Customer ID" becomes "customer_id"
```

## CSV to PyArrow Table

Using PyArrow for working with tabular data:

```python
# Process CSV to PyArrow
result = processor.process_csv(
    file_path="data.csv",
    entity_name="analytics"
)

# Get as PyArrow table
tables = result.to_pyarrow_tables()
main_table = tables["main"]

# Use PyArrow operations
import pyarrow.compute as pc
if "amount" in main_table.column_names:
    total = pc.sum(main_table["amount"]).as_py()
    avg = pc.mean(main_table["amount"]).as_py()
    print(f"Total: {total}, Average: {avg}")
```

## CSV with Different Encodings

Handle CSV files with different character encodings:

```python
# Process a file with custom encoding
result = processor.process_csv(
    file_path="international_data.csv",
    entity_name="global_data",
    encoding="latin-1"  # Or other encodings like "utf-16", "cp1252", etc.
)
```

## Reading Compressed CSV

The processor can handle compressed CSV files:

```python
# Read gzip-compressed CSV
result = processor.process_csv(
    file_path="data.csv.gz",  # .gz extension is detected automatically
    entity_name="compressed_data"
)

# Or bzip2 compressed
result = processor.process_csv(
    file_path="data.csv.bz2",  # .bz2 extension is detected automatically
    entity_name="compressed_data"
)
```

## Memory Considerations

For large files, consider memory management options:

```python
# Process a very large file with memory optimization
processor = tm.Processor(optimize_for_memory=True)
result = processor.process_csv(
    file_path="very_large.csv",
    entity_name="big_data",
    chunk_size=1000  # Smaller chunks use less memory
)
``` 