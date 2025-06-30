---
title: CSV Processing
---

# CSV Processing

Transmog provides functionality for processing CSV files, supporting various configurations for reading
and writing CSV data.

## Reading CSV Files

The primary method for processing CSV files is `flatten_file`:

```python
import transmog as tm

# Process a CSV file
result = tm.flatten_file(
    "data.csv",
    name="customers"
)

# Access the processed data
main_table = result.main
print(f"Processed {len(main_table)} records")
```

### Reading Options

The `flatten_file` method accepts various parameters to configure how CSV files are read:

```python
result = tm.flatten_file(
    "data.csv",
    name="customers",
    delimiter=",",       # Set the delimiter (default: auto-detect or ',')
    quotechar='"',       # Set the quote character (default: '"')
    has_header=True,     # Whether the CSV has a header row (default: True)
    null_values=["", "NULL", "NA", "N/A"],  # Values to treat as NULL
    sanitize_column_names=True,  # Convert column names to safer format
    infer_types=True,    # Convert string values to appropriate types
    skip_rows=0,         # Number of rows to skip before reading
    encoding="utf-8",    # File encoding
    chunk_size=1000      # Process in chunks of 1000 rows (for large files)
)
```

### Type Inference

When `infer_types` is set to `True`, Transmog attempts to convert string values to appropriate types:

- Numeric strings (e.g., "123", "45.67") → integers or floats
- Boolean strings (e.g., "true", "false", "1", "0") → booleans
- Date/time strings → datetime objects (when using PyArrow implementation)

Type inference can be disabled by setting `infer_types=False`, which will keep all values as strings.

### Handling Null Values

The `null_values` parameter specifies which string values should be interpreted as NULL:

```python
result = tm.flatten_file(
    "data.csv",
    name="customers",
    null_values=["", "NULL", "NA", "N/A", "-", "None"]
)
```

### Column Name Sanitization

When `sanitize_column_names` is `True`, column names are transformed to make them more usable:

- Spaces are replaced with underscores
- Special characters are removed
- Names are converted to lowercase
- Reserved words are prefixed with an underscore

For example, "Customer Name" becomes "customer_name", and "SELECT" becomes "_select".

### Processing Large Files

For large CSV files, use the `chunk_size` parameter to process the file in smaller batches:

```python
result = tm.flatten_file(
    "large_data.csv",
    name="large_data",
    chunk_size=10000  # Process 10,000 rows at a time
)
```

This reduces memory usage by avoiding loading the entire file at once.

### Custom Delimiters and Formats

For non-standard CSV formats:

```python
result = tm.flatten_file(
    "data.tsv",
    name="tab_data",
    delimiter="\t",        # Tab-separated
    quotechar="'",         # Single quote for text fields
    encoding="latin-1"     # Specific encoding
)
```

### Edge Cases

Transmog handles several edge cases in CSV processing:

- **Empty Files**: Returns an empty result with no error
- **Files without Headers**: Use `has_header=False` and optionally provide headers with a separate parameter
- **Inconsistent Row Lengths**: Extra columns are ignored, missing columns are filled with nulls
- **Malformed CSV**: Attempts to recover, but may raise an error if the file is severely malformed

## Writing CSV Files

Transmog provides methods for writing processed data to CSV format:

### Writing to Files

The `save` method writes all tables to CSV files:

```python
# Write all tables to CSV files
result.save(
    path="output/csv",
    format="csv",
    delimiter=",",
    include_header=True
)

print(f"Main table written to: output/csv/main.csv")
```

### Writing Options

The CSV writing methods accept various parameters:

```python
result.save(
    path="output/csv",
    format="csv",
    delimiter=",",         # Column delimiter
    include_header=True,   # Include column headers
    quotechar='"',         # Character for quoting fields
    sanitize_header=True,  # Sanitize column names
    separator="_"          # Separator for sanitized names
)
```

### Memory-Efficient Output

For memory-efficient output without intermediate storage, use the `flatten_stream` function:

```python
import transmog as tm

# Stream process a file directly to CSV
tm.flatten_stream(
    file_path="large_data.csv",
    name="large_data",
    output_path="output/csv",
    output_format="csv",
    chunk_size=10000,  # Process in chunks
    include_header=True,
    delimiter=","
)
```

### CSV Dialect Options

The `save` method accepts standard CSV dialect options:

```python
import csv

result.save(
    path="output/csv",
    format="csv",
    delimiter=",",
    quotechar='"',
    quoting=csv.QUOTE_MINIMAL,  # Quoting style
    escapechar='\\',            # Character for escaping
    lineterminator='\n'         # Line ending character(s)
)
```

### Empty Tables

When writing empty tables:

- Tables with no records will produce a file with only the header row (if `include_header=True`)
- Tables with records but no columns will produce a file with empty rows
- Completely empty tables (no records or columns) will produce an empty file

## Performance Considerations

### Adaptive Reader Selection

Transmog now uses intelligent reader selection based on file size and characteristics:

- **Small files (<100K rows)**: Uses native Python CSV reader for optimal performance
- **Medium files (100K-1M rows)**: Uses Polars for better performance
- **Large files (>1M rows)**: Uses Polars for best performance

The selection is automatic and optimizes for the best performance based on your file size.

### Environment Variable Override

For immediate performance improvements, you can force the use of the native CSV reader:

```bash
# Force native CSV reader for better performance on small to medium files
export TRANSMOG_FORCE_NATIVE_CSV=true
```

This is particularly useful when processing files with 50K-500K records where the native reader often outperforms PyArrow.

### PyArrow Integration

When PyArrow is available and selected, Transmog uses optimized processing:

- Single-pass file reading (no double-read when `cast_to_string=True`)
- Batch conversion for columnar to row-oriented data
- Better suited for very large files (>1M rows) with columnar operations

The standard library CSV implementation is preferred for small to medium files due to lower overhead.

### Memory Usage

- For large files, use `chunk_size` to control memory usage
- Type inference requires more memory than leaving values as strings
- Writing large tables directly to files uses less memory than generating bytes first
- Native CSV reader has lower memory overhead for small to medium files

## Implementation Details

The CSV processing functionality has three implementations:

1. **Native Python implementation**: Default for small to medium files
   - Fastest for files under 100K rows
   - Lower memory overhead
   - Excellent performance for row-oriented processing

2. **Polars implementation**: Optimal for large files
   - Best performance for files over 100K rows
   - Efficient memory usage for large datasets
   - Excellent for analytical workloads

3. **PyArrow implementation**: Specialized use cases
   - Used when specific Arrow features are needed
   - Good for integration with Arrow ecosystem
   - Optimized with batch conversion for better performance

These implementations are selected automatically based on file size and available dependencies.
You can override the selection using the `TRANSMOG_FORCE_NATIVE_CSV` environment variable.

## Advanced CSV Processing

### Streaming Large CSV Files

For very large CSV files, use the streaming API for memory-efficient processing:

```python
import transmog as tm

# Stream a large CSV file directly to output files
tm.flatten_stream(
    file_path="very_large.csv",
    name="large_data",
    output_path="output/large_csv",
    output_format="csv",
    chunk_size=50000,  # Process in chunks of 50,000 rows
    low_memory=True    # Use memory-efficient processing
)
```

### Converting CSV to Other Formats

Convert CSV to other formats in a memory-efficient way:

```python
import transmog as tm

# Convert CSV to Parquet
result = tm.flatten_file("data.csv", name="data")
result.save("output/data.parquet")  # Format detected from extension

# Or stream directly to Parquet
tm.flatten_stream(
    file_path="large.csv",
    name="data",
    output_path="output/data",
    output_format="parquet",
    compression="snappy"  # Parquet-specific option
)
```
