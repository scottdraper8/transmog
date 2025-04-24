# CSV Processing

Transmogrify provides functionality for processing CSV files, supporting various configurations for reading and writing CSV data.

## Reading CSV Files

The primary method for processing CSV files is `process_csv`:

```python
from transmogrify import Processor

processor = Processor()

# Process a CSV file
result = processor.process_csv(
    "data.csv",
    entity_name="customers"
)

# Access the processed data
main_table = result.get_main_table()
print(f"Processed {len(main_table)} records")
```

### Reading Options

The `process_csv` method accepts various parameters to configure how CSV files are read:

```python
result = processor.process_csv(
    "data.csv",
    entity_name="customers",
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

When `infer_types` is set to `True`, Transmogrify attempts to convert string values to appropriate types:

- Numeric strings (e.g., "123", "45.67") → integers or floats
- Boolean strings (e.g., "true", "false", "1", "0") → booleans
- Date/time strings → datetime objects (when using PyArrow implementation)

Type inference can be disabled by setting `infer_types=False`, which will keep all values as strings.

### Handling Null Values

The `null_values` parameter specifies which string values should be interpreted as NULL:

```python
result = processor.process_csv(
    "data.csv", 
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
result = processor.process_csv(
    "large_data.csv", 
    chunk_size=10000  # Process 10,000 rows at a time
)
```

This reduces memory usage by avoiding loading the entire file at once.

### Custom Delimiters and Formats

For non-standard CSV formats:

```python
result = processor.process_csv(
    "data.tsv",
    delimiter="\t",        # Tab-separated
    quotechar="'",         # Single quote for text fields
    encoding="latin-1"     # Specific encoding
)
```

### Edge Cases

Transmogrify handles several edge cases in CSV processing:

- **Empty Files**: Returns an empty result with no error
- **Files without Headers**: Use `has_header=False` and optionally provide headers with a separate parameter
- **Inconsistent Row Lengths**: Extra columns are ignored, missing columns are filled with nulls
- **Malformed CSV**: Attempts to recover, but may raise an error if the file is severely malformed

## Writing CSV Files

Transmogrify provides methods for writing processed data to CSV format:

### Writing to Files

The `write_all_csv` method writes all tables to CSV files:

```python
# Write all tables to CSV files
output_paths = result.write_all_csv(
    base_path="output/csv",
    delimiter=",",
    include_header=True
)

print(f"Main table written to: {output_paths['main']}")
```

### Writing Options

The CSV writing methods accept various parameters:

```python
result.write_all_csv(
    base_path="output/csv",
    delimiter=",",         # Column delimiter
    include_header=True,   # Include column headers
    quotechar='"',         # Character for quoting fields
    sanitize_header=True,  # Sanitize column names
    separator="_"          # Separator for sanitized names
)
```

### Memory-Efficient Output

For memory-efficient output without writing to disk, use the `to_csv_bytes` method:

```python
# Get CSV data as bytes
csv_bytes = result.to_csv_bytes(
    include_header=True, 
    delimiter=","
)

# Access individual tables
main_table_bytes = csv_bytes["main"]
```

### CSV Dialect Options

Both `write_all_csv` and `to_csv_bytes` accept standard CSV dialect options:

```python
result.write_all_csv(
    base_path="output/csv",
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

### PyArrow Integration

When PyArrow is available, Transmogrify uses it for CSV processing, providing:

- Faster processing of large files
- Parallel processing capabilities
- Better type inference
- More memory-efficient operation

The standard library CSV implementation is used as a fallback when PyArrow is not available.

### Memory Usage

- For large files, use `chunk_size` to control memory usage
- Type inference requires more memory than leaving values as strings
- Writing large tables directly to files uses less memory than generating bytes first

## Implementation Details

The CSV processing functionality has two implementations:

1. **PyArrow-based implementation**: Used when PyArrow is available
   - Faster for large files
   - Better type inference
   - Parallel processing capabilities
   
2. **Standard library implementation**: Used when PyArrow is not available
   - Compatible with all Python environments
   - More configurable through CSV dialect options
   - Slightly slower for large files

These implementations are used automatically based on available dependencies, with no need for manual configuration.

## Further Reading

For detailed API information and examples:

- [CSV Reader API Reference](../api/csv-reader.md)
- [CSV Processing Examples](../examples/csv-processing.md)
- [Processor API Reference](../api/processor.md) for information on the `process_csv` method

## CSV Processing Options

Transmogrify provides several options to customize CSV processing:

```python
result = processor.process_csv(
    "data.csv",
    entity_name="customers",
    delimiter=",",       # Set the delimiter (default: ',')
    quotechar='"',       # Set the quote character (default: '"')
    has_header=True,     # Whether the CSV has a header row (default: True)
    chunk_size=1000,     # Process in chunks of 1000 rows (for large files)
    column_types={       # Specify column data types
        "age": int,
        "price": float,
        "is_active": bool
    }
)
```

## Processing CSV with Custom Headers

If your CSV file doesn't have headers, you can provide them:

```python
# CSV without headers
result = processor.process_csv(
    "data_no_header.csv",
    entity_name="products",
    has_header=False,
    headers=["id", "name", "category", "price", "stock"]
)
```

## Memory-Optimized CSV Processing

For large CSV files, Transmogrify offers memory-optimized processing:

```python
# Create a processor optimized for memory usage
processor = Processor(optimize_for_memory=True)

# Process a large CSV file in chunks
result = processor.process_csv(
    "large_dataset.csv",
    entity_name="transactions",
    chunk_size=5000,  # Process 5000 rows at a time
    optimize_for_memory=True  # Override the processor's default setting
)
```

## Processing CSV with Nested Data

Transmogrify can handle CSV files containing nested data in JSON format:

```python
# Example CSV with a JSON column
# id,name,metadata
# 1,"Product A","{""color"":""red"",""dimensions"":{""width"":10,""height"":5}}"
# 2,"Product B","{""color"":""blue"",""dimensions"":{""width"":8,""height"":4}}"

result = processor.process_csv(
    "products_with_json.csv",
    entity_name="products",
    json_columns=["metadata"]  # Specify columns containing JSON data
)

# The JSON data will be parsed and flattened like regular JSON
```

## Transforming CSV Data

You can transform CSV data during processing:

```python
def transform_row(row):
    # Convert price from string to float and apply discount
    if "price" in row:
        row["price"] = float(row["price"]) * 0.9
    return row

result = processor.process_csv(
    "products.csv",
    entity_name="products",
    transform_function=transform_row
)
```

## Filtering CSV Data

You can filter CSV data during processing:

```python
def filter_row(row):
    # Only include active products with stock > 0
    return row.get("is_active") == "true" and int(row.get("stock", 0)) > 0

result = processor.process_csv(
    "products.csv",
    entity_name="products",
    filter_function=filter_row
)
```

## Handling CSV Errors

Transmogrify provides options for handling CSV parsing errors:

```python
result = processor.process_csv(
    "data_with_errors.csv",
    entity_name="logs",
    on_error="skip",      # Options: "skip", "raise", "continue"
    error_log="errors.log"  # Log errors to a file
)
```

## Working with CSV Results

The processed CSV data can be exported in various formats:

```python
# Process a CSV file
result = processor.process_csv("customers.csv", entity_name="customers")

# Get the data as a list of dictionaries
data_dicts = result.get_main_table()

# Export as JSON
json_data = result.to_json()

# Export back to CSV
csv_data = result.to_csv()

# Get as a pandas DataFrame (if pandas is installed)
df = result.to_pandas()
```

## Advanced CSV Processing

### Processing Multiple CSV Files

You can process and merge multiple CSV files:

```python
# Process multiple CSV files with the same structure
results = []
for file_path in ["data1.csv", "data2.csv", "data3.csv"]:
    result = processor.process_csv(file_path, entity_name="combined_data")
    results.append(result)

# Combine the results
from transmogrify.processing_result import ProcessingResult
combined_result = ProcessingResult.combine(results)

# Access the combined data
combined_data = combined_result.get_main_table()
print(f"Processed {len(combined_data)} total records")
```

### CSV to Database Integration

Easily move CSV data into databases:

```python
import sqlite3

# Process the CSV file
result = processor.process_csv("sales.csv", entity_name="sales")

# Get the data as a list of dictionaries
sales_data = result.get_main_table()

# Connect to a database
conn = sqlite3.connect("sales_data.db")
cursor = conn.cursor()

# Create a table (example schema)
cursor.execute('''
CREATE TABLE IF NOT EXISTS sales (
    id TEXT PRIMARY KEY,
    product_id TEXT,
    quantity INTEGER,
    price REAL,
    date TEXT
)
''')

# Insert the processed data
for row in sales_data:
    cursor.execute(
        "INSERT INTO sales VALUES (?, ?, ?, ?, ?)",
        (row["id"], row["product_id"], row["quantity"], row["price"], row["date"])
    )

conn.commit()
conn.close()
```