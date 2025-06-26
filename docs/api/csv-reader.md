# CSV Reader API Reference

The CSV Reader functionality in Transmog provides capabilities for processing CSV files and converting
them into structured data.

## Import

```python
from transmog import Processor
```

## Methods

### process_csv

Process a CSV file into a ProcessingResult object.

```python
Processor.process_csv(
    file_path: str,
    entity_name: str,
    transmog_time: Optional[Any] = None,
    delimiter: Optional[str] = None,
    has_header: bool = True,
    null_values: Optional[list[str]] = None,
    sanitize_column_names: bool = True,
    infer_types: bool = True,
    skip_rows: int = 0,
    quote_char: Optional[str] = None,
    encoding: str = "utf-8",
    chunk_size: Optional[int] = None,
) -> ProcessingResult
```

#### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| file_path | str | Required | Path to the CSV file |
| entity_name | str | Required | Name of the entity (used for table naming) |
| transmog_time | Any | None | Extraction timestamp (current time if None) |
| delimiter | str | None | Delimiter character (auto-detect or comma if None) |
| has_header | bool | True | Whether the file has a header row |
| null_values | list[str] | None | List of strings to interpret as null values |
| sanitize_column_names | bool | True | Whether to sanitize column names |
| infer_types | bool | True | Whether to infer data types from string values |
| skip_rows | int | 0 | Number of rows to skip at the beginning |
| quote_char | str | None | Character used for quoting fields (default: double quote) |
| encoding | str | "utf-8" | File encoding (used by standard library implementation) |
| chunk_size | int | None | Size of chunks for processing large files (uses batch_size if None) |

#### Returns

A `ProcessingResult` object containing the CSV data as the main table (no child tables).

#### Example: Basic Usage

```python
import transmog as tm

processor = tm.Processor()

# Process a CSV file
result = processor.process_csv(
    file_path="data.csv",
    entity_name="customers"
)

# Get the data as a dictionary
data = result.to_dict()["main"]
```

#### Example: Custom Configuration

```python
import transmog as tm

processor = tm.Processor()

# Process a CSV file with custom settings
result = processor.process_csv(
    file_path="data.csv",
    entity_name="customers",
    delimiter=",",
    null_values=["", "NULL", "NA", "N/A"],
    infer_types=True,
    sanitize_column_names=True,
    chunk_size=5000  # Process in chunks of 5000 rows
)
```

#### Example: Handling Tab-Separated Values

```python
import transmog as tm

processor = tm.Processor()

# Process a TSV file
result = processor.process_csv(
    file_path="data.tsv",
    entity_name="products",
    delimiter="\t",  # Tab character as delimiter
    quote_char="'",  # Single quote for field quoting
    encoding="latin-1"  # Custom encoding
)
```

## Advanced CSV Processing

### Type Inference

When `infer_types` is set to `True`, Transmog converts string values to appropriate data types:

- Numeric strings → integers or floats
- Boolean strings → booleans (True/False)
- Date/time strings → datetime objects (PyArrow implementation only)

The accuracy of type inference depends on the implementation used:

- **PyArrow implementation**: More accurate type inference, including date/time detection
- **Standard library implementation**: Basic type inference for numbers and booleans

### Null Value Handling

The `null_values` parameter specifies which strings should be treated as NULL values:

```python
result = processor.process_csv(
    "data.csv",
    null_values=["", "NULL", "N/A", "NA", "None", "-"]
)
```

Common null value indicators include:

- Empty strings (`""`)
- The strings "NULL", "null", "NA", "N/A"
- Dashes or other placeholders

### Column Name Sanitization

When `sanitize_column_names` is enabled, column names are transformed to make them more suitable for data processing:

- Spaces are replaced with underscores
- Special characters are removed
- Names are converted to lowercase
- SQL reserved words are prefixed with underscores

For example:

- "First Name" → "first_name"
- "Sales $" → "sales_"
- "GROUP" → "_group"

### Error Handling

The CSV reader attempts to handle common errors:

- **File not found**: Raises a `FileError`
- **File permission issues**: Raises a `FileError`
- **Encoding errors**: Raises a `ProcessingError`
- **Malformed CSV**: Attempts to recover, but may raise a `ProcessingError` if severe

## Implementation Details

### Adaptive Reader Selection

Transmog automatically selects the optimal CSV reader implementation based on file size:

- **Small files (<100K rows)**: Native Python CSV reader
- **Medium files (100K-1M rows)**: Polars
- **Large files (>1M rows)**: Polars for best performance

File size is estimated based on the formula: `file_size_bytes / 100 = estimated_rows`

### Environment Variable Override

You can force the use of the native CSV reader for immediate performance improvements:

```bash
export TRANSMOG_FORCE_NATIVE_CSV=true
```

This is useful when processing files where the native reader provides better performance (typically 50K-500K records).

### Available Implementations

The CSV reader has three implementations:

1. **Native Python CSV reader** - Default for small to medium files
   - Fastest for files under 100K rows
   - Lower memory overhead
   - No additional dependencies required
   - Excellent for row-oriented processing

2. **Polars CSV reader** - Optimal for large files
   - Best performance for files over 100K rows
   - Efficient memory usage for large datasets
   - Excellent for analytical workloads

3. **PyArrow CSV reader** - Specialized use cases
   - Used when specific Arrow features are needed
   - Single-pass file reading (optimized)
   - Batch columnar-to-dict conversion
   - Good for Arrow ecosystem integration

### Performance Optimizations

Recent optimizations have significantly improved CSV processing:

- **2x faster PyArrow processing**: Eliminated double-read issue with `cast_to_string=True`
- **20x faster row conversion**: Batch conversion replaces row-by-row `.as_py()` calls
- **Intelligent reader selection**: Automatically chooses the best reader for your file size

### PyArrow Implementation

When PyArrow is selected, Transmog uses optimized processing:

- Single-pass file reading (no double-read)
- Batch conversion for columnar to row-oriented data
- Memory-efficient processing of large files
- Automatic type inference
- Optimized column-oriented data structure

The PyArrow implementation is now optimized for better performance but is generally not selected for typical
CSV-to-dict processing due to the columnar-to-row conversion overhead.

### Standard Library Implementation

The native Python CSV implementation provides:

- Best performance for small to medium files
- Universal compatibility (no additional dependencies)
- Support for all CSV dialect options
- Basic type inference for numbers and booleans
- Lower overhead for row-oriented processing

The native implementation is the default choice for most use cases.

### Polars Implementation

When Polars is available and selected:

- Excellent performance for large files
- Efficient memory usage
- Fast data type conversion
- Optimized for both row and columnar operations
- Requires `polars` as an optional dependency

### Chunked Processing

For large files, Transmog processes data in chunks to minimize memory usage:

1. The file is read in chunks of `chunk_size` rows
2. Each chunk is processed separately
3. Results are combined into a final ProcessingResult

This approach allows processing of very large files with limited memory.

## Performance Considerations

### Memory Usage

- **chunk_size**: Controls memory usage by limiting the number of rows processed at once
- **infer_types**: Requires more memory but produces more accurate data types
- **PyArrow implementation**: More memory-efficient for large files

### Processing Speed

- **PyArrow implementation**: Faster for large files, especially with type inference
- **Standard library implementation**: Adequate for small to medium files
- **chunk_size**: Smaller chunks use less memory but may increase processing time

### File Size Guidelines

- **Small files (<10MB, ~100K rows)**: Native implementation (fastest)
- **Medium files (10-100MB, 100K-1M rows)**: Polars
- **Large files (>100MB, >1M rows)**: Polars strongly recommended
- **Very large files (>1GB)**: Polars with appropriate chunk_size

## Compatibility

### Supported File Formats

The CSV reader can handle:

- Standard CSV files with various delimiters (comma, tab, pipe, etc.)
- Files with or without headers
- Files with inconsistent column counts (within reason)
- Various encodings (UTF-8, Latin-1, etc.)

### Compressed Files

- When using PyArrow implementation, compressed files (gzip, bzip2, xz) are supported natively
- With the standard library implementation, files must be decompressed before processing

### Special Character Handling

The `quote_char` parameter controls how fields with special characters are processed:

```python
result = processor.process_csv(
    "data.csv",
    quote_char='"',  # Double quote is the default
    delimiter=","
)
```

Fields containing the delimiter character must be quoted to be processed correctly.
