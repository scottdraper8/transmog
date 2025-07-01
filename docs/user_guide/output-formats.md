# Output Formats

This guide covers Transmog's output format options, including JSON, CSV, and Parquet, with
guidance on choosing the right format for different use cases.

## Supported Formats

Transmog supports three output formats:

| Format | Use Case | Advantages | Considerations |
|--------|----------|------------|----------------|
| **JSON** | APIs, web apps, document storage | Human-readable, preserves structure | Larger file size |
| **CSV** | Spreadsheets, databases, analytics | Wide compatibility, compact | Flat structure only |
| **Parquet** | Big data, analytics, data lakes | Columnar, compressed, fast queries | Requires specialized tools |

## JSON Output

### Basic JSON Output

```python
import transmog as tm

data = {
    "product": {
        "name": "Laptop",
        "price": 999.99,
        "reviews": [
            {"rating": 5, "comment": "Excellent"},
            {"rating": 4, "comment": "Good value"}
        ]
    }
}

result = tm.flatten(data, name="products")

# Save as JSON files (default format)
result.save("output")
# Creates:
# output/products.json (main table)
# output/products_reviews.json (reviews table)
```

### JSON File Structure

Each table becomes a separate JSON file:

**products.json:**

```json
[
  {
    "product_name": "Laptop",
    "product_price": "999.99",
    "_id": "generated_id"
  }
]
```

**products_reviews.json:**

```json
[
  {
    "rating": "5",
    "comment": "Excellent",
    "_parent_id": "generated_id"
  },
  {
    "rating": "4",
    "comment": "Good value",
    "_parent_id": "generated_id"
  }
]
```

### JSON Advantages

- **Human-readable**: Easy to inspect and debug
- **Web-compatible**: Direct use in web applications
- **Structure preservation**: Maintains complex data types
- **Universal support**: Works with all programming languages

### JSON Best Practices

```python
# For web APIs
result = tm.flatten(
    data,
    name="api_data",
    preserve_types=True,     # Keep numbers as numbers
    skip_null=False,         # Include null values for completeness
    arrays="separate"        # Enable relationship analysis
)
result.save("api_output", output_format="json")

# For document storage
result = tm.flatten(
    data,
    name="documents",
    arrays="inline",         # Keep arrays as JSON
    preserve_types=True,     # Maintain type information
    add_timestamp=True       # Add processing metadata
)
result.save("document_store", output_format="json")
```

## CSV Output

### Basic CSV Output

```python
# Save as CSV files
result.save("output", output_format="csv")
# Creates:
# output/products.csv (main table)
# output/products_reviews.csv (reviews table)
```

### CSV File Structure

Each table becomes a CSV file with headers:

**products.csv:**

```text
product_name,product_price,_id
Laptop,999.99,generated_id
```

**products_reviews.csv:**

```text
rating,comment,_parent_id
5,Excellent,generated_id
4,Good value,generated_id
```

### CSV Advantages

- **Universal compatibility**: Opens in Excel, databases, analytics tools
- **Compact size**: Efficient storage for large datasets
- **Fast processing**: Quick to read and write
- **Database-friendly**: Direct import into relational databases

### CSV Considerations

- **Flat structure only**: Cannot represent nested data
- **String-based**: All values become strings
- **Limited metadata**: No built-in type information
- **Character encoding**: UTF-8 recommended for international data

### CSV Best Practices

```python
# For database import
result = tm.flatten(
    data,
    name="db_import",
    id_field="id",           # Use natural IDs for foreign keys
    preserve_types=False,    # Convert all to strings
    skip_null=True,          # Clean data for SQL
    arrays="separate"        # Create relational tables
)
result.save("database_import", output_format="csv")

# For Excel analysis
result = tm.flatten(
    data,
    name="excel_data",
    separator="_",           # Excel-friendly field names
    skip_empty=True,         # Remove empty cells
    arrays="separate"        # Multiple worksheets concept
)
result.save("excel_analysis", output_format="csv")
```

## Parquet Output

### Basic Parquet Output

```python
# Save as Parquet files
result.save("output", output_format="parquet")
# Creates:
# output/products.parquet (main table)
# output/products_reviews.parquet (reviews table)
```

### Parquet Advantages

- **Columnar storage**: Efficient for analytics queries
- **Compression**: Smaller file sizes than JSON/CSV
- **Type preservation**: Maintains data types natively
- **Fast queries**: Optimized for analytical workloads
- **Schema evolution**: Supports schema changes over time

### Parquet Requirements

Parquet support requires the `pyarrow` library:

```bash
pip install pyarrow
```

### Parquet Best Practices

```python
# For analytics workloads
result = tm.flatten(
    data,
    name="analytics",
    preserve_types=True,     # Keep numeric types for analysis
    skip_null=False,         # Include nulls for complete picture
    arrays="separate",       # Enable relational analysis
    add_timestamp=True       # Add processing metadata
)
result.save("analytics_data", output_format="parquet")

# For data lake storage
result = tm.flatten(
    data,
    name="lake_data",
    preserve_types=True,     # Maintain type information
    arrays="separate",       # Normalized structure
    id_field="natural_id"    # Consistent identification
)
result.save("data_lake", output_format="parquet")
```

## Streaming Output

### Large Dataset Processing

For large datasets, use streaming to write directly to files:

```python
# Stream large datasets to Parquet
tm.flatten_stream(
    large_dataset,
    output_path="streaming_output/",
    name="large_data",
    output_format="parquet",        # Best for large datasets
    batch_size=1000,
    low_memory=True,
    compression="snappy"     # Format-specific option
)

# Stream to JSON for web processing
tm.flatten_stream(
    web_data,
    output_path="web_output/",
    name="web_data",
    output_format="json",
    batch_size=500,
    preserve_types=True
)

# Stream to CSV for database loading
tm.flatten_stream(
    db_data,
    output_path="db_staging/",
    name="staging_data",
    output_format="csv",
    batch_size=2000,
    preserve_types=False     # Strings for SQL compatibility
)
```

## Format Selection Guide

### Choose JSON When

- Building web applications or APIs
- Need human-readable output for debugging
- Working with document databases (MongoDB, CouchDB)
- Preserving complex data structures is important
- File size is not a primary concern

### Choose CSV When

- Loading data into relational databases
- Working with Excel or spreadsheet applications
- Need maximum compatibility across tools
- Working with legacy systems
- File size efficiency is important for simple data

### Choose Parquet When

- Building analytical data pipelines
- Working with big data tools (Spark, Hadoop)
- Need fast query performance on large datasets
- Type preservation is critical
- Working with data lakes or warehouses
- Compression and storage efficiency are priorities

## Format-Specific Optimizations

### JSON Optimizations

```python
# Optimize JSON for file size
result = tm.flatten(
    data,
    name="optimized",
    skip_null=True,          # Remove null values
    skip_empty=True,         # Remove empty strings
    preserve_types=False     # Use strings (smaller than numbers in JSON)
)
result.save("compact_json", output_format="json")

# Optimize JSON for processing speed
result = tm.flatten(
    data,
    name="fast_json",
    arrays="inline",         # Fewer files to manage
    low_memory=True,         # Reduce memory pressure
    batch_size=500           # Smaller processing batches
)
result.save("fast_processing", output_format="json")
```

### CSV Optimizations

```python
# Optimize CSV for database loading
result = tm.flatten(
    data,
    name="db_optimized",
    preserve_types=False,    # Consistent string types
    skip_null=True,          # Avoid NULL handling issues
    id_field="natural_id",   # Use natural foreign keys
    separator="_"            # Database-friendly column names
)
result.save("database_ready", output_format="csv")

# Optimize CSV for analytics
result = tm.flatten(
    data,
    name="analytics_csv",
    arrays="separate",       # Enable table joins
    add_timestamp=True,      # Add time dimensions
    preserve_types=False     # Consistent for spreadsheet tools
)
result.save("analytics_ready", output_format="csv")
```

### Parquet Optimizations

```python
# Optimize Parquet for query performance
result = tm.flatten(
    data,
    name="query_optimized",
    preserve_types=True,     # Native type support
    skip_null=False,         # Preserve data completeness
    arrays="separate",       # Normalized for joins
    add_timestamp=True       # Time-based partitioning support
)
result.save("query_ready", output_format="parquet")

# Optimize Parquet for storage efficiency
tm.flatten_stream(
    large_data,
    output_path="efficient_storage/",
    name="compressed_data",
    output_format="parquet",
    preserve_types=True,     # Better compression with types
    batch_size=5000,         # Larger batches for compression
    compression="snappy"     # Fast compression algorithm
)
```

## File Organization Patterns

### Single Table Output

```python
# When only main table exists, save as single file
simple_data = {"name": "Product", "price": 99.99}
result = tm.flatten(simple_data, name="simple")

if len(result.tables) == 0:
    result.save("single_product.json")     # Single file
else:
    result.save("product_data")            # Directory with multiple files
```

### Directory Structure

```python
# Multiple tables create directory structure
result.save("product_data", output_format="csv")
# Creates:
# product_data/
#   products.csv
#   products_reviews.csv
#   products_specifications.csv
```

### Naming Conventions

```python
# Use descriptive entity names for clear file names
result = tm.flatten(data, name="customer_orders")
result.save("output", output_format="json")
# Creates:
# output/customer_orders.json
# output/customer_orders_items.json
# output/customer_orders_payments.json
```

## Integration Examples

### Database Integration

```python
# Prepare data for PostgreSQL
result = tm.flatten(
    api_data,
    name="customers",
    id_field="customer_id",
    preserve_types=False,
    skip_null=True,
    arrays="separate"
)
result.save("postgres_import", output_format="csv")

# SQL import commands
# COPY customers FROM 'postgres_import/customers.csv' CSV HEADER;
# COPY customers_orders FROM 'postgres_import/customers_orders.csv' CSV HEADER;
```

### Analytics Pipeline

```python
# Prepare data for Spark/Pandas analysis
tm.flatten_stream(
    analytics_data,
    output_path="spark_input/",
    name="events",
    output_format="parquet",
    preserve_types=True,
    arrays="separate",
    batch_size=10000
)

# Use with Spark
# df = spark.read.parquet("spark_input/events.parquet")
# orders_df = spark.read.parquet("spark_input/events_orders.parquet")
```

### Web Application

```python
# Prepare data for web API
result = tm.flatten(
    user_data,
    name="users",
    preserve_types=True,
    arrays="inline",      # Single JSON per user
    skip_null=False       # Complete user profiles
)
result.save("api_data", output_format="json")

# Use in web application
# with open("api_data/users.json") as f:
#     users = json.load(f)
```

## Next Steps

- **[Error Handling](error-handling.md)** - Handle format-specific processing errors
- **[Streaming Guide](../developer_guide/streaming.md)** - Memory-efficient processing for large outputs
- **[Performance Guide](../developer_guide/performance.md)** - Optimize output performance for different formats
