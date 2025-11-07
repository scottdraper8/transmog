# Output Formats

This guide covers Transmog's output format options, including CSV and Parquet, with
guidance on choosing the right format for different use cases.

## Supported Formats

Transmog supports two output formats optimized for tabular data:

| Format | Use Case | Advantages | Considerations |
|--------|----------|------------|----------------|
| **CSV** | Spreadsheets, databases, analytics | Wide compatibility, compact | Flat structure only |
| **Parquet** | Big data, analytics, data lakes | Columnar, compressed, fast queries | Requires specialized tools |

## CSV Output

### Basic CSV Output

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

# Save as CSV files (default format)
result.save("output")
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
- **String-based**: All values become strings by default
- **Limited metadata**: No built-in type information
- **Character encoding**: UTF-8 recommended for international data

### CSV Best Practices

```python
# For database import / Excel
config = tm.TransmogConfig.for_csv()
result = tm.flatten(data, name="db_import", config=config)
result.save("database_import", output_format="csv")

# Customize if needed
config = tm.TransmogConfig.for_csv()
config.id_field = "id"
result = tm.flatten(data, name="excel_data", config=config)
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
- **Compression**: Smaller file sizes than CSV
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
# Default is already optimized for Parquet (types preserved)
result = tm.flatten(data, name="analytics")
result.save("analytics_data", output_format="parquet")

# Or use performance preset for large datasets
config = tm.TransmogConfig.for_performance()
result = tm.flatten(data, name="analytics", config=config)
result.save("analytics_data", output_format="parquet")

# For data lake storage with custom ID
config = tm.TransmogConfig(id_field="natural_id")
result = tm.flatten(data, name="lake_data", config=config)
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
    compression="snappy"            # Format-specific option
)

# Stream to CSV for database loading
config = tm.TransmogConfig(
    batch_size=2000
)
tm.flatten_stream(
    db_data,
    output_path="db_staging/",
    name="staging_data",
    output_format="csv",
    config=config
)
```

## Format Selection Guide

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

### CSV Optimizations

```python
# Optimize CSV for database loading
config = tm.TransmogConfig(
    skip_null=True,
    id_field="natural_id",
    separator="_"
)
result = tm.flatten(data, name="db_optimized", config=config)
result.save("database_ready", output_format="csv")

# Optimize CSV for analytics
config = tm.TransmogConfig(
    array_mode=tm.ArrayMode.SEPARATE,
    time_field="_timestamp"
)
result = tm.flatten(data, name="analytics_csv", config=config)
result.save("analytics_ready", output_format="csv")
```

### Parquet Optimizations

```python
# Optimize Parquet for query performance
config = tm.TransmogConfig(
    cast_to_string=False,
    skip_null=False,
    array_mode=tm.ArrayMode.SEPARATE,
    time_field="_timestamp"
)
result = tm.flatten(data, name="query_optimized", config=config)
result.save("query_ready", output_format="parquet")

# Optimize Parquet for storage efficiency
config = tm.TransmogConfig(
    cast_to_string=False,
    batch_size=5000
)
tm.flatten_stream(
    large_data,
    output_path="efficient_storage/",
    name="compressed_data",
    output_format="parquet",
    config=config,
    compression="snappy"
)
```

## File Organization Patterns

### Single Table Output

```python
# When only main table exists, save as single file
simple_data = {"name": "Product", "price": 99.99}
result = tm.flatten(simple_data, name="simple")

if len(result.tables) == 0:
    result.save("single_product.csv")      # Single file
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
result.save("output", output_format="csv")
# Creates:
# output/customer_orders.csv
# output/customer_orders_items.csv
# output/customer_orders_payments.csv
```

## Integration Examples

### Database Integration

```python
# Prepare data for PostgreSQL
config = tm.TransmogConfig(
    id_field="customer_id",
    skip_null=True,
    array_mode=tm.ArrayMode.SEPARATE
)
result = tm.flatten(api_data, name="customers", config=config)
result.save("postgres_import", output_format="csv")

# SQL import commands
# COPY customers FROM 'postgres_import/customers.csv' CSV HEADER;
# COPY customers_orders FROM 'postgres_import/customers_orders.csv' CSV HEADER;
```

### Analytics Pipeline

```python
# Prepare data for Spark/Pandas analysis
config = tm.TransmogConfig(
    cast_to_string=False,
    array_mode=tm.ArrayMode.SEPARATE,
    batch_size=10000
)
tm.flatten_stream(
    analytics_data,
    output_path="spark_input/",
    name="events",
    output_format="parquet",
    config=config
)

# Use with Spark
# df = spark.read.parquet("spark_input/events.parquet")
# orders_df = spark.read.parquet("spark_input/events_orders.parquet")
```

## Next Steps

- **[Error Handling](error-handling.md)** - Handle format-specific processing errors
- **[Streaming Guide](../developer_guide/streaming.md)** - Memory-efficient processing for large outputs
- **[Performance Guide](../developer_guide/performance.md)** - Optimize output performance for different formats
