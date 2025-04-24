# Data Transformation Tutorial

This tutorial demonstrates how to use Transmogrify to transform data from one format to another, apply transformations, and handle various data scenarios.

## Basic Transformation

Let's start with a simple example of transforming a CSV file to JSON:

```python
from transmogrify import Processor

# Initialize the processor
processor = Processor()

# Process a CSV file
result = processor.process_csv("input.csv")

# Write the result as JSON
result.write_json("output.json")
```

## Data Transformation with Custom Functions

Transmogrify allows you to apply custom transformations during processing:

```python
from transmogrify import Processor

def transform_data(record):
    # Convert names to uppercase
    if "name" in record:
        record["name"] = record["name"].upper()
    
    # Calculate a new field
    if "price" in record and "quantity" in record:
        record["total"] = record["price"] * record["quantity"]
    
    return record

# Initialize processor
processor = Processor()

# Process CSV with transformation
result = processor.process_csv(
    "sales_data.csv",
    transform_function=transform_data
)

# Write the transformed data
result.write_json("transformed_sales.json")
```

## Converting Between Multiple Formats

You can convert between different formats with Transmogrify:

```python
from transmogrify import Processor

processor = Processor()

# Read JSON data
result = processor.process_json("data.json")

# Write to multiple formats
result.write_csv("output.csv")
result.write_parquet("output.parquet")
```

## Working with Relations and Nested Data

Transmogrify can handle nested data structures:

```python
from transmogrify import Processor

processor = Processor()

# Process a JSON file with nested objects
result = processor.process_json("orders_with_items.json")

# The result identifies relationships
main_table = result.get_main_table()  # Orders
items_table = result.get_table("items")  # Order items

# Write as normalized CSV files
result.write_all_csv("output_dir")
# This creates:
# - output_dir/orders.csv
# - output_dir/items.csv
```

## Cleaning and Validating Data

Example of data cleaning and validation:

```python
from transmogrify import Processor

def validate_and_clean(record):
    # Data validation
    if "email" in record and not "@" in record["email"]:
        record["email"] = None  # Invalid email
    
    # Data cleaning
    if "phone" in record and record["phone"]:
        # Remove non-numeric characters
        record["phone"] = ''.join(c for c in record["phone"] if c.isdigit())
    
    return record

processor = Processor()
result = processor.process_csv(
    "contacts.csv",
    transform_function=validate_and_clean,
    null_values=["", "NULL", "N/A"]
)

# Write the cleaned data
result.write_csv("clean_contacts.csv")
```

## Handling Missing Values

Here's how to handle missing or null values:

```python
from transmogrify import Processor

# Define values that should be treated as null
null_values = ["", "NULL", "NA", "N/A", "-"]

processor = Processor()
result = processor.process_csv(
    "sample_with_nulls.csv",
    null_values=null_values
)

# Optionally, you can fill null values during transformation
def fill_nulls(record):
    # Fill missing age with a default value
    if "age" in record and record["age"] is None:
        record["age"] = 0
    
    # Fill missing names with "Unknown"
    if "name" in record and record["name"] is None:
        record["name"] = "Unknown"
    
    return record

result_filled = processor.process_csv(
    "sample_with_nulls.csv",
    null_values=null_values,
    transform_function=fill_nulls
)

# Write both versions
result.write_csv("data_with_nulls.csv")
result_filled.write_csv("data_filled.csv")
```

## Combining Multiple Data Sources

Transmogrify can process multiple files and combine them:

```python
from transmogrify import Processor

processor = Processor()

# Process multiple data sources
customers = processor.process_csv("customers.csv")
orders = processor.process_csv("orders.csv")

# Manually combine the data
combined_data = []

# Create a lookup dictionary for customers
customer_lookup = {c["id"]: c for c in customers.get_main_table().get_records()}

# For each order, find the corresponding customer
for order in orders.get_main_table().get_records():
    customer_id = order.get("customer_id")
    if customer_id in customer_lookup:
        # Create a combined record
        combined_record = {
            "order_id": order.get("id"),
            "order_date": order.get("date"),
            "customer_id": customer_id,
            "customer_name": customer_lookup[customer_id].get("name"),
            "amount": order.get("amount")
        }
        combined_data.append(combined_record)

# Create a new processor to handle the combined data
combined_processor = Processor()
combined_result = combined_processor.process_records(combined_data, entity_name="combined_data")

# Write the combined data
combined_result.write_csv("combined_data.csv")
```

## Processing Large Files

For large files, use streaming processing:

```python
from transmogrify import Processor

# Process a large CSV file with chunks
processor = Processor()
result = processor.process_csv(
    "large_dataset.csv",
    chunk_size=10000  # Process 10,000 rows at a time
)

# Write to parquet format
result.write_parquet("large_dataset.parquet")
```

## Advanced Type Handling

Control how Transmogrify handles data types:

```python
from transmogrify import Processor

processor = Processor()

# Process CSV with custom type inference
result = processor.process_csv(
    "data.csv",
    infer_types=True
)

# Apply schema-specific transformations
def type_transform(record):
    # Ensure certain fields are specific types
    if "created_at" in record and record["created_at"]:
        # Convert string to datetime if not already
        if isinstance(record["created_at"], str):
            from datetime import datetime
            try:
                record["created_at"] = datetime.fromisoformat(record["created_at"])
            except ValueError:
                # Handle invalid date format
                record["created_at"] = None
    
    # Convert string values to numeric
    if "rating" in record and isinstance(record["rating"], str):
        try:
            record["rating"] = float(record["rating"])
        except ValueError:
            record["rating"] = None
    
    return record

# Process with type handling
result_with_types = processor.process_csv(
    "data.csv",
    transform_function=type_transform
)

# Write with explicit types
result_with_types.write_json("typed_data.json")
```

## Conclusion

This tutorial has demonstrated various data transformation capabilities in Transmogrify, including format conversion, data cleaning, validation, and handling different data scenarios.

For more information, refer to:

- [Processor API Reference](../api/processor.md)
- [CSV Processing Guide](../user/csv-processing.md)
- [JSON Processing Guide](../user/json-processing.md)
- [Result Object Reference](../api/result.md) 