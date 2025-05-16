# Flatten and Normalize Data

This tutorial demonstrates how to flatten nested structures and normalize arrays into separate tables
with relationships.

## Understanding Flattening vs. Normalization

**Flattening** converts nested objects into flat structures using dot notation:

```text
{"user": {"name": "John"}} → {"user.name": "John"}
```

**Normalization** extracts arrays into separate tables with relationships:

```text
{"user": {"orders": [{"id": 1}, {"id": 2}]}}
→ user: {"id": "u1"}
→ user_orders: [{"id": 1, "user_id": "u1"}, {"id": 2, "user_id": "u1"}]
```

## Sample Data

Let's work with customer data containing nested information and arrays:

```json
{
  "customerId": "C100",
  "name": "Sarah Williams",
  "contactInfo": {
    "email": "sarah@example.com",
    "phone": "555-123-4567"
  },
  "shippingAddresses": [
    {
      "addressId": "A1",
      "street": "123 Main St",
      "city": "Portland",
      "state": "OR"
    },
    {
      "addressId": "A2",
      "street": "456 Market St",
      "city": "San Francisco",
      "state": "CA"
    }
  ],
  "orders": [
    {
      "orderId": "O1001",
      "orderDate": "2023-01-15",
      "total": 127.95,
      "items": [
        {
          "productId": "P5001",
          "name": "Wireless Headphones",
          "price": 79.99,
          "quantity": 1
        },
        {
          "productId": "P3002",
          "name": "USB Cable",
          "price": 12.99,
          "quantity": 2
        }
      ]
    }
  ]
}
```

## Implementation

First, import the necessary components:

```python
from transmog import TransmogProcessor, TransmogConfig
```

### Basic Flattening and Normalization

```python
# Define our customer data
customer_data = {
  "customerId": "C100",
  "name": "Sarah Williams",
  "contactInfo": {
    "email": "sarah@example.com",
    "phone": "555-123-4567"
  },
  "shippingAddresses": [
    {
      "addressId": "A1",
      "street": "123 Main St",
      "city": "Portland",
      "state": "OR"
    },
    {
      "addressId": "A2",
      "street": "456 Market St",
      "city": "San Francisco",
      "state": "CA"
    }
  ],
  "orders": [
    {
      "orderId": "O1001",
      "orderDate": "2023-01-15",
      "total": 127.95,
      "items": [
        {
          "productId": "P5001",
          "name": "Wireless Headphones",
          "price": 79.99,
          "quantity": 1
        },
        {
          "productId": "P3002",
          "name": "USB Cable",
          "price": 12.99,
          "quantity": 2
        }
      ]
    }
  ]
}

# Create processor with default configuration
processor = TransmogProcessor()

# Process the data
result = processor.process_data(customer_data)

# Convert to dictionaries
tables = result.to_dict()

# Print the tables to see the structure
for table_name, records in tables.items():
    print(f"\n=== {table_name} ===")
    print(f"Record count: {len(records)}")
    if records:
        print("Fields:", list(records[0].keys()))
```

### Expected Output Structure

The transformation will create four tables:

1. `customer` - Main customer information
   - Includes flattened `contactInfo` as `contactInfo.email` and `contactInfo.phone`

2. `customer_shippingAddresses` - Extracted shipping addresses
   - Each address linked to the customer via `customer_id` foreign key

3. `customer_orders` - Extracted orders
   - Each order linked to the customer via `customer_id` foreign key

4. `customer_orders_items` - Extracted order items
   - Each item linked to the order via `customer_orders_id` foreign key

## Customizing the Flattening Process

You can customize how flattening and normalization work:

```python
# Configure with specific options
config = TransmogConfig().with_flattening(
    # Keep arrays as arrays in the parent object instead of extracting them
    extract_arrays=False,
    # Preserve the original paths in the array tables
    preserve_array_paths=True,
    # Override the default delimiter for flattened field names
    path_delimiter="__"
)

# Create processor with custom configuration
custom_processor = TransmogProcessor(config)

# Process the data
custom_result = custom_processor.process_data(customer_data)
```

## Exporting the Results

You can export the normalized data to various formats:

```python
# Export to CSV files
result.write_all_csv("output_directory")

# Export to Parquet files
result.write_all_parquet("output_directory")

# Export to JSON files
result.write_all_json("output_directory")
```

## Next Steps

- Learn about [streaming large datasets](../intermediate/streaming-large-datasets.md)
- Explore [data transformation](../../user/processing/data-transformation.md) in depth
- Try [customizing ID generation](../intermediate/customizing-id-generation.md)
