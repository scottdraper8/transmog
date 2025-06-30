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
import transmog as tm
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

# Process the data with a single function call
result = tm.flatten(customer_data, name="customer")

# Access the tables directly
main_table = result.main
shipping_addresses = result.tables["customer_shippingAddresses"]
orders = result.tables["customer_orders"]
order_items = result.tables["customer_orders_items"]

# Print the tables to see the structure
print(f"\n=== customer ===")
print(f"Record count: {len(main_table)}")
if main_table:
    print("Fields:", list(main_table[0].keys()))

print(f"\n=== customer_shippingAddresses ===")
print(f"Record count: {len(shipping_addresses)}")
if shipping_addresses:
    print("Fields:", list(shipping_addresses[0].keys()))

print(f"\n=== customer_orders ===")
print(f"Record count: {len(orders)}")
if orders:
    print("Fields:", list(orders[0].keys()))

print(f"\n=== customer_orders_items ===")
print(f"Record count: {len(order_items)}")
if order_items:
    print("Fields:", list(order_items[0].keys()))
```

### Expected Output Structure

The transformation will create four tables:

1. `customer` - Main customer information
   - Includes flattened `contactInfo` as `contactInfo_email` and `contactInfo_phone`

2. `customer_shippingAddresses` - Extracted shipping addresses
   - Each address linked to the customer via `_parent_id` foreign key

3. `customer_orders` - Extracted orders
   - Each order linked to the customer via `_parent_id` foreign key

4. `customer_orders_items` - Extracted order items
   - Each item linked to the order via `_parent_id` foreign key

## Customizing the Flattening Process

You can customize how flattening and normalization work:

```python
# Process with specific options
result = tm.flatten(
    customer_data,
    name="customer",
    # Keep arrays as arrays in the parent object instead of extracting them
    extract_arrays=False,
    # Use a different delimiter for flattened field names
    delimiter="__"
)

# Or customize array handling
result = tm.flatten(
    customer_data,
    name="customer",
    # Keep specific arrays in the parent object
    keep_arrays=["shippingAddresses"]
)
```

## Exporting the Results

You can export the normalized data to various formats:

```python
# Export all tables to a directory
result.save("output_directory")

# Export to specific formats
result.save("output_directory/customer.csv")    # CSV format
result.save("output_directory/customer.json")   # JSON format
result.save("output_directory/customer.parquet") # Parquet format
```

## Working with Files

You can also process JSON files directly:

```python
# Process a JSON file
file_result = tm.flatten_file("customer.json", name="customer")

# Stream process a large file directly to output
tm.flatten_stream(
    file_path="large_customer_data.json",
    name="customer",
    output_path="output_directory",
    output_format="parquet"
)
```

## Next Steps

- Learn about [streaming large datasets](../intermediate/streaming-large-datasets.md)
- Explore [data transformation](../../user/processing/data-transformation.md) in depth
- Try [customizing ID generation](../intermediate/customizing-id-generation.md)
