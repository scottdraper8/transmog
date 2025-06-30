"""Simple example showing the new Transmog v1.1.0 API.

This example demonstrates basic usage of the simplified API.
"""

import transmog as tm

# Example 1: Basic flattening
print("=== Example 1: Basic Flattening ===")

data = {
    "id": 1,
    "name": "Laptop",
    "price": 999.99,
    "specs": {"cpu": "Intel i7", "ram": "16GB", "storage": "512GB SSD"},
    "tags": ["electronics", "computers", "portable"],
}

# Flatten with one line
result = tm.flatten(data, name="product")

# Display results
print(f"\nFlattened into {len(result.all_tables)} tables:")
print(result)

# Access main table
print("\nMain table:")
for record in result.main:
    print(f"  ID: {record.get('_id', record.get('id', 'N/A'))}")
    print(f"  Name: {record['name']}")
    print(f"  CPU: {record['specs_cpu']}")

# Access child table
print("\nTags table:")
for tag in result.tables["product_tags"]:
    print(f"  - {tag['value']} (parent: {tag['_parent_id'][:8]}...)")

# Save to file
print("\nSaving to JSON...")
result.save("output/simple_example.json")

# Example 2: Using existing ID field
print("\n\n=== Example 2: Using Natural IDs ===")

data_with_ids = [
    {
        "sku": "LAPTOP-001",
        "name": "Gaming Laptop",
        "categories": ["gaming", "high-performance"],
    },
    {
        "sku": "LAPTOP-002",
        "name": "Business Laptop",
        "categories": ["business", "lightweight"],
    },
]

# Use existing field as ID
result = tm.flatten(data_with_ids, name="products", id_field="sku")

print("\nUsing 'sku' field as ID:")
for record in result.main:
    # Note: When using natural IDs, _id field is not added
    print(f"  SKU: {record.get('sku', 'N/A')}")
    print(f"  Name: {record['name']}")

# Example 3: Custom separators and error handling
print("\n\n=== Example 3: Custom Options ===")

messy_data = [
    {
        "id": 1,
        "user": {
            "name": "Alice",
            "contact": {"email": "alice@example.com", "phone": "555-0100"},
        },
    },
    {
        "id": 2,
        "user": {
            "name": "Bob",
            "contact": {
                "email": None,  # Missing email
                "phone": "555-0200",
            },
        },
    },
]

# Custom separator and preserve types
result = tm.flatten(
    messy_data, name="users", separator=".", preserve_types=True, skip_null=True
)

print("\nFlattened with dot notation:")
for record in result.main:
    print(f"  User #{record['id']}: {record['user.name']}")
    print(f"    Email: {record.get('user.contact.email', 'Not provided')}")
    print(f"    Phone: {record.get('user.contact.phone', 'Not provided')}")


print("\n\nDone! Check the output/ directory for saved files.")
