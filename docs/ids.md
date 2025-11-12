# ID Management

ID generation strategies track records and maintain relationships between parent and child tables.

## ID Strategies

## Random IDs (Default)

The default strategy generates unique UUIDs for all records:

```python
import transmog as tm

data = {"product": {"name": "Laptop"}}
result = tm.flatten(data, name="products")

print(result.main[0])
# {'product_name': 'Laptop', '_id': 'uuid-generated', '_timestamp': '...'}
```

## Natural IDs

Use existing ID fields from your data:

```python
data = {
    "product": {
        "product_id": "PROD123",
        "name": "Gaming Laptop",
        "reviews": [
            {"review_id": "REV456", "rating": 5},
            {"review_id": "REV789", "rating": 4}
        ]
    }
}

config = tm.TransmogConfig(id_generation="natural", id_field="product_id")
result = tm.flatten(data, name="products", config=config)

print(result.main[0])
# {'product_id': 'PROD123', 'product_name': 'Gaming Laptop'}

print(result.tables["products_reviews"][0])
# {'review_id': 'REV456', 'rating': 5, '_parent_id': 'PROD123'}
```

Strategy `"natural"` requires the specified field to exist in all records.

## Hash-Based IDs

Generate deterministic IDs based on record content:

```python
# Hash entire record
config = tm.TransmogConfig(id_generation="hash")
data = {"name": "Laptop", "price": 999}

result1 = tm.flatten(data, name="products", config=config)
result2 = tm.flatten(data, name="products", config=config)

# Same data produces same ID
assert result1.main[0]["_id"] == result2.main[0]["_id"]
```

## Composite Key IDs

Hash only specific fields to create composite keys:

```python
data1 = {"region": "US", "store": "001", "product": "laptop", "price": 999}
data2 = {"region": "US", "store": "001", "product": "laptop", "price": 899}

config = tm.TransmogConfig(id_generation=["region", "store", "product"])

result1 = tm.flatten(data1, name="sales", config=config)
result2 = tm.flatten(data2, name="sales", config=config)

# Same composite key produces same ID (price is ignored)
assert result1.main[0]["_id"] == result2.main[0]["_id"]
```

## Metadata Field Names

Customize the names of metadata fields:

```python
config = tm.TransmogConfig(
    id_field="record_id",
    parent_field="parent_ref",
    time_field="_created_at"
)
result = tm.flatten(data, config=config)

# Records use custom field names
print(result.main[0])
# {'name': 'Product', 'record_id': '...', '_created_at': '...'}
```

Disable timestamp tracking:

```python
config = tm.TransmogConfig(time_field=None)
result = tm.flatten(data, config=config)
```

## Parent-Child Relationships

Child records reference their parents through the parent ID field:

```python
result = tm.flatten(data, name="products")
main_id = result.main[0]["_id"]

for review in result.tables["products_reviews"]:
    assert review["_parent_id"] == main_id
```
