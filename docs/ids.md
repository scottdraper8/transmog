# ID Management

ID generation strategies track records and maintain relationships between parent and child tables.

Natural IDs are read from each **flattened record**, not from nested paths.
The field named by `id_field` must exist on that record.

## Random IDs (Default)

The default strategy generates unique UUIDs for all records:

```python
import transmog as tm

data = {"name": "Laptop"}
result = tm.flatten(data, name="products")

print(result.main[0])
# {'name': 'Laptop', '_id': '8b596e4b-8c20-413b-a503-3fe15fe766e1', '_timestamp': '...'}
```

## Natural IDs

Use an existing ID field from each record. `id_field` is both the source field
read from the record and the output column name:

```python
data = {
    "product_id": "PROD123",
    "name": "Gaming Laptop",
    "reviews": [
        {"review_id": "REV456", "rating": 5},
        {"review_id": "REV789", "rating": 4}
    ]
}

config = tm.TransmogConfig(id_generation="natural", id_field="product_id")
result = tm.flatten(data, name="products", config=config)

print(result.main[0])
# {'product_id': 'PROD123', 'name': 'Gaming Laptop'}

print(result.tables["products_reviews"][0])
# {..., '_parent_id': 'PROD123'}
```

:::{important}
Strategy `"natural"` requires `id_field` on every **parent** record.
A missing, empty, or null ID field raises `ValidationError`.
:::

Child records use their own value for `id_field` when it is present. When it
is missing, a UUID is generated and written into `id_field` (not `_id`). In
the example above, reviews have `review_id` rather than `product_id`, so each
review row gets a generated UUID in the `product_id` column.

## Hash-Based IDs

Generate deterministic UUID-shaped IDs from the full record content:

```python
# Hash entire record
config = tm.TransmogConfig(id_generation="hash")
data = {"name": "Laptop", "price": 999}

result1 = tm.flatten(data, name="products", config=config)
result2 = tm.flatten(data, name="products", config=config)

# Same data produces the same UUID-shaped ID
assert result1.main[0]["_id"] == result2.main[0]["_id"]
assert len(result1.main[0]["_id"]) == 36
```

## Composite Key IDs

Hash only specific fields. The result is also a UUID-shaped string:

```python
data1 = {"region": "US", "store": "001", "product": "laptop", "price": 999}
data2 = {"region": "US", "store": "001", "product": "laptop", "price": 899}

config = tm.TransmogConfig(id_generation=["region", "store", "product"])

result1 = tm.flatten(data1, name="sales", config=config)
result2 = tm.flatten(data2, name="sales", config=config)

# Same composite key produces same ID (price is ignored)
assert result1.main[0]["_id"] == result2.main[0]["_id"]
```

Missing fields in a composite key are treated as absent; they do not raise.

## Metadata Field Names

`id_field`, `parent_field`, and `time_field` control the **names of metadata
columns in the output**. They do not affect how source data is read, with one
exception: `id_field` doubles as the source field name when
`id_generation="natural"` (see Natural IDs above).

Customize these names when the defaults conflict with your data schema:

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

All three names must be distinct. Supplying the same value for any two raises a
`ConfigurationError`.

Disable timestamp tracking:

```python
config = tm.TransmogConfig(time_field=None)
result = tm.flatten(data, config=config)
```

## Parent-Child Relationships

Child records reference their parents through the `parent_field` output column.
This link is built automatically from the nesting structure — no configuration
beyond the field name is required.

```python
result = tm.flatten(
    {"name": "Laptop", "reviews": [{"rating": 5}]},
    name="products",
)
main_id = result.main[0]["_id"]

for review in result.tables["products_reviews"]:
    assert review["_parent_id"] == main_id
```
