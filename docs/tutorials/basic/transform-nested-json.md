# Transform Nested JSON

This tutorial demonstrates the transformation of a nested JSON structure into flat tables.

## Sample Data

The following nested JSON structure represents a company with departments and employees:

```json
{
  "companyName": "Acme Inc",
  "founded": 1985,
  "headquarters": {
    "city": "New York",
    "country": "USA"
  },
  "departments": [
    {
      "name": "Engineering",
      "headCount": 50,
      "employees": [
        {
          "id": "E001",
          "name": "Jane Smith",
          "title": "Software Engineer"
        },
        {
          "id": "E002",
          "name": "John Doe",
          "title": "Senior Developer"
        }
      ]
    },
    {
      "name": "Marketing",
      "headCount": 20,
      "employees": [
        {
          "id": "M001",
          "name": "Alice Johnson",
          "title": "Marketing Specialist"
        }
      ]
    }
  ]
}
```

## Basic Transformation

The transformation begins with installing Transmog and importing the necessary components:

```python
# Install transmog
pip install transmog

# Import required components
from transmog import Processor, TransmogConfig
```

The nested JSON structure is transformed as follows:

```python
# Sample data
company_data = {
  "companyName": "Acme Inc",
  "founded": 1985,
  "headquarters": {
    "city": "New York",
    "country": "USA"
  },
  "departments": [
    {
      "name": "Engineering",
      "headCount": 50,
      "employees": [
        {
          "id": "E001",
          "name": "Jane Smith",
          "title": "Software Engineer"
        },
        {
          "id": "E002",
          "name": "John Doe",
          "title": "Senior Developer"
        }
      ]
    },
    {
      "name": "Marketing",
      "headCount": 20,
      "employees": [
        {
          "id": "M001",
          "name": "Alice Johnson",
          "title": "Marketing Specialist"
        }
      ]
    }
  ]
}

# Create a processor with default configuration
processor = Processor()

# Process the data
result = processor.process(company_data, entity_name="company")

# Convert to JSON objects and display the result
tables = result.to_json_objects()

# Display the tables
for table_name, records in tables.items():
    print(f"\n=== {table_name} ===")
    for record in records:
        print(record)
```

## Understanding the Output

The transformation creates three separate tables:

1. `company` - The main table with the company information
2. `company_departments` - A child table containing department information
3. `company_departments_employees` - A grandchild table containing employee information

Each table maintains relationships through ID fields.

## Customizing the Transformation

The transformation can be customized using `TransmogConfig`:

```python
# Create a custom configuration
config = (
    TransmogConfig.default()
    .performance_optimized()
    .with_naming(deep_nesting_threshold=4)
)

# Create a processor with custom configuration
processor = Processor(config)

# Process the data
result = processor.process(company_data, entity_name="company")

# Convert to JSON objects
tables = result.to_json_objects()
```

## Related Documentation

- [Flatten and Normalize](flatten-and-normalize.md) tutorial
- [Data Transformation Guide](../../user/processing/data-transformation.md)
- [Output Formats](../../user/output/output-formats.md)
