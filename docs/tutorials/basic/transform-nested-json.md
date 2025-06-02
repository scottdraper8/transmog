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
from transmog import Processor, TransmogConfig
import json
```

The nested JSON structure is transformed as follows:

```python
# Load sample data
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
result = processor.process(data=company_data, entity_name="company")

# Convert the result to a dictionary of tables
tables = result.to_dict()

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
config = TransmogConfig().with_naming(
    entity_name_separator="_",
    array_item_suffix=""
)

# Create a processor with the custom configuration
processor = Processor(config=config)

# Process the data with custom configuration
result = processor.process(data=company_data, entity_name="company")
```

## Example Implementation

For a complete implementation example, see the flattening_basics.py
file at `../../../examples/data_processing/basic/flattening_basics.py`.

Key aspects demonstrated in the example:

- Basic transformation of nested JSON data
- Creating a processor with default configuration
- Accessing and displaying the transformed data
- Customizing the transformation with configuration options

## Related Documentation

- [Flatten and Normalize](flatten-and-normalize.md) tutorial
- [Data Transformation Guide](../../user/processing/data-transformation.md)
- [Output Formats](../../user/output/output-formats.md)
