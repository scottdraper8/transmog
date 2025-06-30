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
import transmog as tm
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

# Transform the data with a single line
result = tm.flatten(data=company_data, name="company")

# Access the tables directly
main_table = result.main
departments_table = result.tables["company_departments"]
employees_table = result.tables["company_departments_employees"]

# Display the tables
print("\n=== company ===")
for record in main_table:
    print(record)

print("\n=== company_departments ===")
for record in departments_table:
    print(record)

print("\n=== company_departments_employees ===")
for record in employees_table:
    print(record)
```

## Understanding the Output

The transformation creates three separate tables:

1. `company` - The main table with the company information
2. `company_departments` - A child table containing department information
3. `company_departments_employees` - A grandchild table containing employee information

Each table maintains relationships through ID fields:
- Each record has an `_id` field that uniquely identifies it
- Child records have a `_parent_id` field that references their parent's ID

## Customizing the Transformation

The transformation can be customized using various parameters:

```python
# Transform with custom naming options
result = tm.flatten(
    data=company_data,
    name="company",
    separator="_",       # Separator for table names
    array_suffix=""      # Suffix for array items
)

# Save the result to files (JSON, CSV, or Parquet)
result.save("output/company")
```

## Working with Files

You can also transform JSON files directly:

```python
# Transform a JSON file
result = tm.flatten_file("company_data.json", name="company")

# Save to different formats
result.save("output/company.json")  # Save as JSON
result.save("output/company.csv")   # Save as CSV
result.save("output/company.parquet")  # Save as Parquet
```

## Example Implementation

For a complete implementation example, see the flattening_basics.py
file at [GitHub](https://github.com/scottdraper8/transmog/blob/main/examples/data_processing/basic/flattening_basics.py).

Key aspects demonstrated in the example:

- Basic transformation of nested JSON data with a single function call
- Accessing the transformed data through intuitive properties
- Saving results to various output formats
- Customizing the transformation with simple parameters

## Related Documentation

- [Flatten and Normalize](flatten-and-normalize.md) tutorial
- [Data Transformation Guide](../../user/processing/data-transformation.md)
- [Output Formats](../../user/output/output-formats.md)
