#!/usr/bin/env python3
"""Example script demonstrating data cleanup with Transmog.

This example shows how to clean and standardize data using various transformations
before flattening with Transmog.
"""

import os
import re
from datetime import datetime

import transmog as tm


def strip_value(value):
    """Remove leading and trailing whitespace."""
    return str(value).strip() if value is not None else ""


def title_case(value):
    """Convert string to title case."""
    return str(value).title() if value else ""


def to_lowercase(value):
    """Convert string to lowercase."""
    return str(value).lower() if value else ""


def to_uppercase(value):
    """Convert string to uppercase."""
    return str(value).upper() if value else ""


def remove_chars(value, chars_to_remove):
    """Remove specified characters from string."""
    if not value:
        return ""
    for char in chars_to_remove:
        value = str(value).replace(char, "")
    return value


def replace_regex(value, pattern, replacement):
    """Replace pattern in string with replacement."""
    if not value:
        return ""
    return re.sub(pattern, replacement, str(value))


def format_phone(value):
    """Format phone number as (XXX) XXX-XXXX."""
    if not value or len(value) != 10:
        return value
    return f"({value[:3]}) {value[3:6]}-{value[6:]}"


def format_currency(value):
    """Format a value as currency."""
    try:
        # Remove any existing currency symbols and commas
        value = str(value).replace("$", "").replace(",", "")
        # Convert to float and format
        return f"${float(value):,.2f}"
    except (ValueError, TypeError):
        return value


def clean_employee_data(messy_data):
    """Clean and standardize employee data."""
    cleaned_data = []
    stats = {
        "empty_values_filled": 0,
        "invalid_values_fixed": 0,
        "format_standardizations": 0,
    }

    for record in messy_data:
        cleaned_record = {}

        # Clean first_name (strip whitespace, title case)
        cleaned_record["id"] = record.get("id", "")

        # Clean first_name
        first_name = strip_value(record.get("first_name", ""))
        first_name = title_case(first_name)
        cleaned_record["first_name"] = first_name
        if first_name != record.get("first_name"):
            stats["format_standardizations"] += 1

        # Clean last_name
        last_name = strip_value(record.get("last_name", ""))
        last_name = title_case(last_name)
        cleaned_record["last_name"] = last_name
        if last_name != record.get("last_name"):
            stats["format_standardizations"] += 1

        # Clean email
        email = to_lowercase(strip_value(record.get("email", "")))
        if not email:
            # Fix missing email
            if first_name and last_name:
                email = f"{first_name.lower()}.{last_name.lower()}@unknown.com"
            else:
                email = "unknown@unknown.com"
            stats["empty_values_filled"] += 1
        elif "@" in email and "." not in email.split("@")[1]:
            # Fix invalid email (missing domain)
            email = f"{email}.com"
            stats["invalid_values_fixed"] += 1

        cleaned_record["email"] = email

        # Clean phone
        phone = record.get("phone", "")
        if not phone or phone == "N/A":
            phone = "(000) 000-0000"
            stats["empty_values_filled"] += 1
        else:
            # Remove non-digit characters
            phone = remove_chars(phone, ".-() ")
            if len(phone) == 10 and phone.isdigit():
                phone = format_phone(phone)
                stats["format_standardizations"] += 1
            else:
                phone = "(000) 000-0000"
                stats["invalid_values_fixed"] += 1

        cleaned_record["phone"] = phone

        # Clean address
        address = strip_value(record.get("address", ""))
        address = title_case(address)
        address = replace_regex(address, r"[,\s]+", " ")
        address = ", ".join(
            [part.strip() for part in address.replace(", ", ",").split(",")]
        )
        cleaned_record["address"] = address
        stats["format_standardizations"] += 1

        # Clean income
        income = record.get("income", "")
        if not income or income.lower() in ["null", "n/a"]:
            income = "0.00"
            stats["empty_values_filled"] += 1
        else:
            income = remove_chars(income, "$,")
            try:
                income = format_currency(income)
                stats["format_standardizations"] += 1
            except (ValueError, TypeError):
                income = "$0.00"
                stats["invalid_values_fixed"] += 1

        cleaned_record["income"] = income

        # Clean start_date
        start_date = strip_value(record.get("start_date", ""))
        if not start_date:
            start_date = "N/A"
            stats["empty_values_filled"] += 1
        else:
            try:
                # Try parsing with multiple formats
                for fmt in ["%m/%d/%Y", "%Y-%m-%d", "%d-%b-%Y", "%m/%d/%y"]:
                    try:
                        date_obj = datetime.strptime(start_date, fmt)
                        start_date = date_obj.strftime("%Y-%m-%d")
                        stats["format_standardizations"] += 1
                        break
                    except ValueError:
                        continue
            except (ValueError, TypeError):
                start_date = "N/A"
                stats["invalid_values_fixed"] += 1

        cleaned_record["start_date"] = start_date

        # Add the cleaned record to the list
        cleaned_data.append(cleaned_record)

    return cleaned_data, stats


def main():
    """Main function to demonstrate data cleanup operations."""
    # Create output directory
    output_dir = os.path.join(
        os.path.dirname(__file__), "..", "data", "output", "data_cleanup"
    )
    os.makedirs(output_dir, exist_ok=True)

    # Sample messy data
    messy_data = [
        {
            "id": "001",
            "first_name": "  JOHN  ",
            "last_name": "smith",
            "email": "john.smith@example.com",
            "phone": "(555) 123-4567",
            "address": "123 Main St, Apt 4B, New York, NY 10001",
            "income": "$75,000.00",
            "start_date": "04/15/2020",
            "department": "engineering",
            "skills": ["python", "javascript", "sql"],
            "projects": [
                {"name": "Project A", "status": "completed", "budget": "$50,000"},
                {"name": "Project B", "status": "in-progress", "budget": "$75,000"},
            ],
        },
        {
            "id": "002",
            "first_name": "jane",
            "last_name": "DOE",
            "email": "jane.doe@example",
            "phone": "555.789.1234",
            "address": "456 PARK AVE., NEW YORK, NY",
            "income": "82,500",
            "start_date": "2020-06-01",
            "department": "marketing",
            "skills": ["design", "marketing", "analytics"],
            "projects": [
                {"name": "Campaign X", "status": "completed", "budget": "$25,000"},
            ],
        },
        {
            "id": "003",
            "first_name": "Robert",
            "last_name": "Johnson",
            "email": "r.johnson@example.com",
            "phone": "5551239876",
            "address": "789 broadway street, new york, ny 10003",
            "income": "null",
            "start_date": "01-Aug-2020",
            "department": "sales",
            "skills": ["sales", "negotiation"],
            "projects": [
                {"name": "Deal 1", "status": "closed", "budget": "$100,000"},
                {"name": "Deal 2", "status": "pending", "budget": "$150,000"},
                {"name": "Deal 3", "status": "closed", "budget": "$200,000"},
            ],
        },
        {
            "id": "004",
            "first_name": "Sarah ",
            "last_name": " Williams",
            "email": "SARAH.WILLIAMS@EXAMPLE.COM",
            "phone": "",
            "address": "321 5th Ave, New York, NY",
            "income": "$67,250",
            "start_date": "10/15/20",
            "department": "hr",
            "skills": ["recruiting", "training", "compliance"],
            "projects": [],
        },
        {
            "id": "005",
            "first_name": " michael",
            "last_name": "brown ",
            "email": "",
            "phone": "N/A",
            "address": "   555 LEXINGTON Avenue, New york, NY 10022   ",
            "income": "$0.00",
            "start_date": "",
            "department": "it",
            "skills": ["networking", "security"],
            "projects": [
                {
                    "name": "Security Audit",
                    "status": "in-progress",
                    "budget": "$30,000",
                },
            ],
        },
    ]

    print("=== Data Cleanup Example ===")
    print(f"Processing {len(messy_data)} employee records...")

    # Step 1: Clean the data
    print("\n=== Step 1: Data Cleaning ===")
    cleaned_data, stats = clean_employee_data(messy_data)

    print("Cleanup statistics:")
    print(f"- Empty values filled: {stats['empty_values_filled']}")
    print(f"- Invalid values fixed: {stats['invalid_values_fixed']}")
    print(f"- Format standardizations: {stats['format_standardizations']}")

    # Step 2: Flatten the cleaned data using Transmog
    print("\n=== Step 2: Flattening with Transmog ===")

    # Flatten the cleaned data
    result = tm.flatten(cleaned_data, name="employees")

    print("Flattening completed!")
    print(f"- Main table: {len(result.main)} records")
    print(f"- Child tables: {len(result.tables)}")

    for table_name, table_data in result.tables.items():
        print(f"  - {table_name}: {len(table_data)} records")

    # Step 3: Compare original vs cleaned data
    print("\n=== Step 3: Before/After Comparison ===")

    # Show original vs cleaned for first record
    print("Original first record:")
    print(f"  Name: '{messy_data[0]['first_name']}' '{messy_data[0]['last_name']}'")
    print(f"  Email: {messy_data[0]['email']}")
    print(f"  Phone: {messy_data[0]['phone']}")
    print(f"  Income: {messy_data[0]['income']}")

    print("\nCleaned first record:")
    cleaned_first = result.main[0]
    print(f"  Name: '{cleaned_first['first_name']}' '{cleaned_first['last_name']}'")
    print(f"  Email: {cleaned_first['email']}")
    print(f"  Phone: {cleaned_first['phone']}")
    print(f"  Income: {cleaned_first['income']}")

    # Step 4: Demonstrate error handling during cleanup
    print("\n=== Step 4: Error Handling During Cleanup ===")

    # Create some problematic data
    problematic_data = [
        {
            "id": "006",
            "first_name": None,  # Null value
            "last_name": "",  # Empty string
            "email": "invalid-email",  # Invalid email format
            "phone": "123",  # Too short phone number
            "income": "not-a-number",  # Invalid income
            "start_date": "invalid-date",  # Invalid date
            "department": "finance",
            "skills": ["accounting"],
            "projects": [{"name": "Budget Review", "status": "pending"}],
        }
    ]

    # Clean and flatten problematic data with error handling
    try:
        cleaned_problematic, problem_stats = clean_employee_data(problematic_data)
        problem_result = tm.flatten(
            cleaned_problematic, name="employees", on_error="warn"
        )

        print("Problematic data processed successfully with warnings:")
        print(f"- Records processed: {len(problem_result.main)}")
        print(f"- Cleanup fixes applied: {problem_stats['invalid_values_fixed']}")
        print(f"- Empty values filled: {problem_stats['empty_values_filled']}")

    except Exception as e:
        print(f"Error processing problematic data: {e}")

    # Step 5: Save results in different formats
    print("\n=== Step 5: Saving Results ===")

    # Save as JSON
    json_output = os.path.join(output_dir, "cleaned_employees.json")
    result.save(json_output)
    print(f"Saved JSON: {json_output}")

    # Save as CSV
    csv_output = os.path.join(output_dir, "cleaned_employees.csv")
    result.save(csv_output)
    print(f"Saved CSV: {csv_output}")

    # Save individual tables
    for table_name, table_data in result.tables.items():
        table_output = os.path.join(output_dir, f"{table_name}.json")
        result.save(table_output, table=table_name)
        print(f"Saved {table_name}: {table_output}")

    # Step 6: Data quality report
    print("\n=== Step 6: Data Quality Report ===")

    # Analyze the cleaned data quality
    quality_report = {
        "total_records": len(result.main),
        "complete_records": 0,
        "records_with_email": 0,
        "records_with_phone": 0,
        "records_with_valid_income": 0,
        "records_with_projects": 0,
    }

    for record in result.main:
        # Check completeness
        if all(
            record.get(field) for field in ["first_name", "last_name", "email", "phone"]
        ):
            quality_report["complete_records"] += 1

        # Check specific fields
        if record.get("email") and "@" in record["email"]:
            quality_report["records_with_email"] += 1

        if record.get("phone") and record["phone"] != "(000) 000-0000":
            quality_report["records_with_phone"] += 1

        if record.get("income") and record["income"] != "$0.00":
            quality_report["records_with_valid_income"] += 1

    # Count records with projects
    if "employees_projects" in result.tables:
        project_record_ids = set(
            proj.get("_parent_id") for proj in result.tables["employees_projects"]
        )
        quality_report["records_with_projects"] = len(project_record_ids)

    print("Data Quality Report:")
    print(f"- Total records: {quality_report['total_records']}")
    print(
        f"- Complete records: {quality_report['complete_records']} ({quality_report['complete_records'] / quality_report['total_records'] * 100:.1f}%)"
    )
    print(
        f"- Records with valid email: {quality_report['records_with_email']} ({quality_report['records_with_email'] / quality_report['total_records'] * 100:.1f}%)"
    )
    print(
        f"- Records with valid phone: {quality_report['records_with_phone']} ({quality_report['records_with_phone'] / quality_report['total_records'] * 100:.1f}%)"
    )
    print(
        f"- Records with valid income: {quality_report['records_with_valid_income']} ({quality_report['records_with_valid_income'] / quality_report['total_records'] * 100:.1f}%)"
    )
    print(
        f"- Records with projects: {quality_report['records_with_projects']} ({quality_report['records_with_projects'] / quality_report['total_records'] * 100:.1f}%)"
    )

    # Step 7: Demonstrate streaming for large datasets
    print("\n=== Step 7: Streaming for Large Datasets ===")

    # Create a larger dataset for streaming demonstration
    large_dataset = []
    for i in range(1000):
        record = {
            "id": f"{i:04d}",
            "first_name": f"Employee{i}",
            "last_name": f"Last{i}",
            "email": f"employee{i}@company.com",
            "department": ["engineering", "marketing", "sales", "hr", "it"][i % 5],
            "projects": [{"name": f"Project {i}", "status": "active"}]
            if i % 3 == 0
            else [],
        }
        large_dataset.append(record)

    print(f"Created large dataset with {len(large_dataset)} records")

    # Stream process the large dataset
    streaming_output = os.path.join(output_dir, "large_dataset_stream.json")
    stream_result = tm.flatten_stream(
        large_dataset, name="employees", output_path=streaming_output, chunk_size=100
    )

    print("Streaming processing completed")
    print(f"Output saved to: {streaming_output}")

    print("\n=== Example Completed Successfully ===")
    print("Key takeaways:")
    print("1. Clean data before flattening for better results")
    print("2. Use error handling to process problematic records")
    print("3. Transmog handles nested structures automatically")
    print("4. Multiple output formats are supported")
    print("5. Streaming is available for large datasets")
    print(f"\nAll outputs saved to: {output_dir}")


if __name__ == "__main__":
    main()
