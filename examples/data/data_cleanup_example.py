#!/usr/bin/env python3
"""Example script demonstrating data cleanup with Transmog.

This example shows how to clean messy data using various methods and filters.
"""

import os

from transmog import CleanupRule, Processor


def main():
    """Main function to demonstrate data cleanup operations."""
    # Initialize processor
    processor = Processor()

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
            "active": "YES",
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
            "active": "true",
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
            "active": "1",
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
            "active": "Y",
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
            "active": "no",
        },
    ]

    # Create a temporary CSV file with the messy data
    os.makedirs("output", exist_ok=True)
    temp_csv = "output/messy_data.csv"
    processor.create_csv_from_records(messy_data, temp_csv)

    print(f"Created sample messy data in {temp_csv}")
    print("Applying cleanup rules...")

    # Define cleanup rules
    cleanup_rules = [
        # Fix names (trim whitespace, proper case)
        CleanupRule(field="first_name", operations=["strip", "title_case"]),
        CleanupRule(field="last_name", operations=["strip", "title_case"]),
        # Fix email addresses (lowercase, handle missing domain)
        CleanupRule(
            field="email",
            operations=["lowercase", "strip"],
            fix_missing=lambda record: f"{record['first_name'].lower()}.{
                record['last_name'].lower()
            }@unknown.com"
            if record["first_name"] and record["last_name"]
            else "unknown@unknown.com",
            validation=lambda x: "@" in x and "." in x.split("@")[1],
            fix_invalid=lambda x: x + ".com"
            if "@" in x and "." not in x.split("@")[1]
            else x,
        ),
        # Standardize phone numbers to (XXX) XXX-XXXX format
        CleanupRule(
            field="phone",
            operations=["remove_chars('.-() ')"],
            validation=lambda x: len(x) == 10 and x.isdigit(),
            fix_missing="(000) 000-0000",
            fix_invalid=lambda x: "(000) 000-0000" if x == "N/A" else x,
            format_pattern="({}{}{}) {}{}{}-{}{}{}{}",
        ),
        # Standardize addresses (proper case, fix inconsistent formatting)
        CleanupRule(
            field="address",
            operations=["strip", "title_case", "replace_regex('[,\\s]+', ' ')"],
            post_process=lambda x: ", ".join(
                [part.strip() for part in x.replace(", ", ",").split(",")]
            ),
        ),
        # Clean and standardize income values as numeric
        CleanupRule(
            field="income",
            operations=["remove_chars('$,')"],
            validation=lambda x: x.replace(".", "", 1).isdigit(),
            fix_missing="0.00",
            fix_invalid=lambda x: "0.00" if x.lower() in ["null", "n/a", ""] else x,
            format_pattern="${:.2f}",
        ),
        # Standardize dates to YYYY-MM-DD format
        CleanupRule(
            field="start_date",
            operations=["strip"],
            date_formats=["%m/%d/%Y", "%Y-%m-%d", "%d-%b-%Y", "%m/%d/%y"],
            target_format="%Y-%m-%d",
            fix_missing="N/A",
        ),
        # Standardize boolean values
        CleanupRule(
            field="active",
            operations=["strip", "uppercase"],
            map_values={
                "YES": "True",
                "Y": "True",
                "TRUE": "True",
                "1": "True",
                "NO": "False",
                "N": "False",
                "FALSE": "False",
                "0": "False",
            },
            fix_missing="False",
        ),
    ]

    # Apply cleanup rules
    cleaned_data = processor.cleanup_csv(temp_csv, cleanup_rules)

    # Write cleaned data to output file
    output_file = "output/cleaned_data.csv"
    cleaned_data.write_csv(output_file)

    print(f"\nCleanup complete. Cleaned data written to {output_file}")

    # Show sample of cleaned data
    print("\nSample of cleaned data (first 3 records):")
    for record in cleaned_data.get_records()[:3]:
        print("\nID:", record["id"])
        print(f"  Name: {record['first_name']} {record['last_name']}")
        print(f"  Email: {record['email']}")
        print(f"  Phone: {record['phone']}")
        print(f"  Address: {record['address']}")
        print(f"  Income: {record['income']}")
        print(f"  Start Date: {record['start_date']}")
        print(f"  Active: {record['active']}")

    # Provide summary of cleanup
    print("\nCleanup Summary:")
    print(f"  Records processed: {len(messy_data)}")
    print(f"  Empty values filled: {cleaned_data.stats.get('empty_values_filled', 0)}")
    print(
        f"  Invalid values fixed: {cleaned_data.stats.get('invalid_values_fixed', 0)}"
    )
    format_standards = cleaned_data.stats.get("format_standardizations", 0)
    print(f"  Format standardizations: {format_standards}")


if __name__ == "__main__":
    main()
