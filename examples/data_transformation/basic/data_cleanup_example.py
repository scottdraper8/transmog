#!/usr/bin/env python3
"""Example script demonstrating data cleanup with Transmog.

This example shows how to clean and standardize data using various transformations.
"""

import csv
import os
import re
from datetime import datetime


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


def main():
    """Main function to demonstrate data cleanup operations."""
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
        },
    ]

    # Create a temporary CSV file with the messy data
    os.makedirs("output", exist_ok=True)
    temp_csv = "output/messy_data.csv"

    # Write messy data to CSV
    with open(temp_csv, "w", newline="") as csvfile:
        if messy_data:
            fieldnames = messy_data[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(messy_data)

    print(f"Created sample messy data in {temp_csv}")
    print("Applying cleanup rules...")

    # Clean the data
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

    # Write cleaned data to CSV
    cleaned_csv = "output/cleaned_data.csv"
    with open(cleaned_csv, "w", newline="") as csvfile:
        if cleaned_data:
            fieldnames = cleaned_data[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(cleaned_data)

    print(f"Cleaned data written to {cleaned_csv}")
    print("\nCleanup Statistics:")
    print(f"- Empty values filled: {stats['empty_values_filled']}")
    print(f"- Invalid values fixed: {stats['invalid_values_fixed']}")
    print(f"- Format standardizations: {stats['format_standardizations']}")


if __name__ == "__main__":
    main()
