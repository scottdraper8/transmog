#!/usr/bin/env python3
"""Example script demonstrating data validation with Transmog.

This example shows how to validate data against defined rules and constraints.
"""

import os

from transmog import Processor, ValidationRule, ValidationSeverity


def main():
    """Main function to demonstrate data validation capabilities."""
    # Initialize processor
    processor = Processor()

    # Sample data with various validation issues
    employee_data = [
        {
            "id": 1,
            "name": "John Smith",
            "email": "john@example.com",
            "department": "Engineering",
            "salary": 85000,
            "hire_date": "2021-03-15",
        },
        {
            "id": 2,
            "name": "Jane Doe",
            "email": "jane@example.com",
            "department": "Marketing",
            "salary": 78000,
            "hire_date": "2020-07-22",
        },
        {
            "id": 3,
            "name": "",
            "email": "missing.name@example.com",
            "department": "Sales",
            "salary": 92000,
            "hire_date": "2019-11-30",
        },
        {
            "id": 4,
            "name": "Alice Brown",
            "email": "not-an-email",
            "department": "Finance",
            "salary": 105000,
            "hire_date": "2022-01-10",
        },
        {
            "id": 5,
            "name": "Bob Johnson",
            "email": "bob@example.com",
            "department": "Unknown",
            "salary": -5000,
            "hire_date": "2021-06-05",
        },
        {
            "id": 6,
            "name": "Charlie Wilson",
            "email": "charlie@example.com",
            "department": "Engineering",
            "salary": 88000,
            "hire_date": "invalid-date",
        },
        {
            "id": "not-a-number",
            "name": "David Miller",
            "email": "david@example.com",
            "department": "Sales",
            "salary": 79500,
            "hire_date": "2020-09-15",
        },
    ]

    # Create a temporary CSV file with the employee data
    os.makedirs("output", exist_ok=True)
    temp_csv = "output/employee_data.csv"
    processor.create_csv_from_records(employee_data, temp_csv)

    print(f"Created sample employee data in {temp_csv}")
    print("Validating data against defined rules...")

    # Define validation rules
    validation_rules = [
        # Required fields
        ValidationRule(
            "name",
            "required",
            severity=ValidationSeverity.ERROR,
            message="Employee name is required",
        ),
        # Email format validation
        ValidationRule(
            "email",
            "regex",
            pattern=r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$",
            severity=ValidationSeverity.ERROR,
            message="Invalid email format",
        ),
        # Department validation (whitelist)
        ValidationRule(
            "department",
            "in_list",
            valid_values=["Engineering", "Marketing", "Sales", "Finance", "HR"],
            severity=ValidationSeverity.WARNING,
            message="Unknown department",
        ),
        # Salary range validation
        ValidationRule(
            "salary",
            "range",
            min_value=30000,
            max_value=200000,
            severity=ValidationSeverity.ERROR,
            message="Salary out of acceptable range",
        ),
        # Date format validation
        ValidationRule(
            "hire_date",
            "date_format",
            format="%Y-%m-%d",
            severity=ValidationSeverity.ERROR,
            message="Invalid hire date format",
        ),
        # ID must be numeric
        ValidationRule(
            "id",
            "type",
            expected_type="int",
            severity=ValidationSeverity.ERROR,
            message="Employee ID must be a number",
        ),
    ]

    # Perform validation
    validation_results = processor.validate_csv(temp_csv, validation_rules)

    # Write validation report to a file
    validation_results.write_report("output/validation_report.txt")

    # Get counts of errors and warnings
    error_count = validation_results.count_by_severity(ValidationSeverity.ERROR)
    warning_count = validation_results.count_by_severity(ValidationSeverity.WARNING)

    # Print summary
    print(
        f"\nValidation complete. Found {error_count} errors and "
        f"{warning_count} warnings."
    )
    print("Detailed validation report saved to output/validation_report.txt")

    # Print some of the validation issues
    print("\nSample validation issues:")
    for issue in validation_results.issues[:5]:  # Show first 5 issues
        severity = "ERROR" if issue.severity == ValidationSeverity.ERROR else "WARNING"
        print(f"  {severity}: Row {issue.row}, {issue.field}: {issue.message}")

    # Separate valid and invalid records
    valid_records, invalid_records = validation_results.split_records()

    # Save valid and invalid records separately
    processor.write_csv_from_records(valid_records, "output/valid_employees.csv")
    processor.write_csv_from_records(invalid_records, "output/invalid_employees.csv")

    print(f"\n{len(valid_records)} valid records saved to output/valid_employees.csv")
    print(
        f"{len(invalid_records)} invalid records saved to output/invalid_employees.csv"
    )


if __name__ == "__main__":
    main()
