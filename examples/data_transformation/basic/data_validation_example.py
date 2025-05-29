#!/usr/bin/env python3
"""Example script demonstrating data validation with Transmog.

This example shows how to validate data against defined rules and constraints.
"""

import csv
import os
import re
from datetime import datetime


# Define severity levels
class Severity:
    """Severity levels for validation issues."""

    ERROR = "ERROR"
    WARNING = "WARNING"
    INFO = "INFO"


# Define a validation issue
class ValidationIssue:
    """Represents a single validation issue found during data validation."""

    def __init__(self, row, field, message, severity=Severity.ERROR):
        """Initialize a validation issue.

        Args:
            row: Row number where the issue was found
            field: Field name where the issue was found
            message: Description of the validation issue
            severity: Severity level of the issue (default: ERROR)
        """
        self.row = row
        self.field = field
        self.message = message
        self.severity = severity


# Simple validator implementation
class DataValidator:
    """Validates data records against defined rules and constraints."""

    def __init__(self):
        """Initialize the validator with empty rules and issues lists."""
        self.rules = []
        self.issues = []

    def add_rule(self, field, rule_type, **options):
        """Add a validation rule for a field.

        Args:
            field: Field name to validate
            rule_type: Type of validation rule
            **options: Additional options for the rule
        """
        self.rules.append({"field": field, "type": rule_type, "options": options})

    def validate_records(self, records):
        """Validate a list of records against defined rules.

        Args:
            records: List of records to validate

        Returns:
            Tuple of (valid_records, invalid_records)
        """
        self.issues = []
        valid_records = []
        invalid_records = []

        for i, record in enumerate(records):
            row_num = i + 1  # 1-based row numbering
            row_issues = []

            for rule in self.rules:
                field = rule["field"]
                rule_type = rule["type"]
                options = rule["options"]

                # Skip if field is not in the record
                if field not in record:
                    if rule_type == "required":
                        issue = ValidationIssue(
                            row_num,
                            field,
                            options.get("message", f"Field '{field}' is required"),
                            options.get("severity", Severity.ERROR),
                        )
                        row_issues.append(issue)
                    continue

                value = record[field]

                # Apply validation based on rule type
                if rule_type == "required" and (value is None or value == ""):
                    issue = ValidationIssue(
                        row_num,
                        field,
                        options.get("message", f"Field '{field}' cannot be empty"),
                        options.get("severity", Severity.ERROR),
                    )
                    row_issues.append(issue)

                elif (
                    rule_type == "regex"
                    and value
                    and not re.match(options.get("pattern", ""), str(value))
                ):
                    issue = ValidationIssue(
                        row_num,
                        field,
                        options.get(
                            "message",
                            f"Field '{field}' does not match required pattern",
                        ),
                        options.get("severity", Severity.ERROR),
                    )
                    row_issues.append(issue)

                elif (
                    rule_type == "in_list"
                    and value
                    and value not in options.get("valid_values", [])
                ):
                    issue = ValidationIssue(
                        row_num,
                        field,
                        options.get("message", f"Field '{field}' has invalid value"),
                        options.get("severity", Severity.WARNING),
                    )
                    row_issues.append(issue)

                elif rule_type == "range" and value is not None:
                    try:
                        num_value = float(value)
                        min_val = options.get("min_value")
                        max_val = options.get("max_value")

                        if (min_val is not None and num_value < min_val) or (
                            max_val is not None and num_value > max_val
                        ):
                            issue = ValidationIssue(
                                row_num,
                                field,
                                options.get(
                                    "message", f"Field '{field}' is out of range"
                                ),
                                options.get("severity", Severity.ERROR),
                            )
                            row_issues.append(issue)
                    except (ValueError, TypeError):
                        issue = ValidationIssue(
                            row_num,
                            field,
                            options.get("message", f"Field '{field}' must be a number"),
                            options.get("severity", Severity.ERROR),
                        )
                        row_issues.append(issue)

                elif rule_type == "date_format" and value:
                    try:
                        datetime.strptime(str(value), options.get("format", "%Y-%m-%d"))
                    except ValueError:
                        issue = ValidationIssue(
                            row_num,
                            field,
                            options.get(
                                "message", f"Field '{field}' has invalid date format"
                            ),
                            options.get("severity", Severity.ERROR),
                        )
                        row_issues.append(issue)

                elif rule_type == "type" and value is not None:
                    expected_type = options.get("expected_type")
                    if expected_type == "int":
                        try:
                            int(value)
                        except (ValueError, TypeError):
                            issue = ValidationIssue(
                                row_num,
                                field,
                                options.get(
                                    "message", f"Field '{field}' must be an integer"
                                ),
                                options.get("severity", Severity.ERROR),
                            )
                            row_issues.append(issue)

            # Add issues for this row to the overall list
            self.issues.extend(row_issues)

            # Classify record as valid or invalid
            if any(issue.severity == Severity.ERROR for issue in row_issues):
                invalid_records.append(record)
            else:
                valid_records.append(record)

        return valid_records, invalid_records

    def count_by_severity(self, severity):
        """Count issues of a specific severity level.

        Args:
            severity: Severity level to count

        Returns:
            Number of issues with the specified severity
        """
        return sum(1 for issue in self.issues if issue.severity == severity)

    def write_report(self, filename):
        """Write validation report to a file.

        Args:
            filename: Path to write the report
        """
        with open(filename, "w") as f:
            f.write("Validation Report\n")
            f.write("=================\n\n")

            # Group issues by severity
            errors = [
                issue for issue in self.issues if issue.severity == Severity.ERROR
            ]
            warnings = [
                issue for issue in self.issues if issue.severity == Severity.WARNING
            ]
            infos = [issue for issue in self.issues if issue.severity == Severity.INFO]

            # Write summary with line length fixed
            summary = (
                f"Summary: {len(errors)} errors, "
                f"{len(warnings)} warnings, "
                f"{len(infos)} info\n\n"
            )
            f.write(summary)

            if errors:
                f.write("ERRORS\n------\n")
                for issue in errors:
                    f.write(f"Row {issue.row}, {issue.field}: {issue.message}\n")
                f.write("\n")

            if warnings:
                f.write("WARNINGS\n--------\n")
                for issue in warnings:
                    f.write(f"Row {issue.row}, {issue.field}: {issue.message}\n")
                f.write("\n")

            if infos:
                f.write("INFO\n----\n")
                for issue in infos:
                    f.write(f"Row {issue.row}, {issue.field}: {issue.message}\n")


def main():
    """Main function to demonstrate data validation operations."""
    # Create a validator
    validator = DataValidator()

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

    # Write the employee data to CSV
    with open(temp_csv, "w", newline="") as csvfile:
        if employee_data:
            fieldnames = employee_data[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(employee_data)

    print(f"Created sample employee data in {temp_csv}")
    print("Validating data against defined rules...")

    # Required fields
    validator.add_rule(
        "name", "required", message="Employee name is required", severity=Severity.ERROR
    )

    # Email format validation
    validator.add_rule(
        "email",
        "regex",
        pattern=r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$",
        message="Invalid email format",
        severity=Severity.ERROR,
    )

    # Department validation (whitelist)
    validator.add_rule(
        "department",
        "in_list",
        valid_values=["Engineering", "Marketing", "Sales", "Finance", "HR"],
        message="Unknown department",
        severity=Severity.WARNING,
    )

    # Salary range validation
    validator.add_rule(
        "salary",
        "range",
        min_value=30000,
        max_value=200000,
        message="Salary out of acceptable range",
        severity=Severity.ERROR,
    )

    # Date format validation
    validator.add_rule(
        "hire_date",
        "date_format",
        format="%Y-%m-%d",
        message="Invalid hire date format",
        severity=Severity.ERROR,
    )

    # ID must be numeric
    validator.add_rule(
        "id",
        "type",
        expected_type="int",
        message="Employee ID must be a number",
        severity=Severity.ERROR,
    )

    # Perform validation
    valid_records, invalid_records = validator.validate_records(employee_data)

    # Write validation report to a file
    validator.write_report("output/validation_report.txt")

    # Get counts of errors and warnings
    error_count = validator.count_by_severity(Severity.ERROR)
    warning_count = validator.count_by_severity(Severity.WARNING)

    # Print summary
    print(
        f"\nValidation complete. Found {error_count} errors and "
        f"{warning_count} warnings."
    )
    print("Detailed validation report saved to output/validation_report.txt")

    # Print some of the validation issues
    print("\nSample validation issues:")
    for issue in validator.issues[:5]:  # Show first 5 issues
        print(f"  {issue.severity}: Row {issue.row}, {issue.field}: {issue.message}")

    # Save valid and invalid records separately
    with open("output/valid_employees.csv", "w", newline="") as csvfile:
        if valid_records:
            fieldnames = valid_records[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(valid_records)

    with open("output/invalid_employees.csv", "w", newline="") as csvfile:
        if invalid_records:
            fieldnames = invalid_records[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(invalid_records)

    print(f"\n{len(valid_records)} valid records saved to output/valid_employees.csv")
    print(
        f"{len(invalid_records)} invalid records saved to output/invalid_employees.csv"
    )


if __name__ == "__main__":
    main()
