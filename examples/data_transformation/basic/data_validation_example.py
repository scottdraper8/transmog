#!/usr/bin/env python3
"""Example script demonstrating data validation with Transmog.

This example shows how to validate data against defined rules and constraints
before flattening with Transmog.
"""

import csv
import os
import re
from datetime import datetime

import transmog as tm


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
        """Count the number of issues by severity level.

        Args:
            severity: Severity level to count

        Returns:
            Number of issues with the given severity level
        """
        return sum(1 for issue in self.issues if issue.severity == severity)

    def write_report(self, filename):
        """Write a validation report to a file.

        Args:
            filename: Path to the output file
        """
        with open(filename, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["Row", "Field", "Severity", "Message"])

            for issue in self.issues:
                writer.writerow([issue.row, issue.field, issue.severity, issue.message])


def create_sample_data():
    """Create sample data with various validation issues."""
    return [
        # Valid record
        {
            "id": 1,
            "name": "Alice Johnson",
            "email": "alice@example.com",
            "age": 28,
            "salary": 75000,
            "department": "Engineering",
            "start_date": "2020-01-15",
            "skills": ["Python", "JavaScript", "SQL"],
            "projects": [
                {"name": "Project Alpha", "status": "completed", "budget": 50000},
                {"name": "Project Beta", "status": "in-progress", "budget": 75000},
            ],
        },
        # Record with missing required field
        {
            "id": 2,
            # "name" is missing
            "email": "bob@example.com",
            "age": 32,
            "salary": 80000,
            "department": "Marketing",
            "start_date": "2019-06-01",
            "skills": ["Marketing", "Analytics"],
            "projects": [
                {"name": "Campaign X", "status": "completed", "budget": 25000},
            ],
        },
        # Record with invalid email format
        {
            "id": 3,
            "name": "Carol Williams",
            "email": "invalid-email",  # Invalid email format
            "age": 35,
            "salary": 90000,
            "department": "Sales",
            "start_date": "2018-03-15",
            "skills": ["Sales", "Negotiation"],
            "projects": [],
        },
        # Record with age out of range
        {
            "id": 4,
            "name": "David Brown",
            "email": "david@example.com",
            "age": 150,  # Age out of range
            "salary": 65000,
            "department": "HR",
            "start_date": "2021-09-01",
            "skills": ["Recruiting", "Training"],
            "projects": [
                {"name": "Onboarding System", "status": "in-progress", "budget": 30000},
            ],
        },
        # Record with invalid department
        {
            "id": 5,
            "name": "Eva Martinez",
            "email": "eva@example.com",
            "age": 29,
            "salary": 70000,
            "department": "InvalidDept",  # Invalid department
            "start_date": "2020-11-15",
            "skills": ["Design", "UX"],
            "projects": [
                {"name": "UI Redesign", "status": "completed", "budget": 40000},
            ],
        },
        # Record with invalid date format
        {
            "id": 6,
            "name": "Frank Wilson",
            "email": "frank@example.com",
            "age": 31,
            "salary": 85000,
            "department": "Engineering",
            "start_date": "invalid-date",  # Invalid date format
            "skills": ["Java", "Spring"],
            "projects": [
                {"name": "Backend Refactor", "status": "pending", "budget": 60000},
            ],
        },
    ]


def main():
    """Main function to demonstrate data validation."""
    # Create output directory
    output_dir = os.path.join(
        os.path.dirname(__file__), "..", "data", "output", "data_validation"
    )
    os.makedirs(output_dir, exist_ok=True)

    print("=== Data Validation Example ===")

    # Step 1: Create sample data with validation issues
    print("\n=== Step 1: Creating Sample Data ===")
    sample_data = create_sample_data()
    print(f"Created {len(sample_data)} sample records")

    # Step 2: Set up validation rules
    print("\n=== Step 2: Setting Up Validation Rules ===")
    validator = DataValidator()

    # Add validation rules
    validator.add_rule("id", "required", message="ID is required")
    validator.add_rule(
        "id", "type", expected_type="int", message="ID must be an integer"
    )
    validator.add_rule("name", "required", message="Name is required")
    validator.add_rule(
        "email",
        "regex",
        pattern=r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
        message="Email must be in valid format",
    )
    validator.add_rule(
        "age",
        "range",
        min_value=18,
        max_value=100,
        message="Age must be between 18 and 100",
    )
    validator.add_rule(
        "salary",
        "range",
        min_value=30000,
        max_value=200000,
        message="Salary must be between $30,000 and $200,000",
    )
    validator.add_rule(
        "department",
        "in_list",
        valid_values=["Engineering", "Marketing", "Sales", "HR", "Finance"],
        message="Department must be one of the valid departments",
        severity=Severity.WARNING,
    )
    validator.add_rule(
        "start_date",
        "date_format",
        format="%Y-%m-%d",
        message="Start date must be in YYYY-MM-DD format",
    )

    print("Validation rules configured:")
    print("- ID: Required, must be integer")
    print("- Name: Required")
    print("- Email: Must match valid email pattern")
    print("- Age: Must be between 18 and 100")
    print("- Salary: Must be between $30,000 and $200,000")
    print("- Department: Must be valid department (warning)")
    print("- Start Date: Must be in YYYY-MM-DD format")

    # Step 3: Validate the data
    print("\n=== Step 3: Validating Data ===")
    valid_records, invalid_records = validator.validate_records(sample_data)

    print(f"Validation completed:")
    print(f"- Valid records: {len(valid_records)}")
    print(f"- Invalid records: {len(invalid_records)}")
    print(f"- Total issues found: {len(validator.issues)}")
    print(f"- Errors: {validator.count_by_severity(Severity.ERROR)}")
    print(f"- Warnings: {validator.count_by_severity(Severity.WARNING)}")

    # Step 4: Show validation issues
    print("\n=== Step 4: Validation Issues ===")
    if validator.issues:
        print("Issues found:")
        for issue in validator.issues:
            print(
                f"  Row {issue.row}, Field '{issue.field}' [{issue.severity}]: {issue.message}"
            )
    else:
        print("No validation issues found!")

    # Step 5: Process valid records with Transmog
    print("\n=== Step 5: Processing Valid Records with Transmog ===")
    if valid_records:
        # Flatten the valid records
        valid_result = tm.flatten(valid_records, name="employees")

        print(f"Valid records processed:")
        print(f"- Main table: {len(valid_result.main)} records")
        print(f"- Child tables: {len(valid_result.tables)}")

        for table_name, table_data in valid_result.tables.items():
            print(f"  - {table_name}: {len(table_data)} records")

        # Save valid results
        valid_output = os.path.join(output_dir, "valid_employees.json")
        valid_result.save(valid_output)
        print(f"Valid records saved to: {valid_output}")

    # Step 6: Process invalid records with error handling
    print("\n=== Step 6: Processing Invalid Records with Error Handling ===")
    if invalid_records:
        print(
            f"Attempting to process {len(invalid_records)} invalid records with error handling..."
        )

        # Process invalid records with warning mode
        try:
            invalid_result = tm.flatten(
                invalid_records, name="employees", on_error="warn"
            )

            print(f"Invalid records processed with warnings:")
            print(f"- Main table: {len(invalid_result.main)} records")
            print(f"- Child tables: {len(invalid_result.tables)}")

            # Save invalid results
            invalid_output = os.path.join(output_dir, "invalid_employees.json")
            invalid_result.save(invalid_output)
            print(f"Invalid records saved to: {invalid_output}")

        except Exception as e:
            print(f"Error processing invalid records: {e}")

    # Step 7: Generate validation report
    print("\n=== Step 7: Generating Validation Report ===")
    report_path = os.path.join(output_dir, "validation_report.csv")
    validator.write_report(report_path)
    print(f"Validation report saved to: {report_path}")

    # Step 8: Demonstrate validation with corrected data
    print("\n=== Step 8: Processing Corrected Data ===")

    # Create corrected version of the data
    corrected_data = []
    for record in sample_data:
        corrected_record = record.copy()

        # Fix missing name
        if not corrected_record.get("name"):
            corrected_record["name"] = (
                f"Employee {corrected_record.get('id', 'Unknown')}"
            )

        # Fix invalid email
        if corrected_record.get("email") == "invalid-email":
            corrected_record["email"] = "corrected@example.com"

        # Fix age out of range
        if corrected_record.get("age", 0) > 100:
            corrected_record["age"] = 35

        # Fix invalid department
        if corrected_record.get("department") == "InvalidDept":
            corrected_record["department"] = "Engineering"

        # Fix invalid date
        if corrected_record.get("start_date") == "invalid-date":
            corrected_record["start_date"] = "2020-01-01"

        corrected_data.append(corrected_record)

    # Validate corrected data
    corrected_validator = DataValidator()
    # Add the same rules
    corrected_validator.add_rule("id", "required")
    corrected_validator.add_rule("name", "required")
    corrected_validator.add_rule(
        "email",
        "regex",
        pattern=r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
    )
    corrected_validator.add_rule("age", "range", min_value=18, max_value=100)
    corrected_validator.add_rule("salary", "range", min_value=30000, max_value=200000)
    corrected_validator.add_rule(
        "department",
        "in_list",
        valid_values=["Engineering", "Marketing", "Sales", "HR", "Finance"],
        severity=Severity.WARNING,
    )
    corrected_validator.add_rule("start_date", "date_format", format="%Y-%m-%d")

    corrected_valid, corrected_invalid = corrected_validator.validate_records(
        corrected_data
    )

    print(f"Corrected data validation:")
    print(f"- Valid records: {len(corrected_valid)}")
    print(f"- Invalid records: {len(corrected_invalid)}")
    print(f"- Total issues: {len(corrected_validator.issues)}")

    # Process corrected data
    if corrected_valid:
        corrected_result = tm.flatten(corrected_valid, name="employees")
        corrected_output = os.path.join(output_dir, "corrected_employees.json")
        corrected_result.save(corrected_output)
        print(f"Corrected records saved to: {corrected_output}")

    # Step 9: Data quality comparison
    print("\n=== Step 9: Data Quality Comparison ===")

    print("Data Quality Summary:")
    print(f"Original data:")
    print(f"  - Total records: {len(sample_data)}")
    print(
        f"  - Valid records: {len(valid_records)} ({len(valid_records) / len(sample_data) * 100:.1f}%)"
    )
    print(f"  - Validation errors: {validator.count_by_severity(Severity.ERROR)}")
    print(f"  - Validation warnings: {validator.count_by_severity(Severity.WARNING)}")

    print(f"Corrected data:")
    print(f"  - Total records: {len(corrected_data)}")
    print(
        f"  - Valid records: {len(corrected_valid)} ({len(corrected_valid) / len(corrected_data) * 100:.1f}%)"
    )
    print(
        f"  - Validation errors: {corrected_validator.count_by_severity(Severity.ERROR)}"
    )
    print(
        f"  - Validation warnings: {corrected_validator.count_by_severity(Severity.WARNING)}"
    )

    print("\n=== Example Completed Successfully ===")
    print("Key takeaways:")
    print("1. Validate data before processing to catch issues early")
    print("2. Use different severity levels (ERROR vs WARNING) appropriately")
    print("3. Transmog can handle invalid data with error handling modes")
    print("4. Data correction improves processing success rates")
    print("5. Validation reports help track data quality over time")
    print(f"\nAll outputs saved to: {output_dir}")


if __name__ == "__main__":
    main()
