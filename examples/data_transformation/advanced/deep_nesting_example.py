"""Deep nesting handling examples for the Transmog package.

This module demonstrates how Transmog handles deeply nested structures
with different configuration options.
"""

import os

import transmog as tm


def create_deeply_nested_data():
    """Create sample deeply nested JSON data."""
    return {
        "customer": {
            "id": 12345,
            "name": "Example Corporation",
            "billing_information": {
                "account_number": "ACC-123456",
                "payment_methods": [
                    {
                        "type": "credit_card",
                        "card_information": {
                            "card_number_last_four": "1234",
                            "expiration_date": "12/2025",
                            "billing_address": {
                                "street": "123 Corporate Drive",
                                "additional_address_line": "Suite 400",
                                "city": "Enterprise City",
                                "state": "CA",
                                "postal_code": "91234",
                                "address_verification_results": {
                                    "verification_timestamp": "2023-01-15T12:34:56Z",
                                    "verification_status": "verified",
                                    "verification_details": {
                                        "service_provider": "Address Validation Inc.",
                                        "reference_id": "VAL-987654321",
                                        "additional_metadata": {
                                            "confidence_score": 0.95,
                                            "validation_method": "api",
                                            "validation_rules": {
                                                "street_validation": True,
                                                "postal_code_validation": True,
                                                "city_validation": True,
                                                "comprehensive_check": {
                                                    "address_exists": True,
                                                    "deliverable": True,
                                                    "business_address": True,
                                                },
                                            },
                                        },
                                    },
                                },
                            },
                        },
                        "authorization_information": {
                            "authorized_users": [
                                {
                                    "user_id": "AUTH001",
                                    "name": "Jane Smith",
                                    "authorization_level": "primary",
                                    "contact_information": {
                                        "email": "jane@example.com",
                                        "phone_numbers": [
                                            {
                                                "type": "work",
                                                "number": "555-123-4567",
                                                "extension": "123",
                                                "verification_status": "verified",
                                                "verification_date": "2023-02-01",
                                                "verification_details": {
                                                    "method": "sms",
                                                    "attempts": 1,
                                                    "success": True,
                                                },
                                            },
                                            {
                                                "type": "mobile",
                                                "number": "555-987-6543",
                                                "verification_status": "pending",
                                                "verification_details": {
                                                    "method": "call",
                                                    "attempts": 2,
                                                    "success": False,
                                                },
                                            },
                                        ],
                                    },
                                },
                            ],
                        },
                    }
                ],
            },
        }
    }


def main():
    """Run the deep nesting example."""
    print("=== Deep Nesting Example ===")

    # Create output directory
    output_dir = os.path.join(
        os.path.dirname(__file__), "..", "data", "output", "deep_nesting"
    )
    os.makedirs(output_dir, exist_ok=True)

    # Get the deeply nested data
    deeply_nested_data = create_deeply_nested_data()

    print("Sample data has deeply nested structures with up to 10+ levels of nesting")

    # Example 1: Default behavior (simple API)
    print("\n=== Example 1: Default Behavior ===")
    print("Using the simple API with default settings")

    default_result = tm.flatten(deeply_nested_data, name="customer")

    print("Results with default settings:")
    print(f"- Main table: {len(default_result.main)} records")
    print(f"- Child tables: {len(default_result.tables)}")

    # Show table names to see nesting structure
    if default_result.tables:
        print("Child table names:")
        for table_name in sorted(default_result.tables.keys()):
            nesting_level = table_name.count("_")
            print(
                f"  {'  ' * nesting_level}{table_name} ({len(default_result.tables[table_name])} records)"
            )

    # Show some field names from the main table
    if default_result.main:
        main_record = default_result.main[0]
        billing_fields = [k for k in main_record.keys() if "billing" in k.lower()]
        print(
            f"\nSample billing-related fields in main table ({len(billing_fields)} total):"
        )
        for field in billing_fields[:5]:  # Show first 5
            print(f"  {field}")

    # Save default results
    default_output = os.path.join(output_dir, "default_threshold")
    os.makedirs(default_output, exist_ok=True)
    default_result.save(os.path.join(default_output, "main.parquet"))
    print(f"Default results saved to: {default_output}/")

    # Example 2: Controlling nesting with advanced configuration
    print("\n=== Example 2: Advanced Configuration - Low Nesting Threshold ===")
    print("Using advanced configuration to control deep nesting behavior")

    # For advanced configuration, use the Processor class directly
    from transmog.config import TransmogConfig
    from transmog.process import Processor

    # Create configuration with lower deep nesting threshold
    low_threshold_config = (
        TransmogConfig.default()
        .with_naming(
            deeply_nested_threshold=2  # Create separate tables after 2 levels
        )
        .with_processing(
            visit_arrays=True  # Make sure arrays are processed
        )
    )

    low_threshold_processor = Processor(low_threshold_config)
    low_threshold_result = low_threshold_processor.process(
        data=deeply_nested_data, entity_name="customer"
    )

    print("Results with low nesting threshold (2):")
    print(f"- Main table: {len(low_threshold_result.get_main_table())} records")
    print(f"- Child tables: {len(low_threshold_result.get_table_names())}")

    # Show table structure
    if low_threshold_result.get_table_names():
        print("Child table names:")
        for table_name in sorted(low_threshold_result.get_table_names()):
            nesting_level = table_name.count("_")
            table_data = low_threshold_result.get_child_table(table_name)
            print(f"  {'  ' * nesting_level}{table_name} ({len(table_data)} records)")

    # Save low threshold results
    low_output = os.path.join(output_dir, "low_threshold")
    low_threshold_result.write_all_parquet(low_output)
    print(f"Low threshold results saved to: {low_output}/")

    # Example 3: High nesting threshold
    print("\n=== Example 3: Advanced Configuration - High Nesting Threshold ===")
    print("Using high nesting threshold to keep more data in main table")

    # Create configuration with higher deep nesting threshold
    high_threshold_config = (
        TransmogConfig.default()
        .with_naming(
            deeply_nested_threshold=6  # Create separate tables after 6 levels
        )
        .with_processing(visit_arrays=True)
    )

    high_threshold_processor = Processor(high_threshold_config)
    high_threshold_result = high_threshold_processor.process(
        data=deeply_nested_data, entity_name="customer"
    )

    print("Results with high nesting threshold (6):")
    print(f"- Main table: {len(high_threshold_result.get_main_table())} records")
    print(f"- Child tables: {len(high_threshold_result.get_table_names())}")

    # Show table structure
    if high_threshold_result.get_table_names():
        print("Child table names:")
        for table_name in sorted(high_threshold_result.get_table_names()):
            nesting_level = table_name.count("_")
            table_data = high_threshold_result.get_child_table(table_name)
            print(f"  {'  ' * nesting_level}{table_name} ({len(table_data)} records)")

    # Show field count comparison
    main_record_high = high_threshold_result.get_main_table()[0]
    print(f"\nField count in main table with high threshold: {len(main_record_high)}")

    # Save high threshold results
    high_output = os.path.join(output_dir, "high_threshold")
    high_threshold_result.write_all_parquet(high_output)
    print(f"High threshold results saved to: {high_output}/")

    # Example 4: Comparison of different approaches
    print("\n=== Example 4: Comparison of Approaches ===")

    approaches = [
        (
            "Default (Simple API)",
            default_result,
            len(default_result.main[0]) if default_result.main else 0,
        ),
        (
            "Low Threshold (2)",
            low_threshold_result,
            len(low_threshold_result.get_main_table()[0])
            if low_threshold_result.get_main_table()
            else 0,
        ),
        (
            "High Threshold (6)",
            high_threshold_result,
            len(high_threshold_result.get_main_table()[0])
            if high_threshold_result.get_main_table()
            else 0,
        ),
    ]

    print("Comparison of different nesting approaches:")
    print(f"{'Approach':<20} {'Child Tables':<12} {'Main Fields':<12} {'Description'}")
    print("-" * 70)

    for name, result, field_count in approaches:
        if hasattr(result, "tables"):
            # Simple API result
            child_count = len(result.tables)
        else:
            # Processor result
            child_count = len(result.get_table_names())

        if name.startswith("Default"):
            description = "Balanced approach"
        elif "Low" in name:
            description = "More tables, fewer fields per table"
        else:
            description = "Fewer tables, more fields per table"

        print(f"{name:<20} {child_count:<12} {field_count:<12} {description}")

    # Example 5: Simple API with nesting control (if supported)
    print("\n=== Example 5: Simple API Optimization ===")
    print("Demonstrating simple API optimizations for deep nesting")

    # Try simple API with optimizations
    optimized_result = tm.flatten(
        deeply_nested_data,
        name="customer",
        add_timestamp=False,  # Skip timestamp for better performance
        natural_ids=False,  # Skip natural ID detection for better performance
    )

    print("Optimized simple API results:")
    print(f"- Main table: {len(optimized_result.main)} records")
    print(f"- Child tables: {len(optimized_result.tables)}")
    print(
        f"- Main table fields: {len(optimized_result.main[0]) if optimized_result.main else 0}"
    )

    # Example 6: Practical recommendations
    print("\n=== Example 6: Practical Recommendations ===")

    print("When to use different nesting approaches:")
    print("1. Simple API (default):")
    print("   - For most general use cases")
    print("   - When you want automatic handling")
    print("   - When you don't need fine-grained control")
    print("   - Quick prototyping and exploration")

    print("\n2. Advanced configuration (low threshold 1-2):")
    print("   - When you need highly normalized data")
    print("   - When working with relational databases")
    print("   - When you want to minimize field count per table")
    print("   - When you need to handle very deep nesting")

    print("\n3. Advanced configuration (high threshold 5-8):")
    print("   - When you want to minimize table count")
    print("   - When working with document databases")
    print("   - When you prefer fewer, wider tables")
    print("   - When nesting is not extremely deep")

    # Example 7: Working with the results
    print("\n=== Example 7: Working with the Results ===")

    # Show how to access deeply nested data from the flattened results
    print("Accessing deeply nested data from flattened results:")

    # Find the deepest table
    deepest_table_name = None
    max_nesting = 0

    for table_name in default_result.tables.keys():
        nesting_level = table_name.count("_")
        if nesting_level > max_nesting:
            max_nesting = nesting_level
            deepest_table_name = table_name

    if deepest_table_name:
        deepest_table = default_result.tables[deepest_table_name]
        print(f"\nDeepest nested table: {deepest_table_name}")
        print(f"Nesting level: {max_nesting}")
        print(f"Records: {len(deepest_table)}")

        if deepest_table:
            print("Sample fields in deepest table:")
            sample_record = deepest_table[0]
            for field in list(sample_record.keys())[:5]:
                print(f"  {field}: {sample_record[field]}")

    print("\n=== Example Completed Successfully ===")
    print("\nKey takeaways:")
    print("1. Simple API works well for most deep nesting cases")
    print("2. Use advanced configuration for fine-grained control")
    print("3. Lower thresholds create more normalized data")
    print("4. Higher thresholds keep more data in main table")
    print("5. Choose approach based on your downstream use case")
    print("6. Simple API optimizations can improve performance")
    print(f"\nAll outputs saved to: {output_dir}")


if __name__ == "__main__":
    main()
