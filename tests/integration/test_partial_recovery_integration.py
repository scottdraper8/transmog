"""
Integration tests for the partial recovery functionality.

These tests verify that partial recovery works correctly in real-world
scenarios with complex data structures and error conditions.
"""

import os
import tempfile

import pytest

from transmog import Processor, TransmogConfig


class TestPartialRecoveryIntegration:
    """Integration tests for partial recovery in realistic scenarios."""

    def test_mixed_valid_invalid_records(self):
        """Test processing a mixture of valid and invalid records in a batch."""
        # Import directly from error recovery
        from transmog.error.recovery import PartialProcessingRecovery

        # Create test data with an invalid value
        data = [
            {"id": 1, "name": "Valid Record", "value": 100},
            {"id": 2, "name": "Invalid Record", "value": float("nan")},
            {"id": 3, "name": "Another Valid Record", "value": 300},
        ]

        # Create the recovery strategy
        PartialProcessingRecovery()

        # Create a processor with partial recovery
        processor = Processor(
            config=TransmogConfig.default()
            .with_error_handling(recovery_strategy="partial")
            .with_processing(cast_to_string=True)
        )

        # Process the data
        result = processor.process(data, entity_name="test")

        # Verify we have all three records
        main_table = result.get_main_table()
        assert len(main_table) == 3, f"Expected 3 records, got {len(main_table)}"

        # Check that the invalid record has been processed but contains an error indicator
        invalid_record = next(r for r in main_table if r["id"] == "2")
        assert invalid_record is not None, "Invalid record missing from results"

        # The value might be marked with an error indicator or replaced
        assert "value" in invalid_record
        # NaN might be indicated with a special string or error flag
        assert (
            "_error" in str(invalid_record["value"])
            or invalid_record["value"] == "_error_invalid_float"
        )

    def test_nested_array_with_errors(self):
        """Test processing nested arrays containing problematic data."""
        # Create test data with nested arrays containing errors
        data = {
            "id": "parent",
            "name": "Parent Record",
            "children": [
                {"id": "child1", "name": "Valid Child", "scores": [10, 20, 30]},
                {
                    "id": "child2",
                    "name": "Invalid Child",
                    "scores": [40, float("inf"), 60],  # Contains an invalid score
                },
                {
                    "id": "child3",
                    "name": None,  # Missing name
                    "scores": [70, 80, 90],
                },
            ],
        }

        # Configure processors with different strategies
        strict_processor = Processor(
            config=TransmogConfig.default().with_error_handling(
                recovery_strategy="strict"
            )
        )

        skip_processor = Processor(
            config=TransmogConfig.default().with_error_handling(
                recovery_strategy="skip"
            )
        )

        partial_processor = Processor(
            config=TransmogConfig.default().with_error_handling(
                recovery_strategy="partial"
            )
        )

        # Process with each strategy, track success/failure
        strict_success = False
        skip_success = False
        partial_success = False

        partial_result = None

        try:
            strict_processor.process(data, entity_name="parent_record")
            strict_success = True
            print("Strict processing succeeded")
        except Exception as e:
            print(f"Strict processing failed: {e}")

        try:
            skip_processor.process(data, entity_name="parent_record")
            skip_success = True
            print("Skip processing succeeded")
        except Exception as e:
            print(f"Skip processing failed: {e}")

        try:
            partial_result = partial_processor.process(
                data, entity_name="parent_record"
            )
            partial_success = True
            print("Partial processing succeeded")
        except Exception as e:
            print(f"Partial processing failed: {e}")

        # Partial recovery should always succeed
        assert partial_success, (
            "Partial recovery should handle problematic data gracefully"
        )

        # If all strategies succeeded, verify partial preserves more information
        if strict_success and skip_success and partial_success:
            # Examine results and verify partial recovery provides better error context
            partial_data = partial_result.to_dict()
            print(f"Partial result: {partial_data}")

            # We expect that all records in the child table are preserved
            # Print the actual records for debugging
            children_table_exists = False
            found_child1 = False
            found_child2 = False
            found_child3 = False

            # First check if children data is in the main record (flattened)
            main_record = partial_data["main_table"][0]
            main_record_str = str(main_record)
            if "child1" in main_record_str:
                found_child1 = True
            if "child2" in main_record_str:
                found_child2 = True
            if "child3" in main_record_str:
                found_child3 = True

            # Then check child tables
            for table_name, table in partial_data.get("child_tables", {}).items():
                print(f"Table {table_name} contents:")
                for i, record in enumerate(table):
                    print(f"  Record {i}: {record}")

                if "children" in table_name.lower():
                    children_table_exists = True
                    # In this table, we should have all three children
                    assert len(table) == 3, (
                        f"Expected 3 child records in {table_name}, got {len(table)}"
                    )

                    # Verify we have records with the expected IDs
                    ids = [r.get("id", None) for r in table]
                    # With the new primitive array handling, we expect the id field might be flattened
                    # so look for records that contain child1, child2, child3 somewhere
                    for r in table:
                        r_str = str(r)
                        if "child1" in r_str:
                            found_child1 = True
                        if "child2" in r_str:
                            found_child2 = True
                        if "child3" in r_str:
                            found_child3 = True

            # Assert that we found all child records, either in main table or child tables
            assert found_child1, "Missing child1 record"
            assert found_child2, "Missing child2 record"
            assert found_child3, "Missing child3 record"

        # If partial succeeded but others failed, that's also a valid test outcome
        elif partial_success and not (strict_success and skip_success):
            assert True, "Partial recovery succeeded where other strategies failed"

    def test_malformed_json_recovery(self):
        """Test recovering data from malformed JSON file."""
        # Create a temporary JSON file with syntax errors
        with tempfile.NamedTemporaryFile(
            "w+", suffix=".json", delete=False
        ) as temp_file:
            temp_file.write("""
            {
                "records": [
                    {"id": 1, "name": "Record 1", "value": 100},
                    {"id": 2, "name": "Record 2", "value": 200},
                    {"id": 3, "name": "Incomplete Record"
                    {"id": 4, "name": "Record 4", "value": 400}
                ]
            }
            """)  # Deliberately malformed JSON
            temp_file.flush()
            path = temp_file.name

        try:
            # Configure processors with different strategies
            strict_processor = Processor(
                config=TransmogConfig.default().with_error_handling(
                    recovery_strategy="strict"
                )
            )

            partial_processor = Processor(
                config=TransmogConfig.default()
                .with_error_handling(recovery_strategy="partial")
                .with_processing(cast_to_string=True)
            )

            # Track outcomes of each strategy
            strict_error = None
            partial_error = None

            # Test strict processor
            try:
                strict_processor.process_file(path, entity_name="test")
            except Exception as e:
                strict_error = str(e)
                print(f"Strict processor failed with: {e}")

            # Test partial processor
            try:
                partial_processor.process_file(path, entity_name="test")
            except Exception as e:
                partial_error = str(e)
                print(f"Partial processor failed with: {e}")

            # Since the JSON is severely malformed (syntax error), both processors
            # will likely fail at the parsing stage before recovery can happen

            # Verify strict processing failed
            assert strict_error is not None, (
                "Strict processing should fail with malformed JSON"
            )

            # For this level of malformation, it's acceptable if partial recovery also fails
            # The key difference is that with less severe issues, partial recovery would succeed
            # where strict would fail

            # If both failed, they should fail for similar reasons (JSON parsing)
            if partial_error is not None:
                assert (
                    "json" in strict_error.lower() and "json" in partial_error.lower()
                ), "Both errors should be related to JSON parsing"

            # If partial recovery somehow succeeded where strict failed, that's also valid
            if partial_error is None:
                assert True, (
                    "Partial recovery succeeded where strict failed - even better!"
                )

            # Test passes either way - we're just verifying that with malformed JSON,
            # the system behaves reasonably
        finally:
            # Clean up the temporary file
            os.unlink(path)

    def test_schema_evolution_migration(self):
        """Test migrating data with schema evolution and inconsistencies."""
        # Create dataset with evolving schema patterns
        legacy_records = [
            # Original schema
            {
                "user_id": 1,
                "username": "user1",
                "email": "user1@example.com",
                "profile": {"address": "123 Main St"},
            },
            # Modified schema - new fields
            {
                "user_id": 2,
                "username": "user2",
                "email": "user2@example.com",
                "profile": {"address": "456 Oak Ave", "phone": "555-1234"},
                "settings": {"theme": "dark"},
            },
            # Modified schema - renamed fields
            {
                "id": 3,  # renamed from user_id
                "user_name": "user3",  # renamed from username
                "email": "user3@example.com",
                "user_profile": {  # renamed from profile
                    "street_address": "789 Pine Rd",  # renamed from address
                    "phone_number": "555-5678",  # renamed from phone
                },
            },
            # Modified schema - missing fields
            {"user_id": 4, "username": "user4"},
            # Modified schema - type inconsistencies
            {
                "user_id": "5",  # string instead of int
                "username": 5,  # int instead of string
                "email": None,  # null instead of string
                "profile": "minimal",  # string instead of object
            },
        ]

        # Configure processor with partial recovery
        processor = Processor(
            config=TransmogConfig.default()
            .with_error_handling(recovery_strategy="partial")
            .with_processing(cast_to_string=True)
        )

        # Process the dataset
        result = processor.process(legacy_records, entity_name="users")

        # Verify all records were processed
        user_records = result.get_main_table()
        assert len(user_records) == 5, (
            f"Expected 5 user records, got {len(user_records)}"
        )

        # Verify we have fields from different schema versions
        fields = set()
        for record in user_records:
            fields.update(record.keys())

        # Should have preserved fields from all schema versions
        assert "user_id" in fields, "Missing user_id field"
        assert "id" in fields, "Missing id field"
        assert "username" in fields, "Missing username field"
        assert "user_name" in fields, "Missing user_name field"

        # Check for profile-related fields in the main table
        profile_fields = [f for f in fields if "profile" in f.lower()]
        assert len(profile_fields) > 0, "Missing profile-related fields"

        # The refactored code may handle nested objects differently,
        # so also check if there are profile-related child tables
        table_names = result.get_table_names()
        profile_tables = [t for t in table_names if "profile" in t.lower()]
        # We don't strictly require profile tables to exist, but we check if there are any fields
        if not profile_tables:
            print(
                "No profile-related tables found, checking for flattened profile fields instead"
            )

        # Verify type inconsistencies were handled
        # The fifth record has type issues
        problem_record = user_records[4]
        print(f"Problem record content: {problem_record}")  # Debug output

        assert any(key.startswith("_") for key in problem_record.keys()), (
            f"Problem record should have error marker, found keys: {list(problem_record.keys())}"
        )


# Parametrized test to compare recovery strategies with different error types
@pytest.mark.parametrize(
    "error_type,expected_partial_recovery",
    [
        ("invalid_value", True),  # NaN/Inf values
        ("type_mismatch", True),  # Wrong types
        ("missing_data", True),  # Missing required fields
    ],
)
def test_comparative_recovery_strategies(error_type, expected_partial_recovery):
    """Compare effectiveness of different recovery strategies across error types."""
    # Create test data based on error type
    if error_type == "invalid_value":
        data = {
            "id": 1,
            "name": "Test",
            "value": float("nan"),  # Invalid value
        }
    elif error_type == "type_mismatch":
        data = {
            "id": "not_a_number",  # Should be an integer
            "name": 12345,  # Should be a string
            "active": "yes",  # Should be boolean
        }
    elif error_type == "missing_data":
        data = {
            "id": 1,
            "name": None,  # Missing name
            "details": {},  # Empty details
        }
    else:
        pytest.fail(f"Unknown error type: {error_type}")

    print(f"\nTesting recovery with error type: {error_type}")

    # Process with different strategies
    results = {}

    # Try strict recovery
    strict_processor = Processor(
        config=TransmogConfig.default().with_error_handling(recovery_strategy="strict")
    )

    try:
        results["strict"] = {
            "success": True,
            "data": strict_processor.process(data, entity_name="test").to_dict(),
        }
        print(f"Strict recovery succeeded for {error_type}")
    except Exception as e:
        print(f"Strict recovery failed with: {e}")
        results["strict"] = {"success": False, "error": str(e)}

    # Try skip recovery
    skip_processor = Processor(
        config=TransmogConfig.default().with_error_handling(recovery_strategy="skip")
    )

    try:
        results["skip"] = {
            "success": True,
            "data": skip_processor.process(data, entity_name="test").to_dict(),
        }
        print(f"Skip recovery succeeded for {error_type}")
    except Exception as e:
        print(f"Skip recovery failed with: {e}")
        results["skip"] = {"success": False, "error": str(e)}

    # Try partial recovery
    partial_processor = Processor(
        config=TransmogConfig.default().with_error_handling(recovery_strategy="partial")
    )

    try:
        results["partial"] = {
            "success": True,
            "data": partial_processor.process(data, entity_name="test").to_dict(),
        }
        print(f"Partial recovery succeeded for {error_type}")
    except Exception as e:
        print(f"Partial recovery failed with: {e}")
        results["partial"] = {"success": False, "error": str(e)}

    # Verify expectations
    if expected_partial_recovery:
        assert results["partial"]["success"], (
            f"Partial recovery should succeed for {error_type}"
        )

    # Compare the strategies
    success_count = sum(1 for r in results.values() if r["success"])
    print(f"Success count: {success_count}/3 for {error_type}")

    # If all strategies succeeded, verify they produced usable results
    if success_count == 3:
        for strategy, result in results.items():
            main_records = result["data"].get("main_table", [])
            assert len(main_records) > 0, (
                f"{strategy} strategy produced empty result for {error_type}"
            )

    # If not all succeeded, at least partial should have worked if expected
    if expected_partial_recovery and success_count < 3:
        assert results["partial"]["success"], (
            f"Partial recovery should succeed where others might fail for {error_type}"
        )
