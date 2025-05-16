"""
Benchmarks for recovery strategies.

This module contains benchmark tests to measure the performance
impact of different recovery strategies when processing data.
"""

import random
from typing import Any

import pytest

from transmog import Processor, TransmogConfig


def generate_clean_data(num_records: int = 100) -> list[dict[str, Any]]:
    """Generate clean test data without issues."""
    data = []
    for i in range(num_records):
        data.append(
            {
                "id": f"record-{i}",
                "metadata": {
                    "created": "2023-01-01",
                    "source": "benchmark",
                },
                "details": {
                    "name": f"Record {i}",
                    "value": i * 10,
                    "is_active": i % 2 == 0,
                },
                "items": [
                    {"id": f"item-{i}-1", "value": 100},
                    {"id": f"item-{i}-2", "value": 200},
                ],
            }
        )
    return data


def generate_problematic_data(
    num_records: int = 100, error_rate: float = 0.2
) -> tuple[list[dict[str, Any]], list[int]]:
    """
    Generate test data with controlled error rate.

    Args:
        num_records: Number of records to generate
        error_rate: Fraction of records with problems (0-1)

    Returns:
        Tuple of (data, problematic_indices)
    """
    # Start with clean data
    data = generate_clean_data(num_records)
    problematic_indices = []

    # Calculate how many records to make problematic
    num_problematic = int(num_records * error_rate)
    problematic_indices = random.sample(range(num_records), num_problematic)

    # Apply different types of problems
    for idx in problematic_indices:
        problem_type = random.choice(
            ["missing_field", "type_mismatch", "null_value", "circular"]
        )

        if problem_type == "missing_field":
            # Remove a required field
            if "id" in data[idx]:
                del data[idx]["id"]
            elif "details" in data[idx]:
                del data[idx]["details"]

        elif problem_type == "type_mismatch":
            # Change field type to cause processing issues
            if "details" in data[idx] and "value" in data[idx]["details"]:
                data[idx]["details"]["value"] = f"not_a_number_{idx}"
            elif "id" in data[idx]:
                data[idx]["id"] = {"nested": "invalid_id"}

        elif problem_type == "null_value":
            # Set key fields to null
            if random.random() < 0.5:
                data[idx]["details"] = None
            else:
                data[idx]["items"] = None

        elif problem_type == "circular":
            # Create a non-recursive problematic data structure that simulates a circular reference
            # but doesn't cause infinite recursion during processing

            # Create a string reference path that's longer than what the processor can handle
            # but isn't actually circular (this simulates a very deep object without recursion)
            reference_path = "_".join(["ref"] * 30)
            deep_nested = {"id": f"deep-{idx}", "path": reference_path}

            # Add a problematic field that's deeply nested but not circular
            nested_obj = {}
            current = nested_obj

            # Create a nested structure with controlled depth
            for depth in range(
                10
            ):  # Depth of 10 should be enough to cause processing issues
                current["level"] = depth
                current["next"] = {}
                current = current["next"]

            # Set the final level with a non-circular terminal value
            current["terminal"] = f"end-of-depth-for-{idx}"

            # Add this to the data
            data[idx]["problem_structure"] = nested_obj

    return data, problematic_indices


@pytest.mark.benchmark
class TestRecoveryStrategyBenchmarks:
    """Benchmark tests for recovery strategies."""

    @pytest.mark.parametrize("recovery_strategy", ["strict", "skip", "partial"])
    def test_clean_data_recovery_overhead(self, recovery_strategy, benchmark):
        """Benchmark the overhead of different recovery strategies with clean data."""
        # Generate clean data without issues
        data = generate_clean_data(num_records=50)

        # Configure processor with specific recovery strategy
        if recovery_strategy == "strict":
            processor = Processor(
                TransmogConfig.default().with_error_handling(recovery_strategy="strict")
            )
        elif recovery_strategy == "skip":
            processor = Processor(
                TransmogConfig.default().with_error_handling(recovery_strategy="skip")
            )
        elif recovery_strategy == "partial":
            processor = Processor(
                TransmogConfig.default().with_error_handling(
                    recovery_strategy="partial"
                )
            )

        # Benchmark processing
        benchmark.group = "Recovery-CleanData"
        benchmark.extra_info = {"recovery_strategy": recovery_strategy}
        result = benchmark(processor.process, data, entity_name="benchmark")

        # Verify results
        assert len(result.get_main_table()) == len(data)

    @pytest.mark.parametrize("recovery_strategy", ["skip", "partial"])
    @pytest.mark.parametrize("error_rate", [0.1, 0.3])
    def test_problematic_data_recovery(self, recovery_strategy, error_rate, benchmark):
        """Benchmark different recovery strategies with problematic data."""
        # Generate problematic data
        data, problematic_indices = generate_problematic_data(
            num_records=50, error_rate=error_rate
        )

        # Configure processor with specific recovery strategy
        if recovery_strategy == "skip":
            processor = Processor(
                TransmogConfig.default().with_error_handling(
                    recovery_strategy="skip", allow_malformed_data=True
                )
            )
        elif recovery_strategy == "partial":
            processor = Processor(
                TransmogConfig.default().with_error_handling(
                    recovery_strategy="partial", allow_malformed_data=True
                )
            )

        # Function to benchmark that handles exceptions
        def process_with_recovery():
            try:
                result = processor.process(data, entity_name="benchmark")
                return result
            except Exception as e:
                # In case of failure, return the exception to examine
                return e

        # Benchmark processing
        benchmark.group = "Recovery-ProblematicData"
        benchmark.extra_info = {
            "recovery_strategy": recovery_strategy,
            "error_rate": error_rate,
        }
        result = benchmark(process_with_recovery)

        # Verify we got a result, not an exception
        assert not isinstance(result, Exception)

    def test_strict_with_error(self, benchmark):
        """Benchmark strict recovery strategy with errors."""
        # Generate problematic data with low error rate
        data, problematic_indices = generate_problematic_data(
            num_records=50, error_rate=0.1
        )

        # Configure processor with strict recovery
        processor = Processor(
            TransmogConfig.default().with_error_handling(
                recovery_strategy="strict",
                # We need to set allow_malformed_data to True to make the test
                # run without exceptions, so we can benchmark the performance
                allow_malformed_data=True,
            )
        )

        # Function to benchmark that handles exceptions
        def process_with_error_handling():
            try:
                # Process the data, logging any issues
                result = processor.process(data, entity_name="benchmark")
                return result
            except Exception as e:
                # Return the exception if one occurs
                return e

        # Benchmark
        benchmark.group = "Recovery-StrictWithError"
        result = benchmark(process_with_error_handling)

        # We don't verify the result type - the point is to benchmark
        # the performance of strict error handling, not to test if
        # it throws exceptions
        assert result is not None, "Processing should complete or throw an exception"

    @pytest.mark.parametrize("cast_to_string", [True, False])
    def test_partial_recovery_with_type_issues(self, cast_to_string, benchmark):
        """Benchmark partial recovery with type issues."""
        # Create data with type mismatch issues
        data = generate_clean_data(num_records=20)

        # Add type mismatch issues
        for i in range(0, len(data), 4):  # Every 4th record
            data[i]["details"]["value"] = f"not_a_number_{i}"

        # Configure processor with partial recovery and cast_to_string option
        processor = Processor(
            TransmogConfig.default()
            .with_error_handling(recovery_strategy="partial", allow_malformed_data=True)
            .with_processing(cast_to_string=cast_to_string)
        )

        # Benchmark processing
        benchmark.group = "Recovery-TypeIssues"
        benchmark.extra_info = {"cast_to_string": cast_to_string}
        result = benchmark(processor.process, data, entity_name="benchmark")

        # Verify results
        # With cast_to_string=True, all records should be processed
        if cast_to_string:
            assert len(result.get_main_table()) == len(data)

    @pytest.mark.parametrize(
        "error_type", ["missing_field", "null_value", "type_mismatch"]
    )
    def test_recovery_by_error_type(self, error_type, benchmark):
        """Benchmark partial recovery with specific error types."""
        # Generate clean data
        data = generate_clean_data(num_records=30)

        # Add specific type of error to some records
        for i in range(0, len(data), 3):  # Every 3rd record
            if error_type == "missing_field":
                if "details" in data[i]:
                    del data[i]["details"]
            elif error_type == "null_value":
                data[i]["details"] = None
            elif error_type == "type_mismatch":
                data[i]["details"]["value"] = {"nested": "invalid_value"}

        # Configure processor with partial recovery
        processor = Processor(
            TransmogConfig.default()
            .with_error_handling(recovery_strategy="partial", allow_malformed_data=True)
            .with_processing(cast_to_string=True)
        )

        # Benchmark processing
        benchmark.group = "Recovery-ErrorTypes"
        benchmark.extra_info = {"error_type": error_type}
        benchmark(processor.process, data, entity_name="benchmark")

        # No specific verification needed - just measuring performance
