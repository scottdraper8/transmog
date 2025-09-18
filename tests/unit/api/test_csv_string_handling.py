"""
Tests for CSV string handling in Transmog v1.1.0 API.

Tests the ability to process CSV data directly from strings without file I/O.
"""

from io import StringIO

import pytest

import transmog as tm


class TestCSVStringHandling:
    """Test CSV string processing functionality."""

    def test_simple_csv_string(self):
        """Test processing a simple CSV string."""
        csv_data = """name,age,city
John Doe,30,New York
Jane Smith,25,Los Angeles
Bob Johnson,35,Chicago"""

        result = tm.flatten(csv_data, name="people")

        assert len(result.main) == 3
        assert result.main[0]["name"] == "John Doe"
        assert result.main[0]["age"] == "30"
        assert result.main[0]["city"] == "New York"
        assert result.main[1]["name"] == "Jane Smith"
        assert result.main[2]["name"] == "Bob Johnson"

    def test_csv_with_different_delimiters(self):
        """Test CSV with different delimiters."""
        # Tab-delimited
        csv_data = "name\tage\tcity\nJohn\t30\tNYC\nJane\t25\tLA"
        result = tm.flatten(csv_data, name="people")
        assert len(result.main) == 2
        assert result.main[0]["name"] == "John"

        # Pipe-delimited
        csv_data = "name|age|city\nJohn|30|NYC\nJane|25|LA"
        result = tm.flatten(csv_data, name="people")
        assert len(result.main) == 2
        assert result.main[0]["name"] == "John"

        # Semicolon-delimited
        csv_data = "name;age;city\nJohn;30;NYC\nJane;25;LA"
        result = tm.flatten(csv_data, name="people")
        assert len(result.main) == 2
        assert result.main[0]["name"] == "John"

    def test_csv_with_quoted_values(self):
        """Test CSV with quoted values containing delimiters."""
        csv_data = """name,description,price
"Product A","High quality, durable",100
"Product B","Affordable, reliable",50"""

        result = tm.flatten(csv_data, name="products")
        assert len(result.main) == 2
        assert result.main[0]["name"] == "Product A"
        assert result.main[0]["description"] == "High quality, durable"
        assert result.main[0]["price"] == "100"

    def test_csv_with_nested_data(self):
        """Test CSV that contains nested data in fields."""
        csv_data = '''id,name,tags
1,"Product A","[""tag1"", ""tag2""]"
2,"Product B","[""tag3"", ""tag4""]"'''

        result = tm.flatten(csv_data, name="products")
        assert len(result.main) == 2
        assert result.main[0]["id"] == "1"
        assert result.main[0]["name"] == "Product A"
        # Tags should be kept as string since CSV doesn't support nested structures
        assert result.main[0]["tags"] == '["tag1", "tag2"]'

    def test_csv_with_special_characters(self):
        """Test CSV with special characters in headers."""
        csv_data = """Product Name,Unit Price ($),In Stock?
Widget A,19.99,Yes
Widget B,29.99,No"""

        result = tm.flatten(csv_data, name="inventory")
        assert len(result.main) == 2
        # Headers should be sanitized
        assert "Product_Name" in result.main[0] or "product_name" in result.main[0]
        assert "Unit_Price" in result.main[0] or "unit_price" in result.main[0]

    def test_csv_with_empty_values(self):
        """Test CSV with empty/null values."""
        csv_data = """name,age,city
John Doe,30,
Jane Smith,,Los Angeles
,25,Chicago"""

        # Test with default null handling (empty strings become None)
        result = tm.flatten(csv_data, name="people")
        assert len(result.main) == 3
        assert result.main[0]["name"] == "John Doe"
        assert result.main[0]["city"] is None  # Empty value converted to None
        assert result.main[1]["age"] is None  # Empty value converted to None
        assert result.main[2]["name"] is None  # Empty value converted to None

    def test_csv_with_type_preservation(self):
        """Test CSV with preserve_types option."""
        csv_data = """name,age,score,active
John,30,95.5,true
Jane,25,87.3,false"""

        # Without type preservation (default)
        result = tm.flatten(csv_data, name="users")
        assert result.main[0]["age"] == "30"  # String
        assert result.main[0]["score"] == "95.5"  # String
        assert result.main[0]["active"] == "true"  # String

        # With type preservation
        result = tm.flatten(csv_data, name="users", preserve_types=True)
        assert result.main[0]["age"] == 30  # Integer
        assert result.main[0]["score"] == 95.5  # Float
        assert result.main[0]["active"] is True  # Boolean

    def test_csv_string_vs_json_string_detection(self):
        """Test that CSV strings are distinguished from JSON strings."""
        # JSON string should be parsed as JSON
        json_data = '{"name": "John", "age": 30}'
        result = tm.flatten(json_data, name="person")
        assert len(result.main) == 1
        assert result.main[0]["name"] == "John"
        assert result.main[0]["age"] == "30"  # Converted to string by default

        # CSV string should be parsed as CSV
        csv_data = "name,age\nJohn,30"
        result = tm.flatten(csv_data, name="person")
        assert len(result.main) == 1
        assert result.main[0]["name"] == "John"
        assert result.main[0]["age"] == "30"

    def test_csv_bytes_input(self):
        """Test processing CSV data as bytes."""
        csv_data = b"name,age,city\nJohn,30,NYC\nJane,25,LA"

        result = tm.flatten(csv_data, name="people")
        assert len(result.main) == 2
        assert result.main[0]["name"] == "John"
        assert result.main[0]["age"] == "30"

    def test_csv_with_unicode(self):
        """Test CSV with Unicode characters."""
        csv_data = """name,city,notes
José García,São Paulo,Café owner
Marie Müller,München,Geschäftsführerin
李明,北京,工程师"""

        result = tm.flatten(csv_data, name="international")
        assert len(result.main) == 3
        assert result.main[0]["name"] == "José García"
        assert result.main[1]["city"] == "München"
        assert result.main[2]["name"] == "李明"

    def test_large_csv_string(self):
        """Test processing a larger CSV string."""
        # Generate a CSV with 100 rows
        csv_lines = ["id,name,email,status"]
        for i in range(100):
            csv_lines.append(f"{i},User{i},user{i}@example.com,active")

        csv_data = "\n".join(csv_lines)

        result = tm.flatten(csv_data, name="users", batch_size=20)
        assert len(result.main) == 100
        assert result.main[0]["id"] == "0"
        assert result.main[99]["id"] == "99"
        assert result.main[50]["email"] == "user50@example.com"

    def test_malformed_csv_handling(self):
        """Test handling of CSV data with some malformed rows."""
        # CSV with some recoverable issues (extra commas, missing values)
        csv_data = """name,age,city
John,30,NYC
Jane,25,
Bob,35,Chicago"""

        # Should handle missing values gracefully
        result = tm.flatten(csv_data, name="people", errors="skip")
        # All rows should be processed (missing values become None)
        assert len(result.main) == 3
        assert result.main[0]["name"] == "John"
        assert result.main[1]["name"] == "Jane"
        assert result.main[1]["city"] is None  # Missing city becomes None
        assert result.main[2]["name"] == "Bob"

    def test_csv_string_with_arrays_option(self):
        """Test CSV processing with different array handling options."""
        csv_data = """id,name,tags
1,Product A,"tag1,tag2,tag3"
2,Product B,"tag4,tag5\""""

        # Default behavior - arrays as separate
        result = tm.flatten(csv_data, name="products", arrays="separate")
        assert len(result.main) == 2
        # Tags should remain as string in CSV since it can't have true arrays
        assert result.main[0]["tags"] == "tag1,tag2,tag3"

        # Inline arrays
        result = tm.flatten(csv_data, name="products", arrays="inline")
        assert len(result.main) == 2
        assert result.main[0]["tags"] == "tag1,tag2,tag3"
