"""Integration tests for natural ID discovery.

This module tests the integration of natural ID discovery with
the processor and other components.
"""

import pytest

import transmog as tm


class TestNaturalIdsIntegration:
    """Integration tests for natural ID discovery."""

    def test_default_natural_id_discovery(self):
        """Test default natural ID discovery behavior."""
        # Sample data with natural IDs
        data = {
            "id": "COMP-001",
            "name": "Test Company",
            "departments": [
                {
                    "id": "DEPT-001",
                    "name": "HR",
                    "employees": [
                        {"employee_id": "EMP-001", "name": "Alice"},
                        {"employee_id": "EMP-002", "name": "Bob"},
                    ],
                }
            ],
        }

        # Process with natural ID discovery
        processor = tm.Processor.with_natural_ids()
        result = processor.process(data, entity_name="company")

        # Check main table uses natural ID
        assert len(result.main_table) == 1
        assert result.main_table[0]["id"] == "COMP-001"
        assert "__transmog_id" not in result.main_table[0]

        # Check departments table uses natural ID
        dept_table = result.child_tables["company_departments"]
        assert len(dept_table) == 1
        assert dept_table[0]["id"] == "DEPT-001"
        assert "__transmog_id" not in dept_table[0]

        # Check employees table (should have transmog ID since employee_id is not in default patterns)
        emp_table = result.child_tables["company_departments_employees"]
        assert len(emp_table) == 2
        assert "__transmog_id" in emp_table[0]

    def test_custom_id_field_mapping(self):
        """Test custom ID field mapping for specific tables."""
        # Sample data with custom ID fields
        data = {
            "id": "COMP-001",
            "name": "Test Company",
            "departments": [
                {
                    "id": "DEPT-001",
                    "name": "HR",
                    "employees": [
                        {"employee_id": "EMP-001", "name": "Alice"},
                        {"employee_id": "EMP-002", "name": "Bob"},
                    ],
                }
            ],
            "products": [
                {"sku": "PROD-001", "name": "Product A"},
                {"sku": "PROD-002", "name": "Product B"},
            ],
        }

        # Custom ID field mapping
        id_mapping = {
            "company_departments_employees": "employee_id",
            "company_products": "sku",
            "*": "id",  # Default for all other tables
        }

        # Process with custom ID field mapping
        processor = tm.Processor.with_natural_ids(id_field_mapping=id_mapping)
        result = processor.process(data, entity_name="company")

        # Check main table uses natural ID
        assert len(result.main_table) == 1
        assert result.main_table[0]["id"] == "COMP-001"
        assert "__transmog_id" not in result.main_table[0]

        # Check employees table uses employee_id
        emp_table = result.child_tables["company_departments_employees"]
        assert len(emp_table) == 2
        assert emp_table[0]["employee_id"] == "EMP-001"
        assert "__transmog_id" not in emp_table[0]

        # Check products table uses sku
        prod_table = result.child_tables["company_products"]
        assert len(prod_table) == 2
        assert prod_table[0]["sku"] == "PROD-001"
        assert "__transmog_id" not in prod_table[0]

    def test_custom_id_field_patterns(self):
        """Test custom ID field patterns."""
        # Sample data with various ID fields
        data = {
            "id": "COMP-001",
            "name": "Test Company",
            "departments": [
                {
                    "dept_code": "DEPT-001",  # Non-standard ID field
                    "name": "HR",
                    "employees": [
                        {
                            "emp_num": "EMP-001",
                            "name": "Alice",
                        },  # Non-standard ID field
                        {"emp_num": "EMP-002", "name": "Bob"},
                    ],
                }
            ],
        }

        # Custom ID field patterns
        patterns = ["id", "dept_code", "emp_num"]

        # Process with custom ID field patterns
        processor = tm.Processor.with_natural_ids(id_field_patterns=patterns)
        result = processor.process(data, entity_name="company")

        # Check main table uses natural ID
        assert len(result.main_table) == 1
        assert result.main_table[0]["id"] == "COMP-001"
        assert "__transmog_id" not in result.main_table[0]

        # Check departments table uses dept_code
        dept_table = result.child_tables["company_departments"]
        assert len(dept_table) == 1
        assert dept_table[0]["dept_code"] == "DEPT-001"
        assert "__transmog_id" not in dept_table[0]

        # Check employees table uses emp_num
        emp_table = result.child_tables["company_departments_employees"]
        assert len(emp_table) == 2
        assert emp_table[0]["emp_num"] == "EMP-001"
        assert "__transmog_id" not in emp_table[0]

    def test_force_transmog_id(self):
        """Test forcing transmog ID generation."""
        # Sample data with natural IDs
        data = {
            "id": "COMP-001",
            "name": "Test Company",
            "departments": [
                {"id": "DEPT-001", "name": "HR"},
                {"id": "DEPT-002", "name": "Engineering"},
            ],
        }

        # Process with force_transmog_id=True
        processor = tm.Processor.with_natural_ids(
            id_field_patterns=["id"]
        ).with_metadata(force_transmog_id=True)
        result = processor.process(data, entity_name="company")

        # Check main table has transmog ID
        assert len(result.main_table) == 1
        assert result.main_table[0]["id"] == "COMP-001"  # Natural ID preserved
        assert "__transmog_id" in result.main_table[0]  # Transmog ID added

        # Check departments table has transmog ID
        dept_table = result.child_tables["company_departments"]
        assert len(dept_table) == 2
        assert dept_table[0]["id"] == "DEPT-001"  # Natural ID preserved
        assert "__transmog_id" in dept_table[0]  # Transmog ID added

    def test_mixed_id_fields(self):
        """Test processing records with mixed ID field presence."""
        # Sample data with mixed ID field presence
        data = {
            "id": "COMP-001",
            "name": "Test Company",
            "departments": [
                {"id": "DEPT-001", "name": "HR"},  # Has natural ID
                {"name": "Engineering"},  # No natural ID
            ],
        }

        # Process with natural ID discovery
        processor = tm.Processor.with_natural_ids()
        result = processor.process(data, entity_name="company")

        # Check departments table
        dept_table = result.child_tables["company_departments"]
        assert len(dept_table) == 2

        # First department should use natural ID
        assert dept_table[0]["id"] == "DEPT-001"
        assert "__transmog_id" not in dept_table[0]

        # Second department should have transmog ID
        assert "id" not in dept_table[1]
        assert "__transmog_id" in dept_table[1]

    def test_parent_child_relationships(self):
        """Test parent-child relationships with natural IDs."""
        # Sample data with natural IDs
        data = {
            "id": "COMP-001",
            "name": "Test Company",
            "departments": [
                {
                    "id": "DEPT-001",
                    "name": "HR",
                    "employees": [
                        {"employee_id": "EMP-001", "name": "Alice"},
                    ],
                }
            ],
        }

        # Custom ID field mapping
        id_mapping = {
            "company_departments_employees": "employee_id",
            "*": "id",
        }

        # Process with custom ID field mapping
        processor = tm.Processor.with_natural_ids(id_field_mapping=id_mapping)
        result = processor.process(data, entity_name="company")

        # Check parent-child relationships
        dept_table = result.child_tables["company_departments"]
        emp_table = result.child_tables["company_departments_employees"]

        # Department should have parent ID linking to company
        assert dept_table[0]["__parent_transmog_id"] == "COMP-001"

        # Employee should have parent ID linking to department
        assert emp_table[0]["__parent_transmog_id"] == "DEPT-001"
