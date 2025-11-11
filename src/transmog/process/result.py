"""Processing result container."""

from transmog.types import JsonDict


class ProcessingResult:
    """Container for processing results including main and child tables."""

    main_table: list[JsonDict]
    child_tables: dict[str, list[JsonDict]]
    entity_name: str

    def __init__(
        self,
        main_table: list[JsonDict],
        child_tables: dict[str, list[JsonDict]],
        entity_name: str,
    ):
        """Initialize with main and child tables."""
        self.main_table = main_table
        self.child_tables = child_tables
        self.entity_name = entity_name

    def add_main_record(self, record: JsonDict) -> None:
        """Add a record to the main table."""
        self.main_table.append(record)

    def add_child_tables(self, tables: dict[str, list[JsonDict]]) -> None:
        """Add child tables to the result."""
        for table_name, records in tables.items():
            if table_name in self.child_tables:
                self.child_tables[table_name].extend(records)
            else:
                self.child_tables[table_name] = records

    def __repr__(self) -> str:
        """String representation of the result."""
        main_count = len(self.main_table)
        child_count = sum(len(records) for records in self.child_tables.values())
        return (
            f"ProcessingResult(entity='{self.entity_name}', "
            f"main_records={main_count}, child_records={child_count}, "
            f"child_tables={len(self.child_tables)})"
        )

    def all_tables(self) -> dict[str, list[JsonDict]]:
        """Gather all tables with the main table first."""
        tables: dict[str, list[JsonDict]] = {self.entity_name: self.main_table}
        tables.update(self.child_tables)
        return tables


__all__ = ["ProcessingResult"]
