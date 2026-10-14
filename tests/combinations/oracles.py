"""Independent payload model for the JSON operations in the covering domain.

Metadata is checked separately. This model follows the documented array
operations, not writer inference or production helper functions.
"""

from __future__ import annotations

import json
from collections import defaultdict


def expected_tables(data, mode, include_nulls, stringify_values):
    """Project source records to parent and child payload tables."""
    tables = defaultdict(list)

    def scalar(value):
        return (
            str(value)
            if stringify_values and value is not None and value != ""
            else value
        )

    def project(record, prefix=()):
        out = {}
        for key, value in record.items():
            if key in ("_id", "_parent_id", "_timestamp"):
                continue
            path = (*prefix, key)
            column = "_".join(path)
            if isinstance(value, dict):
                out.update(project(value, path))
            elif isinstance(value, list):
                if not value or mode == "skip":
                    continue
                if mode == "inline":
                    out[column] = json.dumps(value, ensure_ascii=False)
                elif mode == "smart" and not any(
                    isinstance(item, dict) for item in value
                ):
                    out[column] = [scalar(item) for item in value]
                else:
                    name = "entity_" + column
                    for item in value:
                        if item is None or item == "":
                            if not include_nulls:
                                continue
                        if isinstance(item, dict):
                            if item:
                                tables[name].append(project(item))
                        else:
                            tables[name].append({"value": scalar(item)})
            elif value is None or value == "":
                if include_nulls:
                    out[column] = None
            else:
                out[column] = scalar(value)
        return out

    records = data if isinstance(data, list) else [data]
    tables["entity"] = [project(record) for record in records]
    return dict(tables)


def wire_tables(tables, output_format):
    """Apply only declared wire encodings and fill absent table columns."""
    encoded = {}
    for name, rows in tables.items():
        fields = set().union(*(row.keys() for row in rows))
        projected = []
        for row in rows:
            item = {}
            for field in fields:
                value = row.get(field)
                if output_format == "csv":
                    value = (
                        ""
                        if value is None
                        else json.dumps(value, ensure_ascii=False)
                        if isinstance(value, list)
                        else str(value)
                    )
                elif output_format == "avro" and isinstance(value, list):
                    value = json.dumps(value, ensure_ascii=False)
                item[field] = value
            projected.append(item)
        encoded[name] = projected
    return encoded
