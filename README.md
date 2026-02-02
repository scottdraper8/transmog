<div align="center">

# Transmog - Flatten Nested JSON to Tabular Formats

[![Transmog Version](https://img.shields.io/badge/transmog-2.0.1-ff79c6?logo=github&logoColor=white&labelColor=6272a4)](https://github.com/scottdraper8/transmog/releases)
[![Python 3.10+](https://img.shields.io/badge/Python-3.10+-ffb86c?logo=python&logoColor=white&labelColor=6272a4)](https://www.python.org/downloads/)
[![Poetry](https://img.shields.io/badge/Poetry-1.0+-f1fa8c?logo=poetry&logoColor=282a36&labelColor=6272a4)](https://python-poetry.org/)
[![pre-commit](https://img.shields.io/badge/pre--commit-6.0.0-50fa7b?logo=pre-commit&logoColor=282a36&labelColor=6272a4)](https://github.com/pre-commit/pre-commit)
[![License: MIT](https://img.shields.io/badge/License-MIT-8be9fd?logo=opensourceinitiative&logoColor=white&labelColor=6272a4)](LICENSE)

---

A configurable data flattening tool that transforms nested JSON data into
flat, tabular formats while preserving parent-child relationships.

---

</div>

## Installation

```bash
# Full install (CSV, Parquet, ORC, Avro output)
pip install transmog

# CSV only (no pyarrow, fastavro, or cramjam)
pip install transmog[minimal]
```

## Quick Start

```python
import transmog as tm

data = {"user": "Alice", "orders": [{"id": 101}, {"id": 102}]}
result = tm.flatten(data, name="users")

result.main                    # Main table
result.tables["users_orders"]  # Child tables
result.save("output.csv")      # Save to file
```

### In-Memory vs Streaming

1. **flatten(data, name, config)** — Flatten data in memory

    ```python
    result = tm.flatten("data.json", name="products")
    result = tm.flatten([{"id": 1}, {"id": 2}])
    result.save("output.parquet")
    ```

2. **flatten_stream(data, output_path, name, output_format)** — Stream directly to disk

    ```python
    tm.flatten_stream("large.jsonl", "output/", name="events", output_format="parquet")
    ```

## Configuration

```python
config = tm.TransmogConfig(
    # Array handling
    array_mode=tm.ArrayMode.SMART,   # SMART (default), SEPARATE, INLINE, SKIP

    # ID generation and metadata fields
    id_generation="random",          # random (default), natural, hash, or ["field1", "field2"]
    id_field="_id",                  # Field name for record IDs
    parent_field="_parent_id",       # Field name for parent references
    time_field="_timestamp",         # Field name for timestamps (None to disable)

    # Data transformation
    include_nulls=False,             # Include null/empty values in output
    stringify_values=False,          # Convert all leaf values to strings

    # Processing controls
    max_depth=100,                   # Maximum recursion depth
    batch_size=1000                  # Records per batch for streaming
)

result = tm.flatten(data, config=config)
```

### Array Modes

| Mode       | Behavior                                                        |
| ---------- | --------------------------------------------------------------- |
| `SMART`    | Preserve simple arrays, extract complex arrays to child tables  |
| `SEPARATE` | Extract all arrays to child tables                              |
| `INLINE`   | Serialize arrays as JSON strings                                |
| `SKIP`     | Omit arrays from output                                         |

### ID Generation

| Strategy          | Description                                        |
| ----------------- | -------------------------------------------------- |
| `random`          | Generate random UUID (default)                     |
| `natural`         | Use existing ID field from data                    |
| `hash`            | Deterministic hash of entire record                |
| `["field1", ...]` | Deterministic hash of specified fields             |

## Documentation

Full documentation: [scottdraper8.github.io/transmog](https://scottdraper8.github.io/transmog)

- [Getting Started](https://scottdraper8.github.io/transmog/getting_started.html)
- [Configuration](https://scottdraper8.github.io/transmog/configuration.html)
- [API Reference](https://scottdraper8.github.io/transmog/api.html)
- [Contributing](https://scottdraper8.github.io/transmog/contributing.html)

## License

MIT License - see [LICENSE](LICENSE) file for details.
