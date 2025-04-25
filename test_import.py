#!/usr/bin/env python3
"""
Test script to verify that the transmog package imports work correctly.
"""

import transmog as tm
from transmog.core.flattener import flatten_json

print("Transmog version:", tm.__version__)

# Try a simple flattening operation
data = {
    "name": "test",
    "address": {"street": "123 Main St", "city": "Anytown"},
    "tags": ["tag1", "tag2"],
}

result = flatten_json(data)
print("\nFlattened JSON:")
for key, value in result.items():
    print(f"  {key}: {value}")
