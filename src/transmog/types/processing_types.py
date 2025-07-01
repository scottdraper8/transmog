"""Processing type interfaces for Transmog.

This module defines interfaces for processing strategies to break circular dependencies.
"""

from typing import Literal

# Type for flatten mode
FlattenMode = Literal["standard", "streaming"]
