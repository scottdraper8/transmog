"""
Utilities for unit tests.
"""

import os
from typing import Any, Union, BinaryIO


class WriterMixin:
    """Mixin to help writer classes implement the new protocol interface."""

    def write(self, data: Any, destination: Union[str, BinaryIO], **options) -> Any:
        """
        Implement the WriterProtocol's write method.

        Args:
            data: Data to write
            destination: Path or file-like object
            **options: Format-specific options

        Returns:
            Path to the written file or other format-specific result
        """
        if isinstance(destination, str):
            # Ensure the directory exists
            os.makedirs(os.path.dirname(destination) or ".", exist_ok=True)
            # Call write_table with the destination path
            return self.write_table(data, destination, **options)
        else:
            # Call write_table with the file object
            return self.write_table(data, destination, **options)
