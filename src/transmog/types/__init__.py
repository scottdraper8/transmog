"""Type definitions for Transmog package.

This module contains interface definitions and type aliases to avoid circular
    dependencies.
"""

# Import specific types from each module rather than using * imports
from .base import ArrayDict, ArrayMode, FlatDict, JsonDict
from .context import ProcessingContext
from .io_types import StreamingWriterProtocol, WriterProtocol
from .processing_types import FlattenMode
from .result_types import ConversionModeType, ResultInterface

__all__ = [
    # Base types
    "JsonDict",
    "FlatDict",
    "ArrayDict",
    "ArrayMode",
    # Context types
    "ProcessingContext",
    # IO interfaces
    "WriterProtocol",
    "StreamingWriterProtocol",
    # Result interfaces
    "ResultInterface",
    "ConversionModeType",
    # Processing interfaces
    "FlattenMode",
]
