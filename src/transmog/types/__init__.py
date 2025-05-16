"""Type definitions for Transmog package.

This module contains interface definitions and type aliases to avoid circular
    dependencies.
"""

# Import specific types from each module rather than using * imports
from .base import ArrayDict, FlatDict, JsonDict
from .io_types import StreamingWriterProtocol, WriterProtocol, WriterRegistryProtocol
from .processing_types import FlattenMode, ProcessingStrategyProtocol
from .result_types import ConversionModeType, ResultInterface

__all__ = [
    # Base types
    "JsonDict",
    "FlatDict",
    "ArrayDict",
    # IO interfaces
    "WriterProtocol",
    "WriterRegistryProtocol",
    "StreamingWriterProtocol",
    # Result interfaces
    "ResultInterface",
    "ConversionModeType",
    # Processing interfaces
    "ProcessingStrategyProtocol",
    "FlattenMode",
]
