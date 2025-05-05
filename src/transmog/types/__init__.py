"""
Type definitions for Transmog package.

This module contains interface definitions and type aliases to avoid circular dependencies.
"""

from .base import *
from .io_types import *
from .result_types import *
from .processing_types import *

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
]
