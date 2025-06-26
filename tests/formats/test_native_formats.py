"""
Tests for native format outputs.

This module implements concrete tests for the format conversion interface.
"""

import pytest

from tests.interfaces.test_formats_interface import AbstractFormatConversionTest
from transmog import Processor
from transmog.config import TransmogConfig


class TestNativeFormats(AbstractFormatConversionTest):
    """Concrete tests for native format conversion."""

    @pytest.fixture
    def processor_factory(self):
        """Provide a processor factory function."""

        def create_processor(cast_to_string=True, visit_arrays=True):
            config = TransmogConfig.default().with_metadata(force_transmog_id=True)
            return Processor(config=config)

        return create_processor
