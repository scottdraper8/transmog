# Configuration API Reference

> **User Guide**: For usage guidance and examples, see the [Configuration Guide](../user/essentials/configuration.md).

This document provides a reference for the configuration classes in Transmog.

## TransmogConfig

`TransmogConfig` is the main configuration class that aggregates all settings for Transmog.

```python
class TransmogConfig:
    """Complete configuration for Transmog processing."""

    # Fields
    naming: NamingConfig
    processing: ProcessingConfig
    metadata: MetadataConfig
    error_handling: ErrorHandlingConfig
```

### Class Methods

#### `default()`

Create a default configuration.

```python
@classmethod
def default(cls) -> "TransmogConfig":
    """Create a default configuration."""
    return cls()
```

#### `memory_optimized()`

Create a memory-optimized configuration.

```python
@classmethod
def memory_optimized(cls) -> "TransmogConfig":
    """Create a memory-optimized configuration."""
    return cls(
        processing=ProcessingConfig(
            processing_mode=ProcessingMode.LOW_MEMORY,
            batch_size=100,
            path_parts_optimization=True,
        )
    )
```

#### `performance_optimized()`

Create a performance-optimized configuration.

```python
@classmethod
def performance_optimized(cls) -> "TransmogConfig":
    """Create a performance-optimized configuration."""
    return cls(
        processing=ProcessingConfig(
            processing_mode=ProcessingMode.HIGH_PERFORMANCE,
            batch_size=10000,
            path_parts_optimization=True,
        )
    )
```

#### `with_deterministic_ids(id_fields)`

Create a configuration with deterministic ID generation.

```python
@classmethod
def with_deterministic_ids(cls, id_fields: Union[str, Dict[str, str]]) -> "TransmogConfig":
    """
    Create a config with deterministic ID generation enabled.

    Args:
        id_fields: Field name or dictionary mapping paths to field names for deterministic IDs

    Returns:
        TransmogConfig with deterministic ID generation enabled
    """
    return cls(metadata=MetadataConfig(default_id_field=id_fields))
```

#### `with_custom_id_generation(strategy)`

Create a configuration with custom ID generation.

```python
@classmethod
def with_custom_id_generation(
    cls, strategy: Callable[[Dict[str, Any]], str]
) -> "TransmogConfig":
    """
    Create a configuration with custom ID generation.

    Args:
        strategy: Function that takes a record and returns a string ID

    Returns:
        TransmogConfig: New configuration with custom ID generation
    """
    return cls(metadata=MetadataConfig(id_generation_strategy=strategy))
```

### Instance Methods

#### `with_naming(**kwargs)`

Create a new configuration with updated naming settings.

```python
def with_naming(self, **kwargs) -> "TransmogConfig":
    """
    Create a new configuration with updated naming settings.

    Args:
        **kwargs: Keyword arguments to update in the naming configuration

    Returns:
        TransmogConfig: New configuration with updated naming settings
    """
    return TransmogConfig(
        naming=NamingConfig(**{**self.naming.__dict__, **kwargs}),
        processing=self.processing,
        metadata=self.metadata,
        error_handling=self.error_handling,
    )
```

#### `with_processing(**kwargs)`

Create a new configuration with updated processing settings.

```python
def with_processing(self, **kwargs) -> "TransmogConfig":
    """
    Create a new configuration with updated processing settings.

    Args:
        **kwargs: Keyword arguments to update in the processing configuration

    Returns:
        TransmogConfig: New configuration with updated processing settings
    """
    return TransmogConfig(
        naming=self.naming,
        processing=ProcessingConfig(**{**self.processing.__dict__, **kwargs}),
        metadata=self.metadata,
        error_handling=self.error_handling,
    )
```

#### `with_metadata(**kwargs)`

Create a new configuration with updated metadata settings.

```python
def with_metadata(self, **kwargs) -> "TransmogConfig":
    """
    Create a new configuration with updated metadata settings.

    Args:
        **kwargs: Keyword arguments to update in the metadata configuration

    Returns:
        TransmogConfig: New configuration with updated metadata settings
    """
    return TransmogConfig(
        naming=self.naming,
        processing=self.processing,
        metadata=MetadataConfig(**{**self.metadata.__dict__, **kwargs}),
        error_handling=self.error_handling,
    )
```

#### `with_error_handling(**kwargs)`

Create a new configuration with updated error handling settings.

```python
def with_error_handling(self, **kwargs) -> "TransmogConfig":
    """
    Create a new configuration with updated error handling settings.

    Args:
        **kwargs: Keyword arguments to update in the error handling configuration

    Returns:
        TransmogConfig: New configuration with updated error handling settings
    """
    return TransmogConfig(
        naming=self.naming,
        processing=self.processing,
        metadata=self.metadata,
        error_handling=ErrorHandlingConfig(
            **{**self.error_handling.__dict__, **kwargs}
        ),
    )
```

## NamingConfig

Configuration for naming conventions and abbreviations.

```python
@dataclass
class NamingConfig:
    """Configuration for naming conventions and abbreviations."""

    separator: str = "_"
    abbreviate_table_names: bool = True
    abbreviate_field_names: bool = True
    max_table_component_length: Optional[int] = None
    max_field_component_length: Optional[int] = None
    preserve_leaf_component: bool = True
    custom_abbreviations: Dict[str, str] = field(default_factory=dict)
```

### Parameters

- **separator** (`str`, default: `"_"`): The separator to use between path components.
- **abbreviate_table_names** (`bool`, default: `True`): Whether to abbreviate table names.
- **abbreviate_field_names** (`bool`, default: `True`): Whether to abbreviate field names.
- **max_table_component_length** (`Optional[int]`, default: `None`): Maximum length for table name components.
- **max_field_component_length** (`Optional[int]`, default: `None`): Maximum length for field name components.
- **preserve_leaf_component** (`bool`, default: `True`): Whether to preserve the leaf component in full.
- **custom_abbreviations** (`Dict[str, str]`, default: `{}`): Dictionary of custom abbreviations.

## ProcessingConfig

Configuration for data processing options.

```python
@dataclass
class ProcessingConfig:
    """Configuration for data processing options."""

    cast_to_string: bool = True
    include_empty: bool = False
    skip_null: bool = True
    max_nesting_depth: Optional[int] = None
    max_depth: int = 100  # Maximum recursion depth
    path_parts_optimization: bool = True
    visit_arrays: bool = True
    batch_size: int = 1000
    processing_mode: ProcessingMode = ProcessingMode.STANDARD
```

### Parameters

- **cast_to_string** (`bool`, default: `True`): Whether to cast all values to strings.
- **include_empty** (`bool`, default: `False`): Whether to include empty values.
- **skip_null** (`bool`, default: `True`): Whether to skip null values.
- **max_nesting_depth** (`Optional[int]`, default: `None`): Maximum nesting depth (None for unlimited).
- **max_depth** (`int`, default: `100`): Maximum recursion depth for nested structures.
- **path_parts_optimization** (`bool`, default: `True`): Whether to optimize path handling.
- **visit_arrays** (`bool`, default: `True`): Whether to process arrays as separate tables.
- **batch_size** (`int`, default: `1000`): Batch size for processing.
- **processing_mode** (`ProcessingMode`, default: `ProcessingMode.STANDARD`): Processing mode.

## MetadataConfig

Configuration for metadata generation.

```python
@dataclass
class MetadataConfig:
    """
    Configuration for metadata generation.

    This class configures how metadata fields like IDs and timestamps
    are generated during processing.
    """

    # Generation options
    default_id_field: Optional[Union[str, Dict[str, str]]] = None
    parent_field: str = "__parent_extract_id"
    time_field: str = "__extract_datetime"
    deterministic_id_fields: Dict[str, str] = field(default_factory=dict)
    id_generation_strategy: Optional[Callable[[Dict[str, Any]], str]] = None
```

### Parameters

- **default_id_field** (`Optional[Union[str, Dict[str, str]]]`, default: `None`): Field name or dictionary mapping
  paths to field names used for deterministic ID generation.
- **parent_field** (`str`, default: `"__parent_extract_id"`): Field name for parent IDs.
- **time_field** (`str`, default: `"__extract_datetime"`): Field name for timestamps.
- **deterministic_id_fields** (`Dict[str, str]`, default: `{}`): Dictionary mapping table paths to field names for
  deterministic ID generation.
- **id_generation_strategy** (`Optional[Callable[[Dict[str, Any]], str]]`, default: `None`): Custom function for ID generation.

## ErrorHandlingConfig

Configuration for error handling and recovery.

```python
@dataclass
class ErrorHandlingConfig:
    """Configuration for error handling and recovery."""

    allow_malformed_data: bool = False
    recovery_strategy: str = "strict"  # "strict", "skip", "partial"
    max_retries: int = 3
    error_log_path: Optional[str] = None
```

### Parameters

- **allow_malformed_data** (`bool`, default: `False`): Whether to allow malformed data.
- **recovery_strategy** (`str`, default: `"strict"`): Recovery strategy to use ("strict", "skip",
  or "partial").
- **max_retries** (`int`, default: `3`): Maximum number of retry attempts.
- **error_log_path** (`Optional[str]`, default: `None`): Path to write error logs to (None for no logging).

## ProcessingMode

Enum for processing modes determining memory/performance tradeoff.

```python
class ProcessingMode(Enum):
    """Processing modes determining memory/performance tradeoff."""

    STANDARD = auto()  # Default mode
    LOW_MEMORY = auto()  # Optimize for memory usage
    HIGH_PERFORMANCE = auto()  # Optimize for performance
```

### Values

- **STANDARD**: Default mode balancing memory usage and performance.
- **LOW_MEMORY**: Optimize for memory usage with lower memory footprint.
- **HIGH_PERFORMANCE**: Optimize for performance with higher memory usage.

## ConversionMode

Enum for conversion modes controlling how data is converted and managed in memory when generating output.

```python
class ConversionMode(Enum):
    """Conversion mode for ProcessingResult."""

    EAGER = "eager"  # Convert immediately, keep all data in memory
    LAZY = "lazy"  # Convert only when needed
    MEMORY_EFFICIENT = "memory_efficient"  # Discard intermediate data after conversion
```

### Values

- **EAGER**: Converts data immediately and keeps all formats in memory.
- **LAZY**: Converts data only when needed.
- **MEMORY_EFFICIENT**: Minimizes memory usage by clearing intermediate data.

## Example Usage

```python
from transmog import TransmogConfig, Processor, ProcessingMode

# Create a custom configuration
config = (
    TransmogConfig.default()
    .with_naming(
        separator=".",
        abbreviate_table_names=False,
        max_table_component_length=30
    )
    .with_processing(
        cast_to_string=False,
        batch_size=500,
        processing_mode=ProcessingMode.LOW_MEMORY
    )
    .with_metadata(
        id_field="record_id",
        parent_field="parent_id",
        time_field="processed_at"
    )
    .with_error_handling(
        allow_malformed_data=True,
        recovery_strategy="skip",
        max_retries=5
    )
)

# Create a processor with this configuration
processor = Processor(config=config)

# Process data
result = processor.process(data, entity_name="records")
```
