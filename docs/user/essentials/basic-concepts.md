# Basic Concepts

This document explains the core concepts of Transmog and their interactions.

## What is Transmog?

Transmog is a Python library for transforming nested JSON data into flat, structured formats. It performs
the following functions:

- Converting complex nested structures into simpler flat ones
- Maintaining relationships between parent and child entities
- Generating IDs for records
- Supporting multiple output formats

## Core Components

### Flattener

The flattener converts nested JSON structures into flat dictionaries by:

- Flattening nested objects using path notation
- Extracting arrays into separate tables
- Maintaining parent-child relationships through IDs

### Processor

The processor orchestrates the transformation process through:

- Reading input data from various sources
- Applying flattening transformations
- Managing memory usage during processing
- Handling errors according to the configured strategy
- Outputting data in the requested format

### Naming System

The naming system controls identifier creation through:

- Managing table names derived from the JSON structure
- Special handling for deeply nested structures
- Implementing different naming conventions (snake_case, camelCase)

### Output Formats

Transmog supports multiple output formats:

- Memory structures (dictionaries, objects)
- File formats (JSON, CSV, Parquet)
- In-memory serialized formats (bytes)

## Processing Flow

The processing flow consists of the following steps:

1. Input data is read from a source (file, memory, stream)
2. The processor applies the flattening transformation
3. Arrays are extracted into separate tables with relationships
4. IDs are generated for each record
5. Data is converted to the requested output format

## Configuration System

The configuration system is based on the `TransmogConfig` class which:

- Controls all aspects of processing
- Includes pre-configured modes (memory_optimized, performance_optimized)
- Implements a fluent API for configuration

## Error Handling

Multiple error handling strategies are available:

- Strict - raises errors on any problem
- Skip-and-log - skips problematic records
- Partial recovery - extracts valid portions of records with errors
