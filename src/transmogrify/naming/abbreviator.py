"""
Abbreviation system for table and field names.

Provides functionality to abbreviate long table and field names
with configurable abbreviation strategies and dictionaries.
"""

import functools
import re
from typing import Dict, List, Optional, Set, Tuple

from ..config.settings import settings


def abbreviate_component(
    component: str,
    max_length: int = None,
    abbreviation_dict: Optional[Dict[str, str]] = None,
) -> str:
    """
    Abbreviate a single path component.

    Args:
        component: Path component to abbreviate
        max_length: Maximum length for the abbreviated component
        abbreviation_dict: Dictionary of common abbreviations

    Returns:
        Abbreviated component
    """
    # Use default from settings if max_length is None
    if max_length is None:
        max_length = settings.DEFAULT_MAX_FIELD_COMPONENT_LENGTH

    # If already short enough, return as is
    if len(str(component)) <= max_length:
        return str(component)

    # Check for known abbreviation
    if abbreviation_dict and isinstance(abbreviation_dict, dict):
        component_str = str(component).lower()
        if component_str in abbreviation_dict:
            return abbreviation_dict[component_str]

    # Basic abbreviation: truncate to max length
    return str(component)[:max_length]


@functools.lru_cache(maxsize=256)
def _abbreviate_table_name_cached(
    path: str,
    parent_entity: str,
    separator: str = "_",
    abbreviate_enabled: bool = True,
    max_component_length: int = None,
    preserve_leaf: bool = True,
) -> str:
    """Cached version of abbreviate_table_name without the dictionary parameter."""
    # Use default from settings if max_component_length is None
    if max_component_length is None:
        max_component_length = settings.DEFAULT_MAX_TABLE_COMPONENT_LENGTH

    # If abbreviation is disabled, just return full path
    if not abbreviate_enabled:
        return f"{parent_entity}{separator}{path}" if path != parent_entity else path

    # Split the path into components
    parts = path.split(separator)

    # For a single-part path, it's a direct child of the entity
    if len(parts) <= 1:
        return f"{parent_entity}{separator}{path}" if path != parent_entity else path

    # Process each component
    abbreviated_parts = []
    for i, part in enumerate(parts):
        # Check if this is the leaf component
        is_leaf = i == len(parts) - 1

        # Apply abbreviation rules
        if is_leaf and preserve_leaf:
            # Keep leaf component intact
            abbreviated_parts.append(part)
        else:
            # Abbreviate middle components - simple truncation for cached version
            if len(part) <= max_component_length:
                abbreviated_parts.append(part)
            else:
                abbreviated_parts.append(part[:max_component_length])

    # Join components into the final name
    if parent_entity in abbreviated_parts:
        # If parent entity is already in the path, don't duplicate it
        result = separator.join(abbreviated_parts)
    else:
        # Prepend parent entity if not already in the path
        result = f"{parent_entity}{separator}{separator.join(abbreviated_parts)}"

    return result


def abbreviate_table_name(
    path: str,
    parent_entity: str,
    separator: str = "_",
    abbreviate_enabled: bool = True,
    max_component_length: int = None,
    preserve_leaf: bool = True,
    abbreviation_dict: Optional[Dict[str, str]] = None,
) -> str:
    """
    Generate abbreviated table name from path.

    Args:
        path: Array path (e.g., "orders_items_details")
        parent_entity: Root entity name
        separator: Separator character for path components
        abbreviate_enabled: Whether abbreviation is enabled
        max_component_length: Maximum length for each path component
        preserve_leaf: Whether to preserve the leaf component
        abbreviation_dict: Dictionary of common abbreviations

    Returns:
        Abbreviated table name
    """
    # Use default from settings if max_component_length is None
    if max_component_length is None:
        max_component_length = settings.DEFAULT_MAX_TABLE_COMPONENT_LENGTH

    # Convert to string to be safe
    path = str(path)
    parent_entity = str(parent_entity)

    # If no dictionary, use the cached version
    if abbreviation_dict is None:
        return _abbreviate_table_name_cached(
            path,
            parent_entity,
            separator,
            abbreviate_enabled,
            max_component_length,
            preserve_leaf,
        )

    # If abbreviation is disabled, just return full path
    if not abbreviate_enabled:
        return f"{parent_entity}{separator}{path}" if path != parent_entity else path

    # Split the path into components
    parts = path.split(separator)

    # For a single-part path, it's a direct child of the entity
    if len(parts) <= 1:
        return f"{parent_entity}{separator}{path}" if path != parent_entity else path

    # Process each component
    abbreviated_parts = []
    for i, part in enumerate(parts):
        # Check if this is the leaf component
        is_leaf = i == len(parts) - 1

        # Apply abbreviation rules
        if is_leaf and preserve_leaf:
            # Keep leaf component intact
            abbreviated_parts.append(part)
        else:
            # Abbreviate middle components
            abbreviated_parts.append(
                abbreviate_component(
                    part,
                    max_length=max_component_length,
                    abbreviation_dict=abbreviation_dict,
                )
            )

    # Join components into the final name
    if parent_entity in abbreviated_parts:
        # If parent entity is already in the path, don't duplicate it
        result = separator.join(abbreviated_parts)
    else:
        # Prepend parent entity if not already in the path
        result = f"{parent_entity}{separator}{separator.join(abbreviated_parts)}"

    return result


@functools.lru_cache(maxsize=1024)
def _abbreviate_field_name_cached(
    field_path: str,
    separator: str = "_",
    abbreviate_enabled: bool = True,
    max_component_length: int = None,
    preserve_leaf: bool = True,
) -> str:
    """Cached version of abbreviate_field_name without the dictionary parameter."""
    # Use default from settings if max_component_length is None
    if max_component_length is None:
        max_component_length = settings.DEFAULT_MAX_FIELD_COMPONENT_LENGTH

    # If abbreviation is disabled, return the original path
    if not abbreviate_enabled:
        return field_path

    # Split the path into components
    parts = field_path.split(separator)

    # If single component or very short, return as is
    if len(parts) <= 1:
        return field_path

    # Process each component
    abbreviated_parts = []
    for i, part in enumerate(parts):
        # Check if this is the leaf component
        is_leaf = i == len(parts) - 1

        # Apply abbreviation rules
        if is_leaf and preserve_leaf:
            # Keep leaf component intact
            abbreviated_parts.append(part)
        else:
            # Abbreviate middle components - simple truncation for cached version
            if len(part) <= max_component_length:
                abbreviated_parts.append(part)
            else:
                abbreviated_parts.append(part[:max_component_length])

    # Join components into the final name
    return separator.join(abbreviated_parts)


def abbreviate_field_name(
    field_path: str,
    separator: str = "_",
    abbreviate_enabled: bool = True,
    max_component_length: int = None,
    preserve_leaf: bool = True,
    abbreviation_dict: Optional[Dict[str, str]] = None,
) -> str:
    """
    Abbreviate a field name based on its path.

    Args:
        field_path: Field path (e.g., "customer_billing_address_street")
        separator: Separator character
        abbreviate_enabled: Whether abbreviation is enabled
        max_component_length: Maximum length for each component
        preserve_leaf: Whether to preserve the leaf component
        abbreviation_dict: Dictionary of common abbreviations

    Returns:
        Abbreviated field name
    """
    # Use default from settings if max_component_length is None
    if max_component_length is None:
        max_component_length = settings.DEFAULT_MAX_FIELD_COMPONENT_LENGTH

    # Convert to string to be safe
    field_path = str(field_path)

    # If no dictionary, use the cached version
    if abbreviation_dict is None:
        return _abbreviate_field_name_cached(
            field_path,
            separator,
            abbreviate_enabled,
            max_component_length,
            preserve_leaf,
        )

    # If abbreviation is disabled, return the original path
    if not abbreviate_enabled:
        return field_path

    # Split the path into components
    parts = field_path.split(separator)

    # If single component or very short, return as is
    if len(parts) <= 1:
        return field_path

    # Process each component
    abbreviated_parts = []
    for i, part in enumerate(parts):
        # Check if this is the leaf component
        is_leaf = i == len(parts) - 1

        # Apply abbreviation rules
        if is_leaf and preserve_leaf:
            # Keep leaf component intact
            abbreviated_parts.append(part)
        else:
            # Abbreviate middle components
            abbreviated_parts.append(
                abbreviate_component(
                    part,
                    max_length=max_component_length,
                    abbreviation_dict=abbreviation_dict,
                )
            )

    # Join components into the final name
    return separator.join(abbreviated_parts)


def get_common_abbreviations() -> Dict[str, str]:
    """
    Get dictionary of common terminology abbreviations.

    Returns:
        Dictionary mapping full terms to abbreviations
    """
    return {
        # General terms
        "address": "addr",
        "administration": "admin",
        "alternative": "alt",
        "application": "app",
        "archive": "arch",
        "attachment": "attach",
        "attribute": "attr",
        "authentication": "auth",
        "authorization": "authz",
        "average": "avg",
        # Business terms
        "account": "acct",
        "billing": "bill",
        "business": "bus",
        "catalog": "cat",
        "category": "cat",
        "commercial": "comm",
        "company": "co",
        "configuration": "config",
        "customer": "cust",
        "department": "dept",
        "description": "desc",
        "document": "doc",
        "employee": "emp",
        "enterprise": "ent",
        "government": "govt",
        "information": "info",
        "international": "intl",
        "management": "mgmt",
        "manager": "mgr",
        "manufacturing": "mfg",
        "marketing": "mktg",
        "merchandise": "merch",
        "message": "msg",
        "miscellaneous": "misc",
        "notification": "notif",
        "operation": "op",
        "organization": "org",
        "parameter": "param",
        "payment": "pmt",
        "position": "pos",
        "preference": "pref",
        "product": "prod",
        "production": "prod",
        "professional": "prof",
        "profile": "prof",
        "program": "prog",
        "project": "proj",
        "property": "prop",
        "purchase": "purch",
        "reference": "ref",
        "registration": "reg",
        "relationship": "rel",
        "representative": "rep",
        "request": "req",
        "reservation": "resv",
        "resource": "res",
        "response": "resp",
        "schedule": "sched",
        "service": "svc",
        "shipping": "ship",
        "specification": "spec",
        "statistics": "stats",
        "subscription": "sub",
        "summary": "sum",
        "system": "sys",
        "technology": "tech",
        "telephone": "tel",
        "temperature": "temp",
        "temporary": "temp",
        "transaction": "txn",
        "transfer": "xfer",
        "utility": "util",
        "verification": "verif",
        # Technical terms
        "administrator": "admin",
        "analysis": "anlys",
        "application": "app",
        "architecture": "arch",
        "assignment": "asgn",
        "assistant": "asst",
        "association": "assoc",
        "attribute": "attr",
        "binary": "bin",
        "certificate": "cert",
        "column": "col",
        "command": "cmd",
        "communication": "comm",
        "component": "comp",
        "configuration": "cfg",
        "connection": "conn",
        "coordinate": "coord",
        "database": "db",
        "definition": "def",
        "dependency": "dep",
        "destination": "dest",
        "development": "dev",
        "directory": "dir",
        "document": "doc",
        "environment": "env",
        "equation": "eqn",
        "extension": "ext",
        "external": "ext",
        "frequency": "freq",
        "function": "func",
        "generation": "gen",
        "geography": "geo",
        "graphics": "gfx",
        "hexadecimal": "hex",
        "identifier": "id",
        "implementation": "impl",
        "index": "idx",
        "information": "info",
        "initialization": "init",
        "instance": "inst",
        "integration": "integ",
        "interface": "intf",
        "internal": "int",
        "language": "lang",
        "latitude": "lat",
        "length": "len",
        "library": "lib",
        "longitude": "long",
        "maximum": "max",
        "memory": "mem",
        "message": "msg",
        "minimum": "min",
        "network": "net",
        "object": "obj",
        "operation": "op",
        "optimization": "opt",
        "option": "opt",
        "package": "pkg",
        "parameter": "param",
        "partition": "part",
        "password": "pwd",
        "position": "pos",
        "preference": "pref",
        "presentation": "pres",
        "procedure": "proc",
        "processing": "proc",
        "processor": "proc",
        "production": "prod",
        "property": "prop",
        "protocol": "proto",
        "quantity": "qty",
        "reference": "ref",
        "registration": "reg",
        "repository": "repo",
        "request": "req",
        "resolution": "res",
        "resource": "res",
        "response": "resp",
        "sequence": "seq",
        "serial": "ser",
        "server": "srv",
        "service": "svc",
        "session": "sess",
        "specification": "spec",
        "statistics": "stat",
        "structure": "struct",
        "synchronization": "sync",
        "system": "sys",
        "table": "tbl",
        "template": "tmpl",
        "temporary": "temp",
        "timestamp": "ts",
        "transaction": "txn",
        "transformation": "xform",
        "translation": "xlate",
        "transmission": "xmit",
        "transport": "xport",
        "utility": "util",
        "validation": "val",
        "variable": "var",
        "version": "ver",
        # Location/Address terms
        "address": "addr",
        "apartment": "apt",
        "avenue": "ave",
        "boulevard": "blvd",
        "building": "bldg",
        "center": "ctr",
        "circle": "cir",
        "city": "cty",
        "country": "ctry",
        "county": "cnty",
        "court": "ct",
        "department": "dept",
        "district": "dist",
        "division": "div",
        "drive": "dr",
        "floor": "flr",
        "highway": "hwy",
        "latitude": "lat",
        "longitude": "lng",
        "mountain": "mtn",
        "number": "num",
        "office": "ofc",
        "parkway": "pkwy",
        "place": "pl",
        "post office": "po",
        "region": "rgn",
        "road": "rd",
        "room": "rm",
        "route": "rte",
        "square": "sq",
        "station": "sta",
        "street": "st",
        "suite": "ste",
        "territory": "terr",
        "township": "twp",
        "village": "vlg",
        "zipcode": "zip",
    }


def merge_abbreviation_dicts(
    default_dict: Dict[str, str], custom_dict: Dict[str, str]
) -> Dict[str, str]:
    """
    Merge default and custom abbreviation dictionaries.

    Args:
        default_dict: Default abbreviation dictionary
        custom_dict: Custom abbreviation dictionary to override defaults

    Returns:
        Merged dictionary
    """
    result = default_dict.copy()
    result.update(custom_dict)
    return result
