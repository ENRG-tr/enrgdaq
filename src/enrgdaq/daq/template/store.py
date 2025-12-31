"""
Store configuration template generator.

Uses msgspec's built-in JSON schema generation for store configurations.
"""

from typing import get_args, get_origin, get_type_hints

import msgspec

from enrgdaq.daq.store.models import DAQJobStoreConfig

# Store type labels
STORE_TYPE_LABELS = {
    "csv": "CSV Store",
    "raw": "Raw Store",
    "root": "ROOT Store",
    "hdf5": "HDF5 Store",
    "mysql": "MySQL Store",
    "redis": "Redis Store",
    "memory": "Memory Store",
}

# Store type descriptions
STORE_TYPE_DESCRIPTIONS = {
    "csv": "Store data in CSV format",
    "raw": "Store raw binary data",
    "root": "Store data in CERN ROOT format",
    "hdf5": "Store data in HDF5 format",
    "mysql": "Store data in MySQL database",
    "redis": "Store data in Redis",
    "memory": "In-memory storage (no persistence)",
}


def _schema_hook(tp: type) -> dict:
    """
    Custom schema hook to handle types that msgspec can't process natively.
    """
    # Handle type[...] annotations by returning a simple schema
    origin = get_origin(tp)
    if origin is type:
        return {"type": "string", "description": "Type reference (internal use)"}
    raise NotImplementedError(f"No schema for type: {tp}")


def get_store_config_templates() -> dict[str, dict]:
    """
    Generate store configuration templates using msgspec's JSON schema generation.

    Returns a dictionary of JSON schemas for each store type.
    """
    # Build store type to class mapping from DAQJobStoreConfig annotations
    try:
        resolved_hints = get_type_hints(DAQJobStoreConfig)
    except Exception:
        resolved_hints = {}

    store_type_to_class: dict[str, type] = {}
    for store_type, annotation in resolved_hints.items():
        # Get the actual type from the annotation (unwrap Optional)
        origin = get_origin(annotation)
        if origin is not None:
            args = get_args(annotation)
            non_none_args = [a for a in args if a is not type(None)]
            if non_none_args:
                config_class = non_none_args[0]
                store_type_to_class[store_type] = config_class

    templates: dict[str, dict] = {}

    for store_type, config_class in store_type_to_class.items():
        try:
            # Generate JSON schema using msgspec with custom hook for unsupported types
            schema = msgspec.json.schema(config_class, schema_hook=_schema_hook)
        except TypeError:
            # Skip types that can't be converted to schemas
            continue

        # Add our custom metadata
        schema["type_key"] = store_type
        schema["label"] = STORE_TYPE_LABELS.get(
            store_type, f"{store_type.upper()} Store"
        )
        schema["description"] = STORE_TYPE_DESCRIPTIONS.get(
            store_type, schema.get("description", f"Store data using {store_type}")
        )

        templates[store_type] = schema

    return templates
