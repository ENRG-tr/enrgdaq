"""
DAQ Job message template generator.

Uses msgspec's built-in JSON schema generation for DAQ job messages.
"""

import re

import msgspec

from enrgdaq.daq.models import (
    DAQJobMessage,
)


def _get_message_label(message_name: str) -> str:
    """Generate human-readable label from class name.

    e.g., "DAQJobMessageStop" -> "Stop", "DAQJobMessageStoreTabular" -> "Store Tabular"
    """
    label = message_name
    if label.startswith("DAQJobMessage"):
        label = label[13:]  # Remove "DAQJobMessage" prefix
    # Add spaces before capital letters, but keep acronyms together
    label = re.sub(r"(?<!^)(?=[A-Z][a-z])|(?<=[a-z])(?=[A-Z])", " ", label)
    return label or "Base Message"


def _parse_class_docstring(cls: type) -> str:
    """Extract the first line of a class's docstring as description."""
    if cls.__doc__:
        for line in cls.__doc__.split("\n"):
            line = line.strip()
            if line:
                return line
    return f"Message type: {cls.__name__}"


def get_message_templates() -> dict[str, dict]:
    """
    Generate DAQ job message templates using msgspec's JSON schema generation.

    Returns a dictionary of JSON schemas for each message type that can be
    sent to DAQ jobs via CNC.
    """
    # List of message types that can be sent to DAQ jobs
    # These are the types that the send_message handler can decode and route
    message_types = DAQJobMessage.__subclasses__()

    templates: dict[str, dict] = {}

    for message_cls in message_types:
        message_name = message_cls.__name__

        # Generate JSON schema using msgspec
        try:
            schema = msgspec.json.schema(message_cls)
        except TypeError:
            # Skip messages with unsupported types
            continue

        # Add our custom metadata
        schema["type_key"] = message_name
        schema["label"] = _get_message_label(message_name)
        schema["description"] = _parse_class_docstring(message_cls)

        templates[message_name] = schema

    return templates
