"""
DAQ Job configuration template generator.

Uses msgspec's built-in JSON schema generation for DAQ job configurations.
"""

import re

import msgspec

from enrgdaq.daq.base import DAQJob
from enrgdaq.daq.types import get_all_daq_job_types


def _get_job_label(job_name: str) -> str:
    """Generate human-readable label from class name.

    e.g., "DAQJobTest" -> "Test", "DAQJobStoreCSV" -> "Store CSV", "DAQJobStoreMySQL" -> "Store MySQL"
    """
    label = job_name
    if label.startswith("DAQJob"):
        label = label[6:]  # Remove "DAQJob" prefix
    # Add spaces before capital letters, but keep acronyms together
    label = re.sub(r"(?<!^)(?=[A-Z][a-z])|(?<=[a-z])(?=[A-Z])", " ", label)
    return label


def _parse_class_docstring(cls: type) -> str:
    """Extract the first line of a class's docstring as description."""
    if cls.__doc__:
        for line in cls.__doc__.split("\n"):
            line = line.strip()
            if line:
                return line
    return f"Configuration for {cls.__name__}"


def get_daq_job_config_templates() -> dict[str, dict]:
    """
    Generate DAQ job configuration templates using msgspec's JSON schema generation.

    Returns a dictionary of JSON schemas for each DAQ job type.
    """
    templates: dict[str, dict] = {}

    for daq_job_class in get_all_daq_job_types():
        # Skip base DAQJob class
        if daq_job_class is DAQJob:
            continue

        # Get the config type for this job
        config_type = getattr(daq_job_class, "config_type", None)
        if config_type is None:
            continue

        job_name = daq_job_class.__name__

        # Generate JSON schema using msgspec
        # Some config types may have unsupported types, skip those
        try:
            schema = msgspec.json.schema(config_type)
        except TypeError:
            # Skip configs with unsupported types
            continue

        # Add our custom metadata
        schema["type_key"] = job_name
        schema["label"] = _get_job_label(job_name)
        # Use job class docstring for description (more relevant than config docstring)
        schema["description"] = _parse_class_docstring(daq_job_class)

        templates[job_name] = schema

    return templates
