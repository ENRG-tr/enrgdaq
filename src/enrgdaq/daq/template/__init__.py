"""
Template generation module for ENRGDAQ configurations.

Uses msgspec's built-in JSON schema generation to create schemas for:
- Store configurations (CSV, ROOT, Redis, etc.)
- DAQ job configurations (DAQJobTest, DAQJobHealthcheck, etc.)

Usage:
    from enrgdaq.daq.template import (
        get_store_config_templates,
        get_daq_job_config_templates,
    )

    # Get store config templates (JSON schemas)
    store_templates = get_store_config_templates()

    # Get DAQ job config templates (JSON schemas)
    job_templates = get_daq_job_config_templates()
"""

from enrgdaq.daq.template.config import get_daq_job_config_templates
from enrgdaq.daq.template.store import get_store_config_templates

__all__ = [
    "get_store_config_templates",
    "get_daq_job_config_templates",
]
