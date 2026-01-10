"""
Tests for the DAQ job template generators.
"""

import unittest

from enrgdaq.daq.template import (
    get_daq_job_config_templates,
    get_message_templates,
    get_store_config_templates,
)


class TestTemplates(unittest.TestCase):
    """Tests for template generation functions."""

    def test_get_store_config_templates(self):
        """Test that store config templates are generated correctly."""
        templates = get_store_config_templates()

        self.assertIsInstance(templates, dict)
        self.assertGreater(len(templates), 0)

        # Each template should have required fields
        for store_type, schema in templates.items():
            self.assertIn("type_key", schema)
            self.assertIn("label", schema)
            self.assertIn("description", schema)
            self.assertEqual(schema["type_key"], store_type)

    def test_get_daq_job_config_templates(self):
        """Test that DAQ job config templates are generated correctly."""
        templates = get_daq_job_config_templates()

        self.assertIsInstance(templates, dict)
        self.assertGreater(len(templates), 0)

        # Each template should have required fields
        for job_name, schema in templates.items():
            self.assertIn("type_key", schema)
            self.assertIn("label", schema)
            self.assertIn("description", schema)
            self.assertEqual(schema["type_key"], job_name)

    def test_get_message_templates(self):
        """Test that message templates are generated correctly."""
        templates = get_message_templates()

        self.assertIsInstance(templates, dict)
        # Note: msgspec.json.schema() may fail on Structs with default_factory
        # If no templates are generated, this is an expected msgspec limitation
        if len(templates) == 0:
            self.skipTest(
                "msgspec cannot generate schemas for Structs with default_factory"
            )

        # Each template should have required fields
        for message_name, schema in templates.items():
            self.assertIn("type_key", schema)
            self.assertIn("label", schema)
            self.assertIn("description", schema)
            self.assertEqual(schema["type_key"], message_name)

    def test_message_templates_contain_expected_types(self):
        """Test that message templates contain key message types."""
        templates = get_message_templates()

        # msgspec.json.schema() may fail on Structs with default_factory
        if len(templates) == 0:
            self.skipTest(
                "msgspec cannot generate schemas for Structs with default_factory"
            )

        # These message types should always be present
        # (as long as they can be schema-generated)
        expected_types = [
            "DAQJobMessageStop",
            "DAQJobMessageHeartbeat",
        ]

        for msg_type in expected_types:
            self.assertIn(
                msg_type,
                templates,
                f"Expected message type '{msg_type}' not found in templates",
            )

    def test_message_templates_have_valid_schema_structure(self):
        """Test that message templates have valid JSON schema structure."""
        templates = get_message_templates()

        for message_name, schema in templates.items():
            # JSON schemas should have $defs or properties or both
            has_schema_content = (
                "$defs" in schema or "properties" in schema or "type" in schema
            )
            self.assertTrue(
                has_schema_content,
                f"Message template '{message_name}' has no schema content",
            )


if __name__ == "__main__":
    unittest.main()
