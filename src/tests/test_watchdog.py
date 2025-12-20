"""Tests for the Watchdog class."""

import time
import unittest
from unittest.mock import MagicMock

from enrgdaq.utils.watchdog import Watchdog


class TestWatchdog(unittest.TestCase):
    """Tests for the Watchdog class."""

    def test_init_with_timeout(self):
        """Test watchdog initialization with timeout."""
        watchdog = Watchdog(timeout_seconds=30.0)
        self.assertEqual(watchdog.timeout_seconds, 30.0)
        self.assertTrue(watchdog.is_enabled)
        self.assertFalse(watchdog.force_exit)
        self.assertFalse(watchdog.is_triggered())
        self.assertIsNone(watchdog._timer)

    def test_init_with_force_exit(self):
        """Test watchdog initialization with force_exit enabled."""
        watchdog = Watchdog(timeout_seconds=30.0, force_exit=True)
        self.assertEqual(watchdog.timeout_seconds, 30.0)
        self.assertTrue(watchdog.is_enabled)
        self.assertTrue(watchdog.force_exit)

    def test_init_disabled(self):
        """Test watchdog initialization with zero timeout (disabled)."""
        watchdog = Watchdog(timeout_seconds=0)
        self.assertEqual(watchdog.timeout_seconds, 0)
        self.assertFalse(watchdog.is_enabled)
        self.assertFalse(watchdog.is_triggered())

    def test_init_negative_timeout(self):
        """Test watchdog with negative timeout is disabled."""
        watchdog = Watchdog(timeout_seconds=-5.0)
        self.assertFalse(watchdog.is_enabled)

    def test_reset_creates_timer(self):
        """Test that reset creates a timer."""
        watchdog = Watchdog(timeout_seconds=60.0)
        watchdog.reset()

        self.assertIsNotNone(watchdog._timer)
        self.assertTrue(watchdog._timer.daemon)

        # Clean up
        watchdog.stop()

    def test_reset_disabled_no_timer(self):
        """Test that reset does nothing when disabled."""
        watchdog = Watchdog(timeout_seconds=0)
        watchdog.reset()

        self.assertIsNone(watchdog._timer)

    def test_stop_cancels_timer(self):
        """Test that stop cancels the timer."""
        watchdog = Watchdog(timeout_seconds=60.0)
        watchdog.reset()

        self.assertIsNotNone(watchdog._timer)

        watchdog.stop()
        self.assertIsNone(watchdog._timer)

    def test_stop_when_no_timer(self):
        """Test that stop works when no timer is set."""
        watchdog = Watchdog(timeout_seconds=60.0)
        # Should not raise
        watchdog.stop()
        self.assertIsNone(watchdog._timer)

    def test_is_triggered_initially_false(self):
        """Test that is_triggered is False initially."""
        watchdog = Watchdog(timeout_seconds=60.0)
        self.assertFalse(watchdog.is_triggered())

    def test_on_timeout_sets_triggered(self):
        """Test that _on_timeout sets the triggered event."""
        watchdog = Watchdog(timeout_seconds=60.0)
        self.assertFalse(watchdog.is_triggered())

        watchdog._on_timeout()

        self.assertTrue(watchdog.is_triggered())

    def test_reset_replaces_timer(self):
        """Test that resetting replaces the existing timer."""
        watchdog = Watchdog(timeout_seconds=60.0)
        watchdog.reset()

        first_timer = watchdog._timer

        watchdog.reset()

        second_timer = watchdog._timer

        self.assertIsNot(first_timer, second_timer)

        # Clean up
        watchdog.stop()

    def test_actual_timeout(self):
        """Test that the watchdog actually triggers after timeout."""
        watchdog = Watchdog(timeout_seconds=0.1)  # 100ms
        watchdog.reset()

        self.assertFalse(watchdog.is_triggered())

        # Wait for timeout
        time.sleep(0.2)

        self.assertTrue(watchdog.is_triggered())

    def test_reset_prevents_timeout(self):
        """Test that resetting before timeout prevents triggering."""
        watchdog = Watchdog(timeout_seconds=0.2)  # 200ms
        watchdog.reset()

        # Wait 100ms, then reset
        time.sleep(0.1)
        self.assertFalse(watchdog.is_triggered())
        watchdog.reset()

        # Wait another 100ms
        time.sleep(0.1)
        self.assertFalse(watchdog.is_triggered())

        # Clean up
        watchdog.stop()

    def test_custom_logger(self):
        """Test that custom logger is used."""
        mock_logger = MagicMock()
        watchdog = Watchdog(timeout_seconds=60.0, logger=mock_logger)

        watchdog._on_timeout()

        mock_logger.error.assert_called_once()
        self.assertIn("Watchdog timeout", mock_logger.error.call_args[0][0])


if __name__ == "__main__":
    unittest.main()
