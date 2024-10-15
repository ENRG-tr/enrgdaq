import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from daq.alert.base import DAQAlertInfo, DAQAlertSeverity
from daq.jobs.healthcheck import (
    AlertCondition,
    DAQJobHealthcheck,
    DAQJobHealthcheckConfig,
    HealthcheckStatsItem,
)
from daq.jobs.test_job import DAQJobTest


class TestDAQJobHealthcheck(unittest.TestCase):
    def setUp(self):
        self.alert_info = DAQAlertInfo(
            message="Test Alert", severity=DAQAlertSeverity.INFO
        )
        self.healthcheck_item = HealthcheckStatsItem(
            alert_info=self.alert_info,
            daq_job_type="test",
            stats_key="message_out_stats",
            alert_if_interval_is=AlertCondition.UNSATISFIED,
            interval="5m",
        )
        self.config = DAQJobHealthcheckConfig(
            daq_job_type="test",
            healthcheck_stats=[self.healthcheck_item],
            enable_alerts_on_restart=False,
        )
        self.daq_job_healthcheck = DAQJobHealthcheck(self.config)

    @patch("daq.jobs.healthcheck.DAQJobHealthcheck.send_alert")
    def test_handle_checks_should_alert(self, mock_send_alert):
        mock_stats = MagicMock()
        mock_stats.message_out_stats.last_updated = datetime.now() - timedelta(
            minutes=10
        )
        self.daq_job_healthcheck._current_stats = {DAQJobTest: mock_stats}

        self.daq_job_healthcheck.handle_checks()

        mock_send_alert.assert_called_once_with(self.healthcheck_item)

    @patch("daq.jobs.healthcheck.DAQJobHealthcheck.send_alert")
    def test_handle_checks_should_not_alert(self, mock_send_alert):
        mock_stats = MagicMock()
        mock_stats.message_out_stats.last_updated = datetime.now()
        self.daq_job_healthcheck._current_stats = {DAQJobTest: mock_stats}

        self.daq_job_healthcheck.handle_checks()

        mock_send_alert.assert_not_called()

    @patch("daq.jobs.healthcheck.DAQJobHealthcheck.send_alert")
    def test_handle_checks_should_not_alert_twice_in_a_row(self, mock_send_alert):
        mock_stats = MagicMock()
        mock_stats.message_out_stats.last_updated = datetime.now() - timedelta(
            minutes=10
        )
        self.daq_job_healthcheck._current_stats = {DAQJobTest: mock_stats}

        # First check should trigger an alert
        self.daq_job_healthcheck.handle_checks()
        mock_send_alert.assert_called_once_with(self.healthcheck_item)

        # Reset mock to check for second call
        mock_send_alert.reset_mock()

        # Second check should not trigger an alert again
        self.daq_job_healthcheck.handle_checks()
        mock_send_alert.assert_not_called()

    @patch("daq.jobs.healthcheck.DAQJobHealthcheck.send_alert")
    def test_handle_checks_should_alert_then_not_alert_then_alert_again(
        self, mock_send_alert
    ):
        mock_stats = MagicMock()
        mock_stats.message_out_stats.last_updated = datetime.now() - timedelta(
            minutes=10
        )
        self.daq_job_healthcheck._current_stats = {DAQJobTest: mock_stats}

        # First check should trigger an alert
        self.daq_job_healthcheck.handle_checks()
        mock_send_alert.assert_called_once_with(self.healthcheck_item)

        # Reset mock to check for second call
        mock_send_alert.reset_mock()

        # Update stats to a recent time, should not trigger an alert
        mock_stats.message_out_stats.last_updated = datetime.now()
        self.daq_job_healthcheck.handle_checks()
        mock_send_alert.assert_not_called()

        # Update stats to an old time again, should trigger an alert
        mock_stats.message_out_stats.last_updated = datetime.now() - timedelta(
            minutes=10
        )
        self.daq_job_healthcheck.handle_checks()
        mock_send_alert.assert_called_once_with(self.healthcheck_item)

    def test_parse_interval(self):
        self.assertEqual(self.healthcheck_item.parse_interval(), timedelta(minutes=5))

    def test_parse_interval_invalid_format(self):
        self.healthcheck_item.interval = "5x"
        with self.assertRaises(ValueError):
            self.healthcheck_item.parse_interval()

    def test_parse_interval_null(self):
        self.healthcheck_item.interval = None
        with self.assertRaises(ValueError):
            self.healthcheck_item.parse_interval()

    def test_init_invalid_alert_condition(self):
        self.healthcheck_item.alert_if_interval_is = "invalid_condition"  # type: ignore
        with self.assertRaises(ValueError):
            DAQJobHealthcheck(self.config)

    def test_init_invalid_stats_key(self):
        self.healthcheck_item.stats_key = "invalid_key"
        with self.assertRaises(ValueError):
            DAQJobHealthcheck(self.config)

    def test_init_invalid_daq_job_type(self):
        self.healthcheck_item.daq_job_type = "invalid_job_type"
        with self.assertRaises(ValueError):
            DAQJobHealthcheck(self.config)

    def test_init_interval_and_amount_both_none(self):
        self.healthcheck_item.interval = None
        self.healthcheck_item.amount = None
        with self.assertRaises(ValueError):
            DAQJobHealthcheck(self.config)

    def test_init_interval_and_amount_both_specified(self):
        self.healthcheck_item.interval = "5m"
        self.healthcheck_item.amount = 10
        with self.assertRaises(ValueError):
            DAQJobHealthcheck(self.config)


if __name__ == "__main__":
    unittest.main()
