import unittest
from datetime import datetime
from unittest.mock import patch

from daq.alert.alert_slack import DAQJobAlertSlack, DAQJobAlertSlackConfig
from daq.alert.models import DAQAlertInfo, DAQAlertSeverity, DAQJobMessageAlert
from daq.base import DAQJobInfo


class TestDAQJobAlertSlack(unittest.TestCase):
    @patch("daq.alert.alert_slack.Slack")
    def setUp(self, MockSlack):
        self.mock_slack = MockSlack.return_value
        self.config = DAQJobAlertSlackConfig(
            daq_job_type="", slack_webhook_url="http://fake.url"
        )
        self.daq_job = DAQJobAlertSlack(config=self.config)

    def test_init(self):
        self.assertEqual(self.daq_job.config, self.config)
        self.assertEqual(self.daq_job._slack, self.mock_slack)

    def test_send_alert(self):
        alert = DAQJobMessageAlert(
            daq_job_info=DAQJobInfo.mock(),
            alert_info=DAQAlertInfo(
                severity=DAQAlertSeverity.ERROR,
                message="Test error message",
            ),
            date=datetime(2023, 10, 1, 12, 0, 0),
        )
        self.daq_job.send_webhook(alert)
        self.mock_slack.post.assert_called_once_with(
            attachments=[
                {
                    "fallback": "Test error message",
                    "color": "danger",
                    "author_name": alert.daq_job_info.daq_job_class_name,
                    "title": "Alert!",
                    "fields": [
                        {
                            "title": "Severity",
                            "value": DAQAlertSeverity.ERROR,
                            "short": True,
                        },
                        {
                            "title": "Date",
                            "value": "2023-10-01 12:00:00",
                            "short": True,
                        },
                        {
                            "title": "Message",
                            "value": "Test error message",
                            "short": False,
                        },
                    ],
                }
            ]
        )

    def test_alert_loop(self):
        alert1 = DAQJobMessageAlert(
            daq_job_info=DAQJobInfo.mock(),
            alert_info=DAQAlertInfo(
                severity=DAQAlertSeverity.INFO,
                message="Test info message",
            ),
            date=datetime(2023, 10, 1, 12, 0, 0),
        )
        alert2 = DAQJobMessageAlert(
            daq_job_info=DAQJobInfo.mock(),
            alert_info=DAQAlertInfo(
                severity=DAQAlertSeverity.WARNING,
                message="Test warning message",
            ),
            date=datetime(2023, 10, 1, 12, 0, 0),
        )
        self.daq_job.handle_message(alert1)
        self.daq_job.handle_message(alert2)
        self.assertEqual(self.mock_slack.post.call_count, 2)

    def test_alert_remote_message(self):
        alert = DAQJobMessageAlert(
            daq_job_info=DAQJobInfo.mock(),
            alert_info=DAQAlertInfo(
                severity=DAQAlertSeverity.ERROR,
                message="Test error message",
            ),
            is_remote=True,
            date=datetime(2023, 10, 1, 12, 0, 0),
        )
        self.daq_job.handle_message(alert)
        self.assertEqual(self.mock_slack.post.call_count, 1)


if __name__ == "__main__":
    unittest.main()
