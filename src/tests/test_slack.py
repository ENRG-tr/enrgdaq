import unittest
from datetime import datetime
from unittest.mock import patch

from enrgdaq.daq.alert.alert_slack import DAQJobAlertSlack, DAQJobAlertSlackConfig
from enrgdaq.daq.alert.models import DAQAlertInfo, DAQAlertSeverity, DAQJobMessageAlert
from enrgdaq.daq.base import DAQJobInfo


class TestDAQJobAlertSlack(unittest.TestCase):
    @patch("enrgdaq.daq.alert.alert_slack.Slack")
    def setUp(self, MockSlack):
        self.mock_slack = MockSlack.return_value
        self.mock_slack.post.return_value = "ok"
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
            originated_supervisor_id="mock2",
        )
        self.daq_job.send_webhook(alert)
        assert alert.daq_job_info is not None
        self.mock_slack.post.assert_called_once_with(
            attachments=[
                {
                    "fallback": "Test error message",
                    "color": "danger",
                    "author_name": alert.daq_job_info.daq_job_class_name,
                    "title": "Alert!",
                    "fields": [
                        {
                            "title": "Supervisor ID",
                            "value": "mock",
                            "short": True,
                        },
                        {
                            "title": "Originated Supervisor ID",
                            "value": "mock2",
                            "short": True,
                        },
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
            originated_supervisor_id="mock",
        )
        alert2 = DAQJobMessageAlert(
            daq_job_info=DAQJobInfo.mock(),
            alert_info=DAQAlertInfo(
                severity=DAQAlertSeverity.WARNING,
                message="Test warning message",
            ),
            date=datetime(2023, 10, 1, 12, 0, 0),
            originated_supervisor_id="mock",
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
            originated_supervisor_id="mock",
        )
        self.daq_job.handle_message(alert)
        self.assertEqual(self.mock_slack.post.call_count, 1)


if __name__ == "__main__":
    unittest.main()
