from dataclasses import dataclass

from slack_webhook import Slack

from daq.alert.base import DAQAlertSeverity, DAQJobAlert, DAQJobMessageAlert
from daq.models import DAQJobConfig

ALERT_SEVERITY_TO_SLACK_COLOR = {
    DAQAlertSeverity.INFO: "good",
    DAQAlertSeverity.WARNING: "warning",
    DAQAlertSeverity.ERROR: "danger",
}


@dataclass
class DAQJobAlertSlackConfig(DAQJobConfig):
    slack_webhook_url: str


class DAQJobAlertSlack(DAQJobAlert):
    config_type = DAQJobAlertSlackConfig
    config: DAQJobAlertSlackConfig
    _slack: Slack

    def __init__(self, config: DAQJobAlertSlackConfig):
        super().__init__(config)
        self._slack = Slack(url=config.slack_webhook_url)

    def alert_loop(self):
        for alert in self._alerts:
            self.send_webhook(alert)

    def send_webhook(self, alert: DAQJobMessageAlert):
        self._logger.info(
            f"Sending alert to Slack: [{alert.alert_info.severity}] {alert.alert_info.message}"
        )
        self._slack.post(
            attachments=[
                {
                    "fallback": alert.alert_info.message,
                    "color": ALERT_SEVERITY_TO_SLACK_COLOR[alert.alert_info.severity],
                    "author_name": alert.daq_job_info.daq_job_class_name,
                    "title": "Alert!",
                    "fields": [
                        {
                            "title": "Severity",
                            "value": alert.alert_info.severity,
                            "short": True,
                        },
                        {
                            "title": "Date",
                            "value": alert.date.strftime("%Y-%m-%d %H:%M:%S"),
                            "short": True,
                        },
                        {
                            "title": "Message",
                            "value": alert.alert_info.message,
                            "short": False,
                        },
                    ],
                }
            ]
        )
