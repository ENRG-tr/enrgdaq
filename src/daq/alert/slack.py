from dataclasses import dataclass

from slack_webhook import Slack

from daq.alert.base import DAQJobAlert, DAQJobMessageAlert, DAQJobMessageAlertSeverity
from daq.models import DAQJobConfig

ALERT_SEVERITY_TO_SLACK_COLOR = {
    DAQJobMessageAlertSeverity.INFO: "good",
    DAQJobMessageAlertSeverity.WARNING: "warning",
    DAQJobMessageAlertSeverity.ERROR: "danger",
}


@dataclass
class DAQJobAlertSlackConfig(DAQJobConfig):
    slack_webhook_url: str


class DAQJobAlertSlack(DAQJobAlert):
    config: DAQJobAlertSlackConfig
    _slack: Slack

    def __init__(self, config: DAQJobAlertSlackConfig):
        super().__init__(config)
        self._slack = Slack(url=config.slack_webhook_url)

    def alert_loop(self):
        for alert in self._alerts:
            self.send_alert(alert)

    def send_alert(self, alert: DAQJobMessageAlert):
        self._slack.post(
            attachments=[
                {
                    "fallback": alert.message,
                    "color": ALERT_SEVERITY_TO_SLACK_COLOR[alert.severity],
                    "author_name": type(alert.daq_job).__name__,
                    "title": "Alert!",
                    "fields": [
                        {
                            "title": "Severity",
                            "value": alert.severity,
                            "short": True,
                        },
                        {
                            "title": "Date",
                            "value": alert.date.strftime("%Y-%m-%d %H:%M:%S"),
                            "short": True,
                        },
                        {
                            "title": "Message",
                            "value": alert.message,
                            "short": False,
                        },
                    ],
                }
            ]
        )
