import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Optional

from dataclasses_json import DataClassJsonMixin

from daq.alert.base import DAQAlertInfo, DAQAlertSeverity, DAQJobMessageAlert
from daq.base import DAQJob
from daq.jobs.handle_stats import DAQJobMessageStats, DAQJobStatsDict
from daq.models import DAQJobConfig, DAQJobStats


class AlertCondition(str, Enum):
    SATISFIED = "satisfied"
    UNSATISFIED = "unsatisfied"


@dataclass
class HealthcheckItem(DataClassJsonMixin):
    alert_info: DAQAlertInfo


@dataclass
class HealthcheckStatsItem(HealthcheckItem):
    daq_job_type: str
    stats_key: str
    alert_if_interval_is: AlertCondition
    interval: Optional[str] = None
    amount: Optional[int] = None

    def parse_interval(self) -> timedelta:
        if self.interval is None:
            raise ValueError("interval is null")

        if not self.interval[:-1].isdigit() or self.interval[-1] not in "smh":
            raise ValueError(f"Invalid interval format: {self.interval}")

        unit = self.interval[-1]
        value = int(self.interval[:-1])

        if unit == "s":
            return timedelta(seconds=value)
        elif unit == "m":
            return timedelta(minutes=value)
        elif unit == "h":
            return timedelta(hours=value)
        else:
            raise ValueError(f"Invalid interval unit: {unit}")


@dataclass
class DAQJobHealthcheckConfig(DAQJobConfig):
    healthcheck_stats: list[HealthcheckStatsItem]
    enable_alerts_on_restart: bool = True


class DAQJobHealthcheck(DAQJob):
    allowed_message_in_types = [DAQJobMessageStats]
    config_type = DAQJobHealthcheckConfig
    config: DAQJobHealthcheckConfig
    _sent_alert_items: set[int]
    _current_stats: DAQJobStatsDict
    _daq_job_type_to_class: dict[str, type[DAQJob]]

    _healthcheck_stats: list[HealthcheckStatsItem]

    def __init__(self, config: DAQJobHealthcheckConfig):
        from daq.types import DAQ_JOB_TYPE_TO_CLASS

        self._daq_job_type_to_class = DAQ_JOB_TYPE_TO_CLASS
        self._current_stats = {}

        super().__init__(config)

        self._healthcheck_stats = []

        if config.enable_alerts_on_restart:
            for daq_job_type, daq_job_type_class in self._daq_job_type_to_class.items():
                self._healthcheck_stats.append(
                    HealthcheckStatsItem(
                        alert_info=DAQAlertInfo(
                            message=f"{daq_job_type_class.__name__} crashed and got restarted!",
                            severity=DAQAlertSeverity.ERROR,
                        ),
                        daq_job_type=daq_job_type,
                        alert_if_interval_is=AlertCondition.SATISFIED,
                        stats_key="restart_stats",
                        interval="1m",
                    )
                )

        self._healthcheck_stats.extend(list(self.config.healthcheck_stats))

        # Sanity check config
        for item in self._healthcheck_stats:
            if item.alert_if_interval_is not in AlertCondition:
                raise ValueError(
                    f"Invalid alert condition: {item.alert_if_interval_is}"
                )
            if item.stats_key not in DAQJobStats.__annotations__.keys():
                raise ValueError(f"Invalid stats key: {item.stats_key}")
            if item.daq_job_type not in self._daq_job_type_to_class:
                raise ValueError(f"Invalid DAQ job type: {item.daq_job_type}")
            if item.interval is None and item.amount is None:
                raise ValueError("interval or amount must be specified")
            if item.interval is not None and item.amount is not None:
                raise ValueError(
                    "interval and amount cannot be specified at the same time"
                )
            if item.interval is not None:
                item.parse_interval()

        self._sent_alert_items = set()

    def start(self):
        while True:
            self.consume()
            self.handle_checks()
            time.sleep(0.5)

    def handle_message(self, message: DAQJobMessageStats) -> bool:
        if not super().handle_message(message):
            return False

        self._current_stats = message.stats
        return True

    def handle_checks(self):
        res: list[tuple[HealthcheckItem, bool]] = []

        for item in self._healthcheck_stats:
            # Get the current DAQJobStats by daq_job_type of item
            item_daq_job_type = self._daq_job_type_to_class[item.daq_job_type]
            if item_daq_job_type not in self._current_stats:
                continue

            daq_job_stats = self._current_stats[item_daq_job_type]
            should_alert = False
            if item.interval is not None:
                interval_from_now = datetime.now() - item.parse_interval()
                daq_job_stats_date = getattr(daq_job_stats, item.stats_key).last_updated

                if daq_job_stats_date is None:
                    continue

                if item.alert_if_interval_is == AlertCondition.UNSATISFIED:
                    should_alert = interval_from_now > daq_job_stats_date
                else:
                    should_alert = interval_from_now < daq_job_stats_date

            if item.amount is not None:
                raise NotImplementedError

            res.append((item, should_alert))

        # Alert if it's new
        for item, should_alert in res:
            item_id = hash(item.to_json())
            if should_alert and item_id not in self._sent_alert_items:
                self._sent_alert_items.add(item_id)
                self.send_alert(item)
            elif not should_alert and item_id in self._sent_alert_items:
                self._sent_alert_items.remove(item_id)

    def send_alert(self, item: HealthcheckItem):
        self.message_out.put(
            DAQJobMessageAlert(
                daq_job=self,
                date=datetime.now(),
                alert_info=item.alert_info,
            )
        )
