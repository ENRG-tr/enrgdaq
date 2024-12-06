import time
from datetime import datetime, timedelta
from enum import Enum
from typing import Callable, Optional

import msgspec
from msgspec import Struct

from enrgdaq.daq.alert.base import DAQJobMessageAlert
from enrgdaq.daq.alert.models import DAQAlertInfo, DAQAlertSeverity
from enrgdaq.daq.base import DAQJob
from enrgdaq.daq.jobs.handle_stats import DAQJobMessageStats, DAQJobStatsDict
from enrgdaq.daq.models import DAQJobConfig, DAQJobStats

HEALTHCHECK_LOOP_INTERVAL_SECONDS = 0.1


class AlertCondition(str, Enum):
    """Enumeration for alert conditions."""

    SATISFIED = "satisfied"
    UNSATISFIED = "unsatisfied"


class HealthcheckItem(Struct, kw_only=True):
    """Represents a healthcheck item with alert information.

    Attributes:
        alert_info (DAQAlertInfo): The alert information.
        alive_alert_info (Optional[DAQAlertInfo]): The alert information for when the item gets back alive (after being down).
    """

    alert_info: DAQAlertInfo
    alive_alert_info: Optional[DAQAlertInfo] = None


class HealthcheckStatsItem(HealthcheckItem):
    """
    Represents a healthcheck stats item with additional attributes.
    Attributes:
        daq_job_type (str): The type of the DAQ (Data Acquisition) job.
        stats_key (str): The key associated with the stats item.
        alert_if_interval_is (AlertCondition): The condition to alert if the interval meets certain criteria. "satisfied" means the condition is met, "unsatisfied" means the condition is not met.
        interval (Optional[str]): The interval string representing time duration (e.g., '5s' for 5 seconds, '10m' for 10 minutes, '1h' for 1 hour).
        amount (Optional[int]): An optional amount associated with the stats item.
    """

    daq_job_type: str
    stats_key: str
    alert_if_interval_is: AlertCondition
    interval: Optional[str] = None
    amount: Optional[int] = None

    def parse_interval(self) -> timedelta:
        """Parses the interval string into a timedelta object."""
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


class DAQJobHealthcheckConfig(DAQJobConfig):
    """
    This class holds the configuration settings for the DAQJobHealthcheck, which is responsible for monitoring the health of the DAQ (Data Acquisition) jobs.

    Attributes:
        healthcheck_stats (list[HealthcheckStatsItem]):
            A list of HealthcheckStatsItem objects that represent various health check statistics.
            Each item in the list provides detailed information about a specific aspect of the DAQ job's health,
            such as the interval for checking the job's stats, the key for the stats, and the condition for alerting.
        enable_alerts_on_restart (bool):
            A boolean flag indicating whether alerts should be enabled when the DAQ job is restarted.
            If set to True, alerts will be generated and sent to the appropriate channels whenever the job is restarted.
            This can be useful for monitoring and ensuring that the restart process does not introduce any issues.
            The default value is True.
    """

    healthcheck_stats: list[HealthcheckStatsItem]
    enable_alerts_on_restart: bool = True


class DAQJobHealthcheck(DAQJob):
    """Healthcheck job class for monitoring DAQ jobs.

    This class is responsible for performing health checks on various DAQ jobs
    based on the provided configuration. It monitors the stats of DAQ jobs and
    sends alerts if certain conditions are met, such as if a job has not been
    updated within a specified interval or if a job has restarted.

    Attributes:
        allowed_message_in_types (list): List of allowed message types for this job.
        config_type (type): The configuration class type for this job.
        config (DAQJobHealthcheckConfig): The configuration instance for this job.
        _sent_alert_items (set): Set of alert item hashes that have been sent.
        _current_stats (DAQJobStatsDict): Dictionary holding the current stats of DAQ jobs.
        _daq_job_type_to_class (dict): Mapping of DAQ job type names to their class types.
        _healthcheck_stats (list): List of healthcheck stats items to monitor.
        _get_daq_job_class (Callable): Function to get the DAQ job class by its type name.
    """

    allowed_message_in_types = [DAQJobMessageStats]
    config_type = DAQJobHealthcheckConfig
    config: DAQJobHealthcheckConfig
    _sent_alert_items: set[int]
    _current_stats: dict[str, DAQJobStatsDict]
    _daq_job_type_to_class: dict[str, type[DAQJob]]

    _healthcheck_stats: list[HealthcheckStatsItem]
    _get_daq_job_class: Callable[[str], Optional[type[DAQJob]]]

    def __init__(self, config: DAQJobHealthcheckConfig, **kwargs):
        super().__init__(config, **kwargs)

        from enrgdaq.daq.types import get_all_daq_job_types, get_daq_job_class

        self._get_daq_job_class = get_daq_job_class
        self._current_stats = {}

        self._healthcheck_stats = []

        if config.enable_alerts_on_restart:
            for daq_job_type_class in get_all_daq_job_types():
                self._healthcheck_stats.append(
                    HealthcheckStatsItem(
                        alert_info=DAQAlertInfo(
                            message=f"{daq_job_type_class.__name__} crashed and got restarted!",
                            severity=DAQAlertSeverity.ERROR,
                        ),
                        daq_job_type=daq_job_type_class.__name__,
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
            if self._get_daq_job_class(item.daq_job_type) is None:
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
            time.sleep(HEALTHCHECK_LOOP_INTERVAL_SECONDS)

    def handle_message(self, message: DAQJobMessageStats) -> bool:
        """Handles incoming messages and updates current stats."""
        if not super().handle_message(message):
            return False

        self._current_stats[message.supervisor_id] = message.stats
        return True

    def handle_checks(self):
        """Performs health checks and sends alerts if necessary."""
        res: list[tuple[HealthcheckItem, str, bool]] = []

        for item in self._healthcheck_stats:
            # Get the current DAQJobStats by daq_job_type of item
            item_daq_job_type = self._get_daq_job_class(item.daq_job_type)
            for supervisor_id, daq_job_stats in self._current_stats.items():
                if item_daq_job_type not in daq_job_stats:
                    continue

                daq_job_stats = daq_job_stats[item_daq_job_type]
                should_alert = False
                if item.interval is not None:
                    interval_from_now = datetime.now() - item.parse_interval()
                    daq_job_stats_date = getattr(
                        daq_job_stats, item.stats_key
                    ).last_updated

                    if daq_job_stats_date is None:
                        continue

                    if item.alert_if_interval_is == AlertCondition.UNSATISFIED:
                        should_alert = interval_from_now > daq_job_stats_date
                    else:
                        should_alert = interval_from_now < daq_job_stats_date

                if item.amount is not None:
                    raise NotImplementedError

                res.append((item, supervisor_id, should_alert))

        # Alert if it's new
        for item, supervisor_id, should_alert in res:
            item_id = hash(msgspec.json.encode(item))
            if should_alert and item_id not in self._sent_alert_items:
                self._sent_alert_items.add(item_id)
                self._send_alert(item.alert_info, supervisor_id)
            elif not should_alert and item_id in self._sent_alert_items:
                self._sent_alert_items.remove(item_id)
                if item.alive_alert_info:
                    self._send_alert(item.alive_alert_info, supervisor_id)

    def _send_alert(self, alert_info: DAQAlertInfo, originated_supervisor_id: str):
        self._put_message_out(
            DAQJobMessageAlert(
                date=datetime.now(),
                alert_info=alert_info,
                originated_supervisor_id=originated_supervisor_id,
            )
        )
