from datetime import datetime
from enum import Enum

import psutil
from msgspec import field

from enrgdaq.daq.base import DAQJob
from enrgdaq.daq.models import DAQJobMessage
from enrgdaq.daq.store.models import DAQJobMessageStoreTabular, StorableDAQJobConfig
from enrgdaq.utils.time import get_now_unix_timestamp_ms, sleep_for

DAQ_JOB_PC_METRICS_QUERY_INTERVAL_SECONDS = 1


class PCMetric(str, Enum):
    CPU = "cpu"
    MEMORY = "memory"
    DISK = "disk"


class DAQJobPCMetricsConfig(StorableDAQJobConfig):
    """
    Configuration class for DAQJobPCMetrics.

    Attributes:
        metrics_to_store (list[PCMetric]): List of metrics to store. If not specified, all metrics will be stored.
        poll_interval_seconds (int): Polling interval in seconds. If not specified, the default polling interval will be used.
    """

    metrics_to_store: list[PCMetric] = field(default_factory=lambda: list(PCMetric))
    poll_interval_seconds: int = DAQ_JOB_PC_METRICS_QUERY_INTERVAL_SECONDS


class DAQJobPCMetrics(DAQJob):
    """
    DAQ job for monitoring the CPU, memory, and disk usage of the PC.
    """

    config_type = DAQJobPCMetricsConfig
    config: DAQJobPCMetricsConfig

    def __init__(self, config: DAQJobPCMetricsConfig, **kwargs):
        super().__init__(config, **kwargs)

    def handle_message(self, message: DAQJobMessage):
        super().handle_message(message)

    def start(self):
        while True:
            start_time = datetime.now()
            self._poll_metrics()
            sleep_for(self.config.poll_interval_seconds, start_time)

    def _poll_metrics(self):
        data = {}
        if PCMetric.CPU in self.config.metrics_to_store:
            cpu_percent = psutil.cpu_percent(interval=None)
            # psutil returns 0 on the first run if we set the interval
            # to None, so skip the initial polling
            if cpu_percent == 0:
                return

            data[PCMetric.CPU] = cpu_percent
        if PCMetric.MEMORY in self.config.metrics_to_store:
            data[PCMetric.MEMORY] = psutil.virtual_memory().percent
        if PCMetric.DISK in self.config.metrics_to_store:
            data[PCMetric.DISK] = psutil.disk_usage("/").percent

        data = {k.value: v for k, v in data.items()}
        self._send_store_message(data)

    def _send_store_message(self, data: dict):
        keys = ["timestamp", *data.keys()]
        values = [get_now_unix_timestamp_ms(), *data.values()]
        self._put_message_out(
            DAQJobMessageStoreTabular(
                store_config=self.config.store_config,
                tag=self._supervisor_config.supervisor_id,
                keys=keys,
                data=[values],
            )
        )
