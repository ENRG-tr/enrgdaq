from datetime import datetime
from typing import Dict, Optional

from enrgdaq.daq.base import DAQJob
from enrgdaq.daq.models import DAQJobMessage, DAQJobStats, DAQJobStatsRecord
from enrgdaq.daq.store.models import (
    DAQJobMessageStoreTabular,
    StorableDAQJobConfig,
)
from enrgdaq.utils.time import get_unix_timestamp_ms, sleep_for

DAQJobStatsDict = Dict[type[DAQJob], DAQJobStats]

DAQ_JOB_HANDLE_STATS_SLEEP_INTERVAL_SECONDS = 1


class DAQJobHandleStatsConfig(StorableDAQJobConfig):
    """Configuration class for DAQJobHandleStats."""

    pass


class DAQJobMessageStats(DAQJobMessage):
    """Message class containing DAQ job statistics."""

    stats: DAQJobStatsDict


class DAQJobHandleStats(DAQJob):
    """
    Handles statistics for DAQ jobs.

    This class is responsible for consuming and processing DAQ job statistics messages.
    It extracts relevant statistics from the messages and stores them.
    """

    allowed_message_in_types = [DAQJobMessageStats]
    config_type = DAQJobHandleStatsConfig
    config: DAQJobHandleStatsConfig

    _stats: dict[str, DAQJobStatsDict]

    def __init__(self, config: DAQJobHandleStatsConfig, **kwargs):
        super().__init__(config, **kwargs)
        self._stats = {}

    def start(self):
        while True:
            start_time = datetime.now()
            self.consume()
            self._save_stats()
            sleep_for(DAQ_JOB_HANDLE_STATS_SLEEP_INTERVAL_SECONDS, start_time)

    def handle_message(self, message: DAQJobMessageStats) -> bool:
        if not super().handle_message(message):
            return False

        # Ignore if the message has no supervisor info
        if not message.daq_job_info or not message.daq_job_info.supervisor_config:
            return True

        self._stats[message.daq_job_info.supervisor_config.supervisor_id] = (
            message.stats
        )
        return True

    def _save_stats(self):
        def datetime_to_str(dt: Optional[datetime]):
            if dt is None:
                return "N/A"
            return get_unix_timestamp_ms(dt)

        def unpack_record(record: DAQJobStatsRecord):
            return [
                datetime_to_str(record.last_updated),
                record.count,
            ]

        keys = [
            "supervisor",
            "daq_job",
            "is_alive",
            "last_message_in_date",
            "message_in_count",
            "last_message_out_date",
            "message_out_count",
            "last_restart_date",
            "restart_count",
        ]
        data_to_send = []

        for supervisor_id, stats in self._stats.items():
            for daq_job_type, msg in stats.items():
                data_to_send.append(
                    [
                        supervisor_id,
                        daq_job_type.__name__,
                        str(msg.is_alive).lower(),
                        *unpack_record(msg.message_in_stats),
                        *unpack_record(msg.message_out_stats),
                        *unpack_record(msg.restart_stats),
                    ]
                )

        self._put_message_out(
            DAQJobMessageStoreTabular(
                store_config=self.config.store_config,
                keys=keys,
                data=data_to_send,
            )
        )
