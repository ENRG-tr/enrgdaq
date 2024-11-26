from datetime import datetime
from typing import Dict, Optional

from enrgdaq.daq.base import DAQJob
from enrgdaq.daq.models import DAQJobMessage, DAQJobStats, DAQJobStatsRecord
from enrgdaq.daq.store.models import (
    DAQJobMessageStoreTabular,
    StorableDAQJobConfig,
)
from enrgdaq.utils.time import get_unix_timestamp_ms

DAQJobStatsDict = Dict[type[DAQJob], DAQJobStats]


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

    def start(self):
        while True:
            self.consume(nowait=False)

    def handle_message(self, message: DAQJobMessageStats) -> bool:
        if not super().handle_message(message):
            return False

        keys = [
            "daq_job",
            "is_alive",
            "last_message_in_date",
            "message_in_count",
            "last_message_out_date",
            "message_out_count",
            "last_restart_date",
            "restart_count",
        ]

        def datetime_to_str(dt: Optional[datetime]):
            if dt is None:
                return "N/A"
            return get_unix_timestamp_ms(dt)

        def unpack_record(record: DAQJobStatsRecord):
            return [
                datetime_to_str(record.last_updated),
                record.count,
            ]

        data_to_send = []
        for daq_job_type, msg in message.stats.items():
            data_to_send.append(
                [
                    daq_job_type.__name__,
                    str(msg.is_alive).lower(),
                    *unpack_record(msg.message_in_stats),
                    *unpack_record(msg.message_out_stats),
                    *unpack_record(msg.restart_stats),
                ]
            )

        if message.daq_job_info and message.daq_job_info.supervisor_config:
            tag = message.daq_job_info.supervisor_config.supervisor_id
        else:
            tag = None

        self._put_message_out(
            DAQJobMessageStoreTabular(
                store_config=self.config.store_config,
                keys=keys,
                data=data_to_send,
                tag=tag,
            )
        )

        return True
