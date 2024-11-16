from datetime import datetime
from typing import Dict, Optional

from daq.base import DAQJob
from daq.models import DAQJobMessage, DAQJobStats, DAQJobStatsRecord
from daq.store.models import DAQJobMessageStore, StorableDAQJobConfig
from utils.time import get_unix_timestamp_ms

DAQJobStatsDict = Dict[type[DAQJob], DAQJobStats]


class DAQJobHandleStatsConfig(StorableDAQJobConfig):
    pass


class DAQJobMessageStats(DAQJobMessage):
    stats: DAQJobStatsDict


class DAQJobHandleStats(DAQJob):
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
            DAQJobMessageStore(
                store_config=self.config.store_config,
                keys=keys,
                data=data_to_send,
                tag=tag,
            )
        )

        return True
