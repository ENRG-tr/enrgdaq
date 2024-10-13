import time
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional

from daq.base import DAQJob
from daq.models import DAQJobMessage, DAQJobStats
from daq.store.models import DAQJobMessageStore, StorableDAQJobConfig

DAQJobStatsDict = Dict[type[DAQJob], DAQJobStats]


@dataclass
class DAQJobHandleStatsConfig(StorableDAQJobConfig):
    pass


@dataclass
class DAQJobMessageStats(DAQJobMessage):
    stats: DAQJobStatsDict


class DAQJobHandleStats(DAQJob):
    allowed_message_in_types = [DAQJobMessageStats]
    config_type = DAQJobHandleStatsConfig
    config: DAQJobHandleStatsConfig

    def start(self):
        while True:
            self.consume()
            time.sleep(1)

    def handle_message(self, message: DAQJobMessageStats) -> bool:
        if not super().handle_message(message):
            return False

        keys = [
            "daq_job",
            "last_message_in_date",
            "message_in_count",
            "last_message_out_date",
            "message_out_count",
        ]

        def datetime_to_str(dt: Optional[datetime]):
            if dt is None:
                return "N/A"
            return dt.strftime("%Y-%m-%d %H:%M:%S")

        data_to_send = []
        for daq_job_type, msg in message.stats.items():
            data_to_send.append(
                [
                    daq_job_type.__name__,
                    datetime_to_str(msg.last_message_in_date),
                    msg.message_in_count,
                    datetime_to_str(msg.last_message_out_date),
                    msg.message_out_count,
                ]
            )

        self.message_out.put(
            DAQJobMessageStore(
                store_config=self.config.store_config,
                daq_job=self,
                keys=keys,
                data=data_to_send,
            )
        )

        return True
