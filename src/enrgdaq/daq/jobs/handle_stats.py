from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, Optional

import msgspec

from enrgdaq.daq.base import DAQJob
from enrgdaq.daq.jobs.remote import (
    DAQJobMessageStatsRemote,
    DAQJobMessageStatsRemoteDict,
    SupervisorRemoteStats,
)
from enrgdaq.daq.models import DAQJobMessage, DAQJobStats, DAQJobStatsRecord
from enrgdaq.daq.store.models import (
    DAQJobMessageStoreTabular,
    StorableDAQJobConfig,
)
from enrgdaq.utils.time import get_unix_timestamp_ms, sleep_for

DAQJobStatsDict = Dict[type[DAQJob], DAQJobStats]

DAQ_JOB_HANDLE_STATS_SLEEP_INTERVAL_SECONDS = 1
DAQ_JOB_HANDLE_STATS_REMOTE_ALIVE_SECONDS = 30


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

    allowed_message_in_types = [DAQJobMessageStats, DAQJobMessageStatsRemote]
    config_type = DAQJobHandleStatsConfig
    config: DAQJobHandleStatsConfig

    _stats: dict[str, DAQJobStatsDict]
    _remote_stats: dict[str, DAQJobMessageStatsRemoteDict]

    def __init__(self, config: DAQJobHandleStatsConfig, **kwargs):
        super().__init__(config, **kwargs)
        self._stats = {}
        self._remote_stats = defaultdict()

    def start(self):
        while True:
            start_time = datetime.now()
            self.consume()
            self._save_stats()
            self._save_remote_stats()
            sleep_for(DAQ_JOB_HANDLE_STATS_SLEEP_INTERVAL_SECONDS, start_time)

    def handle_message(
        self, message: DAQJobMessageStats | DAQJobMessageStatsRemote
    ) -> bool:
        if not super().handle_message(message):
            return False

        # Ignore if the message has no supervisor info
        if not message.daq_job_info or not message.daq_job_info.supervisor_config:
            return True

        if isinstance(message, DAQJobMessageStats):
            self._stats[message.daq_job_info.supervisor_config.supervisor_id] = (
                message.stats
            )
        elif isinstance(message, DAQJobMessageStatsRemote):
            self._remote_stats[message.daq_job_info.supervisor_config.supervisor_id] = (
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

    def _save_remote_stats(self):
        keys = [
            "supervisor",
            "is_alive",
            "last_active",
            "message_in_count",
            "message_in_bytes",
            "message_out_count",
            "message_out_bytes",
        ]
        data_to_send = []

        # Combine remote stats from all supervisors
        remote_stats_combined = defaultdict(lambda: SupervisorRemoteStats())
        for _, remote_stats_dict in self._remote_stats.items():
            # For each remote stats dict, combine the values
            for (
                supervisor_id,
                remote_stats_dict_serialized_item,
            ) in remote_stats_dict.items():
                # Convert the supervisor remote stats to a dict
                remote_stats_dict_serialized = msgspec.structs.asdict(
                    remote_stats_dict_serialized_item
                )
                for item, value in remote_stats_dict_serialized.items():
                    setattr(remote_stats_combined[supervisor_id], item, value)

        for supervisor_id, remote_stats in remote_stats_combined.items():
            is_remote_alive = datetime.now() - remote_stats.last_active <= timedelta(
                seconds=DAQ_JOB_HANDLE_STATS_REMOTE_ALIVE_SECONDS
            )
            data_to_send.append(
                [
                    supervisor_id,
                    str(is_remote_alive).lower(),
                    remote_stats.last_active,
                    remote_stats.message_in_count,
                    remote_stats.message_in_bytes,
                    remote_stats.message_out_count,
                    remote_stats.message_out_bytes,
                ]
            )
        self._put_message_out(
            DAQJobMessageStoreTabular(
                store_config=self.config.store_config,
                keys=keys,
                data=data_to_send,
                tag="remote",
            )
        )
