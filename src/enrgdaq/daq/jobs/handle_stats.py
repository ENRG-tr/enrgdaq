from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, Optional

import msgspec

from enrgdaq.daq.base import DAQJob
from enrgdaq.daq.jobs.remote import (
    DAQJobMessageStatsRemote,
    DAQJobRemoteStatsDict,
    SupervisorRemoteStats,
)
from enrgdaq.daq.models import DAQJobMessage, DAQJobStats, DAQJobStatsRecord
from enrgdaq.daq.store.models import (
    DAQJobMessageStoreTabular,
    StorableDAQJobConfig,
)
from enrgdaq.utils.time import get_unix_timestamp_ms, sleep_for

DAQJobStatsDict = Dict[type[DAQJob], DAQJobStats]
DAQJobRemoteStatsDict = Dict[str, DAQJobRemoteStatsDict]

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
    _remote_stats: DAQJobRemoteStatsDict  # type:ignore

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
            self._stats[message.supervisor_id] = message.stats
        elif isinstance(message, DAQJobMessageStatsRemote):
            self._remote_stats[message.supervisor_id] = message.stats
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
            "message_in_queue_size",
            "message_out_queue_size",
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
                        msg.message_in_queue_stats.count,
                        msg.message_out_queue_stats.count,
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
            "message_in_megabytes",
            "message_out_count",
            "message_out_megabytes",
        ]
        data_to_send = []

        # Combine remote stats from all supervisors
        remote_stats_combined = defaultdict(lambda: SupervisorRemoteStats())
        if (
            self._supervisor_config
            and self._supervisor_config.supervisor_id in self._remote_stats
        ):
            for supervisor_id, remote_stats in self._remote_stats[
                self._supervisor_config.supervisor_id
            ].items():
                remote_stats_combined[supervisor_id] = remote_stats

        for remote_supervisor_id, remote_stats_dict in self._remote_stats.items():
            # For each remote stats dict, combine the values
            for (
                supervisor_id,
                remote_stats_dict_serialized_item,
            ) in remote_stats_dict.items():
                # Skip if the remote supervisor id is the same as the local supervisor id or
                # other supervisors try to overwrite other supervisors
                if supervisor_id != remote_supervisor_id or (
                    self._supervisor_config
                    and self._supervisor_config.supervisor_id == remote_supervisor_id
                ):
                    continue
                # Convert the supervisor remote stats to a dict
                remote_stats_dict_serialized = msgspec.structs.asdict(
                    remote_stats_dict_serialized_item
                )
                # Set each value
                for item, value in remote_stats_dict_serialized.items():
                    if value == 0 or not value:
                        continue
                    setattr(remote_stats_combined[supervisor_id], item, value)
        for supervisor_id, remote_stats in remote_stats_combined.items():
            is_remote_alive = datetime.now() - remote_stats.last_active <= timedelta(
                seconds=DAQ_JOB_HANDLE_STATS_REMOTE_ALIVE_SECONDS
            )

            def _byte_to_mb(x):
                return "{:.3f}".format(x / 1024 / 1024)

            data_to_send.append(
                [
                    supervisor_id,
                    str(is_remote_alive).lower(),
                    remote_stats.last_active,
                    remote_stats.message_in_count,
                    _byte_to_mb(remote_stats.message_in_bytes),
                    remote_stats.message_out_count,
                    _byte_to_mb(remote_stats.message_out_bytes),
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
