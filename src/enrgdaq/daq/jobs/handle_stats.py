from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, Optional

import msgspec

from enrgdaq.daq.base import DAQJob
from enrgdaq.daq.models import (
    DAQJobMessage,
    DAQJobMessageStatsRemote,
    DAQJobMessageStatsReport,
    DAQJobRemoteStatsDict,
    DAQJobStats,
    DAQJobStatsRecord,
    SupervisorRemoteStats,
)
from enrgdaq.daq.store.models import (
    DAQJobMessageStoreTabular,
    StorableDAQJobConfig,
)
from enrgdaq.daq.topics import Topic
from enrgdaq.models import SupervisorInfo
from enrgdaq.utils.time import get_unix_timestamp_ms, sleep_for

DAQJobStatsDict = Dict[str, DAQJobStats]
DAQJobRemoteStatsDict = Dict[str, DAQJobRemoteStatsDict]

DAQ_JOB_HANDLE_STATS_SLEEP_INTERVAL_SECONDS = 1
DAQ_JOB_HANDLE_STATS_REMOTE_ALIVE_SECONDS = 30


class DAQJobHandleStatsConfig(StorableDAQJobConfig):
    """Configuration class for DAQJobHandleStats."""

    pass


class DAQJobMessageCombinedStats(DAQJobMessage):
    """Message class containing combined DAQ job statistics."""

    stats: dict[str, "DAQJobStatsDict"]


class DAQJobMessageCombinedRemoteStats(DAQJobMessage):
    """Message class containing combined remote statistics."""

    stats: dict[str, SupervisorRemoteStats]


class DAQJobHandleStats(DAQJob):
    """
    Handles statistics for DAQ jobs.

    This class is responsible for consuming and processing DAQ job statistics messages.
    It extracts relevant statistics from the messages and stores them.
    """

    allowed_message_in_types = [
        DAQJobMessageStatsRemote,
        DAQJobMessageStatsReport,
    ]
    # Subscribe to "stats" prefix to receive stats from all supervisors
    config_type = DAQJobHandleStatsConfig
    config: DAQJobHandleStatsConfig

    _stats: dict[str, DAQJobStatsDict]
    _remote_stats: DAQJobRemoteStatsDict  # type:ignore
    _supervisor_activity: dict[str, SupervisorRemoteStats]

    def __init__(
        self, config: DAQJobHandleStatsConfig, supervisor_info: SupervisorInfo, **kwargs
    ):
        self.topics_to_subscribe.append(Topic.stats(supervisor_info.supervisor_id))
        super().__init__(config, supervisor_info, **kwargs)
        self._stats = {}
        self._remote_stats = defaultdict()
        self._supervisor_activity = {}  # Track supervisor activity from stats reports

    def start(self):
        while not self._has_been_freed:
            start_time = datetime.now()
            self._save_stats()
            self._save_remote_stats()
            sleep_for(DAQ_JOB_HANDLE_STATS_SLEEP_INTERVAL_SECONDS, start_time)

    def handle_message(
        self,
        message: DAQJobMessageStatsRemote | DAQJobMessageStatsReport,
    ) -> bool:
        if not super().handle_message(message):
            return False

        # Ignore if the message has no supervisor info
        if not message.daq_job_info or not message.daq_job_info.supervisor_info:
            return True

        supervisor_id = message.supervisor_id

        if isinstance(message, DAQJobMessageStatsReport):
            # Per-job stats report from individual DAQJob
            daq_job_type = message.daq_job_info.daq_job_type
            if supervisor_id not in self._stats:
                self._stats[supervisor_id] = {}
            if daq_job_type not in self._stats[supervisor_id]:
                self._stats[supervisor_id][daq_job_type] = DAQJobStats()
            stats = self._stats[supervisor_id][daq_job_type]
            stats.latency_stats = message.latency
            stats.message_in_stats.set(message.processed_count)
            stats.message_out_stats.set(message.sent_count)

            # Track supervisor activity and accumulate bytes
            if supervisor_id not in self._supervisor_activity:
                self._supervisor_activity[supervisor_id] = SupervisorRemoteStats()
            activity = self._supervisor_activity[supervisor_id]
            activity.last_active = datetime.now()
            # Accumulate bytes (aggregate across all DAQJobs)
            activity.message_in_bytes += message.processed_bytes
            activity.message_out_bytes += message.sent_bytes
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
                        daq_job_type,
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
        self._put_message_out(
            DAQJobMessageCombinedStats(
                stats=self._stats, topics={Topic.stats_combined(self.supervisor_id)}
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

        # Build remote stats by aggregating per-supervisor stats from all DAQJobs
        remote_stats_combined = defaultdict(lambda: SupervisorRemoteStats())

        # Sum stats from all DAQJobs for each supervisor
        for supervisor_id, daq_job_stats in self._stats.items():
            if supervisor_id not in remote_stats_combined:
                remote_stats_combined[supervisor_id] = SupervisorRemoteStats()

            total_message_in = 0
            total_message_out = 0
            for _, stats in daq_job_stats.items():
                total_message_in += stats.message_in_stats.count
                total_message_out += stats.message_out_stats.count

            remote_stats_combined[supervisor_id].message_in_count = total_message_in
            remote_stats_combined[supervisor_id].message_out_count = total_message_out

            # Copy activity data including bytes from _supervisor_activity
            if supervisor_id in self._supervisor_activity:
                activity = self._supervisor_activity[supervisor_id]
                remote_stats_combined[supervisor_id].last_active = activity.last_active
                remote_stats_combined[
                    supervisor_id
                ].message_in_bytes = activity.message_in_bytes
                remote_stats_combined[
                    supervisor_id
                ].message_out_bytes = activity.message_out_bytes

        # Also include stats from DAQJobMessageStatsRemote if received
        if (
            self._supervisor_info
            and self._supervisor_info.supervisor_id in self._remote_stats
        ):
            for supervisor_id, remote_stats in self._remote_stats[
                self._supervisor_info.supervisor_id
            ].items():
                # Merge with existing activity stats
                if supervisor_id in remote_stats_combined:
                    # Keep the more recent last_active
                    if (
                        remote_stats.last_active
                        > remote_stats_combined[supervisor_id].last_active
                    ):
                        remote_stats_combined[supervisor_id] = remote_stats
                else:
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
                    self._supervisor_info
                    and self._supervisor_info.supervisor_id == remote_supervisor_id
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
        self._put_message_out(
            DAQJobMessageCombinedRemoteStats(
                stats=dict(remote_stats_combined),
                topics={Topic.stats_combined(self.supervisor_id)},
            )
        )
