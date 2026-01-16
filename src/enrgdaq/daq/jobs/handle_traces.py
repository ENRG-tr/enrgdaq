from collections import defaultdict
from datetime import datetime

from enrgdaq.daq.base import DAQJob
from enrgdaq.daq.models import (
    DAQJobMessage,
    DAQJobMessageTraceEvent,
    DAQJobMessageTraceReport,
)
from enrgdaq.daq.store.models import (
    DAQJobMessageStoreTabular,
    StorableDAQJobConfig,
)
from enrgdaq.daq.topics import Topic
from enrgdaq.models import SupervisorInfo
from enrgdaq.utils.time import get_unix_timestamp_ms, sleep_for

DAQ_JOB_HANDLE_TRACES_SLEEP_INTERVAL_SECONDS = 1


class DAQJobHandleTracesConfig(StorableDAQJobConfig):
    """Configuration class for DAQJobHandleTraces."""

    pass


class DAQJobMessageCombinedTraces(DAQJobMessage):
    """Message class containing combined trace events from a supervisor."""

    traces: dict[str, list[DAQJobMessageTraceEvent]]  # keyed by message_id


class DAQJobHandleTraces(DAQJob):
    """
    Handles trace events for DAQ jobs.

    This class is responsible for consuming and processing DAQ job trace messages.
    It aggregates trace events by message_id and outputs them to storage.
    """

    allowed_message_in_types = [DAQJobMessageTraceReport]
    config_type = DAQJobHandleTracesConfig
    config: DAQJobHandleTracesConfig

    _traces: dict[str, list[DAQJobMessageTraceEvent]]

    def __init__(
        self,
        config: DAQJobHandleTracesConfig,
        supervisor_info: SupervisorInfo,
        **kwargs,
    ):
        self.topics_to_subscribe.append(Topic.traces(supervisor_info.supervisor_id))
        super().__init__(config, supervisor_info, **kwargs)
        self._traces = defaultdict(list)

    def start(self):
        while not self._has_been_freed:
            start_time = datetime.now()
            self._save_traces()
            sleep_for(DAQ_JOB_HANDLE_TRACES_SLEEP_INTERVAL_SECONDS, start_time)

    def handle_message(
        self,
        message: DAQJobMessageTraceReport,
    ) -> bool:
        if not super().handle_message(message):
            return False

        # Aggregate trace events by message_id
        for event in message.events:
            self._traces[event.message_id].append(event)

        return True

    def _save_traces(self):
        if not self._traces:
            return

        # Raw trace events output
        trace_keys = [
            "message_id",
            "message_type",
            "event_type",
            "topics",
            "timestamp",
            "size_bytes",
            "source_job",
            "source_supervisor",
        ]
        trace_data = []

        # Latency stats output (for messages with both sent and received)
        latency_keys = [
            "message_id",
            "message_type",
            "source_job",
            "dest_job",
            "source_supervisor",
            "dest_supervisor",
            "sent_timestamp",
            "received_timestamp",
            "latency_ms",
            "size_bytes",
        ]
        latency_data = []

        for message_id, events in self._traces.items():
            # Collect raw trace data
            for event in events:
                trace_data.append(
                    [
                        message_id,
                        event.message_type,
                        event.event_type,
                        ",".join(event.topics),
                        get_unix_timestamp_ms(event.timestamp),
                        event.size_bytes,
                        event.source_job,
                        event.source_supervisor,
                    ]
                )

            # Calculate latency if we have both sent and received events
            sent_events = [e for e in events if e.event_type == "sent"]
            received_events = [e for e in events if e.event_type == "received"]

            for sent in sent_events:
                for received in received_events:
                    latency_ms = (
                        received.timestamp - sent.timestamp
                    ).total_seconds() * 1000
                    latency_data.append(
                        [
                            message_id,
                            sent.message_type,
                            sent.source_job,
                            received.source_job,
                            sent.source_supervisor,
                            received.source_supervisor,
                            get_unix_timestamp_ms(sent.timestamp),
                            get_unix_timestamp_ms(received.timestamp),
                            f"{latency_ms:.2f}",
                            received.size_bytes,
                        ]
                    )

        # Output raw traces
        self._put_message_out(
            DAQJobMessageStoreTabular(
                store_config=self.config.store_config,
                keys=trace_keys,
                data=trace_data,
            )
        )

        # Output latency stats if we have any
        if latency_data:
            self._put_message_out(
                DAQJobMessageStoreTabular(
                    store_config=self.config.store_config,
                    keys=latency_keys,
                    data=latency_data,
                    tag="latency",
                )
            )

        # Broadcast combined traces for cross-supervisor visibility
        self._put_message_out(
            DAQJobMessageCombinedTraces(
                traces=dict(self._traces),
                topics={Topic.traces_combined(self.supervisor_id)},
            )
        )

        # Clear traces after sending
        self._traces.clear()
