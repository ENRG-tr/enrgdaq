"""
Topic builder for ZMQ pub/sub message routing.

This module centralizes all topic string construction patterns used in the
ENRGDAQ framework. Using these methods instead of hand-coded strings ensures
consistency, enables IDE autocomplete, and makes refactoring easier.

Topic Hierarchy:
    supervisor.{id}.daq_job.broadcast - Broadcast to all jobs under a supervisor
    supervisor.{id}.internal          - Internal supervisor messages
    daq_job.{class}.{unique_id}       - Direct addressing to specific job instance
    store.{class}                     - Store routing (e.g., store.DAQJobStoreCSV)
    stats.{id}                        - Stats from jobs to stats handler
    stats                             - Stats handler subscription prefix
"""

from typing import final


@final
class Topic:
    """Centralized topic builder for ZMQ pub/sub message routing."""

    # Topic prefixes
    SUPERVISOR = "supervisor"
    DAQ_JOB = "daq_job"
    STORE = "store"
    STATS = "stats"

    # Topic suffixes
    BROADCAST = "broadcast"
    INTERNAL = "internal"

    @staticmethod
    def supervisor_broadcast(supervisor_id: str) -> str:
        """
        Topic for broadcasting to all DAQJobs under a supervisor.

        Args:
            supervisor_id: The supervisor's unique identifier.

        Returns:
            Topic string: supervisor.{supervisor_id}.daq_job.broadcast
        """
        return f"{Topic.SUPERVISOR}.{supervisor_id}.{Topic.DAQ_JOB}.{Topic.BROADCAST}"

    @staticmethod
    def supervisor_internal(supervisor_id: str) -> str:
        """
        Topic for internal supervisor messages (e.g., routes, job started).

        Args:
            supervisor_id: The supervisor's unique identifier.

        Returns:
            Topic string: supervisor.{supervisor_id}.internal
        """
        return f"{Topic.SUPERVISOR}.{supervisor_id}.{Topic.INTERNAL}"

    @staticmethod
    def daq_job_direct(job_class_name: str, unique_id: str) -> str:
        """
        Topic for direct messages to a specific DAQJob instance.

        Args:
            job_class_name: The DAQJob class name (e.g., DAQJobStoreCSV).
            unique_id: The job's unique identifier.

        Returns:
            Topic string: daq_job.{job_class_name}.{unique_id}
        """
        return f"{Topic.DAQ_JOB}.{job_class_name}.{unique_id}"

    @staticmethod
    def store(store_class_name: str) -> str:
        """
        Topic for messages routed to a specific store type.

        Args:
            store_class_name: The store class name (e.g., DAQJobStoreCSV).

        Returns:
            Topic string: store.{store_class_name}
        """
        return f"{Topic.STORE}.{store_class_name}"

    @staticmethod
    def stats(supervisor_id: str) -> str:
        """
        Topic for stats messages from DAQJobs to stats handler.

        Args:
            supervisor_id: The supervisor's unique identifier.

        Returns:
            Topic string: stats.supervisor.{supervisor_id}
        """
        return f"{Topic.STATS}.supervisor.{supervisor_id}"

    @staticmethod
    def stats_combined(supervisor_id: str) -> str:
        """
        Topic for combined stats messages from DAQJobs to stats handler.

        Args:
            supervisor_id: The supervisor's unique identifier.

        Returns:
            Topic string: stats.combined.supervisor.{supervisor_id}
        """
        return f"{Topic.STATS}.combined.supervisor.{supervisor_id}"

    @staticmethod
    def stats_prefix() -> str:
        """
        Topic prefix for stats handler subscription (receives all stats).

        Returns:
            Topic string: stats
        """
        return Topic.STATS
