#!/usr/bin/env python3
"""
ENRGDAQ Benchmark Script

This script benchmarks the ENRGDAQ system by running multiple supervisor instances
with benchmark jobs that stress test message throughput, serialization, and networking.

Usage:
    python benchmark.py [--clients N] [--payload-size N] [--duration SECONDS]

Example:
    python benchmark.py --clients 5 --payload-size 10000 --duration 30
"""

import argparse
import atexit
import os
import signal
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime
from multiprocessing import Event, Process, Value
from statistics import fmean
from threading import Thread
from typing import Any, Optional

import psutil

from enrgdaq.daq.base import _create_queue
from enrgdaq.daq.daq_job import _create_daq_job_process
from enrgdaq.daq.jobs.benchmark import DAQJobBenchmark, DAQJobBenchmarkConfig
from enrgdaq.daq.jobs.handle_stats import DAQJobHandleStats, DAQJobHandleStatsConfig
from enrgdaq.daq.jobs.remote import DAQJobRemote, DAQJobRemoteConfig
from enrgdaq.daq.jobs.remote_proxy import DAQJobRemoteProxy, DAQJobRemoteProxyConfig
from enrgdaq.daq.jobs.store.csv import DAQJobStoreCSV, DAQJobStoreCSVConfig
from enrgdaq.daq.jobs.store.root import DAQJobStoreROOT, DAQJobStoreROOTConfig
from enrgdaq.daq.models import LogVerbosity
from enrgdaq.daq.store.models import (
    DAQJobStoreConfig,
    DAQJobStoreConfigCSV,
    DAQJobStoreConfigROOT,
)
from enrgdaq.models import SupervisorConfig, SupervisorInfo
from enrgdaq.supervisor import Supervisor

# Default configuration
DEFAULT_ZMQ_XSUB_URL = "tcp://localhost:10001"
DEFAULT_ZMQ_XPUB_URL = "tcp://localhost:10002"
DEFAULT_NUM_CLIENTS = 5
DEFAULT_PAYLOAD_SIZE = 1000
DEFAULT_DURATION_SECONDS = 60
DEFAULT_STATS_INTERVAL_SECONDS = 1


@dataclass
class BenchmarkStats:
    """Statistics collected during benchmark run."""

    timestamp: datetime
    supervisor_id: str
    msg_in_out_mb: float
    msg_in_count: int
    msg_out_count: int
    msg_in_out_mb_per_s: float
    avg_queue_size: float
    active_job_count: int


@dataclass
class BenchmarkConfig:
    """Configuration for benchmark run."""

    num_clients: int = DEFAULT_NUM_CLIENTS
    payload_size: int = DEFAULT_PAYLOAD_SIZE
    duration_seconds: int = DEFAULT_DURATION_SECONDS
    stats_interval_seconds: float = DEFAULT_STATS_INTERVAL_SECONDS
    zmq_xsub_url: str = DEFAULT_ZMQ_XSUB_URL
    zmq_xpub_url: str = DEFAULT_ZMQ_XPUB_URL
    output_stats_csv: str = "benchmark_stats.csv"
    output_remote_stats_csv: str = "benchmark_remote_stats.csv"
    void_memory_data: bool = True


def create_supervisor_info(supervisor_id: str) -> SupervisorInfo:
    """Create a SupervisorInfo instance."""
    return SupervisorInfo(supervisor_id=supervisor_id)


def create_supervisor_config(supervisor_id: str) -> SupervisorConfig:
    """Create a SupervisorConfig instance."""
    return SupervisorConfig(info=create_supervisor_info(supervisor_id))


def kill_process_tree(pid: int, sig=signal.SIGTERM):
    """Kill a process and all its children using psutil."""
    try:
        parent = psutil.Process(pid)
        children = parent.children(recursive=True)

        # Kill children first
        for child in children:
            try:
                child.send_signal(sig)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass

        # Kill parent
        try:
            parent.send_signal(sig)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass

        # Wait for processes to terminate
        gone, alive = psutil.wait_procs(children + [parent], timeout=1)

        # Force kill any remaining
        for p in alive:
            try:
                p.kill()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass

    except psutil.NoSuchProcess:
        pass


def cleanup_supervisor(supervisor: Supervisor):
    """Clean up supervisor and all its child processes."""
    try:
        supervisor.stop()
        # Give a moment for clean shutdown
        time.sleep(0.1)
        # Force kill any remaining DAQ job processes
        for process in supervisor.daq_job_processes:
            if process.process and process.process.is_alive():
                kill_process_tree(process.process.pid)
    except Exception:
        pass


def run_main_supervisor(
    config: BenchmarkConfig,
    stats_queue: Any,
    stop_flag: Value,
):
    """Run the main supervisor that collects stats and runs the proxy."""

    # Create temporary config directory for the supervisor
    temp_config_dir = tempfile.mkdtemp(prefix="enrgdaq_benchmark_")

    supervisor_id = "benchmark_supervisor"
    supervisor_info = create_supervisor_info(supervisor_id)
    supervisor_config = create_supervisor_config(supervisor_id)

    # Create DAQ job processes using the proper factory function
    daq_job_processes = [
        # Memory store that voids data (for benchmark purposes)
        _create_daq_job_process(
            # DAQJobStoreMemory,
            DAQJobStoreROOT,
            # DAQJobStoreMemoryConfig(
            #    daq_job_type="DAQJobStoreMemory",
            #    dispose_after_n_entries=10,
            #    void_data=config.void_memory_data,
            # ),
            DAQJobStoreROOTConfig(
                daq_job_type="DAQJobStoreROOT", verbosity=LogVerbosity.DEBUG
            ),
            supervisor_info,
        ),
        # CSV store for stats output
        _create_daq_job_process(
            DAQJobStoreCSV,
            DAQJobStoreCSVConfig(daq_job_type="DAQJobStoreCSV"),
            supervisor_info,
        ),
        # Remote job for receiving from clients
        _create_daq_job_process(
            DAQJobRemote,
            DAQJobRemoteConfig(
                daq_job_type="DAQJobRemote",
                zmq_proxy_sub_urls=[config.zmq_xpub_url],
            ),
            supervisor_info,
        ),
        # Stats handler
        _create_daq_job_process(
            DAQJobHandleStats,
            DAQJobHandleStatsConfig(
                daq_job_type="DAQJobHandleStats",
                store_config=DAQJobStoreConfig(
                    csv=DAQJobStoreConfigCSV(
                        file_path=config.output_stats_csv,
                        overwrite=True,
                    ),
                ),
            ),
            supervisor_info,
        ),
        # Remote proxy (XSUB/XPUB)
        _create_daq_job_process(
            DAQJobRemoteProxy,
            DAQJobRemoteProxyConfig(
                daq_job_type="DAQJobRemoteProxy",
                zmq_xsub_url=config.zmq_xsub_url,
                zmq_xpub_url=config.zmq_xpub_url,
            ),
            supervisor_info,
        ),
    ]

    supervisor = Supervisor(
        config=supervisor_config,
        daq_job_processes=daq_job_processes,
        daq_job_config_path=temp_config_dir,
    )

    # Register cleanup on exit
    atexit.register(cleanup_supervisor, supervisor)

    run_supervisor_with_stats(supervisor, config, stats_queue, stop_flag)


def run_client_supervisor(
    client_id: int,
    config: BenchmarkConfig,
    stop_flag: Value,
    ready_event: Event,
):
    """Run a client supervisor that generates benchmark data."""

    # Create temporary config directory for the supervisor
    temp_config_dir = tempfile.mkdtemp(prefix="enrgdaq_benchmark_client_")

    supervisor_id = f"benchmark_client_{client_id}"
    supervisor_info = create_supervisor_info(supervisor_id)
    supervisor_config = create_supervisor_config(supervisor_id)

    daq_job_processes = [
        # Benchmark job that generates data
        _create_daq_job_process(
            DAQJobBenchmark,
            DAQJobBenchmarkConfig(
                daq_job_type="DAQJobBenchmark",
                payload_size=config.payload_size,
                use_shm=True,
                store_config=DAQJobStoreConfig(
                    root=DAQJobStoreConfigROOT(
                        file_path="test.root",
                        add_date=False,
                        tree_name="benchmark_tree",
                    )
                ),
            ),
            supervisor_info,
        ),
        # Remote job for sending to main supervisor
        _create_daq_job_process(
            DAQJobRemote,
            DAQJobRemoteConfig(
                daq_job_type="DAQJobRemote",
                zmq_proxy_sub_urls=[],
                zmq_proxy_pub_url=config.zmq_xsub_url,
            ),
            supervisor_info,
        ),
    ]

    supervisor = Supervisor(
        config=supervisor_config,
        daq_job_processes=daq_job_processes,
        daq_job_config_path=temp_config_dir,
    )

    # Register cleanup on exit
    atexit.register(cleanup_supervisor, supervisor)

    # Initialize supervisor and signal ready
    supervisor.init()
    ready_event.set()  # Signal that this client is ready

    run_supervisor_with_stats(
        supervisor, config, None, stop_flag, collect_stats=False, skip_init=True
    )


def run_supervisor_with_stats(
    supervisor: Supervisor,
    config: BenchmarkConfig,
    stats_queue: Any,
    stop_flag: Value,
    collect_stats: bool = True,
    skip_init: bool = False,
):
    """Run a supervisor and optionally collect stats."""
    assert supervisor.config is not None

    if not skip_init:
        supervisor.init()

    # Start supervisor in a separate thread
    supervisor_thread = Thread(target=supervisor.run, daemon=True)
    supervisor_thread.start()

    try:
        if not collect_stats or stats_queue is None:
            # Just keep the process alive
            while not stop_flag.value:
                time.sleep(0.5)
        else:
            # Collect and report stats
            last_stats: Optional[dict] = None
            last_iteration = datetime.now()

            while not stop_flag.value:
                # Get stats from remote stats dict
                stats_list = [
                    v
                    for k, v in supervisor.daq_job_remote_stats.items()
                    if k == supervisor.config.info.supervisor_id
                ]

                # Calculate current stats
                msg_in_out_mb = (
                    sum([x.message_in_bytes + x.message_out_bytes for x in stats_list])
                    / 10**6
                    if stats_list
                    else 0.0
                )
                msg_in_count = (
                    sum([x.message_in_count for x in stats_list]) if stats_list else 0
                )
                msg_out_count = (
                    sum([x.message_out_count for x in stats_list]) if stats_list else 0
                )

                # Calculate queue sizes (macOS doesn't support qsize, so use fallback)
                try:
                    avg_queue_size = fmean(
                        [
                            x.message_out.qsize() + x.message_in.qsize()
                            for x in supervisor.daq_job_processes
                        ]
                    )
                except (NotImplementedError, Exception):
                    avg_queue_size = 0.0

                # Calculate active job count
                active_job_count = len(
                    [
                        x
                        for x in supervisor.daq_job_processes
                        if x.process and x.process.is_alive()
                    ]
                )

                # Calculate MB/s
                now = datetime.now()
                elapsed = (now - last_iteration).total_seconds()
                if last_stats and elapsed > 0:
                    mb_diff = msg_in_out_mb - last_stats["msg_in_out_mb"]
                    msg_in_out_mb_per_s = mb_diff / elapsed
                else:
                    msg_in_out_mb_per_s = 0.0

                current_stats = {
                    "timestamp": now.isoformat(),
                    "supervisor_id": supervisor.config.info.supervisor_id,
                    "msg_in_out_mb": msg_in_out_mb,
                    "msg_in_count": msg_in_count,
                    "msg_out_count": msg_out_count,
                    "msg_in_out_mb_per_s": msg_in_out_mb_per_s,
                    "avg_queue_size": avg_queue_size,
                    "active_job_count": active_job_count,
                }

                try:
                    stats_queue.put_nowait(current_stats)
                except Exception:
                    pass

                last_stats = current_stats
                last_iteration = now

                time.sleep(config.stats_interval_seconds)
    finally:
        # Always clean up the supervisor
        cleanup_supervisor(supervisor)


class BenchmarkRunner:
    """Runs the ENRGDAQ benchmark and collects statistics."""

    def __init__(self, config: BenchmarkConfig):
        self.config = config
        self._stats_queue: Any = _create_queue()
        self._stop_flag = Value("b", False)
        self._processes: list[Process] = []
        self._stats_history: list[BenchmarkStats] = []
        self._main_pid = os.getpid()

    def _dict_to_stats(self, d: dict) -> BenchmarkStats:
        """Convert dictionary to BenchmarkStats."""
        return BenchmarkStats(
            timestamp=datetime.fromisoformat(d["timestamp"]),
            supervisor_id=d["supervisor_id"],
            msg_in_out_mb=d["msg_in_out_mb"],
            msg_in_count=d["msg_in_count"],
            msg_out_count=d["msg_out_count"],
            msg_in_out_mb_per_s=d["msg_in_out_mb_per_s"],
            avg_queue_size=d["avg_queue_size"],
            active_job_count=d["active_job_count"],
        )

    def _print_stats(self, stats: BenchmarkStats):
        """Print statistics to console."""
        print(
            f"[{stats.timestamp.strftime('%H:%M:%S')}] "
            f"Throughput: {stats.msg_in_out_mb_per_s:7.2f} MB/s | "
            f"Total: {stats.msg_in_out_mb:10.2f} MB | "
            f"Msgs In: {stats.msg_in_count:8d} | "
            f"Msgs Out: {stats.msg_out_count:8d} | "
            f"Avg Queue: {stats.avg_queue_size:5.1f} | "
            f"Active Jobs: {stats.active_job_count:3d}"
        )

    def _handle_signal(self, signum, frame):
        """Handle termination signals."""
        print("\nReceived termination signal, stopping...")
        self._stop_flag.value = True

    def _cleanup_all_processes(self):
        """Forcefully clean up all child processes using psutil."""
        print("\nTerminating processes...")

        # First, signal all processes to stop gracefully
        self._stop_flag.value = True

        # Give processes time to clean up their children
        time.sleep(0.5)

        # Kill entire process tree for each child process
        for p in self._processes:
            if p.pid:
                kill_process_tree(p.pid)

        # Also terminate using Process API as backup
        for p in self._processes:
            try:
                if p.is_alive():
                    p.terminate()
            except Exception:
                pass

        # Wait for processes to terminate
        for p in self._processes:
            try:
                p.join(timeout=1)
            except Exception:
                pass

        # Force kill any remaining processes
        for p in self._processes:
            try:
                if p.is_alive():
                    p.kill()
            except Exception:
                pass

    def run(self):
        """Run the benchmark."""
        # Set up signal handlers
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

        # Register cleanup on exit
        atexit.register(self._cleanup_all_processes)

        print("=" * 80)
        print("ENRGDAQ Benchmark")
        print("=" * 80)
        print("Configuration:")
        print(f"  - Clients:        {self.config.num_clients}")
        print(f"  - Payload Size:   {self.config.payload_size} values/message")
        print(f"  - Duration:       {self.config.duration_seconds} seconds")
        print(f"  - ZMQ XSUB URL:   {self.config.zmq_xsub_url}")
        print(f"  - ZMQ XPUB URL:   {self.config.zmq_xpub_url}")
        print("=" * 80)
        print()

        # Start main supervisor process
        print("Starting main supervisor...")
        main_process = Process(
            target=run_main_supervisor,
            args=(self.config, self._stats_queue, self._stop_flag),
        )
        main_process.start()
        self._processes.append(main_process)

        # Give main supervisor time to start (ZMQ needs to bind first)
        time.sleep(1)

        # Create ready events for each client
        ready_events = [Event() for _ in range(self.config.num_clients)]

        # Start client processes
        print(f"Starting {self.config.num_clients} client(s)...")
        for i in range(self.config.num_clients):
            client_process = Process(
                target=run_client_supervisor,
                args=(i, self.config, self._stop_flag, ready_events[i]),
            )
            client_process.start()
            self._processes.append(client_process)

        # Wait for all clients to signal ready (with timeout)
        print("Waiting for clients to be ready...")
        all_ready = all(event.wait(timeout=30) for event in ready_events)
        if not all_ready:
            print("WARNING: Not all clients signaled ready within timeout")

        print()
        print("Benchmark running... (waiting for first data)")
        print("-" * 80)

        # Timer starts when first data arrives, not now
        start_time: datetime | None = None
        end_time_seconds = self.config.duration_seconds

        try:
            while not self._stop_flag.value:
                # Check for stats in queue
                try:
                    stats_dict = self._stats_queue.get(timeout=0.5)
                    stats = self._dict_to_stats(stats_dict)

                    # Start timer when first data arrives
                    if start_time is None and stats.msg_in_count > 0:
                        start_time = datetime.now()
                        print("First data received, starting timer...")

                    self._stats_history.append(stats)
                    self._print_stats(stats)
                except Exception:
                    pass

                # Check duration (only if timer has started)
                if start_time is not None:
                    elapsed = (datetime.now() - start_time).total_seconds()
                    if elapsed >= end_time_seconds:
                        print(f"\nDuration of {end_time_seconds}s reached, stopping...")
                        break

        finally:
            self._stop_flag.value = True

            # Print summary
            self._print_summary()

            # Clean up all processes
            self._cleanup_all_processes()

            # Unregister atexit since we've already cleaned up
            try:
                atexit.unregister(self._cleanup_all_processes)
            except Exception:
                pass

    def _print_summary(self):
        """Print benchmark summary statistics."""
        # Filter to stats with actual data
        data_stats = [s for s in self._stats_history if s.msg_in_count > 0]

        if not data_stats:
            print("\nNo statistics collected.")
            return

        print()
        print("=" * 80)
        print("Benchmark Summary")
        print("=" * 80)

        # Calculate duration from first data to last data
        total_duration = (
            data_stats[-1].timestamp - data_stats[0].timestamp
        ).total_seconds()

        if total_duration <= 0:
            total_duration = 1.0

        avg_throughput = fmean([s.msg_in_out_mb_per_s for s in data_stats])
        max_throughput = max([s.msg_in_out_mb_per_s for s in data_stats])
        total_mb = data_stats[-1].msg_in_out_mb
        total_msgs = data_stats[-1].msg_in_count
        avg_queue = fmean([s.avg_queue_size for s in data_stats])

        print(f"Duration:              {total_duration:.1f} seconds")
        print(f"Average Throughput:    {avg_throughput:.2f} MB/s")
        print(f"Peak Throughput:       {max_throughput:.2f} MB/s")
        print(f"Total Data:            {total_mb:.2f} MB")
        print(f"Total Messages:        {total_msgs:,}")
        print(f"Average Queue Size:    {avg_queue:.1f}")
        print(f"Messages/Second:       {total_msgs / total_duration:,.0f}")
        print("=" * 80)


def parse_args() -> BenchmarkConfig:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="ENRGDAQ Benchmark Script",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python benchmark.py                           # Run with defaults
  python benchmark.py --clients 10              # Run with 10 clients
  python benchmark.py --payload-size 50000      # Larger payloads
  python benchmark.py --duration 120            # Run for 2 minutes
        """,
    )
    parser.add_argument(
        "--clients",
        type=int,
        default=DEFAULT_NUM_CLIENTS,
        help=f"Number of benchmark clients (default: {DEFAULT_NUM_CLIENTS})",
    )
    parser.add_argument(
        "--payload-size",
        type=int,
        default=DEFAULT_PAYLOAD_SIZE,
        help=f"Number of values per message (default: {DEFAULT_PAYLOAD_SIZE})",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=DEFAULT_DURATION_SECONDS,
        help=f"Benchmark duration in seconds (default: {DEFAULT_DURATION_SECONDS})",
    )
    parser.add_argument(
        "--stats-interval",
        type=float,
        default=DEFAULT_STATS_INTERVAL_SECONDS,
        help=f"Stats collection interval in seconds (default: {DEFAULT_STATS_INTERVAL_SECONDS})",
    )
    parser.add_argument(
        "--zmq-xsub",
        type=str,
        default=DEFAULT_ZMQ_XSUB_URL,
        help=f"ZMQ XSUB URL (default: {DEFAULT_ZMQ_XSUB_URL})",
    )
    parser.add_argument(
        "--zmq-xpub",
        type=str,
        default=DEFAULT_ZMQ_XPUB_URL,
        help=f"ZMQ XPUB URL (default: {DEFAULT_ZMQ_XPUB_URL})",
    )
    parser.add_argument(
        "--no-void-data",
        action="store_true",
        help="Don't void memory store data (uses more memory)",
    )

    args = parser.parse_args()

    return BenchmarkConfig(
        num_clients=args.clients,
        payload_size=args.payload_size,
        duration_seconds=args.duration,
        stats_interval_seconds=args.stats_interval,
        zmq_xsub_url=args.zmq_xsub,
        zmq_xpub_url=args.zmq_xpub,
        void_memory_data=not args.no_void_data,
    )


if __name__ == "__main__":
    config = parse_args()
    runner = BenchmarkRunner(config)
    runner.run()
