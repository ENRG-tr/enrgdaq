#!/usr/bin/env python3
"""
ENRGDAQ Benchmark Script

This script benchmarks the ENRGDAQ system by running a supervisor instance
with benchmark jobs that stress test message throughput, serialization, and storage.

The new architecture uses ZMQ pub/sub messaging through a single supervisor's
message broker. For distributed benchmarking, use the federation feature.

Usage:
    python benchmark_runner.py [--clients N] [--payload-size N] [--duration SECONDS]

Example:
    python benchmark_runner.py --clients 5 --payload-size 10000 --duration 30
"""

import argparse
import atexit
import os
import signal
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime
from multiprocessing import Value
from statistics import fmean
from threading import Thread

import psutil

from enrgdaq.daq.daq_job import _create_daq_job_process
from enrgdaq.daq.jobs.benchmark import DAQJobBenchmark, DAQJobBenchmarkConfig
from enrgdaq.daq.jobs.handle_stats import DAQJobHandleStats, DAQJobHandleStatsConfig
from enrgdaq.daq.jobs.store.csv import DAQJobStoreCSV, DAQJobStoreCSVConfig
from enrgdaq.daq.jobs.store.memory import DAQJobStoreMemory, DAQJobStoreMemoryConfig
from enrgdaq.daq.jobs.store.root import DAQJobStoreROOT, DAQJobStoreROOTConfig
from enrgdaq.daq.store.models import (
    DAQJobStoreConfig,
    DAQJobStoreConfigCSV,
    DAQJobStoreConfigMemory,
    DAQJobStoreConfigROOT,
)
from enrgdaq.models import LogVerbosity, SupervisorConfig, SupervisorInfo
from enrgdaq.supervisor import Supervisor

# Default configuration
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
    active_job_count: int
    cpu_usage_percent: float
    rss_mb: float
    latency_p95_ms: float
    latency_p99_ms: float
    data_mb_per_s: float = 0.0


@dataclass
class BenchmarkConfig:
    """Configuration for benchmark run."""

    num_clients: int = DEFAULT_NUM_CLIENTS
    payload_size: int = DEFAULT_PAYLOAD_SIZE
    duration_seconds: int = DEFAULT_DURATION_SECONDS
    stats_interval_seconds: float = DEFAULT_STATS_INTERVAL_SECONDS
    output_stats_csv: str = "benchmark_stats.csv"
    void_memory_data: bool = True
    use_memory_store: bool = False
    use_shm: bool = True


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
            if process.process and process.process.is_alive() and process.process.pid:
                kill_process_tree(process.process.pid)
    except Exception:
        pass


class BenchmarkRunner:
    """Runs the ENRGDAQ benchmark and collects statistics."""

    def __init__(self, config: BenchmarkConfig):
        self.config = config
        self._stop_flag = Value("b", False)
        self._stats_history: list[BenchmarkStats] = []
        self._main_pid = os.getpid()
        self._supervisor: Supervisor | None = None

    def _print_stats(self, stats: BenchmarkStats):
        """Print statistics to console."""
        print(
            f"[{stats.timestamp.strftime('%H:%M:%S')}] "
            f"Data: {stats.data_mb_per_s:7.2f} MB/s | "
            f"CPU: {stats.cpu_usage_percent:5.1f}% | "
            f"p95 Latency: {stats.latency_p95_ms:5.2f}ms | "
            f"Active Jobs: {stats.active_job_count:3d}"
        )

    def _handle_signal(self, signum, frame):
        """Handle termination signals."""
        print("\nReceived termination signal, stopping...")
        self._stop_flag.value = True

    def _cleanup(self):
        """Clean up the supervisor."""
        print("\nTerminating...")
        self._stop_flag.value = True
        if self._supervisor:
            cleanup_supervisor(self._supervisor)

    def _create_supervisor(self) -> Supervisor:
        """Create and configure the benchmark supervisor."""
        config = self.config

        # Create temporary config directory for the supervisor
        temp_config_dir = tempfile.mkdtemp(prefix="enrgdaq_benchmark_")

        supervisor_id = "benchmark_supervisor"
        supervisor_info = create_supervisor_info(supervisor_id)
        supervisor_config = create_supervisor_config(supervisor_id)
        supervisor_config.ring_buffer_size_mb = 1024
        supervisor_config.ring_buffer_slot_size_kb = 10 * 1024

        # Create DAQ job processes using the proper factory function
        daq_job_processes = []

        # Benchmark jobs that generate data
        for i in range(config.num_clients):
            daq_job_processes.append(
                _create_daq_job_process(
                    DAQJobBenchmark,
                    DAQJobBenchmarkConfig(
                        daq_job_type="DAQJobBenchmark",
                        payload_size=config.payload_size,
                        use_shm=config.use_shm,
                        store_config=DAQJobStoreConfig(
                            memory=DAQJobStoreConfigMemory(),
                            target_local_supervisor=True,  # Enable SHM for local transfer
                        )
                        if config.use_memory_store
                        else DAQJobStoreConfig(
                            root=DAQJobStoreConfigROOT(
                                file_path=f"benchmark_{i}.root",
                                add_date=False,
                                tree_name="benchmark_tree",
                            ),
                            target_local_supervisor=True,  # Enable SHM for local transfer
                        ),
                    ),
                    supervisor_info,
                )
            )

        # Main store - either Memory (fast, for testing) or ROOT (slow, for production)
        if config.use_memory_store:
            daq_job_processes.append(
                _create_daq_job_process(
                    DAQJobStoreMemory,
                    DAQJobStoreMemoryConfig(
                        daq_job_type="DAQJobStoreMemory",
                        void_data=config.void_memory_data,
                    ),
                    supervisor_info,
                )
            )
        else:
            daq_job_processes.append(
                _create_daq_job_process(
                    DAQJobStoreROOT,
                    DAQJobStoreROOTConfig(
                        daq_job_type="DAQJobStoreROOT", verbosity=LogVerbosity.DEBUG
                    ),
                    supervisor_info,
                )
            )

        # CSV store for stats output
        daq_job_processes.append(
            _create_daq_job_process(
                DAQJobStoreCSV,
                DAQJobStoreCSVConfig(daq_job_type="DAQJobStoreCSV"),
                supervisor_info,
            )
        )

        # Stats handler
        daq_job_processes.append(
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
            )
        )

        return Supervisor(
            config=supervisor_config,
            daq_job_processes=daq_job_processes,
            daq_job_config_path=temp_config_dir,
        )

    def _collect_stats(self) -> BenchmarkStats | None:
        """Collect current statistics from the supervisor."""
        if self._supervisor is None or self._supervisor.config is None:
            return None

        supervisor = self._supervisor
        supervisor_id = supervisor.config.info.supervisor_id

        # Calculate active job count
        active_job_count = len(
            [
                x
                for x in supervisor.daq_job_processes
                if x.process and x.process.is_alive()
            ]
        )

        # Get byte stats from daq_job_remote_stats (like old benchmark did)
        # Format: dict[supervisor_id, SupervisorRemoteStats]
        remote_stats = supervisor.daq_job_remote_stats

        # Get our supervisor's stats
        our_stats = remote_stats.get(supervisor_id)
        if our_stats:
            msg_in_out_bytes = our_stats.message_in_bytes + our_stats.message_out_bytes
            msg_in_out_mb = msg_in_out_bytes / 10**6
            msg_in_count = our_stats.message_in_count
            msg_out_count = our_stats.message_out_count
        else:
            msg_in_out_mb = 0.0
            msg_in_count = 0
            msg_out_count = 0

        # Get latency stats from daq_job_stats
        # Format: dict[supervisor_id, dict[daq_job_type, DAQJobStats]]
        stats_nested = supervisor.daq_job_stats
        all_stats: list = []
        for sup_id, daq_job_stats_dict in stats_nested.items():
            if isinstance(daq_job_stats_dict, dict):
                for daq_job_type, job_stats in daq_job_stats_dict.items():
                    all_stats.append(job_stats)

        # Calculate CPU and Memory usage from stats
        cpu_usage = (
            sum(s.resource_stats.cpu_percent for s in all_stats) if all_stats else 0.0
        )
        rss_mb_total = (
            sum(s.resource_stats.rss_mb for s in all_stats) if all_stats else 0.0
        )

        # Calculate Latency (max of p95/p99 across jobs to be conservative)
        p95_latencies = [
            s.latency_stats.p95_ms for s in all_stats if s.latency_stats.count > 0
        ]
        p99_latencies = [
            s.latency_stats.p99_ms for s in all_stats if s.latency_stats.count > 0
        ]

        latency_p95 = max(p95_latencies) if p95_latencies else 0.0
        latency_p99 = max(p99_latencies) if p99_latencies else 0.0

        return BenchmarkStats(
            timestamp=datetime.now(),
            supervisor_id=supervisor_id,
            msg_in_out_mb=msg_in_out_mb,
            msg_in_count=msg_in_count,
            msg_out_count=msg_out_count,
            msg_in_out_mb_per_s=0.0,  # Will be calculated from deltas
            active_job_count=active_job_count,
            cpu_usage_percent=cpu_usage,
            rss_mb=rss_mb_total,
            latency_p95_ms=latency_p95,
            latency_p99_ms=latency_p99,
            data_mb_per_s=0.0,  # Will be calculated from deltas
        )

    def run(self):
        """Run the benchmark."""
        # Set up signal handlers
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

        # Register cleanup on exit
        atexit.register(self._cleanup)

        print("=" * 80)
        print("ENRGDAQ Benchmark")
        print("=" * 80)
        print("Configuration:")
        print(f"  - Benchmark Jobs: {self.config.num_clients}")
        print(f"  - Payload Size:   {self.config.payload_size} values/message")
        print(f"  - Duration:       {self.config.duration_seconds} seconds")
        print(f"  - Use SHM:        {self.config.use_shm}")
        print(
            f"  - Store Type:     {'Memory' if self.config.use_memory_store else 'ROOT'}"
        )
        print("=" * 80)
        print()

        # Clean up any existing output files from previous runs
        output_files_to_clean = ["out/benchmark_*.root"]
        for pattern in output_files_to_clean:
            import glob

            for output_file in glob.glob(pattern):
                os.remove(output_file)
                print(f"Removed existing output file: {output_file}")

        # Create and initialize supervisor
        print("Creating supervisor...")
        self._supervisor = self._create_supervisor()

        print("Initializing supervisor...")
        self._supervisor.init()

        # Start supervisor in a separate thread
        supervisor_thread = Thread(target=self._supervisor.run, daemon=True)
        supervisor_thread.start()

        print()
        print("Benchmark running... (waiting for first data)")
        print("-" * 80)

        # Timer starts when first data arrives, not now
        start_time: datetime | None = None
        end_time_seconds = self.config.duration_seconds
        last_stats: BenchmarkStats | None = None
        last_iteration = datetime.now()

        try:
            while not self._stop_flag.value:
                current_stats = self._collect_stats()

                if current_stats is None:
                    time.sleep(self.config.stats_interval_seconds)
                    continue

                # Start timer when first data arrives
                if start_time is None and current_stats.msg_in_count > 0:
                    start_time = datetime.now()
                    print("First data received, starting timer...")

                # Calculate deltas for MB/s
                now = datetime.now()
                elapsed = (now - last_iteration).total_seconds()
                if last_stats and elapsed > 0:
                    mb_diff = current_stats.msg_in_out_mb - last_stats.msg_in_out_mb
                    current_stats.msg_in_out_mb_per_s = mb_diff / elapsed

                    # Calculate data throughput
                    msg_diff = current_stats.msg_in_count - last_stats.msg_in_count
                    msg_size_bytes = self.config.payload_size * 16  # 2 float64 columns
                    current_stats.data_mb_per_s = (
                        (msg_diff * msg_size_bytes) / elapsed / 10**6
                    )

                self._stats_history.append(current_stats)
                self._print_stats(current_stats)

                last_stats = current_stats
                last_iteration = now

                # Check duration (only if timer has started)
                if start_time is not None:
                    elapsed = (datetime.now() - start_time).total_seconds()
                    if elapsed >= end_time_seconds:
                        print(f"\nDuration of {end_time_seconds}s reached, stopping...")
                        break

                time.sleep(self.config.stats_interval_seconds)

        finally:
            self._stop_flag.value = True

            # Print summary
            self._print_summary()

            # Clean up
            self._cleanup()

            # Unregister atexit since we've already cleaned up
            try:
                atexit.unregister(self._cleanup)
            except Exception:
                pass

    def _print_summary(self):
        """Print benchmark summary statistics."""
        # Filter to stats with actual data, skipping the first 3 seconds of warmup
        data_stats = [s for s in self._stats_history if s.msg_in_count > 0]
        if len(data_stats) > 3:
            data_stats = data_stats[3:]

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
        avg_data_throughput = fmean([s.data_mb_per_s for s in data_stats])
        max_data_throughput = max([s.data_mb_per_s for s in data_stats])
        total_mb = data_stats[-1].msg_in_out_mb
        total_msgs = data_stats[-1].msg_in_count

        print(f"Duration:              {total_duration:.1f} seconds")
        print(f"Avg Data Throughput:   {avg_data_throughput:.2f} MB/s")
        print(f"Peak Data Throughput:  {max_data_throughput:.2f} MB/s")
        print(f"Total Data:            {total_mb:.2f} MB")
        print(f"ZMQ Throughput:        {avg_throughput:.2f} MB/s")
        print(f"Total Messages:        {total_msgs:,}")
        print(f"Messages/Second:       {total_msgs / total_duration:,.0f}")
        print(
            f"Avg CPU Usage:         {fmean([s.cpu_usage_percent for s in data_stats]):.1f}%"
        )
        print(
            f"Avg p95 Latency:       {fmean([s.latency_p95_ms for s in data_stats]):.2f} ms"
        )
        print(
            f"Peak p99 Latency:      {max([s.latency_p99_ms for s in data_stats]):.2f} ms"
        )
        print("=" * 80)


def parse_args() -> BenchmarkConfig:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="ENRGDAQ Benchmark Script",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python benchmark_runner.py                           # Run with defaults
  python benchmark_runner.py --clients 10              # Run with 10 benchmark jobs
  python benchmark_runner.py --payload-size 50000      # Larger payloads
  python benchmark_runner.py --duration 120            # Run for 2 minutes
        """,
    )
    parser.add_argument(
        "--clients",
        type=int,
        default=DEFAULT_NUM_CLIENTS,
        help=f"Number of benchmark jobs (default: {DEFAULT_NUM_CLIENTS})",
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
        "--no-void-data",
        action="store_true",
        help="Don't void memory store data (uses more memory)",
    )
    parser.add_argument(
        "--use-shm",
        action="store_true",
        default=True,
        help="Use SHM for zero-copy (default: True)",
    )
    parser.add_argument(
        "--no-shm",
        action="store_false",
        dest="use_shm",
        help="Disable SHM",
    )
    parser.add_argument(
        "--use-memory-store",
        action="store_true",
        help="Use Memory store instead of ROOT store",
    )

    args = parser.parse_args()

    return BenchmarkConfig(
        num_clients=args.clients,
        payload_size=args.payload_size,
        duration_seconds=args.duration,
        stats_interval_seconds=args.stats_interval,
        void_memory_data=not args.no_void_data,
        use_memory_store=args.use_memory_store,
        use_shm=args.use_shm,
    )


if __name__ == "__main__":
    config = parse_args()
    runner = BenchmarkRunner(config)
    runner.run()
