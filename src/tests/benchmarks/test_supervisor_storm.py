#!/usr/bin/env python3
import tempfile
import time
from multiprocessing import Value
from threading import Thread

from enrgdaq.daq.daq_job import _create_daq_job_process
from enrgdaq.daq.jobs.benchmark import DAQJobBenchmark, DAQJobBenchmarkConfig
from enrgdaq.daq.jobs.store.memory import DAQJobStoreMemory, DAQJobStoreMemoryConfig
from enrgdaq.daq.store.models import DAQJobStoreConfig, DAQJobStoreConfigMemory
from enrgdaq.models import SupervisorConfig, SupervisorInfo
from enrgdaq.supervisor import Supervisor


def run_storm_test():
    """
    Supervisor Storm Test:
    Floods the supervisor with control messages while a high-speed data job is running.
    Objective: Prove data path is decoupled from control path.
    """
    print("=" * 60)
    print("SUPERVISOR STORM TEST (JINST Rigor)")
    print("=" * 60)

    # 1. Setup Supervisor
    temp_config_dir = tempfile.mkdtemp(prefix="enrgdaq_storm_")
    supervisor_info = SupervisorInfo(supervisor_id="storm_test_node")
    supervisor_config = SupervisorConfig(info=supervisor_info)

    stop_flag = Value("b", False)
    _ = stop_flag  # Mark as used for lint

    daq_job_processes = [
        _create_daq_job_process(
            DAQJobBenchmark,
            DAQJobBenchmarkConfig(
                daq_job_type="DAQJobBenchmark",
                payload_size=100000,  # Large payload for high throughput
                use_shm=True,
                store_config=DAQJobStoreConfig(memory=DAQJobStoreConfigMemory()),
            ),
            supervisor_info,
        ),
        _create_daq_job_process(
            DAQJobStoreMemory,
            DAQJobStoreMemoryConfig(daq_job_type="DAQJobStoreMemory", void_data=True),
            supervisor_info,
        ),
    ]

    supervisor = Supervisor(
        config=supervisor_config,
        daq_job_processes=daq_job_processes,
        daq_job_config_path=temp_config_dir,
    )

    supervisor.init()

    # Run supervisor in thread
    t = Thread(target=supervisor.run, daemon=True)
    t.start()

    print("System started. Warming up (5s)...")
    time.sleep(5)

    # 2. Baseline measurement
    def get_store_msgs():
        for job_cls in supervisor.daq_job_stats:
            if hasattr(job_cls, "__name__") and "StoreMemory" in job_cls.__name__:
                return supervisor.daq_job_stats[job_cls].message_in_stats.count
        return 0

    print("Measuring baseline (10s)...")
    m0 = get_store_msgs()
    time.sleep(10)
    m1 = get_store_msgs()
    baseline_rate = (m1 - m0) / 10.0
    print(f"Baseline Data Rate (Store): {baseline_rate:.1f} messages/sec")

    # 3. START THE STORM
    print("\nStarting Control Storm (Status flooding @ ~200 Hz)...")
    storm_active = True
    storm_count = [0]

    def storm_thread():
        while storm_active:
            try:
                # Hammer the supervisor's main loop via get_status()
                supervisor.get_status()
                storm_count[0] += 1
                time.sleep(0.005)  # ~200 Hz
            except Exception:
                pass

    st = Thread(target=storm_thread, daemon=True)
    st.start()

    # 4. Measure during storm
    time.sleep(2)  # let the storm settle
    print("Measuring during storm (10s)...")
    m2 = get_store_msgs()
    time.sleep(10)
    m3 = get_store_msgs()
    storm_rate = (m3 - m2) / 10.0

    print(f"Storm Data Rate (Store):    {storm_rate:.1f} messages/sec")
    print(f"Control Messages:   {storm_count[0]} handled during test")

    # 5. Result
    diff = abs(storm_rate - baseline_rate) / (baseline_rate if baseline_rate > 0 else 1)
    print(f"\nThroughput Variance: {diff*100:.2f}%")

    if diff < 0.20:  # Allow 20% variance for OS scheduling noise on Mac
        print("\n[SUCCESS] Supervisor is resilient to control message storms.")
        print("Data throughput remained stable within 20% margin.")
    else:
        print("\n[WARNING] Significant throughput drop detected during storm.")

    global storm_active_flag
    storm_active_flag = False
    supervisor.stop()
    print("=" * 60)


if __name__ == "__main__":
    storm_active_flag = True
    run_storm_test()
