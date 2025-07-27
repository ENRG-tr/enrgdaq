from multiprocessing import Process

from enrgdaq.daq.jobs.benchmark import DAQJobBenchmark, DAQJobBenchmarkConfig
from enrgdaq.daq.jobs.handle_stats import DAQJobHandleStats, DAQJobHandleStatsConfig
from enrgdaq.daq.jobs.remote import DAQJobRemote, DAQJobRemoteConfig
from enrgdaq.daq.jobs.remote_proxy import DAQJobRemoteProxy, DAQJobRemoteProxyConfig
from enrgdaq.daq.jobs.store.csv import DAQJobStoreCSV, DAQJobStoreCSVConfig
from enrgdaq.daq.jobs.store.memory import DAQJobStoreMemory, DAQJobStoreMemoryConfig
from enrgdaq.daq.store.models import (
    DAQJobStoreConfig,
    DAQJobStoreConfigCSV,
    DAQJobStoreConfigMemory,
)
from enrgdaq.models import SupervisorConfig
from enrgdaq.supervisor import Supervisor


def run_supervisor():
    supervisor_config = SupervisorConfig(supervisor_id="benchmark_supervisor")
    supervisor = Supervisor(
        config=supervisor_config,
        daq_jobs=[
            DAQJobStoreMemory(
                config=DAQJobStoreMemoryConfig(
                    daq_job_type="", dispose_after_n_entries=10
                ),
                supervisor_config=supervisor_config,
            ),
            DAQJobStoreCSV(config=DAQJobStoreCSVConfig(daq_job_type="")),
            DAQJobRemote(
                config=DAQJobRemoteConfig(
                    daq_job_type="",
                    zmq_proxy_sub_urls=["tcp://localhost:10002"],
                ),
                supervisor_config=supervisor_config,
            ),
            DAQJobHandleStats(
                config=DAQJobHandleStatsConfig(
                    daq_job_type="",
                    store_config=DAQJobStoreConfig(
                        csv=DAQJobStoreConfigCSV(
                            file_path="benchmark_stats.csv",
                            overwrite=True,
                        ),
                    ),
                )
            ),
            DAQJobRemoteProxy(
                config=DAQJobRemoteProxyConfig(
                    daq_job_type="",
                    zmq_xsub_url="tcp://localhost:10001",
                    zmq_xpub_url="tcp://localhost:10002",
                ),
                supervisor_config=supervisor_config,
            ),
        ],
    )
    supervisor.init()
    supervisor.run()


def run_client(id: int):
    supervisor_config = SupervisorConfig(supervisor_id="client_supervisor_" + str(id))
    supervisor = Supervisor(
        config=supervisor_config,
        daq_jobs=[
            DAQJobBenchmark(
                config=DAQJobBenchmarkConfig(
                    daq_job_type="",
                    payload_size=100000,
                    store_config=DAQJobStoreConfig(memory=DAQJobStoreConfigMemory()),
                ),
                supervisor_config=supervisor_config,
            ),
            DAQJobRemote(
                config=DAQJobRemoteConfig(
                    daq_job_type="",
                    zmq_proxy_sub_urls=[],
                    zmq_proxy_pub_url="tcp://localhost:10001",
                ),
                supervisor_config=supervisor_config,
            ),
        ],
    )
    supervisor.init()
    supervisor.run()


def start_process(func, args=()):
    p = Process(target=func, args=args, daemon=True)
    p.start()
    return p


if __name__ == "__main__":
    active_processes = [
        start_process(thread, args)
        for thread, args in [(run_supervisor, ())]
        + [(run_client, (id,)) for id in range(5)]
    ]
    for thread in active_processes:
        thread.join()
