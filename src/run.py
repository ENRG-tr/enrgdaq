import argparse
import logging

import coloredlogs

from enrgdaq.supervisor import Supervisor

if __name__ == "__main__":
    coloredlogs.install(
        level=logging.DEBUG,
        datefmt="%Y-%m-%d %H:%M:%S",
        fmt="%(asctime)s %(hostname)s %(name)s %(levelname)s %(message)s",
    )

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--daq-job-config-path",
        type=str,
        default="configs/",
        help="Path to the daq job config files",
    )
    args = parser.parse_args()
    supervisor = Supervisor(daq_job_config_path=args.daq_job_config_path)

    supervisor.init()
    supervisor.run()
