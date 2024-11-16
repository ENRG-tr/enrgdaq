import logging

import coloredlogs

from enrgdaq.supervisor import Supervisor

if __name__ == "__main__":
    coloredlogs.install(
        level=logging.DEBUG,
        datefmt="%Y-%m-%d %H:%M:%S",
        fmt="%(asctime)s %(hostname)s %(name)s %(levelname)s %(message)s",
    )
    supervisor = Supervisor()
    supervisor.init()
    supervisor.run()
