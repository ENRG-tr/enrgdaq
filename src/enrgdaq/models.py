from copy import deepcopy

from msgspec import Struct


class SupervisorConfig(Struct):
    """
    A configuration class for a supervisor.
    Attributes:
        supervisor_id (str): The unique identifier for the supervisor that is going to be used primarily for DAQJobRemote.
    """

    info: "SupervisorInfo"

    def clone(self):
        return deepcopy(self)


class SupervisorInfo(Struct):
    """
    A class to represent the information of a supervisor.
    Attributes:
        supervisor_id (str): The unique identifier for the supervisor.
    """

    supervisor_id: str
