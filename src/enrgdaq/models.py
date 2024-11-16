from copy import deepcopy

from msgspec import Struct


class SupervisorConfig(Struct):
    """
    A configuration class for a supervisor.
    Attributes:
        supervisor_id (str): The unique identifier for the supervisor that is going to be used primarily for DAQJobRemote.
    """

    supervisor_id: str

    def clone(self):
        return deepcopy(self)
