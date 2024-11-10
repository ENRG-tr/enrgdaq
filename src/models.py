from copy import deepcopy

from msgspec import Struct


class SupervisorConfig(Struct):
    supervisor_id: str

    def clone(self):
        print(deepcopy(self))
        return deepcopy(self)
