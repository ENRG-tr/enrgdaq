from msgspec import Struct


class SupervisorConfig(Struct):
    supervisor_id: str
