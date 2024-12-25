import os
import subprocess
import tempfile
from datetime import datetime
from shutil import which

from msgspec import Struct

from enrgdaq.daq.base import DAQJob
from enrgdaq.daq.store.models import (
    DAQJobMessageStoreTabular,
    StorableDAQJobConfig,
)
from enrgdaq.utils.time import get_now_unix_timestamp_ms, sleep_for

DAQ_JOB_CAEN_TOOLBOX_SLEEP_INTERVAL = 1


class RegisterLabel(Struct):
    register: str
    label: str


class DAQJobCAENToolboxConfig(StorableDAQJobConfig):
    """
    Configuration for the DAQ job that dumps register data of a CAEN digitizer using the caen-toolbox cli.
    Attributes:
        digitizer_type (str): Type of the CAEN digitizer, e.g. `dig1`
        connection_string (str): Connection string of the CAEN digitizer, e.g. `-c OPTICAL_LINK -l 0`
        register_labels (dict[str, str]): Dictionary of register labels, e.g. `{"0x11a8": "ADC_ch_1_Temperature"}`
    """

    digitizer_type: str
    connection_string: str
    register_labels: list[RegisterLabel]


class DAQJobCAENToolbox(DAQJob):
    """
    Dumps register data of a CAEN digitizer using the caen-toolbox cli.
    Attributes:
        config (DAQJobCAENToolboxConfig): Configuration for the DAQ job.
        config_type (DAQJobCAENToolboxConfig): Configuration type for the DAQ job.
    """

    config_type = DAQJobCAENToolboxConfig
    config: DAQJobCAENToolboxConfig

    def __init__(self, config: DAQJobCAENToolboxConfig, **kwargs):
        super().__init__(config, **kwargs)
        # Check if 'caen-toolbox' cli is present
        if which("caen-toolbox") is None:
            raise Exception("caen-toolbox cli not found")

    def _dump_digitizer(self) -> dict[str, int]:
        temp_file = os.path.join(tempfile.gettempdir(), "reg-dump.csv")
        # Dump registers. Might not be the best way of constructing a shell command
        # but we don't need to worry about injection attacks here
        res = subprocess.run(
            [
                f"caen-toolbox {self.config.digitizer_type} dump {self.config.connection_string} {temp_file}"
            ],
            shell=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
        )
        if res.returncode != 0:
            raise Exception(f"caen-toolbox dump failed: {res.stderr}")
        if not os.path.exists(temp_file):
            raise Exception("Register dump file not found")
        with open(temp_file, "r") as f:
            lines = f.readlines()
        raw_registers = {}
        # Load CSV file
        for line in lines:
            if line.startswith("#"):
                continue
            parts = line.split(",")
            raw_registers[parts[0]] = int(parts[1], 16)
        registers = {}
        for reg_label in self.config.register_labels:
            key, label = reg_label.register, reg_label.label
            registers[label] = raw_registers[key]
        return registers

    def start(self):
        while True:
            start_time = datetime.now()
            registers = self._dump_digitizer()
            self._put_message_out(
                DAQJobMessageStoreTabular(
                    store_config=self.config.store_config,
                    tag="caen-toolbox",
                    keys=["timestamp", *[x for x in registers]],
                    data=[
                        [
                            get_now_unix_timestamp_ms(),
                            *[registers[x] for x in registers],
                        ]
                    ],
                )
            )
            sleep_for(DAQ_JOB_CAEN_TOOLBOX_SLEEP_INTERVAL, start_time)
