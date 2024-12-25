import os
import subprocess
import tempfile
from datetime import datetime
from shutil import which

from enrgdaq.daq.base import DAQJob
from enrgdaq.daq.store.models import (
    DAQJobMessageStoreTabular,
    StorableDAQJobConfig,
)
from enrgdaq.utils.time import get_now_unix_timestamp_ms, sleep_for

DAQ_JOB_CAEN_TOOLBOX_SLEEP_INTERVAL = 1


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
    register_labels: dict[str, str]


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
        temp_file = os.path.join(tempfile.mkdtemp(), "reg-dump.csv")
        # dump registers
        res = subprocess.run(
            [
                "caen-toolbox",
                self.config.digitizer_type,
                "dump",
                self.config.connection_string,
            ]
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
        for key, label in self.config.register_labels.items():
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
                    keys=["timestamp", *[f"caen_{x}" for x in registers]],
                    data=[
                        [
                            get_now_unix_timestamp_ms(),
                            *[registers[x] for x in registers],
                        ]
                    ],
                )
            )
            sleep_for(DAQ_JOB_CAEN_TOOLBOX_SLEEP_INTERVAL, start_time)
