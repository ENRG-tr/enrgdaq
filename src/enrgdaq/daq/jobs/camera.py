from datetime import datetime

import cv2

from enrgdaq.daq.base import DAQJob
from enrgdaq.daq.store.models import (
    DAQJobMessageStoreRaw,
    StorableDAQJobConfig,
)
from enrgdaq.utils.time import sleep_for


class DAQJobCameraConfig(StorableDAQJobConfig):
    """Configuration class for DAQJobCamera."""

    camera_device_index: int = 0
    store_interval_seconds: int = 5


class DAQJobCamera(DAQJob):
    """
    DAQ job for capturing images from a camera using PIL and pygrabber.
    """

    allowed_message_in_types = []
    config_type = DAQJobCameraConfig
    config: DAQJobCameraConfig
    _cam: cv2.VideoCapture

    def __init__(self, config: DAQJobCameraConfig, **kwargs):
        super().__init__(config, **kwargs)
        self._cam = cv2.VideoCapture(self.config.camera_device_index)

    def start(self):
        while True:
            start_time = datetime.now()
            self.capture_image()
            sleep_for(self.config.store_interval_seconds, start_time)

    def capture_image(self):
        self._logger.debug("Capturing image...")
        res, frame = self._cam.read()
        assert res
        # get bytes from frame
        res, buffer = cv2.imencode(".jpg", frame)
        assert res
        image_data = buffer.tobytes()

        self._put_message_out(
            DAQJobMessageStoreRaw(
                data=image_data,
                store_config=self.config.store_config,
            )
        )
        self._logger.debug("Image captured and sent")
