from datetime import datetime
from typing import Optional

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
    _cam: Optional[cv2.VideoCapture]

    def __init__(self, config: DAQJobCameraConfig, **kwargs):
        super().__init__(config, **kwargs)
        self._cam = None

    def start(self):
        self._cam = cv2.VideoCapture(self.config.camera_device_index)

        while True:
            start_time = datetime.now()
            self.capture_image()
            sleep_for(self.config.store_interval_seconds, start_time)

    def capture_image(self):
        assert self._cam is not None

        self._logger.debug("Capturing image...")
        res, frame = self._cam.read()
        assert res
        # insert date & time
        frame = self._insert_date_and_time(frame)
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

    def _insert_date_and_time(self, frame):
        # insert date and time into image
        height, width, _ = frame.shape
        # Constants for font scale and thickness calculation
        FONT_SCALE_MULTIPLIER = 0.002
        THICKNESS_MULTIPLIER = 0.004

        font_scale = min(width, height) * FONT_SCALE_MULTIPLIER
        thickness = max(1, int(min(width, height) * THICKNESS_MULTIPLIER))

        # Calculate the average color of the background
        avg_color_per_row = frame.mean(axis=0)
        avg_color = avg_color_per_row.mean(axis=0)
        avg_brightness = avg_color.mean()

        # Set text color based on background brightness
        text_color = (0, 0, 0) if avg_brightness > 127 else (255, 255, 255)

        frame = cv2.putText(
            frame,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            (10, int(30 * font_scale)),
            cv2.FONT_HERSHEY_SIMPLEX,
            font_scale,
            text_color,
            thickness,
        )

        return frame
