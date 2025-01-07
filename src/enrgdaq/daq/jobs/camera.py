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
    add_date_and_time: bool = True


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
        if self.config.add_date_and_time:
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
        """
        Inserts the current date and time as a text overlay on the given frame.
        The text color adjusts based on the brightness of the background.
        """
        # Get frame dimensions
        frame_height, frame_width, _ = frame.shape

        # Constants for font properties
        FONT_SCALE_FACTOR = 2e-3
        THICKNESS_FACTOR = 5e-3

        # Calculate font scale and thickness dynamically based on frame size
        font_scale = min(frame_width, frame_height) * FONT_SCALE_FACTOR
        thickness = max(1, int(min(frame_width, frame_height) * THICKNESS_FACTOR))

        # Generate the text to overlay
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Initial coordinates for text placement
        x_offset = 10
        y_offset = int(30 * font_scale)

        # Iterate through each character in the text
        for char in current_time:
            # Measure the size of the character
            (char_width, char_height), _ = cv2.getTextSize(
                char, cv2.FONT_HERSHEY_SIMPLEX, font_scale, thickness
            )

            # Extract the region of the frame where the character will be placed
            char_region = frame[
                y_offset : y_offset + char_height, x_offset : x_offset + char_width
            ]

            # Calculate the average brightness of the region
            avg_color_per_row = char_region.mean(axis=0)
            avg_color = avg_color_per_row.mean(axis=0)
            avg_brightness = avg_color.mean()

            # Choose text color based on brightness (white text for dark backgrounds, black for light backgrounds)
            text_color = (0, 0, 0) if avg_brightness > 140 else (255, 255, 255)

            # Overlay the character on the frame
            frame = cv2.putText(
                frame,
                char,
                (x_offset, y_offset),
                cv2.FONT_HERSHEY_SIMPLEX,
                font_scale,
                text_color,
                thickness,
            )

            # Move the x_offset to position the next character
            x_offset += char_width

        return frame
