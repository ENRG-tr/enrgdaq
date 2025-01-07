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
        FONT_SCALE_FACTOR = 1.8e-3
        THICKNESS_FACTOR = 5e-3

        # Calculate font scale and thickness dynamically based on frame size
        font_scale = min(frame_width, frame_height) * FONT_SCALE_FACTOR
        thickness = max(1, int(min(frame_width, frame_height) * THICKNESS_FACTOR))

        # Generate the text to overlay
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Initial coordinates for text placement
        x_offset = 10
        y_offset = int(30 * font_scale)

        # Draw the background rectangle
        (text_width, text_height), baseline = cv2.getTextSize(
            current_time, cv2.FONT_HERSHEY_SIMPLEX, font_scale, thickness
        )
        # Create a transparent overlay
        overlay = frame.copy()

        # Draw the rectangle on the overlay
        cv2.rectangle(
            overlay,
            (x_offset, y_offset - text_height - baseline),
            (x_offset + text_width, y_offset + baseline),
            (0, 0, 0),
            cv2.FILLED,
        )

        # Blend the overlay with the frame to achieve the desired opacity
        alpha = 0.75  # Transparency factor
        cv2.addWeighted(overlay, alpha, frame, 1 - alpha, 0, frame)

        # Put the text on the frame
        cv2.putText(
            frame,
            current_time,
            (x_offset, y_offset),
            cv2.FONT_HERSHEY_SIMPLEX,
            font_scale,
            (255, 255, 255),
            thickness,
        )

        return frame
