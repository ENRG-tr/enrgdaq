import os
from datetime import datetime, timedelta
from enum import Enum
from typing import Optional

import cv2
import numpy as np

from enrgdaq.daq.base import DAQJob
from enrgdaq.daq.store.models import (
    DAQJobMessageStoreRaw,
    StorableDAQJobConfig,
)
from enrgdaq.utils.time import sleep_for


class TimeTextPosition(str, Enum):
    """Position of the time text on the image."""

    TOP_LEFT = "top_left"
    TOP_RIGHT = "top_right"
    BOTTOM_LEFT = "bottom_left"
    BOTTOM_RIGHT = "bottom_right"


class DAQJobCameraConfig(StorableDAQJobConfig):
    """
    Configuration class for DAQJobCamera.
    Attributes:
        camera_device_index: The index of the camera device to use.
        store_interval_seconds: The interval in seconds to store images.
        enable_time_text: Whether to enable the time text overlay.
        time_text_position: The position of the time text on the image.
        time_text_background_opacity: The opacity of the time text background.
    """

    camera_device_index: int | None = None
    camera_device_name: str | None = None
    store_interval_seconds: float = 1
    """The interval in seconds to store images when there is movement."""
    store_interval_seconds_no_movement: float = 5
    """The interval in seconds to check for images when there is no movement."""
    enable_time_text: bool = True
    """Whether to enable the time text overlay."""
    time_text_position: TimeTextPosition = TimeTextPosition.TOP_LEFT
    """The position of the time text on the image."""
    time_text_background_opacity = 0.7
    """The opacity of the time text overlay."""
    movement_detection_threshold: float = 0.02
    """The threshold for detecting movement."""
    movement_detection_frame_width: int = 500
    """The width of the frame used for movement detection."""


class DAQJobCamera(DAQJob):
    """
    DAQ job for capturing images from a camera using PIL and pygrabber.
    """

    allowed_message_in_types = []
    config_type = DAQJobCameraConfig
    config: DAQJobCameraConfig
    _cam: Optional[cv2.VideoCapture]
    _previous_frame: Optional[cv2.typing.MatLike] = None

    def __init__(self, config: DAQJobCameraConfig, **kwargs):
        super().__init__(config, **kwargs)
        self._cam = None
        self._previous_frame = None
        if (
            self.config.camera_device_index is None
            and self.config.camera_device_name is None
        ):
            raise ValueError(
                "Either camera_device_index or camera_device_name must be set in the configuration."
            )

    def start(self):
        camera_index = self.config.camera_device_index
        if self.config.camera_device_name is not None:
            # We search for: /dev/v4l/by-id/usb-CAMERA_NAME-video-index0
            dev_path = (
                f"/dev/v4l/by-id/usb-{self.config.camera_device_name}-video-index0"
            )
            if not os.path.exists(dev_path):
                raise ValueError(
                    f"Camera device with name {self.config.camera_device_name} not found."
                )
            # Get symbolic link to the camera device
            camera_device_path = os.readlink(dev_path)
            # Extract from: ../../video0, get index 0
            camera_index = int(camera_device_path.split("video")[-1])
        assert camera_index is not None, "Camera not found"
        self._cam = cv2.VideoCapture(camera_index)

        last_no_movement_capture_time = datetime.now()
        while True:
            start_time = datetime.now()
            should_capture = False
            movement_detected = self._detect_movement_from_camera()

            if movement_detected:
                should_capture = True
            elif datetime.now() - last_no_movement_capture_time >= timedelta(
                seconds=self.config.store_interval_seconds_no_movement
            ):
                should_capture = True
                last_no_movement_capture_time = datetime.now()

            if should_capture:
                self.capture_and_process_image()

            sleep_for(self.config.store_interval_seconds, start_time)

    def _detect_movement(self, current_frame: cv2.typing.MatLike) -> bool:
        """Detects movement by comparing the current frame with the previous one."""
        aspect_ratio = current_frame.shape[0] / current_frame.shape[1]
        new_width = self.config.movement_detection_frame_width
        new_height = int(new_width * aspect_ratio)
        resized_frame = cv2.resize(current_frame, (new_width, new_height))

        gray_frame = cv2.cvtColor(resized_frame, cv2.COLOR_BGR2GRAY)
        gray_frame = cv2.GaussianBlur(gray_frame, (7, 7), 0)

        if self._previous_frame is None:
            self._previous_frame = gray_frame
            return False

        frame_delta = cv2.absdiff(self._previous_frame, gray_frame)
        thresh = cv2.threshold(frame_delta, 30, 255, cv2.THRESH_BINARY)[1]
        # Dilate the thresholded image to fill in holes
        kernel = np.ones((3, 3), np.uint8)
        thresh = cv2.dilate(thresh, kernel, iterations=2)

        # The percentage of non-zero pixels in the thresholded image
        change_percentage = cv2.countNonZero(thresh) / (
            thresh.shape[0] * thresh.shape[1]
        )

        self._previous_frame = gray_frame

        movement_detected = change_percentage > self.config.movement_detection_threshold
        if movement_detected:
            self._logger.debug(f"Movement detected: {change_percentage:.4f}")

        return movement_detected

    def _detect_movement_from_camera(self) -> bool:
        assert self._cam is not None
        res, frame = self._cam.read()
        assert res
        return self._detect_movement(frame)

    def capture_and_process_image(self):
        assert self._cam is not None

        self._logger.debug("Capturing image...")
        res, frame = self._cam.read()
        assert res

        if self.config.enable_time_text:
            frame = self._insert_date_and_time(frame)

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

        # Draw the background rectangle
        (text_width, text_height), baseline = cv2.getTextSize(
            current_time, cv2.FONT_HERSHEY_SIMPLEX, font_scale, thickness
        )

        # Determine the position of the text based on the configuration
        if self.config.time_text_position == TimeTextPosition.TOP_LEFT:
            x_offset = 0
            y_offset = int(30 * font_scale)
        elif self.config.time_text_position == TimeTextPosition.TOP_RIGHT:
            x_offset = frame_width - text_width
            y_offset = int(30 * font_scale)
        elif self.config.time_text_position == TimeTextPosition.BOTTOM_LEFT:
            x_offset = 0
            y_offset = frame_height - 10
        elif self.config.time_text_position == TimeTextPosition.BOTTOM_RIGHT:
            x_offset = frame_width - text_width
            y_offset = frame_height - 10

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
        alpha = self.config.time_text_background_opacity
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
