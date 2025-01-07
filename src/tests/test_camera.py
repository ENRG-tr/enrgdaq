import unittest
from unittest.mock import MagicMock, patch

from enrgdaq.daq.jobs.camera import DAQJobCamera, DAQJobCameraConfig


class TestDAQJobCamera(unittest.TestCase):
    @patch("cv2.VideoCapture")
    def setUp(self, mock_video_capture):
        self.mock_video_capture = mock_video_capture
        self.mock_video_capture.return_value.read.return_value = (True, MagicMock())
        self.config = DAQJobCameraConfig(
            daq_job_type=MagicMock(),
            store_config=MagicMock(),
            camera_device_index=3,
            store_interval_seconds=5,
        )
        self.daq_job_camera = DAQJobCamera(self.config)
        self.daq_job_camera._cam = MagicMock()
        self.daq_job_camera._cam.read.return_value = (True, b"test_bytes")
        self.daq_job_camera._insert_date_and_time = MagicMock()

    @patch("time.sleep", return_value=None, side_effect=StopIteration)
    @patch("cv2.VideoCapture")
    @patch("cv2.imencode")
    def test_start(self, mock_imencode, mock_video_capture, mock_time_sleep):
        mock_imencode.return_value = (True, MagicMock(tobytes=lambda: b"test_bytes"))
        mock_video_capture.return_value.read.return_value = (True, b"test_bytes")
        with self.assertRaises(StopIteration):
            self.daq_job_camera.start()
        mock_video_capture.assert_called_once_with(3)

    @patch("cv2.imencode")
    def test_capture_image(self, mock_imencode):
        mock_imencode.return_value = (True, MagicMock(tobytes=lambda: b"test_bytes"))
        with patch.object(
            self.daq_job_camera, "_put_message_out"
        ) as mock_put_message_out:
            self.daq_job_camera.capture_image()
            mock_put_message_out.assert_called_once()
            args, kwargs = mock_put_message_out.call_args
            self.assertEqual(args[0].data, b"test_bytes")

    @patch("cv2.imencode")
    def test_capture_image_fail(self, mock_imencode):
        mock_imencode.return_value = (False, None)
        with self.assertRaises(AssertionError):
            self.daq_job_camera.capture_image()

    @patch("cv2.imencode")
    def test_imencode_fail(self, mock_imencode):
        mock_imencode.return_value = (False, None)
        with self.assertRaises(AssertionError):
            self.daq_job_camera.capture_image()


if __name__ == "__main__":
    unittest.main()
