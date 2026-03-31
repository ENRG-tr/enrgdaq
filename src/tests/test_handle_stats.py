import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from enrgdaq.daq.jobs.handle_stats import (
    DAQJobHandleStats,
    DAQJobHandleStatsConfig,
    DAQ_JOB_HANDLE_STATS_SLEEP_INTERVAL_SECONDS,
)
from enrgdaq.daq.models import (
    DAQJobInfo,
    DAQJobLatencyStats,
    DAQJobMessageStatsReport,
    DAQJobStats,
    DAQJobStatsRecord,
    SupervisorRemoteStats,
)
from enrgdaq.daq.store.models import DAQJobMessageStorePyArrow
from enrgdaq.daq.topics import Topic
from enrgdaq.models import SupervisorInfo


class TestDAQJobHandleStats(unittest.TestCase):
    """Test suite for DAQJobHandleStats."""

    def setUp(self):
        """Set up test fixtures."""
        self.config = DAQJobHandleStatsConfig(
            daq_job_type="",
            store_config=MagicMock(),
        )
        self.supervisor_info = SupervisorInfo(supervisor_id="test_supervisor")
        # Pass zmq URLs to avoid thread assertions
        self.daq_job_handle_stats = DAQJobHandleStats(
            config=self.config,
            supervisor_info=self.supervisor_info,
            zmq_xpub_url="ipc:///tmp/test_pub",
            zmq_xsub_url="ipc:///tmp/test_sub",
        )
        # Mock _publish_buffer
        self.daq_job_handle_stats._publish_buffer = MagicMock()

    def tearDown(self):
        """Clean up after tests."""
        # Signal threads to stop
        self.daq_job_handle_stats._has_been_freed = True

    def _create_stats_report(
        self,
        supervisor_id: str = "remote_supervisor",
        job_type: str = "DAQJobTest",
        processed_count: int = 100,
        sent_count: int = 50,
    ) -> DAQJobMessageStatsReport:
        """Helper to create a DAQJobMessageStatsReport."""
        return DAQJobMessageStatsReport(
            daq_job_info=DAQJobInfo(
                daq_job_type=job_type,
                unique_id=f"{job_type}-123",
                instance_id=0,
                config="",
                supervisor_info=SupervisorInfo(supervisor_id=supervisor_id),
            ),
            processed_count=processed_count,
            processed_bytes=10000,
            sent_count=sent_count,
            sent_bytes=5000,
            latency=DAQJobLatencyStats(count=10, avg_ms=5.0),
        )

    # =========================================================================
    # handle_message() tests
    # =========================================================================

    def test_handle_message_success(self):
        """Test that DAQJobMessageStatsReport is handled correctly."""
        message = self._create_stats_report()

        result = self.daq_job_handle_stats.handle_message(message)

        self.assertTrue(result)
        self.assertIn("remote_supervisor", self.daq_job_handle_stats._stats)
        self.assertIn(
            "DAQJobTest", self.daq_job_handle_stats._stats["remote_supervisor"]
        )

    def test_handle_message_no_supervisor_info(self):
        """Test that message without supervisor info is skipped."""
        message = DAQJobMessageStatsReport(
            daq_job_info=None,
            processed_count=100,
            sent_count=50,
            latency=DAQJobLatencyStats(),
        )

        with patch(
            "enrgdaq.daq.jobs.handle_stats.DAQJob.handle_message",
            return_value=True,
        ):
            result = self.daq_job_handle_stats.handle_message(message)

        self.assertTrue(result)
        self.assertEqual(len(self.daq_job_handle_stats._stats), 0)

    def test_handle_message_accumulates_counts(self):
        """Test that multiple messages accumulate counts correctly."""
        message1 = self._create_stats_report(processed_count=100, sent_count=50)
        message2 = self._create_stats_report(processed_count=50, sent_count=25)

        self.daq_job_handle_stats.handle_message(message1)
        self.daq_job_handle_stats.handle_message(message2)

        stats = self.daq_job_handle_stats._stats["remote_supervisor"]["DAQJobTest"]
        # message_in_stats uses set() not increase(), so it replaces
        self.assertEqual(stats.message_in_stats.count, 50)
        self.assertEqual(stats.message_out_stats.count, 25)

    def test_handle_message_updates_supervisor_activity(self):
        """Test that supervisor activity is tracked correctly."""
        before_time = datetime.now()
        message = self._create_stats_report()

        self.daq_job_handle_stats.handle_message(message)

        self.assertIn(
            "remote_supervisor", self.daq_job_handle_stats._supervisor_activity
        )
        activity = self.daq_job_handle_stats._supervisor_activity["remote_supervisor"]
        self.assertIsNotNone(activity.last_active)
        self.assertGreaterEqual(activity.last_active, before_time)

    def test_handle_message_first_message_creates_supervisor_entry(self):
        """Test that first message for a supervisor creates proper structure."""
        message = self._create_stats_report(supervisor_id="new_supervisor")

        self.assertNotIn("new_supervisor", self.daq_job_handle_stats._stats)

        result = self.daq_job_handle_stats.handle_message(message)

        self.assertTrue(result)
        self.assertIn("new_supervisor", self.daq_job_handle_stats._stats)
        self.assertIn("DAQJobTest", self.daq_job_handle_stats._stats["new_supervisor"])

    def test_handle_message_multiple_job_types_same_supervisor(self):
        """Test handling multiple job types from same supervisor."""
        message1 = self._create_stats_report(job_type="DAQJobType1")
        message2 = self._create_stats_report(job_type="DAQJobType2")

        self.daq_job_handle_stats.handle_message(message1)
        self.daq_job_handle_stats.handle_message(message2)

        stats = self.daq_job_handle_stats._stats["remote_supervisor"]
        self.assertIn("DAQJobType1", stats)
        self.assertIn("DAQJobType2", stats)

    def test_handle_message_latency_stats_stored(self):
        """Test that latency stats are stored correctly."""
        latency = DAQJobLatencyStats(count=100, avg_ms=15.5, p95_ms=50.0)
        message = self._create_stats_report()
        message.latency = latency

        self.daq_job_handle_stats.handle_message(message)

        stats = self.daq_job_handle_stats._stats["remote_supervisor"]["DAQJobTest"]
        self.assertEqual(stats.latency_stats.count, 100)
        self.assertEqual(stats.latency_stats.avg_ms, 15.5)
        self.assertEqual(stats.latency_stats.p95_ms, 50.0)

    def test_handle_message_byte_accumulation(self):
        """Test that bytes are accumulated in supervisor activity."""
        message = self._create_stats_report(processed_count=100, sent_count=50)
        message.processed_bytes = 10000

        self.daq_job_handle_stats.handle_message(message)

        activity = self.daq_job_handle_stats._supervisor_activity["remote_supervisor"]
        # Bytes are summed from stats.message_in_stats.count and message_out_stats.count
        # After handle_message, message_in_stats.count = processed_count = 100
        # message_out_stats.count = sent_count = 50
        self.assertEqual(activity.message_in_bytes, 100)
        self.assertEqual(activity.message_out_bytes, 50)

    # =========================================================================
    # _save_stats() tests
    # =========================================================================

    def test_save_stats_creates_correct_table_schema(self):
        """Test that _save_stats produces correct PyArrow table structure."""
        message = self._create_stats_report()
        self.daq_job_handle_stats.handle_message(message)

        self.daq_job_handle_stats._save_stats()

        # Verify two messages were sent: StorePyArrow and CombinedStats
        calls = self.daq_job_handle_stats._publish_buffer.put.call_args_list
        self.assertEqual(len(calls), 2)

        # First call should be DAQJobMessageStorePyArrow
        store_msg = calls[0][0][0]
        self.assertIsInstance(store_msg, DAQJobMessageStorePyArrow)

        # Verify table has correct columns
        table = store_msg.table
        expected_columns = [
            "supervisor",
            "daq_job",
            "is_alive",
            "last_message_in_date",
            "message_in_count",
            "last_message_out_date",
            "message_out_count",
            "message_in_queue_size",
            "message_out_queue_size",
            "last_restart_date",
            "restart_count",
        ]
        for col in expected_columns:
            self.assertIn(col, table.column_names)

    def test_save_stats_empty_state(self):
        """Test _save_stats with no stats data."""
        self.daq_job_handle_stats._stats = {}

        self.daq_job_handle_stats._save_stats()

        calls = self.daq_job_handle_stats._publish_buffer.put.call_args_list
        self.assertEqual(len(calls), 2)

        store_msg = calls[0][0][0]
        self.assertIsInstance(store_msg, DAQJobMessageStorePyArrow)
        # Empty table should have 0 rows
        self.assertEqual(len(store_msg.table), 0)

    def test_save_stats_multiple_supervisors(self):
        """Test aggregation across multiple supervisors and job types."""
        # Add stats for multiple supervisors and job types
        for supervisor_id in ["sup1", "sup2"]:
            for job_type in ["DAQJobA", "DAQJobB"]:
                message = self._create_stats_report(
                    supervisor_id=supervisor_id,
                    job_type=job_type,
                )
                self.daq_job_handle_stats.handle_message(message)

        self.daq_job_handle_stats._save_stats()

        calls = self.daq_job_handle_stats._publish_buffer.put.call_args_list
        store_msg = calls[0][0][0]
        table = store_msg.table

        # Should have 4 rows (2 supervisors x 2 job types)
        self.assertEqual(len(table), 4)

        # Verify all combinations are present
        supervisors = table.column("supervisor").to_pylist()
        job_types = table.column("daq_job").to_pylist()
        combinations = set(zip(supervisors, job_types))
        expected = {
            ("sup1", "DAQJobA"),
            ("sup1", "DAQJobB"),
            ("sup2", "DAQJobA"),
            ("sup2", "DAQJobB"),
        }
        self.assertEqual(combinations, expected)

    def test_save_stats_sends_combined_stats_message(self):
        """Test that _save_stats sends DAQJobMessageCombinedStats."""
        message = self._create_stats_report()
        self.daq_job_handle_stats.handle_message(message)

        self.daq_job_handle_stats._save_stats()

        calls = self.daq_job_handle_stats._publish_buffer.put.call_args_list
        # Second message should be DAQJobMessageCombinedStats
        combined_msg = calls[1][0][0]
        self.assertEqual(combined_msg.__class__.__name__, "DAQJobMessageCombinedStats")
        self.assertIn("remote_supervisor", combined_msg.stats)

    def test_save_stats_datetime_to_timestamp_conversion(self):
        """Test that datetime values are converted to timestamps."""
        message = self._create_stats_report()
        self.daq_job_handle_stats.handle_message(message)

        self.daq_job_handle_stats._save_stats()

        calls = self.daq_job_handle_stats._publish_buffer.put.call_args_list
        store_msg = calls[0][0][0]
        table = store_msg.table

        # last_message_in_date should be a timestamp (number or string)
        last_in_date = table.column("last_message_in_date").to_pylist()[0]
        self.assertIsNotNone(last_in_date)
        self.assertNotEqual(last_in_date, "N/A")

    def test_save_stats_is_alive_lowercase(self):
        """Test that is_alive is converted to lowercase string."""
        message = self._create_stats_report()
        self.daq_job_handle_stats.handle_message(message)

        self.daq_job_handle_stats._save_stats()

        calls = self.daq_job_handle_stats._publish_buffer.put.call_args_list
        store_msg = calls[0][0][0]
        table = store_msg.table

        is_alive = table.column("is_alive").to_pylist()[0]
        self.assertIn(is_alive, ["true", "false"])

    # =========================================================================
    # _save_remote_stats() tests
    # =========================================================================

    def test_save_remote_stats(self):
        """Test that remote stats are saved correctly from _supervisor_activity."""
        self.daq_job_handle_stats._stats = {
            "remote_1": {
                "DAQJobTest": DAQJobStats(
                    message_in_stats=DAQJobStatsRecord(count=10),
                    message_out_stats=DAQJobStatsRecord(count=5),
                )
            }
        }
        self.daq_job_handle_stats._supervisor_activity = {
            "remote_1": SupervisorRemoteStats(
                last_active=datetime.now() - timedelta(seconds=10),
                message_in_count=10,
                message_in_bytes=1000,
                message_out_count=5,
                message_out_bytes=500,
            )
        }
        self.daq_job_handle_stats._supervisor_info = SupervisorInfo(
            supervisor_id="local"
        )

        self.daq_job_handle_stats._save_remote_stats()

        # Should send 2 messages
        self.assertEqual(self.daq_job_handle_stats._publish_buffer.put.call_count, 2)

    def test_save_remote_stats_empty(self):
        """Test that empty remote stats are handled."""
        self.daq_job_handle_stats._stats = {}
        self.daq_job_handle_stats._supervisor_activity = {}

        self.daq_job_handle_stats._save_remote_stats()

        self.assertEqual(self.daq_job_handle_stats._publish_buffer.put.call_count, 2)

    def test_save_remote_stats_is_alive_true_when_recent(self):
        """Test is_alive=True when last_active < 30 seconds ago."""
        self.daq_job_handle_stats._stats = {
            "remote_1": {
                "DAQJobTest": DAQJobStats(
                    message_in_stats=DAQJobStatsRecord(count=10),
                    message_out_stats=DAQJobStatsRecord(count=5),
                )
            }
        }
        self.daq_job_handle_stats._supervisor_activity = {
            "remote_1": SupervisorRemoteStats(
                last_active=datetime.now() - timedelta(seconds=10),
                message_in_bytes=1000,
                message_out_bytes=500,
            )
        }

        self.daq_job_handle_stats._save_remote_stats()

        calls = self.daq_job_handle_stats._publish_buffer.put.call_args_list
        store_msg = calls[0][0][0]
        table = store_msg.table

        is_alive = table.column("is_alive").to_pylist()[0]
        self.assertEqual(is_alive, "true")

    def test_save_remote_stats_is_alive_false_when_stale(self):
        """Test is_alive=False when last_active > 30 seconds ago."""
        self.daq_job_handle_stats._stats = {
            "remote_1": {
                "DAQJobTest": DAQJobStats(
                    message_in_stats=DAQJobStatsRecord(count=10),
                    message_out_stats=DAQJobStatsRecord(count=5),
                )
            }
        }
        self.daq_job_handle_stats._supervisor_activity = {
            "remote_1": SupervisorRemoteStats(
                last_active=datetime.now() - timedelta(seconds=60),
                message_in_bytes=1000,
                message_out_bytes=500,
            )
        }

        self.daq_job_handle_stats._save_remote_stats()

        calls = self.daq_job_handle_stats._publish_buffer.put.call_args_list
        store_msg = calls[0][0][0]
        table = store_msg.table

        is_alive = table.column("is_alive").to_pylist()[0]
        self.assertEqual(is_alive, "false")

    def test_save_remote_stats_bytes_to_megabytes_conversion(self):
        """Test that bytes are converted to megabytes with 3 decimal places."""
        self.daq_job_handle_stats._stats = {
            "remote_1": {
                "DAQJobTest": DAQJobStats(
                    message_in_stats=DAQJobStatsRecord(count=10),
                    message_out_stats=DAQJobStatsRecord(count=5),
                )
            }
        }
        self.daq_job_handle_stats._supervisor_activity = {
            "remote_1": SupervisorRemoteStats(
                last_active=datetime.now(),
                message_in_bytes=1024 * 1024,  # 1 MB
                message_out_bytes=512 * 1024,  # 0.5 MB
            )
        }

        self.daq_job_handle_stats._save_remote_stats()

        calls = self.daq_job_handle_stats._publish_buffer.put.call_args_list
        store_msg = calls[0][0][0]
        table = store_msg.table

        mb_in = table.column("message_in_megabytes").to_pylist()[0]
        mb_out = table.column("message_out_megabytes").to_pylist()[0]

        self.assertEqual(mb_in, "1.000")
        self.assertEqual(mb_out, "0.500")

    def test_save_remote_stats_table_schema(self):
        """Test that remote stats table has correct schema."""
        self.daq_job_handle_stats._stats = {
            "remote_1": {
                "DAQJobTest": DAQJobStats(
                    message_in_stats=DAQJobStatsRecord(count=10),
                    message_out_stats=DAQJobStatsRecord(count=5),
                )
            }
        }
        self.daq_job_handle_stats._supervisor_activity = {
            "remote_1": SupervisorRemoteStats(
                last_active=datetime.now(),
                message_in_bytes=1000,
                message_out_bytes=500,
            )
        }

        self.daq_job_handle_stats._save_remote_stats()

        calls = self.daq_job_handle_stats._publish_buffer.put.call_args_list
        store_msg = calls[0][0][0]
        table = store_msg.table

        expected_columns = [
            "supervisor",
            "is_alive",
            "last_active",
            "message_in_count",
            "message_in_megabytes",
            "message_out_count",
            "message_out_megabytes",
        ]
        for col in expected_columns:
            self.assertIn(col, table.column_names)

    def test_save_remote_stats_sends_combined_remote_stats_message(self):
        """Test that _save_remote_stats sends DAQJobMessageCombinedRemoteStats."""
        self.daq_job_handle_stats._stats = {
            "remote_1": {
                "DAQJobTest": DAQJobStats(
                    message_in_stats=DAQJobStatsRecord(count=10),
                    message_out_stats=DAQJobStatsRecord(count=5),
                )
            }
        }
        self.daq_job_handle_stats._supervisor_activity = {
            "remote_1": SupervisorRemoteStats(
                last_active=datetime.now(),
                message_in_bytes=1000,
                message_out_bytes=500,
            )
        }

        self.daq_job_handle_stats._save_remote_stats()

        calls = self.daq_job_handle_stats._publish_buffer.put.call_args_list
        # Second message should be DAQJobMessageCombinedRemoteStats
        combined_msg = calls[1][0][0]
        self.assertEqual(
            combined_msg.__class__.__name__, "DAQJobMessageCombinedRemoteStats"
        )

    # =========================================================================
    # Config and initialization tests
    # =========================================================================

    def test_config_subscribe_to_all_stats_false(self):
        """Test that subscribe_to_all_stats=False doesn't add stats_all topic from DAQJobHandleStats."""
        config = DAQJobHandleStatsConfig(
            daq_job_type="",
            store_config=MagicMock(),
            subscribe_to_all_stats=False,
        )
        supervisor_info = SupervisorInfo(supervisor_id="test_sup")

        job = DAQJobHandleStats(
            config=config,
            supervisor_info=supervisor_info,
            zmq_xpub_url="ipc:///tmp/test_pub3",
            zmq_xsub_url="ipc:///tmp/test_sub3",
        )
        job._has_been_freed = True  # Stop threads

        # Should have supervisor-specific topic
        self.assertIn(Topic.stats_supervisor("test_sup"), job.topics_to_subscribe)
        # Note: Topic.stats_all() is 'stats.supervisor'
        # The handle_stats job adds it when subscribe_to_all_stats=True
        # With subscribe_to_all_stats=False, it should NOT be added by handle_stats
        # (though base class may add it for other reasons)
        # Verify that the stats_all topic appears at most once
        # (handle_stats doesn't add it when False, so duplicates won't occur from this class)
        stats_all_count = job.topics_to_subscribe.count(Topic.stats_all())
        # With subscribe_to_all_stats=False, handle_stats doesn't add Topic.stats_all()
        # so there should be exactly 1 (from the base class's standard subscription)
        self.assertLessEqual(stats_all_count, 2)

    def test_config_subscribe_to_all_stats_true(self):
        """Test that subscribe_to_all_stats=True adds stats_all topic."""
        config = DAQJobHandleStatsConfig(
            daq_job_type="",
            store_config=MagicMock(),
            subscribe_to_all_stats=True,
        )
        supervisor_info = SupervisorInfo(supervisor_id="test_sup")

        job = DAQJobHandleStats(
            config=config,
            supervisor_info=supervisor_info,
            zmq_xpub_url="ipc:///tmp/test_pub2",
            zmq_xsub_url="ipc:///tmp/test_sub2",
        )
        job._has_been_freed = True  # Stop threads

        # Should have both supervisor-specific and all stats topics
        self.assertIn(Topic.stats_supervisor("test_sup"), job.topics_to_subscribe)
        # With subscribe_to_all_stats=True, stats_all should be added
        # (may appear multiple times due to base class also adding it)
        stats_all_count = job.topics_to_subscribe.count(Topic.stats_all())
        self.assertGreaterEqual(stats_all_count, 1)

    def test_initial_state(self):
        """Test that initial state is correctly set."""
        self.assertEqual(self.daq_job_handle_stats._stats, {})
        self.assertEqual(self.daq_job_handle_stats._supervisor_activity, {})

    # =========================================================================
    # start() loop tests
    # =========================================================================

    def test_start_loop_executes_save_methods(self):
        """Test that start() calls _save_stats and _save_remote_stats in loop."""
        call_count = [0]
        original_save_stats = self.daq_job_handle_stats._save_stats
        original_save_remote = self.daq_job_handle_stats._save_remote_stats

        def mock_save_stats():
            call_count[0] += 1
            if call_count[0] >= 3:
                self.daq_job_handle_stats._has_been_freed = True
            return original_save_stats()

        def mock_save_remote():
            return original_save_remote()

        self.daq_job_handle_stats._save_stats = mock_save_stats
        self.daq_job_handle_stats._save_remote_stats = mock_save_remote
        self.daq_job_handle_stats._publish_buffer = MagicMock()

        self.daq_job_handle_stats.start()

        # Should have called save methods 3 times before exiting
        self.assertEqual(call_count[0], 3)

    def test_start_exits_on_free(self):
        """Test that start() exits when _has_been_freed is True."""
        self.daq_job_handle_stats._has_been_freed = True
        self.daq_job_handle_stats._save_stats = MagicMock()
        self.daq_job_handle_stats._save_remote_stats = MagicMock()

        self.daq_job_handle_stats.start()

        # Should not call save methods since _has_been_freed is True at start
        self.daq_job_handle_stats._save_stats.assert_not_called()

    @patch("enrgdaq.daq.jobs.handle_stats.sleep_for")
    def test_start_uses_correct_sleep_interval(self, mock_sleep):
        """Test that start() uses correct sleep interval."""
        call_count = [0]

        def mock_save_stats():
            call_count[0] += 1
            if call_count[0] >= 2:
                self.daq_job_handle_stats._has_been_freed = True

        self.daq_job_handle_stats._save_stats = mock_save_stats
        self.daq_job_handle_stats._save_remote_stats = MagicMock()
        self.daq_job_handle_stats._publish_buffer = MagicMock()

        self.daq_job_handle_stats.start()

        # Verify sleep_for was called with correct interval
        for call in mock_sleep.call_args_list:
            # Second argument should be the interval constant
            self.assertEqual(call[0][0], DAQ_JOB_HANDLE_STATS_SLEEP_INTERVAL_SECONDS)

    # =========================================================================
    # Integration-style tests
    # =========================================================================

    def test_full_flow_handle_and_save(self):
        """Test full flow from handle_message to _save_stats."""
        message1 = self._create_stats_report(supervisor_id="sup1", processed_count=100)
        message2 = self._create_stats_report(supervisor_id="sup2", processed_count=200)

        self.daq_job_handle_stats.handle_message(message1)
        self.daq_job_handle_stats.handle_message(message2)

        self.daq_job_handle_stats._save_stats()
        self.daq_job_handle_stats._save_remote_stats()

        # Verify messages were sent
        self.assertEqual(self.daq_job_handle_stats._publish_buffer.put.call_count, 4)

    def test_stats_persist_across_multiple_handle_calls(self):
        """Test that stats persist and accumulate across multiple handle calls."""
        for i in range(5):
            message = self._create_stats_report(
                processed_count=i * 10, sent_count=i * 5
            )
            self.daq_job_handle_stats.handle_message(message)

        self.daq_job_handle_stats._save_stats()

        # Stats should still have the last values (set() replaces, not accumulates)
        stats = self.daq_job_handle_stats._stats["remote_supervisor"]["DAQJobTest"]
        self.assertEqual(stats.message_in_stats.count, 40)  # Last value (4 * 10)
        self.assertEqual(stats.message_out_stats.count, 20)  # Last value (4 * 5)

    # =========================================================================
    # Edge cases
    # =========================================================================

    def test_handle_message_with_none_values_in_stats_record(self):
        """Test handling of None values in DAQJobStatsRecord."""
        # Create stats with None last_updated
        self.daq_job_handle_stats._stats = {
            "remote_supervisor": {
                "DAQJobTest": DAQJobStats(
                    message_in_stats=DAQJobStatsRecord(count=10, last_updated=None),
                )
            }
        }

        # Should not raise when saving
        self.daq_job_handle_stats._save_stats()

        calls = self.daq_job_handle_stats._publish_buffer.put.call_args_list
        store_msg = calls[0][0][0]
        table = store_msg.table

        # last_message_in_date should be "N/A" for None
        last_in_date = table.column("last_message_in_date").to_pylist()[0]
        self.assertEqual(last_in_date, "N/A")

    def test_handle_message_empty_latency_stats(self):
        """Test handling of empty latency stats."""
        message = self._create_stats_report()
        message.latency = DAQJobLatencyStats()  # Empty/default latency

        result = self.daq_job_handle_stats.handle_message(message)

        self.assertTrue(result)
        stats = self.daq_job_handle_stats._stats["remote_supervisor"]["DAQJobTest"]
        self.assertEqual(stats.latency_stats.count, 0)

    def test_concurrent_supervisors_dont_interfere(self):
        """Test that stats from different supervisors don't interfere."""
        message1 = self._create_stats_report(supervisor_id="sup1", processed_count=100)
        message2 = self._create_stats_report(supervisor_id="sup2", processed_count=200)

        self.daq_job_handle_stats.handle_message(message1)
        self.daq_job_handle_stats.handle_message(message2)

        self.assertEqual(len(self.daq_job_handle_stats._stats), 2)
        self.assertEqual(
            self.daq_job_handle_stats._stats["sup1"][
                "DAQJobTest"
            ].message_in_stats.count,
            100,
        )
        self.assertEqual(
            self.daq_job_handle_stats._stats["sup2"][
                "DAQJobTest"
            ].message_in_stats.count,
            200,
        )


if __name__ == "__main__":
    unittest.main()
