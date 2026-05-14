"""
Tests for the SharedMemoryRingBuffer, focusing on slot lifecycle and occupancy
reporting used for Section 4 metrics.
"""

import sys
import unittest


from enrgdaq.utils.shared_ring_buffer import (
    SharedMemoryRingBuffer,
    cleanup_global_ring_buffer,
    get_global_ring_buffer,
)


@unittest.skipIf(
    sys.platform == "win32", "Shared memory ring buffer not supported on Windows"
)
class TestSharedMemoryRingBufferSlotOccupancy(unittest.TestCase):
    """Tests for get_slot_occupancy() and basic slot lifecycle."""

    SLOT_COUNT = 4
    SLOT_SIZE = 1024  # 1 KB per slot
    TOTAL_SIZE = SLOT_COUNT * SLOT_SIZE

    def setUp(self):
        self.buffer = SharedMemoryRingBuffer(
            name=f"test_ring_{id(self)}",
            total_size=self.TOTAL_SIZE,
            slot_size=self.SLOT_SIZE,
            create=True,
        )
        # Force initialization
        self.buffer._ensure_initialized()

    def tearDown(self):
        self.buffer.cleanup()
        # Clean up the global singleton if it was touched
        try:
            cleanup_global_ring_buffer()
        except Exception:
            pass

    def _count_slots_in_state(self, state: int) -> int:
        """Helper: count slots in a given state."""
        count = 0
        for i in range(self.buffer.slot_count):
            if self.buffer._slot_states[i] == state:
                count += 1
        return count

    # --- get_slot_occupancy() ---

    def test_occupancy_all_free_after_init(self):
        """After initialization, all slots should be FREE."""
        occ = self.buffer.get_slot_occupancy()
        self.assertEqual(occ["free"], self.SLOT_COUNT)
        self.assertEqual(occ["writing"], 0)
        self.assertEqual(occ["ready"], 0)

    def test_occupancy_one_slot_after_allocate(self):
        """After allocating one slot, it should be WRITING."""
        slot = self.buffer.allocate(128)
        self.assertIsNotNone(slot)
        occ = self.buffer.get_slot_occupancy()
        self.assertEqual(occ["free"], self.SLOT_COUNT - 1)
        self.assertEqual(occ["writing"], 1)
        self.assertEqual(occ["ready"], 0)

    def test_occupancy_one_ready_after_mark_ready(self):
        """After marking a slot as ready, it should be counted as READY."""
        slot = self.buffer.allocate(128)
        self.assertIsNotNone(slot)
        self.buffer.mark_ready(slot.index, 64)
        occ = self.buffer.get_slot_occupancy()
        self.assertEqual(occ["free"], self.SLOT_COUNT - 1)
        self.assertEqual(occ["writing"], 0)
        self.assertEqual(occ["ready"], 1)

    def test_occupancy_after_release(self):
        """After releasing a READY slot, it should be FREE again."""
        slot = self.buffer.allocate(128)
        self.assertIsNotNone(slot)
        self.buffer.mark_ready(slot.index, 64)
        self.buffer.release(slot.index)
        occ = self.buffer.get_slot_occupancy()
        self.assertEqual(occ["free"], self.SLOT_COUNT)
        self.assertEqual(occ["writing"], 0)
        self.assertEqual(occ["ready"], 0)

    def test_occupancy_all_slots_round_robin(self):
        """Allocate all slots, verify all become WRITING."""
        slots = []
        for _ in range(self.SLOT_COUNT):
            s = self.buffer.allocate(128)
            self.assertIsNotNone(s)
            slots.append(s)

        occ = self.buffer.get_slot_occupancy()
        self.assertEqual(occ["free"], 0)
        self.assertEqual(occ["writing"], self.SLOT_COUNT)
        self.assertEqual(occ["ready"], 0)

        # Mark them all ready
        for s in slots:
            self.buffer.mark_ready(s.index, 64)

        occ = self.buffer.get_slot_occupancy()
        self.assertEqual(occ["free"], 0)
        self.assertEqual(occ["writing"], 0)
        self.assertEqual(occ["ready"], self.SLOT_COUNT)

    def test_occupancy_overwrite_oldest(self):
        """When all slots are used, allocate should overwrite oldest READY slot."""
        # Fill all slots and mark ready
        slots = []
        for _ in range(self.SLOT_COUNT):
            s = self.buffer.allocate(128)
            self.assertIsNotNone(s)
            self.buffer.mark_ready(s.index, 64)
            slots.append(s)

        # Allocate another — should overwrite slot 0 (the oldest)
        new_slot = self.buffer.allocate(128)
        self.assertIsNotNone(new_slot)

        occ = self.buffer.get_slot_occupancy()
        # One slot is WRITING (the new one), rest are READY
        self.assertEqual(occ["writing"], 1)
        self.assertEqual(occ["ready"], self.SLOT_COUNT - 1)
        self.assertEqual(occ["free"], 0)

    def test_occupancy_read_acquire_release(self):
        """Acquiring for read increments ref count; releasing decrements."""
        slot = self.buffer.allocate(128)
        self.assertIsNotNone(slot)
        self.buffer.mark_ready(slot.index, 64)

        # Acquire for read (ref count goes from READY=2 to 3)
        acquired = self.buffer.acquire_for_read(slot.index, 64)
        self.assertTrue(acquired)

        # Slot is still READY (state >= READY) so counted as ready
        occ = self.buffer.get_slot_occupancy()
        self.assertEqual(occ["ready"], 1)

        # Release — goes back to base READY state
        self.buffer.release(slot.index)
        occ = self.buffer.get_slot_occupancy()
        self.assertEqual(occ["ready"], 1)

        # Release again — should go FREE
        self.buffer.release(slot.index)
        occ = self.buffer.get_slot_occupancy()
        self.assertEqual(occ["free"], self.SLOT_COUNT)
        self.assertEqual(occ["ready"], 0)

    # --- get_stats() ---

    def test_get_stats_initial_zero(self):
        """Initial stats should be zero."""
        written, read = self.buffer.get_stats()
        self.assertEqual(written, 0)
        self.assertEqual(read, 0)

    def test_get_stats_after_write(self):
        """After writing and marking ready, bytes_written should increase."""
        slot = self.buffer.allocate(256)
        self.assertIsNotNone(slot)
        self.buffer.mark_ready(slot.index, 200)
        written, read = self.buffer.get_stats()
        self.assertEqual(written, 200)
        self.assertEqual(read, 0)

    def test_get_stats_after_read(self):
        """After acquiring for read, bytes_read should increase."""
        slot = self.buffer.allocate(256)
        self.assertIsNotNone(slot)
        self.buffer.mark_ready(slot.index, 200)
        self.buffer.acquire_for_read(slot.index, 150)
        written, read = self.buffer.get_stats()
        self.assertEqual(written, 200)
        self.assertEqual(read, 150)

    def test_get_stats_multiple_ops(self):
        """Multiple write/read cycles should accumulate correctly."""
        total_written = 0
        total_read = 0
        for i in range(3):
            slot = self.buffer.allocate(256)
            self.assertIsNotNone(slot)
            self.buffer.mark_ready(slot.index, 100 + i)
            total_written += 100 + i
            self.buffer.acquire_for_read(slot.index, 50 + i)
            total_read += 50 + i

        written, read = self.buffer.get_stats()
        self.assertEqual(written, total_written)
        self.assertEqual(read, total_read)

    # --- Global ring buffer singleton ---

    def test_global_ring_buffer_singleton(self):
        """get_global_ring_buffer should return the same instance."""
        rb1 = get_global_ring_buffer(
            name=f"test_global_{id(self)}",
            total_size=1024 * 1024,
            slot_size=4096,
        )
        rb2 = get_global_ring_buffer(
            name=f"test_global_{id(self)}",
            total_size=1024 * 1024,
            slot_size=4096,
        )
        self.assertIs(rb1, rb2)

    def test_global_ring_buffer_occupancy(self):
        """Global ring buffer should support get_slot_occupancy."""
        rb = get_global_ring_buffer(
            name=f"test_global_occ_{id(self)}",
            total_size=512 * 1024,
            slot_size=4096,
        )
        occ = rb.get_slot_occupancy()
        self.assertIn("free", occ)
        self.assertIn("writing", occ)
        self.assertIn("ready", occ)
        self.assertGreater(occ["free"], 0)


if __name__ == "__main__":
    unittest.main()
