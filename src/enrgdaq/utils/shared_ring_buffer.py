"""
Shared Memory Ring Buffer for zero-copy inter-process communication.

This module provides a pre-allocated shared memory circular buffer with slot-based
allocation, designed for high-throughput data transfer between processes with
minimal memory copies.
"""

import ctypes
import logging
import threading
from dataclasses import dataclass
from multiprocessing import RawArray, RawValue
from multiprocessing.shared_memory import SharedMemory
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Default configuration
DEFAULT_RING_BUFFER_SIZE = 256 * 1024 * 1024  # 256 MB
DEFAULT_SLOT_SIZE = 10 * 1024 * 1024  # 10 MB per slot

# Slot states
SLOT_FREE = 0
SLOT_WRITING = 1
SLOT_READY = 2  # Ready for reading (reference count starts here)


@dataclass
class RingBufferSlot:
    """Handle to a slot in the ring buffer."""

    buffer: "SharedMemoryRingBuffer"
    index: int
    offset: int
    max_size: int
    _memoryview: Optional[memoryview] = None

    @property
    def view(self) -> memoryview:
        """Get a memoryview into this slot for reading/writing."""
        if self._memoryview is None:
            self._memoryview = self.buffer.get_slot_view(self.index)
        return self._memoryview

    def release(self):
        """Release this slot back to the buffer pool."""
        if self._memoryview is not None:
            self._memoryview.release()
            self._memoryview = None
        self.buffer.release(self.index)


class SharedMemoryRingBuffer:
    """
    A pre-allocated shared memory ring buffer with fixed-size slots.

    This buffer divides a large shared memory region into fixed-size slots
    that can be allocated and released for zero-copy data transfer between
    processes.

    The buffer is designed for single-producer, single-consumer scenarios
    but supports multiple consumers through reference counting.
    """

    def __init__(
        self,
        name: str,
        total_size: int = DEFAULT_RING_BUFFER_SIZE,
        slot_size: int = DEFAULT_SLOT_SIZE,
        create: bool = True,
    ):
        """
        Initialize or attach to a shared memory ring buffer.

        Args:
            name: Unique name for the shared memory region.
            total_size: Total size of the buffer in bytes.
            slot_size: Size of each slot in bytes.
            create: If True, create the buffer. If False, attach to existing.
        """
        self.name = name
        self.slot_size = slot_size
        self.slot_count = total_size // slot_size
        self.total_size = self.slot_count * slot_size  # Align to slot boundaries

        self._create = create
        self._shm: Optional[SharedMemory] = None
        self._slot_states: Any = None  # RawArray[ctypes.c_int]
        self._write_index: Any = None  # RawValue[ctypes.c_int]
        self._bytes_written: Any = None  # RawValue[ctypes.c_longlong]
        self._bytes_read: Any = None  # RawValue[ctypes.c_longlong]
        self._lock = threading.Lock()
        self._initialized = False

    def _ensure_initialized(self):
        """Lazy initialization of shared memory resources."""
        if self._initialized:
            return

        with self._lock:
            if self._initialized:
                return

            try:
                if self._create:
                    # Create new shared memory
                    try:
                        self._shm = SharedMemory(
                            name=self.name, create=True, size=self.total_size
                        )
                        logger.info(
                            f"Created shared memory ring buffer '{self.name}' "
                            f"with {self.slot_count} slots of {self.slot_size} bytes each"
                        )
                    except FileExistsError:
                        # Already exists, attach to it
                        self._shm = SharedMemory(name=self.name, create=False)
                        logger.info(
                            f"Attached to existing shared memory ring buffer '{self.name}'"
                        )
                else:
                    # Attach to existing shared memory
                    self._shm = SharedMemory(name=self.name, create=False)

                # Slot states array: one int per slot for reference counting
                # Using ctypes shared array for cross-process access
                self._slot_states = RawArray(ctypes.c_int, self.slot_count)

                # Write index for round-robin allocation
                self._write_index = RawValue(ctypes.c_int, 0)

                # Byte counters for throughput measurement
                self._bytes_written = RawValue(ctypes.c_longlong, 0)
                self._bytes_read = RawValue(ctypes.c_longlong, 0)

                self._initialized = True

            except Exception as e:
                logger.error(f"Failed to initialize ring buffer: {e}")
                raise

    def allocate(self, required_size: int) -> Optional[RingBufferSlot]:
        """
        Allocate a slot that can hold the required size.

        Args:
            required_size: Minimum size needed in bytes.

        Returns:
            RingBufferSlot if allocation successful, None if no slots available
            or size too large.
        """
        self._ensure_initialized()

        if required_size > self.slot_size:
            logger.warning(
                f"Requested size {required_size} exceeds slot size {self.slot_size}"
            )
            return None

        assert self._slot_states is not None
        assert self._write_index is not None

        # Try to find a free slot using round-robin starting from write_index
        start_index = self._write_index.value
        for i in range(self.slot_count):
            slot_index = (start_index + i) % self.slot_count

            # Try to atomically set slot from FREE to WRITING
            if self._slot_states[slot_index] == SLOT_FREE:
                # Simple compare-and-swap using the GIL
                # For truly lock-free, we'd need atomics from a C extension
                with self._lock:
                    if self._slot_states[slot_index] == SLOT_FREE:
                        self._slot_states[slot_index] = SLOT_WRITING
                        self._write_index.value = (slot_index + 1) % self.slot_count

                        return RingBufferSlot(
                            buffer=self,
                            index=slot_index,
                            offset=slot_index * self.slot_size,
                            max_size=self.slot_size,
                        )

        # No free slots - overwrite oldest READY slot (true ring buffer behavior)
        # This drops old data if consumer can't keep up
        for i in range(self.slot_count):
            slot_index = (start_index + i) % self.slot_count

            if self._slot_states[slot_index] == SLOT_READY:
                with self._lock:
                    # Only overwrite if still READY (not being read)
                    if self._slot_states[slot_index] == SLOT_READY:
                        self._slot_states[slot_index] = SLOT_WRITING
                        self._write_index.value = (slot_index + 1) % self.slot_count
                        # logger.debug(
                        #    f"Overwriting slot {slot_index} (consumer too slow)"
                        # )

                        return RingBufferSlot(
                            buffer=self,
                            index=slot_index,
                            offset=slot_index * self.slot_size,
                            max_size=self.slot_size,
                        )

        # All slots are either WRITING or READING - can't allocate
        logger.warning("No free or overwritable slots available in ring buffer")
        return None

    def mark_ready(self, slot_index: int, bytes_written: int = 0):
        """Mark a slot as ready for reading (after writing is complete)."""
        self._ensure_initialized()
        assert self._slot_states is not None

        if self._slot_states[slot_index] == SLOT_WRITING:
            self._slot_states[slot_index] = SLOT_READY
            if bytes_written > 0 and self._bytes_written is not None:
                self._bytes_written.value += bytes_written

    def acquire_for_read(self, slot_index: int, bytes_to_read: int = 0) -> bool:
        """
        Acquire a slot for reading (increment reference count).

        Returns True if successful, False if slot is not ready.
        """
        self._ensure_initialized()
        assert self._slot_states is not None

        with self._lock:
            state = self._slot_states[slot_index]
            if state >= SLOT_READY:
                self._slot_states[slot_index] = state + 1
                if bytes_to_read > 0 and self._bytes_read is not None:
                    self._bytes_read.value += bytes_to_read
                return True
        return False

    def release(self, slot_index: int):
        """
        Release a slot (decrement reference count, free if zero).
        """
        self._ensure_initialized()
        assert self._slot_states is not None

        with self._lock:
            state = self._slot_states[slot_index]
            if state == SLOT_READY:
                # Last reference, free the slot
                self._slot_states[slot_index] = SLOT_FREE
            elif state > SLOT_READY:
                # Decrement reference count
                self._slot_states[slot_index] = state - 1
            elif state == SLOT_WRITING:
                # Writing was aborted, free the slot
                self._slot_states[slot_index] = SLOT_FREE

    def get_stats(self) -> tuple[int, int]:
        """Get (bytes_written, bytes_read) stats."""
        self._ensure_initialized()
        return (
            self._bytes_written.value if self._bytes_written else 0,
            self._bytes_read.value if self._bytes_read else 0,
        )

    def get_slot_view(self, slot_index: int) -> memoryview:
        """Get a memoryview into a specific slot."""
        self._ensure_initialized()
        assert self._shm is not None

        offset = slot_index * self.slot_size
        buf = self._shm.buf
        assert buf is not None, "Shared memory buffer is None"
        return memoryview(buf)[offset : offset + self.slot_size]

    def get_slot_address(self, slot_index: int) -> int:
        """Get the memory address of a slot (for pa.foreign_buffer)."""
        self._ensure_initialized()
        assert self._shm is not None

        # Get base address of the shared memory buffer
        buf = self._shm.buf
        assert buf is not None, "Shared memory buffer is None"
        offset = slot_index * self.slot_size

        # Create a ctypes pointer to get the address
        c_buf = (ctypes.c_char * len(buf)).from_buffer(buf)
        base_addr = ctypes.addressof(c_buf)
        return base_addr + offset

    def cleanup(self):
        """Clean up resources. Only call from the creating process at shutdown."""
        if self._shm is not None:
            try:
                self._shm.close()
                if self._create:
                    self._shm.unlink()
                    logger.info(f"Unlinked shared memory ring buffer '{self.name}'")
            except Exception as e:
                logger.warning(f"Error during ring buffer cleanup: {e}")
            self._shm = None

        self._initialized = False

    def __del__(self):
        try:
            if self._shm is not None:
                self._shm.close()
        except Exception:
            pass


# Global singleton instance
_global_ring_buffer: Optional[SharedMemoryRingBuffer] = None
_global_ring_buffer_lock = threading.Lock()


def get_global_ring_buffer(
    name: str = "enrgdaq_ring_buffer",
    total_size: int = DEFAULT_RING_BUFFER_SIZE,
    slot_size: int = DEFAULT_SLOT_SIZE,
) -> SharedMemoryRingBuffer:
    """
    Get or create the global ring buffer singleton.

    This is lazily initialized on first access.

    Args:
        name: Name for the shared memory region.
        total_size: Total buffer size (only used on first call).
        slot_size: Size per slot (only used on first call).

    Returns:
        The global SharedMemoryRingBuffer instance.
    """
    global _global_ring_buffer

    if _global_ring_buffer is None:
        with _global_ring_buffer_lock:
            if _global_ring_buffer is None:
                _global_ring_buffer = SharedMemoryRingBuffer(
                    name=name,
                    total_size=total_size,
                    slot_size=slot_size,
                    create=True,
                )

    return _global_ring_buffer


def attach_to_ring_buffer(
    name: str = "enrgdaq_ring_buffer",
    total_size: int = DEFAULT_RING_BUFFER_SIZE,
    slot_size: int = DEFAULT_SLOT_SIZE,
) -> SharedMemoryRingBuffer:
    """
    Attach to an existing ring buffer (for use in child processes).

    Args:
        name: Name of the shared memory region.
        total_size: Total buffer size.
        slot_size: Size per slot.

    Returns:
        A SharedMemoryRingBuffer attached to the existing shared memory.
    """
    return SharedMemoryRingBuffer(
        name=name,
        total_size=total_size,
        slot_size=slot_size,
        create=False,
    )


def cleanup_global_ring_buffer():
    """Clean up the global ring buffer. Call at supervisor shutdown."""
    global _global_ring_buffer

    if _global_ring_buffer is not None:
        with _global_ring_buffer_lock:
            if _global_ring_buffer is not None:
                _global_ring_buffer.cleanup()
                _global_ring_buffer = None


def get_global_ring_buffer_stats() -> tuple[int, int]:
    """Get stats from the global ring buffer (bytes_written, bytes_read)."""
    global _global_ring_buffer
    if _global_ring_buffer is not None:
        return _global_ring_buffer.get_stats()
    return (0, 0)
