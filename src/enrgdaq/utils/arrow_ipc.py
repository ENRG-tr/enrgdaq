"""
Arrow IPC utilities for zero-copy serialization/deserialization.

This module provides functions to write and read PyArrow tables directly
to/from shared memory buffers without intermediate copies.
"""

import ctypes
import logging
from typing import TYPE_CHECKING, Any

import pyarrow as pa

if TYPE_CHECKING:
    from enrgdaq.daq.models import RingBufferHandle

logger = logging.getLogger(__name__)


def get_table_ipc_size(table: pa.Table) -> int:
    """
    Calculate the size needed to serialize a PyArrow table in IPC format.

    This includes schema overhead, so the actual size may vary slightly.
    We add a buffer for safety.

    Args:
        table: The PyArrow table to measure.

    Returns:
        Estimated size in bytes.
    """
    # Use a mock stream to calculate size
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().size


def write_table_to_buffer(table: pa.Table, buffer: memoryview) -> int:
    """
    Write a PyArrow table to a buffer using Arrow IPC streaming format.

    The table is serialized directly into the provided buffer memory.

    Args:
        table: The PyArrow table to write.
        buffer: A memoryview providing the destination memory.

    Returns:
        Number of bytes written.

    Raises:
        ValueError: If the buffer is too small.
    """
    # Create a fixed-size buffer writer that writes directly to the memoryview
    # We need to wrap the memoryview in an Arrow buffer first
    arrow_buf = pa.py_buffer(buffer)
    output_stream = pa.FixedSizeBufferWriter(arrow_buf)

    with pa.ipc.new_stream(output_stream, table.schema) as writer:
        writer.write_table(table)

    bytes_written = output_stream.tell()
    output_stream.close()

    return bytes_written


def read_table_from_buffer(buffer: memoryview, size: int) -> pa.Table:
    """
    Read a PyArrow table from a buffer using zero-copy.

    This uses pa.foreign_buffer to wrap the shared memory, allowing
    PyArrow to read directly from the memory without copying.

    Args:
        buffer: A memoryview containing the IPC data.
        size: Number of bytes of valid data in the buffer.

    Returns:
        A PyArrow Table that references the shared memory directly.

    Important:
        The returned table references the shared memory buffer. The buffer
        must remain valid for as long as the table is in use.
    """
    # Get the memory address of the buffer
    # For a memoryview backed by shared memory, this gives us the shared address
    c_array = (ctypes.c_char * len(buffer)).from_buffer(buffer)
    address = ctypes.addressof(c_array)

    # Create an Arrow buffer that references the shared memory
    # The `buffer` parameter keeps the Python memoryview alive
    arrow_buf = pa.foreign_buffer(address, size, base=buffer)

    # Create a buffer reader and read the IPC stream
    reader = pa.ipc.open_stream(arrow_buf)
    table = reader.read_all()

    return table


def read_table_from_address(address: int, size: int, base: object) -> pa.Table:
    """
    Read a PyArrow table from a memory address using zero-copy.

    This is useful when you have the raw memory address (e.g., from
    get_slot_address) rather than a memoryview.

    Args:
        address: Memory address of the IPC data.
        size: Number of bytes of valid data.
        base: Python object to keep alive (prevents garbage collection
              of the underlying memory).

    Returns:
        A PyArrow Table that references the memory directly.
    """
    arrow_buf = pa.foreign_buffer(address, size, base=base)
    reader = pa.ipc.open_stream(arrow_buf)
    return reader.read_all()


def table_to_ipc_bytes(table: pa.Table) -> bytes:
    """
    Serialize a PyArrow table to bytes using IPC format.

    This is a fallback for when zero-copy is not available.
    Creates a copy of the data.

    Args:
        table: The PyArrow table to serialize.

    Returns:
        Bytes containing the IPC-serialized table.
    """
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def ipc_bytes_to_table(data: bytes) -> pa.Table:
    """
    Deserialize a PyArrow table from IPC bytes.

    This is a fallback for when zero-copy is not available.

    Args:
        data: Bytes containing the IPC-serialized table.

    Returns:
        The deserialized PyArrow Table.
    """
    reader = pa.ipc.open_stream(data)
    return reader.read_all()


def try_zero_copy_pyarrow(
    table: pa.Table,
    store_config: Any,
    tag: Any,
) -> tuple["RingBufferHandle | None", bool]:
    """
    Try to write a PyArrow table to the ring buffer for zero-copy transfer.

    Args:
        table: The PyArrow table to transfer.
        store_config: The store config to copy to the new message.
        tag: The tag to copy to the new message.

    Returns:
        A tuple of (RingBufferHandle or None, success: bool).
        If successful, returns the handle; otherwise returns None.
    """
    try:
        from enrgdaq.daq.models import RingBufferHandle
        from enrgdaq.utils.shared_ring_buffer import get_global_ring_buffer

        ring_buffer = get_global_ring_buffer()

        # Calculate required size
        table_size = get_table_ipc_size(table)

        # Try to allocate a slot
        slot = ring_buffer.allocate(table_size)
        if slot is None:
            logger.debug(
                f"Ring buffer allocation failed (size={table_size}), "
                "falling back to pickle-based SHM"
            )
            return None, False

        try:
            # Write table directly to slot
            bytes_written = write_table_to_buffer(table, slot.view)

            # Mark slot as ready for reading and track bytes
            ring_buffer.mark_ready(slot.index, bytes_written)

            # Create the handle
            handle = RingBufferHandle(
                buffer_name=ring_buffer.name,
                slot_index=slot.index,
                data_size=bytes_written,
                is_pyarrow=True,
                total_size=ring_buffer.total_size,
                slot_size=ring_buffer.slot_size,
            )

            return handle, True
        except Exception as e:
            # If writing fails, release the slot and fall back
            logger.warning(f"Zero-copy write failed: {e}, falling back")
            slot.release()
            return None, False

    except Exception as e:
        logger.debug(f"Zero-copy setup failed: {e}, falling back to pickle")
        return None, False
