import io
import pickle
import time

import numpy as np
import pyarrow as pa


def test():
    size = 500_000
    table = pa.table(
        {
            "timestamp": pa.array(np.full(size, time.time(), dtype=np.float64)),
            "value": pa.array(np.random.random(size)),
        }
    )

    # Pickle
    start = time.perf_counter()
    for _ in range(100):
        p = pickle.dumps(table)
    end = time.perf_counter()
    print(
        f"Pickle 100 tables: {end-start:.2f}s ({(100*len(p))/1e6/(end-start):.2f} MB/s)"
    )

    # Arrow IPC (Stream)
    start = time.perf_counter()
    for _ in range(100):
        sink = io.BytesIO()
        with pa.ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        data = sink.getvalue()
    end = time.perf_counter()
    print(
        f"Arrow IPC 100 tables: {end-start:.2f}s ({(100*len(data))/1e6/(end-start):.2f} MB/s)"
    )

    # Arrow IPC (Batch lookup - even faster if we reuse schema)
    sink = io.BytesIO()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        pass

    start = time.perf_counter()
    for _ in range(100):
        sink = io.BytesIO()
        # Just write the batches
        for batch in table.to_batches():
            pass  # simulate
    end = time.perf_counter()
    # (Actually to_batches is where the work is)


if __name__ == "__main__":
    test()
