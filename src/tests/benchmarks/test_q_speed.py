import os
import time
from multiprocessing import Process


from enrgdaq.utils.queue import ZMQQueue


def producer(q, size, count):
    data = os.urandom(size)
    start = time.perf_counter()
    for _ in range(count):
        q.put(data)
    end = time.perf_counter()
    print(
        f"Producer: {count} messages of {size/1e6:.2f} MB in {end-start:.2f}s ({(count*size)/1e6/(end-start):.2f} MB/s)"
    )


def consumer(q, count):
    for _ in range(count):
        q.get()


if __name__ == "__main__":
    q = ZMQQueue()
    size = 10_000_000  # 10MB
    count = 100

    p = Process(target=producer, args=(q, size, count))
    c = Process(target=consumer, args=(q, count))

    p.start()
    c.start()

    p.join()
    c.join()
