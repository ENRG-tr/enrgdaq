from asyncio import Queue
from typing import Generic, TypeVar

T = TypeVar("T")


class MultiConsumerQueue(Generic[T]):
    _queue_list: list[Queue[T]]

    def __init__(self):
        self._queue_list = []

    def subscribe(self) -> Queue[T]:
        q = Queue[T]()
        self._queue_list.append(q)
        return q

    def unsubscribe(self, q: Queue[T]):
        self._queue_list.remove(q)

    def put(self, item: T):
        for q in self._queue_list:
            q.put_nowait(item)

    def get_subscriber_count(self):
        return len(self._queue_list)
