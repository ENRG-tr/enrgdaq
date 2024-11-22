import time
from datetime import datetime
from typing import Optional


def get_unix_timestamp_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def get_now_unix_timestamp_ms() -> int:
    return get_unix_timestamp_ms(datetime.now())


def sleep_for(seconds: float, start_time: Optional[datetime] = None):
    if start_time is None:
        start_time = datetime.now()
    sleep_time = seconds - (datetime.now() - start_time).total_seconds()
    sleep_time = max(0, sleep_time)
    time.sleep(sleep_time)
