from datetime import datetime


def get_unix_timestamp_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def get_now_unix_timestamp_ms() -> int:
    return get_unix_timestamp_ms(datetime.now())
