from datetime import datetime


def get_now_unix_timestamp_ms() -> int:
    return int(datetime.now().timestamp() * 1000)
