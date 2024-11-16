import os
from datetime import datetime


def modify_file_path(file_path: str, add_date: bool, prefix: str | None) -> str:
    split = os.path.splitext(file_path)
    date_text = datetime.now().strftime("%Y-%m-%d")
    if add_date:
        if len(split) > 1:
            file_path = f"{split[0]}_{date_text}{split[1]}"
        else:
            file_path = f"{split[0]}_{date_text}"
    if prefix is not None:
        head, tail = os.path.split(file_path)
        file_path = os.path.join(head, f"{prefix}_{tail}")

    return file_path
