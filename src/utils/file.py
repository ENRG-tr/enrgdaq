import os
from datetime import datetime


def add_date_to_file_name(file_path: str, add_date: bool) -> str:
    splitted_file_path = os.path.splitext(file_path)
    date_text = datetime.now().strftime("%Y-%m-%d")
    if len(splitted_file_path) > 1:
        file_path = f"{splitted_file_path[0]}_{date_text}{splitted_file_path[1]}"
    else:
        file_path = f"{splitted_file_path[0]}_{date_text}"

    return file_path
