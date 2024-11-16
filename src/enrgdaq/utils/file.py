import os
from datetime import datetime


def modify_file_path(file_path: str, add_date: bool, prefix: str | None) -> str:
    """
    Modifies a file path by adding a date and/or a prefix.

    Args:
        file_path (str): The original file path.
        add_date (bool): Whether to add a date to the file path.
        prefix (str | None): The prefix to add to the file path.

    Returns:
        str: The modified file path.
    """

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
