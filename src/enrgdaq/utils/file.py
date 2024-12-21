import os
from datetime import datetime


def modify_file_path(
    file_path: str, add_date: bool, tag: str | None, date: datetime | None = None
) -> str:
    """
    Modifies a file path by adding a date and/or a tag.

    Args:
        file_path (str): The original file path.
        add_date (bool): Whether to add a date to the file path.
        tag (str | None): The tag to add to the file path.

    Returns:
        str: The modified file path.

    Example:
        >>> modify_file_path("test.csv", True, "tag")
        '2023/01/01/test_tag.csv'
    """

    if add_date:
        if date is None:
            date = datetime.now()
        sep = os.path.sep
        file_dir = f"{date.year}{sep}{date.month:02d}{sep}{date.day:02d}"
        file_path = os.path.join(file_dir, file_path)
    if tag is not None:
        head, tail = os.path.splitext(file_path)
        file_path = f"{head}_{tag}{tail}"

    return file_path
