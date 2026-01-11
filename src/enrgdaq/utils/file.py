import os
from datetime import datetime

# Extensions that should be treated as compression suffixes (applied after primary extension)
COMPRESSION_EXTENSIONS = {".zst", ".gz", ".bz2", ".xz", ".lz4"}


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
        >>> modify_file_path("test.csv", True, "tag1")
        '2023-01-01_test_tag1.csv'
        >>> modify_file_path("boo/test.csv", True, "tag2")
        'boo/2023-01-01_test_tag2.csv'
        >>> modify_file_path("test.csv.zst", False, "latency")
        'test_latency.csv.zst'
    """

    if add_date:
        if date is None:
            date = datetime.now()
        sep = "-"
        file_prefix = f"{date.year}{sep}{date.month:02d}{sep}{date.day:02d}"
        # Add prefix to only the file name
        filename = os.path.basename(file_path)
        file_path = os.path.join(
            os.path.dirname(file_path), f"{file_prefix}_{filename}"
        )
    if tag is not None:
        # Handle compound extensions like .csv.zst, .csv.gz
        head, ext = os.path.splitext(file_path)
        if ext.lower() in COMPRESSION_EXTENSIONS:
            # Split again to get the primary extension
            head2, ext2 = os.path.splitext(head)
            file_path = f"{head2}_{tag}{ext2}{ext}"
        else:
            file_path = f"{head}_{tag}{ext}"

    return file_path
