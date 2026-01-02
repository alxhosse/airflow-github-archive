"""Path utilities for creating partitioned data paths."""
from datetime import datetime
from pathlib import Path
from typing import Optional


def get_hourly_archive_path(
    base_dir: str,
    year: int,
    month: int,
    day: int,
    hour: int,
    file_type: str = "json.gz",
) -> Path:
    """
    Generate partitioned path for hourly archive data.
    
    Structure: base_dir/year=YYYY/month=MM/day=DD/hour=HH/file.json.gz
    
    Args:
        base_dir: Base directory for data storage
        year: Year (e.g., 2024)
        month: Month (1-12)
        day: Day (1-31)
        hour: Hour (0-23)
        file_type: File extension (default: json.gz)
    
    Returns:
        Path object for the partitioned file location
    """
    path = Path(base_dir) / f"year={year:04d}" / f"month={month:02d}" / f"day={day:02d}" / f"hour={hour:02d}"
    path.mkdir(parents=True, exist_ok=True)
    
    if file_type == "parquet":
        return path / "data.parquet"
    elif file_type == "json.gz":
        return path / f"{year:04d}-{month:02d}-{day:02d}-{hour:02d}.json.gz"
    else:
        return path / f"data.{file_type}"


def get_gh_archive_url(year: int, month: int, day: int, hour: int) -> str:
    """
    Generate GH Archive URL for a specific hour.
    
    Args:
        year: Year (e.g., 2024)
        month: Month (1-12)
        day: Day (1-31)
        hour: Hour (0-23)
    
    Returns:
        URL string for the GH Archive file
    """
    # GH Archive uses single digit for hours 0-9, two digits for 10-23
    return f"https://data.gharchive.org/{year:04d}-{month:02d}-{day:02d}-{hour}.json.gz"


def parse_datetime_from_execution_date(execution_date: Optional[datetime] = None) -> tuple[int, int, int, int]:
    """
    Parse year, month, day, hour from execution date.
    
    Args:
        execution_date: Datetime object (defaults to current UTC time)
    
    Returns:
        Tuple of (year, month, day, hour)
    """
    if execution_date is None:
        execution_date = datetime.utcnow()
    
    return (
        execution_date.year,
        execution_date.month,
        execution_date.day,
        execution_date.hour,
    )

