"""Date utilities for handling date ranges and increments."""
from datetime import datetime, timedelta
from typing import Generator, Tuple, Optional

def get_date_range(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    days_back: int = 30
) -> Tuple[datetime, datetime]:
    """
    Get start and end dates for data fetching.
    
    Args:
        start_date: Optional start date in YYYY-MM-DD format
        end_date: Optional end date in YYYY-MM-DD format
        days_back: Number of days to look back if no start_date provided
    
    Returns:
        Tuple of (start_date, end_date) as datetime objects
    """
    if end_date:
        end = datetime.strptime(end_date, "%Y-%m-%d")
    else:
        end = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    
    if start_date:
        start = datetime.strptime(start_date, "%Y-%m-%d")
    else:
        start = end - timedelta(days=days_back)
    
    return start, end

def date_range_iterator(
    start_date: datetime,
    end_date: datetime,
    increment_days: int = 1
) -> Generator[Tuple[datetime, datetime], None, None]:
    """
    Generate date ranges in increments.
    
    Args:
        start_date: Start datetime
        end_date: End datetime
        increment_days: Number of days for each increment
        
    Yields:
        Tuple of (period_start, period_end) for each increment
    """
    current = start_date
    while current < end_date:
        period_end = min(current + timedelta(days=increment_days), end_date)
        yield current, period_end
        current = period_end

def format_date(dt: datetime) -> str:
    """Format datetime object to YYYY-MM-DD string."""
    return dt.strftime("%Y-%m-%d") 