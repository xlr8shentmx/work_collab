"""
Utility functions for NICU Analytics Pipeline.
"""
import logging
from datetime import datetime, date, timedelta
from typing import Union, Tuple
import pandas as pd
from dateutil.relativedelta import relativedelta
from snowflake.snowpark import Session


# Configure logging
logger = logging.getLogger(__name__)


def setup_logging(level: int = logging.INFO) -> None:
    """
    Configure logging for the pipeline.

    Args:
        level: Logging level (default: INFO)
    """
    logging.basicConfig(
        level=level,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[logging.StreamHandler()]
    )


def to_pydate(x: Union[datetime, date, pd.Timestamp]) -> date:
    """
    Convert various date types to Python date object.

    Args:
        x: Date value (datetime, date, or pandas Timestamp)

    Returns:
        Python date object

    Raises:
        TypeError: If input type is not supported
    """
    if hasattr(x, "to_pydatetime"):
        return x.to_pydatetime().date()
    if isinstance(x, datetime):
        return x.date()
    if isinstance(x, date):
        return x
    raise TypeError(f"Unsupported date type: {type(x)}")


def calculate_birth_window(
    session: Session,
    client_data: str,
    table_config
) -> Tuple[datetime, datetime, datetime, datetime]:
    """
    Auto-calculate birth window dates from source data.

    Determines the appropriate birth window and runout periods based on
    available data in the medical claims table.

    Args:
        session: Snowpark session
        client_data: Client identifier
        table_config: Table configuration object

    Returns:
        Tuple of (birth_window_start, birth_window_end, birth_window_mid, runout_end)

    Raises:
        ValueError: If insufficient data is available or date range is invalid
    """
    logger.info("Calculating birth and runout window from source data")

    table = f"{table_config.database}.{table_config.stage_schema}.{table_config.medical_table(client_data)}"
    query = f"""
    SELECT
        MIN(SRVC_FROM_DT) AS MIN_FROMDATE,
        MAX(SRVC_FROM_DT) AS MAX_FROMDATE,
        MAX(PROCESS_DT) AS MAX_PAIDDATE
    FROM {table}
    WHERE SRVC_FROM_DT IS NOT NULL AND PROCESS_DT IS NOT NULL
    """

    df = session.sql(query).to_pandas()
    min_dt = pd.to_datetime(df.at[0, 'MIN_FROMDATE'])
    max_dt = pd.to_datetime(df.at[0, 'MAX_FROMDATE'])
    max_ro_dt = pd.to_datetime(df.at[0, 'MAX_PAIDDATE'])

    if pd.isna(min_dt) or pd.isna(max_dt):
        raise ValueError("FROMDATE range is invalid. Cannot determine birth window.")

    num_months = (max_dt.year - min_dt.year) * 12 + (max_dt.month - min_dt.month) + 1
    if num_months < 24:
        raise ValueError(
            f"Only {num_months} months available. Minimum 24 months required."
        )

    # Calculate windows
    last_complete_month_end = max_dt.replace(day=1) - pd.Timedelta(days=1)
    runout_end = last_complete_month_end
    runout_start = runout_end - relativedelta(months=3) + pd.Timedelta(days=1)
    birth_window_end = runout_start - pd.Timedelta(days=1)
    birth_window_start = birth_window_end - relativedelta(months=24) + pd.Timedelta(days=1)
    birth_window_mid = birth_window_start + relativedelta(months=12)

    # Convert to datetime
    birth_window_start = pd.to_datetime(birth_window_start).to_pydatetime()
    birth_window_end = pd.to_datetime(birth_window_end).to_pydatetime()
    runout_end = pd.to_datetime(runout_end).to_pydatetime()

    logger.info(f"Birth window: {birth_window_start.date()} to {birth_window_end.date()}")
    logger.info(f"Runout end: {runout_end.date()}")

    return birth_window_start, birth_window_end, birth_window_mid, runout_end


def format_period_string(start_date: date, end_date: date) -> str:
    """
    Format a date range as a period string (e.g., "Jan 2023 - Dec 2023").

    Args:
        start_date: Start date
        end_date: End date

    Returns:
        Formatted period string
    """
    return f"{start_date:%b %Y} - {end_date:%b %Y}"


def calculate_age_at_date(birth_date: date, reference_date: date) -> int:
    """
    Calculate age in years at a reference date.

    Args:
        birth_date: Date of birth
        reference_date: Date to calculate age at

    Returns:
        Age in years
    """
    years = reference_date.year - birth_date.year
    if (reference_date.month, reference_date.day) < (birth_date.month, birth_date.day):
        years -= 1
    return years


def validate_date_range(
    start_date: Union[datetime, date],
    end_date: Union[datetime, date],
    min_days: int = 1
) -> bool:
    """
    Validate that a date range is valid and meets minimum duration.

    Args:
        start_date: Start date
        end_date: End date
        min_days: Minimum number of days required

    Returns:
        True if valid, False otherwise
    """
    if start_date is None or end_date is None:
        return False

    start = to_pydate(start_date) if not isinstance(start_date, date) else start_date
    end = to_pydate(end_date) if not isinstance(end_date, date) else end_date

    return end >= start and (end - start).days >= min_days


def safe_division(numerator: float, denominator: float, default: float = 0.0) -> float:
    """
    Safely perform division, returning default value if denominator is zero.

    Args:
        numerator: Numerator value
        denominator: Denominator value
        default: Default value to return if division is invalid

    Returns:
        Result of division or default value
    """
    if denominator == 0 or denominator is None:
        return default
    return numerator / denominator


def coalesce(*args):
    """
    Return the first non-None value from arguments.

    Args:
        *args: Variable number of arguments

    Returns:
        First non-None value, or None if all are None
    """
    for arg in args:
        if arg is not None:
            return arg
    return None


class Timer:
    """Context manager for timing code execution."""

    def __init__(self, name: str = "Operation", log_level: int = logging.INFO):
        """
        Initialize timer.

        Args:
            name: Name of the operation being timed
            log_level: Logging level for output
        """
        self.name = name
        self.log_level = log_level
        self.start_time = None
        self.end_time = None

    def __enter__(self):
        """Start timer."""
        self.start_time = datetime.now()
        logger.log(self.log_level, f"{self.name} started")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """End timer and log duration."""
        self.end_time = datetime.now()
        duration = (self.end_time - self.start_time).total_seconds()
        logger.log(self.log_level, f"{self.name} completed in {duration:.2f} seconds")

    @property
    def elapsed(self) -> float:
        """Get elapsed time in seconds."""
        if self.start_time is None:
            return 0.0
        end = self.end_time or datetime.now()
        return (end - self.start_time).total_seconds()
