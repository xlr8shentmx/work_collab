"""
Membership data processing for NICU Analytics Pipeline.
"""
import logging
from datetime import date, timedelta
from snowflake.snowpark import Session, DataFrame
from snowflake.snowpark.functions import (
    col, row_number, to_date, concat, lit, when,
    min as smin, max as smax, greatest, least, datediff
)
from snowflake.snowpark.window import Window

from .utils import to_pydate, format_period_string
from .data_sources import export_to_snowflake

logger = logging.getLogger(__name__)


def process_membership(
    session: Session,
    client: str,
    birth_start: date,
    birth_mid: date,
    birth_end: date,
    client_nm: str,
    table_config
) -> DataFrame:
    """
    Process membership data for the study periods.

    Calculates member demographics, coverage windows, and months of membership
    for both previous and current study periods.

    Args:
        session: Snowpark session
        client: Client identifier
        birth_start: Start of birth window
        birth_mid: Midpoint of birth window (divides previous/current)
        birth_end: End of birth window
        client_nm: Client name for labeling
        table_config: Table configuration object

    Returns:
        Snowpark DataFrame with processed membership data
    """
    logger.info("Processing Membership Data")

    # Convert to Python dates
    birth_start = to_pydate(birth_start)
    birth_mid = to_pydate(birth_mid)
    birth_end = to_pydate(birth_end)

    # Load source membership data
    src = (
        session.table(f"FA_MEMBERSHIP_{client}")
        .filter(col("INDV_ID").is_not_null() & col("YEARMO").is_not_null())
        # YEARMO like '202401' -> 2024-01-01
        .with_column("MM_DATE", to_date(concat(col("YEARMO"), lit("01")), "YYYYMMDD"))
    )

    # Get most recent demographics per member (by MM_DATE desc)
    w = Window.partition_by("INDV_ID").order_by(col("MM_DATE").desc())
    latest = (
        src.with_column("RN", row_number().over(w))
        .filter(col("RN") == 1)
        .select("INDV_ID", "GENDER", "BTH_DT", "BUS_LINE_CD", "PRODUCT_CD", "STATE")
    )

    # Calculate coverage window per member
    cov = (
        src.group_by("INDV_ID")
        .agg(
            smin("MM_DATE").alias("MEM_EFF"),
            smax("MM_DATE").alias("MEM_EXP")
        )
    )

    # Join demographics with coverage
    base = latest.join(cov, ["INDV_ID"], "inner")

    # Calculate period boundaries
    prev_high = birth_mid - timedelta(days=1)  # first window ends day before mid
    prev_high = to_pydate(prev_high)

    # Effective and expiration dates for each period
    eff_prev = greatest(col("MEM_EFF"), lit(birth_start))
    exp_prev = least(col("MEM_EXP"), lit(prev_high))
    eff_curr = greatest(col("MEM_EFF"), lit(birth_mid))
    exp_curr = least(col("MEM_EXP"), lit(birth_end))

    # Calculate months of membership for each period
    with_mmyr = (
        base
        .with_column(
            "MMYR1",
            when(exp_prev < eff_prev, lit(0))
            .otherwise(datediff("month", eff_prev, exp_prev) + lit(1))
        )
        .with_column(
            "MMYR2",
            when(exp_curr < eff_curr, lit(0))
            .otherwise(datediff("month", eff_curr, exp_curr) + lit(1))
        )
        .with_column(
            "AGE",
            when(col("BTH_DT").is_null(), lit(None))
            .otherwise(datediff("year", col("BTH_DT"), lit(birth_end)))
        )
        .with_column("CLIENT_NAME", lit(client_nm))
        .with_column("PREVIOUS_PERIOD", lit(format_period_string(birth_start, prev_high)))
        .with_column("CURRENT_PERIOD", lit(format_period_string(birth_mid, birth_end)))
    )

    # Materialize two study-year slices
    prev_df = (
        with_mmyr
        .filter(col("MMYR1") > 0)
        .with_column("STUDY_YR", lit("Previous"))
    )

    curr_df = (
        with_mmyr
        .filter(col("MMYR2") > 0)
        .with_column("STUDY_YR", lit("Current"))
    )

    member_df = prev_df.union_all(curr_df)

    # Export to Snowflake
    output_table = table_config.membership_output_table(client)
    export_to_snowflake(member_df, output_table)

    return member_df


def create_eligibility_table(session: Session, client: str, table_config) -> DataFrame:
    """
    Create eligibility table from processed membership.

    Returns the most recent row per INDV_ID, preferring 'Current' over 'Previous'.

    Args:
        session: Snowpark session
        client: Client identifier
        table_config: Table configuration object

    Returns:
        Snowpark DataFrame with eligibility data
    """
    logger.info("Creating eligibility table")

    src = session.table(table_config.membership_output_table(client))

    # Prefer 'Current' study year; use MEM_EXP as tie-breaker if it exists
    prefer = when(col("STUDY_YR") == lit("Current"), lit(0)).otherwise(lit(1))
    order_cols = [prefer]

    if "MEM_EXP" in src.columns:
        order_cols.append(col("MEM_EXP").desc_nulls_last())

    w = Window.partition_by("INDV_ID").order_by(*order_cols)

    elig_df = (
        src.with_column("RN", row_number().over(w))
        .filter(col("RN") == 1)
        .select(
            col("INDV_ID"),
            col("GENDER"),
            col("BTH_DT"),
            col("BUS_LINE_CD"),
            col("PRODUCT_CD"),
            col("STATE")
        )
    )

    return elig_df


def merge_eligibility(
    claims_df: DataFrame,
    elig_df: DataFrame
) -> DataFrame:
    """
    Join eligibility details to claims data.

    Args:
        claims_df: Claims DataFrame
        elig_df: Eligibility DataFrame

    Returns:
        Claims DataFrame with eligibility columns joined
    """
    logger.info("Merging eligibility data")

    merged = claims_df.join(
        elig_df,
        claims_df["INDV_ID"] == elig_df["INDV_ID"],
        "left"
    )

    # Get eligibility columns (excluding INDV_ID which is already in claims)
    elig_non_key_cols = [c for c in elig_df.columns if c.upper() != "INDV_ID"]

    # Select claims columns plus eligibility columns, avoiding duplicate INDV_ID
    result = merged.select(
        claims_df["INDV_ID"].alias("INDV_ID"),
        *[col(c) for c in claims_df.columns if c.upper() != "INDV_ID"],
        *[col(f'"{c}"') for c in elig_non_key_cols]
    )

    return result
