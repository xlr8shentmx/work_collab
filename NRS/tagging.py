"""
Reference flag tagging for NICU Analytics Pipeline.

Tags claims with various flags based on ICD codes, revenue codes, and DRG codes.
"""
import logging
from typing import List
from snowflake.snowpark import Session, DataFrame
from snowflake.snowpark.functions import col

from .reference_manager import ReferenceDataManager

logger = logging.getLogger(__name__)


def tag_icd_flag(
    session: Session,
    claims_df: DataFrame,
    ref_table_name: str,
    diag_cols: List[str],
    flag_name: str
) -> DataFrame:
    """
    Tag claims with a flag based on ICD code matches in diagnosis columns.

    Args:
        session: Snowpark session
        claims_df: Claims DataFrame
        ref_table_name: Reference table name (fully qualified)
        diag_cols: List of diagnosis column names to check
        flag_name: Name of the flag column to add

    Returns:
        Claims DataFrame with flag column added
    """
    ref_icd = (
        session.table(ref_table_name)
        .select(col("CODE").cast("STRING").alias("ICD_CODE"))
        .distinct()
    )

    # Union all diagnosis columns
    diag_union = None
    for diag_col in diag_cols:
        diag_part = claims_df.select(
            col("INDV_ID"),
            col("CLAIMNO"),
            col(diag_col).cast("STRING").alias("DIAG_CODE")
        )
        diag_union = diag_part if diag_union is None else diag_union.union_all(diag_part)

    # Join with reference and identify matching claims
    diag_flagged = (
        diag_union.join(
            ref_icd,
            diag_union["DIAG_CODE"] == ref_icd["ICD_CODE"]
        )
        .select(
            col("INDV_ID").alias("MATCH_INDV_ID"),
            col("CLAIMNO").alias("MATCH_CLAIMNO")
        )
        .distinct()
    )

    # Tag claims
    flagged_claims_df = (
        claims_df.join(
            diag_flagged,
            on=(claims_df["INDV_ID"] == diag_flagged["MATCH_INDV_ID"]) &
               (claims_df["CLAIMNO"] == diag_flagged["MATCH_CLAIMNO"]),
            how="left"
        )
        .with_column(flag_name, col("MATCH_CLAIMNO").is_not_null())
        .drop("MATCH_INDV_ID", "MATCH_CLAIMNO")
    )

    return flagged_claims_df


def tag_rev_flag(
    session: Session,
    claims_df: DataFrame,
    ref_table_name: str,
    flag_name: str
) -> DataFrame:
    """
    Tag claims with a flag based on revenue code matches.

    Args:
        session: Snowpark session
        claims_df: Claims DataFrame
        ref_table_name: Reference table name (fully qualified)
        flag_name: Name of the flag column to add

    Returns:
        Claims DataFrame with flag column added
    """
    ref_rev = (
        session.table(ref_table_name)
        .select(col("CODE").cast("STRING").alias("REV_CODE"))
        .distinct()
    )

    flagged = (
        claims_df.join(
            ref_rev,
            claims_df["REV_CD"].cast("STRING") == ref_rev["REV_CODE"],
            how="left"
        )
        .with_column(flag_name, col("REV_CODE").is_not_null())
        .drop("REV_CODE")
    )

    return flagged


def tag_drg_flag(
    session: Session,
    claims_df: DataFrame,
    ref_table_name: str,
    flag_name: str
) -> DataFrame:
    """
    Tag claims with a flag based on DRG code matches.

    Args:
        session: Snowpark session
        claims_df: Claims DataFrame
        ref_table_name: Reference table name (fully qualified)
        flag_name: Name of the flag column to add

    Returns:
        Claims DataFrame with flag column added
    """
    ref_drg = (
        session.table(ref_table_name)
        .select(col("CODE").cast("STRING").alias("DRG_CODE"))
        .distinct()
    )

    flagged = (
        claims_df.with_column("DRG_3", col("DRG").cast("STRING").substr(1, 3))
        .join(
            ref_drg,
            col("DRG_3") == ref_drg["DRG_CODE"],
            how="left"
        )
        .with_column(flag_name, col("DRG_CODE").is_not_null())
        .drop("DRG_CODE", "DRG_3")
    )

    return flagged


def tag_all_reference_flags(
    session: Session,
    claims_df: DataFrame,
    ref_manager: ReferenceDataManager
) -> DataFrame:
    """
    Tag claims with all reference flags (ICD, revenue, DRG).

    This includes:
    - NEWBORN_ICD: Newborn ICD codes
    - SINGLE, TWIN, MULTIPLE: Birth type ICD codes
    - NEWBORN_REV, NICU_REV: Revenue codes
    - NICU_MSDRG, NICU_APRDRG: DRG codes

    Args:
        session: Snowpark session
        claims_df: Claims DataFrame
        ref_manager: Reference data manager

    Returns:
        Claims DataFrame with all flag columns added
    """
    logger.info("Flagging newborn and NICU enrichments")

    diag_cols = ['DIAG1', 'DIAG2', 'DIAG3', 'DIAG4', 'DIAG5']

    # ICD tags
    icd_tags = [
        ('SUPP_DATA.REF_NEWBORN_ICD', 'NEWBORN_ICD'),
        ('SUPP_DATA.REF_SINGLETON_ICD', 'SINGLE'),
        ('SUPP_DATA.REF_TWIN_ICD', 'TWIN'),
        ('SUPP_DATA.REF_MULTIPLE_ICD', 'MULTIPLE')
    ]
    for ref_table, flag in icd_tags:
        claims_df = tag_icd_flag(
            session, claims_df, ref_table, diag_cols, flag
        ).cache_result()

    # Revenue code tags
    rev_tags = [
        ('SUPP_DATA.REF_NEWBORN_REVCODE', 'NEWBORN_REV'),
        ('SUPP_DATA.REF_NICU_REVCODE', 'NICU_REV')
    ]
    for ref_table, flag in rev_tags:
        claims_df = tag_rev_flag(
            session, claims_df, ref_table, flag
        ).cache_result()

    # DRG tags
    drg_tags = [
        ('SUPP_DATA.REF_NICU_MSDRG', 'NICU_MSDRG'),
        ('SUPP_DATA.REF_NICU_APRDRG', 'NICU_APRDRG')
    ]
    for ref_table, flag in drg_tags:
        claims_df = tag_drg_flag(
            session, claims_df, ref_table, flag
        ).cache_result()

    return claims_df
