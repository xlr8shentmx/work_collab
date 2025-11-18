"""
Claims data processing for NICU Analytics Pipeline.
"""
import logging
from datetime import date
from typing import List
from snowflake.snowpark import Session, DataFrame
from snowflake.snowpark.functions import col, when, lit

from .reference_manager import ReferenceDataManager

logger = logging.getLogger(__name__)


def fetch_newborn_keys(
    session: Session,
    client_data: str,
    birth_start: date,
    birth_end: date,
    runout_end: date,
    ref_manager: ReferenceDataManager
) -> List[str]:
    """
    Identify all distinct INDV_IDs with birth-related claims during the birth window.

    Uses reference tables to identify newborns via ICD codes, revenue codes, and DRG codes.

    Args:
        session: Snowpark session
        client_data: Client identifier
        birth_start: Start of birth window
        birth_end: End of birth window
        runout_end: End of runout period
        ref_manager: Reference data manager

    Returns:
        List of INDV_IDs representing newborns
    """
    logger.info("Fetching newborn keys")

    table_name = f"FA_MEDICAL_{client_data}"
    df = session.table(table_name).filter(
        (col("SRVC_FROM_DT") >= birth_start) &
        (col("SRVC_FROM_DT") <= birth_end) &
        (col("PROCESS_DT") <= runout_end) &
        col("INDV_ID").is_not_null()
    )

    # Load reference tables
    rev_ref = ref_manager.get_newborn_revcode().select(col("CODE").alias("REV_CODE"))
    icd_ref = ref_manager.get_newborn_icd().select(col("CODE").alias("ICD_CODE"))
    msdrg_ref = ref_manager.get_nicu_msdrg().select(col("CODE").alias("MSDRG"))
    aprdrg_ref = ref_manager.get_nicu_aprdrg().select(col("CODE").alias("APRDRG"))

    # Join claims to reference tables
    cond_rev = df["RVNU_CD"].cast("string") == rev_ref["REV_CODE"]
    cond_icd = (
        (df["DIAG_1_CD"].cast("string") == icd_ref["ICD_CODE"]) |
        (df["DIAG_2_CD"].cast("string") == icd_ref["ICD_CODE"]) |
        (df["DIAG_3_CD"].cast("string") == icd_ref["ICD_CODE"]) |
        (df["DIAG_4_CD"].cast("string") == icd_ref["ICD_CODE"]) |
        (df["DIAG_5_CD"].cast("string") == icd_ref["ICD_CODE"])
    )
    cond_msdrg = df["DRG"].substr(1, 3) == msdrg_ref["MSDRG"]
    cond_aprdrg = df["DRG"].substr(1, 3) == aprdrg_ref["APRDRG"]

    newborn_keys = (
        df.join(rev_ref, cond_rev, "left")
        .join(icd_ref, cond_icd, "left")
        .join(msdrg_ref, cond_msdrg, "left")
        .join(aprdrg_ref, cond_aprdrg, "left")
        .filter(
            col("REV_CODE").is_not_null() |
            col("ICD_CODE").is_not_null() |
            col("MSDRG").is_not_null() |
            col("APRDRG").is_not_null()
        )
        .select("INDV_ID")
        .distinct()
        .to_pandas()
    )

    keys = newborn_keys["INDV_ID"].tolist()
    logger.info(f"Found {len(keys)} unique newborn keys")

    return keys


def load_newborn_claims(
    session: Session,
    client_data: str,
    newborn_keys: List[str],
    birth_start: date,
    birth_end: date,
    runout_end: date
) -> DataFrame:
    """
    Load claims data for identified newborns.

    Args:
        session: Snowpark session
        client_data: Client identifier
        newborn_keys: List of INDV_IDs to load
        birth_start: Start of birth window
        birth_end: End of birth window
        runout_end: End of runout period

    Returns:
        Snowpark DataFrame with newborn claims
    """
    logger.info("Loading newborn claims")

    fa_medical = session.table(f"FA_MEDICAL_{client_data}")

    if not newborn_keys:
        logger.warning("No newborn keys found - skipping claims pull")
        return session.create_dataframe([], schema=["INDV_ID"])

    # Ensure keys are strings
    newborn_keys = [str(k) for k in newborn_keys if k is not None]

    claims_df = (
        fa_medical
        .filter(
            (fa_medical['SRVC_FROM_DT'] >= birth_start) &
            (fa_medical['SRVC_FROM_DT'] <= birth_end)
        )
        .filter(fa_medical['PROCESS_DT'] <= runout_end)
        .filter(fa_medical['INDV_ID'].isin(newborn_keys))
        .select(
            fa_medical['INDV_ID'],
            fa_medical['CLM_AUD_NBR'].alias('CLAIMNO'),
            fa_medical['SRVC_FROM_DT'].alias('FROMDATE'),
            fa_medical['SRVC_THRU_DT'].alias('THRUDATE'),
            fa_medical['PROCESS_DT'].alias('PAIDDATE'),
            fa_medical['ADMIT_DT'],
            fa_medical['DSCHRG_DT'].alias('DSCHRG_DT'),
            fa_medical['DIAG_1_CD'].alias('DIAG1'),
            fa_medical['DIAG_2_CD'].alias('DIAG2'),
            fa_medical['DIAG_3_CD'].alias('DIAG3'),
            fa_medical['DIAG_4_CD'].alias('DIAG4'),
            fa_medical['DIAG_5_CD'].alias('DIAG5'),
            fa_medical['PROC_1_CD'].alias('PROC1'),
            fa_medical['PROC_2_CD'].alias('PROC2'),
            fa_medical['PROC_3_CD'].alias('PROC3'),
            fa_medical['PROC_CD'].alias('CPTCODE'),
            fa_medical['DSCHRG_STS'].alias('DSCHRG_STS_CD'),
            fa_medical['SBMT_CHRG_AMT'].alias('BILLED'),
            fa_medical['DRG'].substr(0, 3).alias('DRG'),
            fa_medical['DRG_TYPE'],
            fa_medical['DRG_OTLR_FLG'],
            fa_medical['DRG_OTLR_COST'],
            fa_medical['NET_PD_AMT'].alias('AMTPAID'),
            fa_medical['PL_OF_SRVC_CD'].alias('POS'),
            fa_medical['RVNU_CD'].alias('REV_CD'),
            fa_medical['PROV_NPI'].alias("PROV_ID"),
            fa_medical['PROV_TIN'],
            fa_medical['PROV_FULL_NM'].alias('PROV_FULL_NM'),
            fa_medical['PROV_STATE'],
            fa_medical['PROV_TYP_CD'].alias('PROV_TYPE')
        )
    )

    return claims_df


def assign_claim_type(df: DataFrame) -> DataFrame:
    """
    Assign claim type (IP, ER, OP) based on place of service, revenue codes, CPT codes, and DRG.

    Args:
        df: Claims DataFrame

    Returns:
        DataFrame with CLAIM_TYPE column added
    """
    logger.info("Assigning Claim Types")

    return df.with_column(
        "CLAIM_TYPE",
        when(
            (col("POS") == "21") |
            (col("REV_CD").between("0100", "0210")) |
            (col("REV_CD") == "0987") |
            (col("CPTCODE").between("99221", "99239")) |
            (col("CPTCODE").between("99251", "99255")) |
            (col("CPTCODE").between("99261", "99263")) |
            (col("DRG").is_not_null()),
            lit("IP")
        ).when(
            (col("POS") == "23") |
            (col("CPTCODE").isin([
                "99281", "99282", "99283", "99284",
                "99285", "99286", "99287", "99288"
            ])) |
            (col("REV_CD").startswith("045")) |
            (col("REV_CD") == "0981"),
            lit("ER")
        ).otherwise(lit("OP"))
    )
