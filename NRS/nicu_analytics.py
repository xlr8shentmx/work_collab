"""
NICU-specific analytics and rollup logic for NICU Analytics Pipeline.
"""
import logging
from datetime import date
from typing import Dict
from snowflake.snowpark import Session, DataFrame
from snowflake.snowpark.functions import (
    col, row_number, when, lit, concat,
    min as smin, max as smax, sum as ssum, abs as sabs,
    datediff, first_value, coalesce, length, lag,
    sql_expr, to_char, substring, count_distinct, greatest, least
)
from snowflake.snowpark.window import Window

from .config import BUSINESS_RULES, CPT_CONFIG, REV_CODE_CONFIG

logger = logging.getLogger(__name__)


def newborn_rollup(
    session: Session,
    client: str,
    claims_df: DataFrame
) -> tuple:
    """
    Apply birth type hierarchy and aggregate newborn data.

    Args:
        session: Snowpark session
        client: Client identifier
        claims_df: Claims DataFrame with flags

    Returns:
        Tuple of (newborns_df, claims_df) with birth type and NICU flags
    """
    logger.info("Applying newborn rollup logic")

    # Ensure date types
    c = (
        claims_df
        .with_column("BTH_DT", col("BTH_DT").cast("DATE"))
        .with_column("FROMDATE", col("FROMDATE").cast("DATE"))
    )

    # Per-claim birth type priority (Multiple > Twin > Single)
    c = (
        c
        .with_column(
            "BIRTH_PRI",
            when(col("MULTIPLE"), lit(3))
            .when(col("TWIN"), lit(2))
            .when(col("SINGLE"), lit(1))
            .otherwise(lit(0))
        )
        .with_column(
            "BIRTH_TYPE",
            when(col("BIRTH_PRI") == 3, lit("Multiple"))
            .when(col("BIRTH_PRI") == 2, lit("Twin"))
            .when(col("BIRTH_PRI") == 1, lit("Single"))
            .otherwise(lit("Unknown"))
        )
    )

    # Likely newborn records
    newborn_only = c.filter(col("NEWBORN_ICD") | col("NEWBORN_REV"))

    # Group per baby: INDV_ID + BTH_DT
    newborns = (
        newborn_only
        .group_by("INDV_ID", "BTH_DT")
        .agg(
            smax("BIRTH_PRI").alias("MAX_PRI"),
            smin("FROMDATE").alias("SVC_DATE"),
            smax(when(col("NICU_REV"), lit(1)).otherwise(lit(0))).alias("HAS_NICU_REV"),
            smax(when(col("NICU_MSDRG"), lit(1)).otherwise(lit(0))).alias("HAS_NICU_MSDRG"),
            smax(when(col("NICU_APRDRG"), lit(1)).otherwise(lit(0))).alias("HAS_NICU_APRDRG"),
        )
        .with_column(
            "BIRTH_TYPE",
            when(col("MAX_PRI") == 3, lit("Multiple"))
            .when(col("MAX_PRI") == 2, lit("Twin"))
            .when(col("MAX_PRI") == 1, lit("Single"))
            .otherwise(lit("Unknown"))
        )
        .drop("MAX_PRI")
    )

    # Derived baby-level fields
    newborns = (
        newborns
        .with_column(
            "IN_DAYS",
            (sabs(datediff("day", col("SVC_DATE"), col("BTH_DT"))) <= lit(BUSINESS_RULES.init_hosp_threshold_days))
        )
        .with_column(
            "BABY_TYPE",
            when(
                (col("HAS_NICU_REV") == 1) |
                (col("HAS_NICU_MSDRG") == 1) |
                (col("HAS_NICU_APRDRG") == 1),
                lit("NICU")
            ).otherwise(lit("Normal Newborn"))
        )
        .with_column(
            "CONTRACT",
            when(
                (col("HAS_NICU_MSDRG") == 1) | (col("HAS_NICU_APRDRG") == 1),
                lit("DRG")
            ).otherwise(lit("Per-Diem"))
        )
        .with_column_renamed("BIRTH_TYPE", "EP_BIRTH_TYPE")
    )

    # DELIVERY_DT = earliest service date per INDV_ID
    w_key = Window.partition_by("INDV_ID").order_by(col("SVC_DATE").asc())
    newborns = newborns.with_column("DELIVERY_DT", first_value("SVC_DATE").over(w_key))

    # Join back to all claims
    joined = (
        newborns
        .select("INDV_ID", "EP_BIRTH_TYPE", "DELIVERY_DT", "IN_DAYS", "BABY_TYPE", "CONTRACT")
        .join(c, "INDV_ID", "left")
    )

    # Filter claims to on/after delivery date
    newborn_claims = joined.filter(col("FROMDATE") >= col("DELIVERY_DT"))

    # High-cost flag
    newborn_claims = newborn_claims.with_column(
        "HIGH_COST",
        col("AMTPAID") > lit(BUSINESS_RULES.high_cost_threshold)
    )

    return newborns, newborn_claims


def build_hosp_rollup(claims_df: DataFrame, runout_end: date) -> DataFrame:
    """
    Build hospital stay rollup from claims.

    Stitches together inpatient claims into continuous hospital stays.

    Args:
        claims_df: Claims DataFrame
        runout_end: End of runout period

    Returns:
        DataFrame with hospital stays (INDV_ID, DELIVERY_DT, HOSP_STAY, ADMIT, DSCHRG, etc.)
    """
    logger.info("Rolling up hospital stays")

    # Fill missing dates
    c = (
        claims_df
        .with_column("ADMIT_DT_FIL", coalesce(col("ADMIT_DT"), col("FROMDATE")))
        .with_column("DSCHRG_DT_FIL", coalesce(col("DSCHRG_DT"), col("THRUDATE")))
        .with_column("ADMIT_DT_FIL", col("ADMIT_DT_FIL").cast("DATE"))
        .with_column("DSCHRG_DT_FIL", col("DSCHRG_DT_FIL").cast("DATE"))
        .with_column("DELIVERY_DT", col("DELIVERY_DT").cast("DATE"))
    )

    # Core filter
    base_filter = (
        (col("CLAIM_TYPE") == lit("IP")) &
        ((col("ADMIT_DT_FIL") >= col("DELIVERY_DT")) | (col("FROMDATE") >= col("DELIVERY_DT")))
    )
    if "HIGH_COST" in c.columns:
        c = c.filter(~col("HIGH_COST") & base_filter)
    else:
        c = c.filter(base_filter)

    # Gap logic (new stay when gap > 4 days)
    w_sort = Window.partition_by("INDV_ID", "DELIVERY_DT").order_by(
        col("ADMIT_DT_FIL"), col("DSCHRG_DT_FIL")
    )
    prev_dis = lag(col("DSCHRG_DT_FIL")).over(w_sort)
    gap_days = datediff("day", prev_dis, col("ADMIT_DT_FIL"))
    new_stay_flag = when(
        prev_dis.is_null() | (gap_days > lit(BUSINESS_RULES.hospital_gap_days)),
        lit(1)
    ).otherwise(lit(0))

    c = c.with_column("NEW_STAY", new_stay_flag)

    # Cumulative sum for HOSP_STAY number
    w_cume = Window.partition_by("INDV_ID", "DELIVERY_DT").order_by(
        col("ADMIT_DT_FIL"), col("DSCHRG_DT_FIL")
    ).rows_between(Window.UNBOUNDED_PRECEDING, Window.CURRENT_ROW)
    c = c.with_column("HOSP_STAY", ssum(col("NEW_STAY")).over(w_cume))

    # Aggregate to stay level
    stays = (
        c.group_by("INDV_ID", "DELIVERY_DT", "HOSP_STAY")
        .agg(
            smin("ADMIT_DT_FIL").alias("ADMIT"),
            smax("DSCHRG_DT_FIL").alias("DSCHRG"),
            ssum("AMTPAID").alias("PAID_AMT")
        )
    )

    # Clip to runout, floor to delivery date, compute LOS
    stays = (
        stays
        .with_column("OUT_END_DATE", (col("DSCHRG") > lit(runout_end)).cast("int"))
        .with_column("DSCHRG", least(col("DSCHRG"), lit(runout_end)))
        .with_column("ADMIT", greatest(col("ADMIT"), col("DELIVERY_DT")))
        .with_column("LOS_RAW", datediff("day", col("ADMIT"), col("DSCHRG")))
        .with_column("LOS", when(col("DSCHRG") == col("ADMIT"), lit(1)).otherwise(col("LOS_RAW")))
        .drop("LOS_RAW")
        .filter(col("LOS") >= lit(1))
    )

    return stays


def build_newborn_and_nicu_ids(
    claims_df: DataFrame,
    hosp_rollup_df: DataFrame,
    birth_window_start: date,
    birth_window_mid: date
) -> Dict[str, DataFrame]:
    """
    Build newborn and NICU identification tables with episode-level enrichment.

    Args:
        claims_df: Claims DataFrame
        hosp_rollup_df: Hospital stays DataFrame
        birth_window_start: Start of birth window
        birth_window_mid: Midpoint of birth window

    Returns:
        Dictionary with keys: newborn_hosp_clms, newborn_ident_df, nicu_ident,
        nicu_claims_df, nicu_dischg_provider, rev_out, drg_out
    """
    logger.info("Building NICU artifact tables")

    # Join claims to episode windows
    nh = claims_df.join(
        hosp_rollup_df.select("INDV_ID", "DELIVERY_DT", "ADMIT", "DSCHRG", "LOS"),
        ["INDV_ID", "DELIVERY_DT"],
        "inner"
    )

    # Core in-window filter
    base_filter = (col("ADMIT") <= col("FROMDATE")) & (col("FROMDATE") <= col("DSCHRG"))
    nh = nh.filter(base_filter)
    if "HIGH_COST" in nh.columns:
        nh = nh.filter(~col("HIGH_COST"))

    # Keep only needed columns
    keep_cols = [
        "INDV_ID", "DELIVERY_DT", "ADMIT", "DSCHRG", "LOS",
        "CLAIMNO", "FROMDATE", "THRUDATE", "ADMIT_DT", "DSCHRG_DT", "PAIDDATE",
        "AMTPAID", "CPTCODE", "REV_CD", "DRG",
        "DIAG1", "DIAG2", "DIAG3", "DIAG4", "DIAG5",
        "PROC1", "PROC2", "PROC3",
        "BUS_LINE_CD", "PRDCT_CD", "BTH_DT",
        "BIRTH_TYPE", "BABY_TYPE", "CONTRACT",
        "DSCHRG_STS_CD",
        "PROV_ID", "PROV_TIN", "PROV_FULL_NM", "PROV_STATE"
    ]
    keep_cols = [c for c in keep_cols if c in nh.columns]
    nh = nh.select(*[col(c) for c in keep_cols])

    # Episode metadata & study year
    nh = (
        nh
        .with_column(
            "STAY_TYPE",
            when(col("LOS") >= lit(BUSINESS_RULES.long_stay_threshold_days), lit("Long Stay"))
            .otherwise(lit("Short Stay"))
        )
        .with_column(
            "IN_DAYS",
            col("ADMIT") <= (col("DELIVERY_DT") + lit(BUSINESS_RULES.init_hosp_threshold_days))
        )
        .with_column(
            "STUDY_YR",
            when(
                (col("DELIVERY_DT") >= lit(birth_window_start)) &
                (col("DELIVERY_DT") < lit(birth_window_mid)),
                lit("Previous")
            ).otherwise(lit("Current"))
        )
        .with_column("ADMIT_GAP", datediff("day", col("DELIVERY_DT"), col("ADMIT")))
        .filter((col("ADMIT_GAP") < lit(BUSINESS_RULES.readmit_threshold_days)) & col("IN_DAYS"))
    )

    # Claim-level de-dup per episode
    w_claim = Window.partition_by("INDV_ID", "DELIVERY_DT", "CLAIMNO").order_by(
        col("THRUDATE").desc_nulls_last(), col("FROMDATE").desc_nulls_last()
    )
    claim_base = (
        nh.with_column("RN_CLAIM", row_number().over(w_claim))
        .filter(col("RN_CLAIM") == 1)
        .drop("RN_CLAIM")
    )

    # Map PRDCT_CD→LOB if LOB not present
    if "LOB" not in claim_base.columns and "PRDCT_CD" in claim_base.columns:
        claim_base = claim_base.with_column("LOB", col("PRDCT_CD"))

    # Episode-level rollup
    grp_ep = [
        "INDV_ID", "BTH_DT", "DELIVERY_DT", "ADMIT", "DSCHRG", "LOS",
        "STAY_TYPE", "BIRTH_TYPE", "CONTRACT", "BUS_LINE_CD", "LOB", "STUDY_YR"
    ]
    grp_ep = [g for g in grp_ep if g in claim_base.columns]

    newborn_ident_ep = (
        claim_base.group_by(*grp_ep)
        .agg(
            ssum("AMTPAID").alias("AMTPAID"),
            smin("BABY_TYPE").alias("BABY_TYPE")
        )
    )

    # Newborn-level rollup
    newborn_ident_df = (
        newborn_ident_ep
        .group_by("INDV_ID", "BTH_DT", "DELIVERY_DT", "BUS_LINE_CD", "LOB", "STUDY_YR")
        .agg(
            smin("ADMIT").alias("ADMIT"),
            smax("DSCHRG").alias("DSCHRG"),
            ssum("AMTPAID").alias("AMTPAID"),
            smax(when(col("BABY_TYPE") == lit("NICU"), lit(1)).otherwise(lit(0))).alias("ANY_NICU"),
            smax(
                when(col("BIRTH_TYPE") == lit("Multiple"), lit(3))
                .when(col("BIRTH_TYPE") == lit("Twin"), lit(2))
                .when(col("BIRTH_TYPE") == lit("Single"), lit(1))
                .otherwise(lit(0))
            ).alias("BT_PRI"),
            smax(when(col("CONTRACT") == lit("DRG"), lit(1)).otherwise(lit(0))).alias("ANY_DRG")
        )
        .with_column("LOS_RAW", datediff("day", col("ADMIT"), col("DSCHRG")))
        .with_column("LOS", when(col("DSCHRG") == col("ADMIT"), lit(1)).otherwise(col("LOS_RAW")))
        .drop("LOS_RAW")
        .with_column(
            "BABY_TYPE",
            when(col("ANY_NICU") == lit(1), lit("NICU")).otherwise(lit("Normal Newborn"))
        )
        .with_column(
            "BIRTH_TYPE",
            when(col("BT_PRI") == lit(3), lit("Multiple"))
            .when(col("BT_PRI") == lit(2), lit("Twin"))
            .when(col("BT_PRI") == lit(1), lit("Single"))
            .otherwise(lit("Unknown"))
        )
        .with_column(
            "CONTRACT",
            when(col("ANY_DRG") == lit(1), lit("DRG")).otherwise(lit("Per-Diem"))
        )
        .drop("ANY_NICU", "BT_PRI", "ANY_DRG")
    )

    # NICU subset
    nicu_ident = (
        newborn_ident_df
        .filter(col("BABY_TYPE") == lit("NICU"))
        .with_column_renamed("AMTPAID", "TOTAL_NICU_COST")
    )

    # Build nicu_claims_df
    claim_cols = [
        "INDV_ID", "CLAIMNO", "FROMDATE", "THRUDATE", "ADMIT_DT", "DSCHRG_DT",
        "PROV_ID", "CPTCODE", "REV_CD", "DRG", "DSCHRG_STS_CD", "AMTPAID",
        "DIAG1", "DIAG2", "DIAG3", "DIAG4", "DIAG5", "PROC1", "PROC2", "PROC3"
    ]
    claim_cols = [c for c in claim_cols if c in claim_base.columns]

    nicu_claims_df = (
        claim_base.select(*claim_cols, "DELIVERY_DT", "LOS")
        .join(nicu_ident.select("INDV_ID", "ADMIT", "DSCHRG"), ["INDV_ID"], "inner")
        .filter((col("FROMDATE") >= col("ADMIT")) & (col("FROMDATE") <= col("DSCHRG")))
    )

    # Discharge status
    nicu_claims_df = _add_discharge_status(nicu_claims_df)

    # Provider attribution
    nicu_dischg_provider = _build_discharge_provider(claims_df, nicu_claims_df)

    # REV & DRG features
    rev_features = _build_rev_features(nicu_claims_df)
    drg_features = _build_drg_features(nicu_claims_df)

    return {
        "newborn_hosp_clms": claim_base,
        "newborn_ident_df": newborn_ident_df,
        "nicu_ident": nicu_ident,
        "nicu_claims_df": nicu_claims_df,
        "nicu_dischg_provider": nicu_dischg_provider,
        "rev_out": rev_features,
        "drg_out": drg_features
    }


def _add_discharge_status(nicu_claims_df: DataFrame) -> DataFrame:
    """Add LAST_DISCHARGE_STATUS to NICU claims."""
    order_col = (
        when(col("DSCHRG_STS_CD") == lit("20"), lit(0))
        .when(col("DSCHRG_STS_CD") == lit("07"), lit(1))
        .when(col("DSCHRG_STS_CD").isin(["02", "05", "66", "43", "62", "63", "65"]), lit(2))
        .when(col("DSCHRG_STS_CD") == lit("30"), lit(3))
        .when(col("DSCHRG_STS_CD").isin(["01", "06"]), lit(4))
        .when(
            (length(col("DSCHRG_STS_CD")) < lit(2)) |
            (col("DSCHRG_STS_CD").isin(["04", "41", "50", "51", "70", "03", "64"])) |
            (col("DSCHRG_STS_CD").between(lit("08"), lit("19"))) |
            (col("DSCHRG_STS_CD").between(lit("21"), lit("29"))) |
            (col("DSCHRG_STS_CD").between(lit("31"), lit("39"))) |
            (col("DSCHRG_STS_CD").between(lit("44"), lit("49"))) |
            (col("DSCHRG_STS_CD").between(lit("52"), lit("60"))) |
            (col("DSCHRG_STS_CD").between(lit("67"), lit("69"))) |
            (col("DSCHRG_STS_CD").between(lit("71"), lit("99"))),
            lit(6)
        ).otherwise(lit(9))
    )

    ranked = (
        nicu_claims_df
        .filter((col("DSCHRG_STS_CD") != lit("00")) | col("DSCHRG_STS_CD").is_not_null())
        .with_column("ORDER", order_col)
        .with_column(
            "RN",
            row_number().over(
                Window.partition_by("INDV_ID", "ADMIT", "DSCHRG")
                .order_by(
                    col("ORDER").asc(),
                    col("DSCHRG_DT").desc(),
                    col("FROMDATE").desc(),
                    col("DSCHRG_STS_CD").asc()
                )
            )
        )
    )

    last_status = (
        ranked.filter(col("RN") == 1)
        .select("INDV_ID", "ADMIT", "DSCHRG", col("DSCHRG_STS_CD").alias("LAST_DISCHARGE_STATUS"))
    )

    return nicu_claims_df.join(last_status, ["INDV_ID", "ADMIT", "DSCHRG"], "left")


def _build_discharge_provider(claims_df: DataFrame, nicu_claims_df: DataFrame) -> DataFrame:
    """Build discharge provider attribution."""
    if not all(x in nicu_claims_df.columns for x in ["AMTPAID", "ADMIT_DT", "DSCHRG_DT", "PROV_ID"]):
        return None

    ep = (
        nicu_claims_df
        .filter(col("PROV_ID").is_not_null())
        .group_by("INDV_ID", "ADMIT", "DSCHRG", "DELIVERY_DT", "LOS", "PROV_ID")
        .agg(
            ssum("AMTPAID").alias("HOSPPAID"),
            smin("ADMIT_DT").alias("HOSPADMIT"),
            smax("DSCHRG_DT").alias("HOSPDISCHG")
        )
        .with_column(
            "HOSPLOS",
            when(
                col("HOSPDISCHG") == col("DSCHRG"),
                datediff("day", col("HOSPADMIT"), col("HOSPDISCHG"))
            ).otherwise(datediff("day", col("HOSPADMIT"), col("HOSPDISCHG")) + lit(1))
        )
    )

    w_best = (
        Window.partition_by("INDV_ID", "ADMIT", "DSCHRG")
        .order_by(
            col("HOSPDISCHG").desc(),
            col("HOSPLOS").desc(),
            col("HOSPPAID").desc()
        )
    )
    best = ep.with_column("RN", row_number().over(w_best)).filter(col("RN") == 1).drop("RN")

    hosplist = (
        claims_df.select("PROV_ID", "PROV_FULL_NM", "PROV_STATE")
        .filter(col("PROV_ID").is_not_null())
        .distinct()
    )

    return (
        best.join(hosplist, ["PROV_ID"], "left")
        .with_column("PROV_FULL_NM", coalesce(col("PROV_FULL_NM"), lit("Unknown")))
        .with_column("PROV_STATE", coalesce(col("PROV_STATE"), lit("Unknown")))
    )


def _build_rev_features(nicu_claims_df: DataFrame) -> DataFrame:
    """Build revenue code features."""
    rev_ep = (
        nicu_claims_df
        .select("INDV_ID", "ADMIT", "DSCHRG", "REV_CD")
        .with_column("REV_NUM", sql_expr("TRY_TO_NUMBER(REV_CD)"))
        .filter(col("REV_NUM").between(REV_CODE_CONFIG.nicu_level_min, REV_CODE_CONFIG.nicu_level_max))
        .select("INDV_ID", "ADMIT", "DSCHRG", "REV_NUM")
        .distinct()
    )

    rev_min = (
        rev_ep.group_by("INDV_ID", "ADMIT", "DSCHRG")
        .agg(smin("REV_NUM").alias("FINAL_REV_NUM"))
        .with_column("FINAL_REV_CD", sql_expr("TO_VARCHAR(FINAL_REV_NUM)"))
        .select("INDV_ID", "ADMIT", "DSCHRG", "FINAL_REV_CD")
    )

    w_rev = Window.partition_by("INDV_ID", "ADMIT", "DSCHRG").order_by(col("REV_NUM").asc())
    rev_second = (
        rev_ep.with_column("RN", row_number().over(w_rev))
        .filter(col("RN") == 2)
        .select("INDV_ID", "ADMIT", "DSCHRG", col("REV_NUM").alias("REV_NUM_2"))
    )

    return (
        rev_min.join(rev_second, ["INDV_ID", "ADMIT", "DSCHRG"], "left")
        .with_column("REV_LEVELING", col("REV_NUM_2").is_not_null())
        .select("INDV_ID", "ADMIT", "DSCHRG", "FINAL_REV_CD", "REV_LEVELING")
    )


def _build_drg_features(nicu_claims_df: DataFrame) -> DataFrame:
    """Build DRG code features."""
    from .config import DRG_CONFIG

    drg_ep = (
        nicu_claims_df
        .select("INDV_ID", "ADMIT", "DSCHRG", "DRG")
        .with_column("DRG_NUM", sql_expr("TRY_TO_NUMBER(DRG)"))
        .filter(
            (col("DRG_NUM").between(580, 640)) |
            (col("DRG_NUM").between(789, 795))
        )
        .select("INDV_ID", "ADMIT", "DSCHRG", "DRG_NUM")
        .distinct()
    )

    return (
        drg_ep.group_by("INDV_ID", "ADMIT", "DSCHRG")
        .agg(smin("DRG_NUM").alias("FINAL_DRG_NUM"))
        .with_column("FINAL_DRG_CD", sql_expr("TO_VARCHAR(FINAL_DRG_NUM)"))
        .select("INDV_ID", "ADMIT", "DSCHRG", "FINAL_DRG_CD")
    )


def _prof_fee_aggregates(nicu_claims_df: DataFrame) -> tuple:
    """Calculate professional fee aggregates."""
    # Only professional claims (CPT present)
    prof = (
        nicu_claims_df.filter(col("CPTCODE").is_not_null())
        .select("INDV_ID", "ADMIT", "DSCHRG", "LOS", "FROMDATE", "CPTCODE", "AMTPAID", "ADMIT_DT", "DSCHRG_DT")
    )

    # All professional fees
    all_prof = (
        prof.group_by("INDV_ID", "ADMIT", "DSCHRG")
        .agg(ssum("AMTPAID").alias("ALL_PROFFEE"))
    )

    # Manageable CPT set
    manageable = CPT_CONFIG.manageable_cpts
    man = prof.filter(col("CPTCODE").isin(manageable))

    # Unique service-days
    man_days_key = concat(
        col("INDV_ID").cast("string"), lit("-"),
        to_char(col("ADMIT"), "YYYY-MM-DD"), lit("-"),
        to_char(col("DSCHRG"), "YYYY-MM-DD"), lit("-"),
        to_char(col("FROMDATE"), "YYYY-MM-DD"), lit("-"),
        col("CPTCODE")
    )
    man_aggs = (
        man.with_column("CPT_DAYS_KEY", man_days_key)
        .group_by("INDV_ID", "ADMIT", "DSCHRG")
        .agg(
            ssum("AMTPAID").alias("MANAGEABLE_PROFFEE"),
            count_distinct("CPT_DAYS_KEY").alias("MANAGEABLE_SVC_DAYS")
        )
    )

    # Critical care CPT set
    critical = CPT_CONFIG.critical_care_cpts
    crit = prof.filter(col("CPTCODE").isin(critical))

    crit_days_key = concat(
        col("INDV_ID").cast("string"), lit("-"),
        to_char(col("ADMIT"), "YYYY-MM-DD"), lit("-"),
        to_char(col("DSCHRG"), "YYYY-MM-DD"), lit("-"),
        to_char(col("FROMDATE"), "YYYY-MM-DD")
    )
    crit_aggs = (
        crit.with_column("CRITICAL_DAYS_KEY", crit_days_key)
        .group_by("INDV_ID", "ADMIT", "DSCHRG")
        .agg(
            ssum("AMTPAID").alias("CRITICAL_CARE_PROFFEE"),
            count_distinct("CRITICAL_DAYS_KEY").alias("CRITICAL_CARE_DAYS")
        )
    )

    return all_prof, man_aggs, crit_aggs


def _room_and_board(nicu_claims_df: DataFrame) -> DataFrame:
    """Calculate room and board costs."""
    rb_prefix_ok = substring(col("REV_CD"), 1, 3).isin(REV_CODE_CONFIG.room_board_prefixes)
    room = (
        nicu_claims_df
        .filter(rb_prefix_ok & col("CPTCODE").is_null())
        .group_by("INDV_ID", "ADMIT", "DSCHRG")
        .agg(ssum("AMTPAID").alias("FACILITY_RM_COST"))
    )
    return room


def _readmissions(nicu_ident: DataFrame, hosp_rollup_df: DataFrame) -> DataFrame:
    """Calculate readmissions within 30 days."""
    future = (
        hosp_rollup_df
        .select(
            col("INDV_ID"),
            col("ADMIT").alias("READMIT_DT"),
            col("PAID_AMT").alias("READMIT_PAID_AMT"),
            col("LOS").alias("READMIT_LOS")
        )
    )

    # Join and keep when READMIT_DT in (DSCHRG+1, DSCHRG+30]
    j = (
        nicu_ident.select("INDV_ID", "ADMIT", "DSCHRG")
        .join(future, ["INDV_ID"], "inner")
        .filter(
            (col("READMIT_DT") > col("DSCHRG")) &
            (datediff("day", col("DSCHRG"), col("READMIT_DT")) <= lit(BUSINESS_RULES.readmit_threshold_days))
        )
    )

    readm = (
        j.group_by("INDV_ID", "ADMIT", "DSCHRG")
        .agg(
            count_distinct("READMIT_DT").alias("READMIT"),
            ssum("READMIT_PAID_AMT").alias("READMIT_PAID_AMT"),
            ssum("READMIT_LOS").alias("READMIT_LOS")
        )
    )
    return readm


def _union_diag_proc(nicu_claims_df: DataFrame) -> tuple:
    """Unpivot DIAG/PROC columns using unions."""
    diag_cols = [c for c in nicu_claims_df.columns if c.upper().startswith("DIAG")]
    proc_cols = [c for c in nicu_claims_df.columns if c.upper().startswith("PROC")]

    # DIAGTMP rows
    diag_parts = []
    for d in diag_cols:
        diag_parts.append(
            nicu_claims_df.select(
                col("INDV_ID"), col("ADMIT"), col("DSCHRG"),
                col(d).alias("DIAGTMP")
            ).filter(col("DIAGTMP").is_not_null())
        )
    diag_tmp = None
    for p in diag_parts:
        diag_tmp = p if diag_tmp is None else diag_tmp.union_all(p)
    if diag_tmp is None:
        diag_tmp = nicu_claims_df.session.create_dataframe([], schema=["INDV_ID", "ADMIT", "DSCHRG", "DIAGTMP"])

    # PROCTMP rows
    proc_parts = []
    for pr in proc_cols:
        proc_parts.append(
            nicu_claims_df.select(
                col("INDV_ID"), col("ADMIT"), col("DSCHRG"),
                col(pr).alias("PROCTMP")
            ).filter(col("PROCTMP").is_not_null())
        )
    proc_tmp = None
    for p in proc_parts:
        proc_tmp = p if proc_tmp is None else proc_tmp.union_all(p)
    if proc_tmp is None:
        proc_tmp = nicu_claims_df.session.create_dataframe([], schema=["INDV_ID", "ADMIT", "DSCHRG", "PROCTMP"])

    # De-dup
    diag_tmp = diag_tmp.distinct()
    proc_tmp = proc_tmp.distinct()
    return diag_tmp, proc_tmp


def _bw_ga_nas(session: Session, diag_tmp: DataFrame) -> tuple:
    """Extract birthweight, gestational age, and NAS from diagnosis codes."""
    # Reference tables
    bw_ref = session.table("SUPP_DATA.REF_BIRTHWEIGHT_ICD").select(
        col("CODE").alias("ICD_CODE"), col("DESCRIPTION").alias("BW_CAT")
    )
    ga_ref = session.table("SUPP_DATA.REF_GEST_AGE_ICD").select(
        col("CODE").alias("ICD_CODE"), col("DESCRIPTION").alias("GA_CAT")
    )

    # Birthweight (first category per INDV_ID)
    bw = (
        diag_tmp.join(bw_ref, diag_tmp["DIAGTMP"] == bw_ref["ICD_CODE"], "inner")
        .select("INDV_ID", "ADMIT", "DSCHRG", "BW_CAT")
    )
    w_bw = Window.partition_by("INDV_ID").order_by(col("BW_CAT").asc())
    bw = (
        bw.with_column("RN", row_number().over(w_bw))
        .filter(col("RN") == 1)
        .drop("RN")
    )

    # Gestational age
    ga = (
        diag_tmp.join(ga_ref, diag_tmp["DIAGTMP"] == ga_ref["ICD_CODE"], "inner")
        .select("INDV_ID", "ADMIT", "DSCHRG", "GA_CAT")
    )
    w_ga = Window.partition_by("INDV_ID").order_by(col("GA_CAT").asc())
    ga = (
        ga.with_column("RN", row_number().over(w_ga))
        .filter(col("RN") == 1)
        .drop("RN")
    )

    # NAS flag (ICD-10 code "P961")
    nas = (
        diag_tmp.filter(col("DIAGTMP") == lit("P961"))
        .select("INDV_ID", "ADMIT", "DSCHRG")
        .with_column("NAS", lit(True))
        .distinct()
    )

    return bw, ga, nas


def build_nicu_rollup(
    session: Session,
    nicu_ident: DataFrame,
    nicu_claims_df: DataFrame,
    hosp_rollup_df: DataFrame,
    rev_out: DataFrame,
    drg_out: DataFrame,
    nicu_dischg_provider: DataFrame
) -> DataFrame:
    """
    Build final NICU rollup with all metrics.

    Args:
        session: Snowpark session
        nicu_ident: NICU identification DataFrame
        nicu_claims_df: NICU claims DataFrame
        hosp_rollup_df: Hospital stays DataFrame
        rev_out: Revenue features DataFrame
        drg_out: DRG features DataFrame
        nicu_dischg_provider: Discharge provider DataFrame

    Returns:
        Complete NICU rollup DataFrame
    """
    logger.info("Building NICU rollup")

    # Professional fee rollups
    all_prof, man_aggs, crit_aggs = _prof_fee_aggregates(nicu_claims_df)

    # Room & board
    room = _room_and_board(nicu_claims_df)

    # Readmissions
    readm = _readmissions(nicu_ident, hosp_rollup_df)

    # Diag/proc unpivot
    diag_tmp, proc_tmp = _union_diag_proc(nicu_claims_df)

    # Birthweight / gest age / NAS
    bw, ga, nas = _bw_ga_nas(session, diag_tmp)

    # Start from episode-level NICU set
    base = nicu_ident

    # Left-join all features
    keys = ("INDV_ID", "ADMIT", "DSCHRG")

    out = (
        base
        .join(all_prof.select(*keys, "ALL_PROFFEE"), keys, "left")
        .join(man_aggs.select(*keys, "MANAGEABLE_PROFFEE", "MANAGEABLE_SVC_DAYS"), keys, "left")
        .join(crit_aggs.select(*keys, "CRITICAL_CARE_PROFFEE", "CRITICAL_CARE_DAYS"), keys, "left")
        .join(room.select(*keys, "FACILITY_RM_COST"), keys, "left")
        .join(readm.select(*keys, "READMIT", "READMIT_PAID_AMT", "READMIT_LOS"), keys, "left")
        .join(nas.select(*keys, "NAS"), keys, "left")
        .join(ga.select(*keys, "GA_CAT"), keys, "left")
        .join(bw.select(*keys, "BW_CAT"), keys, "left")
        .join(rev_out.select(*keys, "FINAL_REV_CD", "REV_LEVELING"), keys, "left")
        .join(drg_out.select(*keys, "FINAL_DRG_CD"), keys, "left")
    )

    if nicu_dischg_provider is not None:
        out = out.join(
            nicu_dischg_provider.select(*keys, "PROV_TIN", "PROV_FULL_NM", "PROV_STATE"),
            keys, "left"
        )

    # Derived rollup metrics
    out = (
        out
        .with_column("ALL_PROFFEE", coalesce(col("ALL_PROFFEE"), lit(0)))
        .with_column("MANAGEABLE_PROFFEE", coalesce(col("MANAGEABLE_PROFFEE"), lit(0)))
        .with_column("CRITICAL_CARE_PROFFEE", coalesce(col("CRITICAL_CARE_PROFFEE"), lit(0)))
        .with_column("FACILITY_RM_COST", coalesce(col("FACILITY_RM_COST"), lit(0)))
        .with_column("TOTAL_NICU_COST", coalesce(col("TOTAL_NICU_COST"), lit(0)))
        .with_column("LOS", coalesce(col("LOS"), lit(0)))
        .with_column("ALL_FACILITY_COST", col("TOTAL_NICU_COST") - col("ALL_PROFFEE"))
        .with_column(
            "LOW_PAID_NICU",
            when(
                (col("LOS") > lit(0)) &
                ((col("TOTAL_NICU_COST") / col("LOS")) < lit(BUSINESS_RULES.low_paid_nicu_threshold)),
                lit(True)
            ).otherwise(lit(False))
        )
        .with_column(
            "INAPPROPRIATE_NICU",
            (col("CONTRACT") == lit("DRG")) &
            (col("LOS") <= lit(BUSINESS_RULES.inappropriate_nicu_max_los)) &
            col("FINAL_REV_CD").isin(BUSINESS_RULES.inappropriate_nicu_rev_codes)
        )
    )

    return out


def prepare_final_export(newborn_df: DataFrame, nicu_df: DataFrame) -> DataFrame:
    """
    Merge newborn and NICU DataFrames for final export.

    Args:
        newborn_df: Newborn identification DataFrame
        nicu_df: NICU rollup DataFrame

    Returns:
        Combined DataFrame ready for export
    """
    logger.info("Merging Newborns and NICU tables")

    join_keys = ['INDV_ID', 'ADMIT', 'DSCHRG']

    # Simply do a left join - Snowpark will handle column deduplication
    newborns_out = newborn_df.join(nicu_df, join_keys, "left")

    return newborns_out
