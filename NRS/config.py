"""
Configuration and constants for NICU Analytics Pipeline.
"""
from typing import Dict, List
from dataclasses import dataclass


@dataclass
class SnowflakeConfig:
    """Snowflake connection configuration."""
    account: str = "uhgdwaas.east-us-2.azure"
    role: str = "AZU_SDRP_CSZNB_PRD_DEVELOPER_ROLE"
    warehouse: str = "CSZNB_PRD_ANALYTICS_XS_WH"
    database: str = "CSZNB_PRD_PS_PFA_DB"
    schema: str = "STAGE"


@dataclass
class TableConfig:
    """Table naming configuration."""
    database: str = "CSZNB_PRD_PS_PFA_DB"
    stage_schema: str = "STAGE"
    base_schema: str = "BASE"
    supp_data_schema: str = "SUPP_DATA"

    def membership_table(self, client: str) -> str:
        """Get membership source table name."""
        return f"FA_MEMBERSHIP_{client}"

    def medical_table(self, client: str) -> str:
        """Get medical claims source table name."""
        return f"FA_MEDICAL_{client}"

    def membership_output_table(self, client: str) -> str:
        """Get membership output table name."""
        return f"{self.database}.{self.base_schema}.PS_MEMBERSHIP_{client}_TST"

    def newborns_output_table(self, client: str) -> str:
        """Get newborns output table name."""
        return f"{self.database}.{self.base_schema}.PS_NEWBORNS_{client}_TST"


@dataclass
class ReferenceTableConfig:
    """Reference table configuration."""
    schema: str = "SUPP_DATA"

    # ICD reference tables
    newborn_icd: str = "REF_NEWBORN_ICD"
    singleton_icd: str = "REF_SINGLETON_ICD"
    twin_icd: str = "REF_TWIN_ICD"
    multiple_icd: str = "REF_MULTIPLE_ICD"
    birthweight_icd: str = "REF_BIRTHWEIGHT_ICD"
    gest_age_icd: str = "REF_GEST_AGE_ICD"

    # Revenue code reference tables
    newborn_revcode: str = "REF_NEWBORN_REVCODE"
    nicu_revcode: str = "REF_NICU_REVCODE"

    # DRG reference tables
    nicu_msdrg: str = "REF_NICU_MSDRG"
    nicu_aprdrg: str = "REF_NICU_APRDRG"

    def get_table_name(self, table: str) -> str:
        """Get fully qualified reference table name."""
        return f"{self.schema}.{table}"


@dataclass
class BusinessRulesConfig:
    """Business rules and thresholds."""
    # Window calculations
    min_months_required: int = 24
    birth_window_months: int = 24
    runout_window_months: int = 3

    # Episode thresholds
    init_hosp_threshold_days: int = 4
    readmit_threshold_days: int = 30
    hospital_gap_days: int = 4
    high_cost_threshold: int = 500000
    low_paid_nicu_threshold: int = 150

    # Short vs long stay
    long_stay_threshold_days: int = 3

    # DRG contract inappropriateness
    inappropriate_nicu_max_los: int = 5
    inappropriate_nicu_rev_codes: List[str] = None

    def __post_init__(self):
        if self.inappropriate_nicu_rev_codes is None:
            self.inappropriate_nicu_rev_codes = ["170", "171"]


@dataclass
class CPTConfig:
    """CPT code sets for professional fee calculations."""
    # Manageable CPT codes
    manageable_cpts: List[str] = None

    # Critical care CPT codes
    critical_care_cpts: List[str] = None

    def __post_init__(self):
        if self.manageable_cpts is None:
            self.manageable_cpts = [
                "99233", "99479", "99480", "99478",
                "99231", "99232", "99462"
            ]

        if self.critical_care_cpts is None:
            self.critical_care_cpts = [
                "99468", "99469", "99471", "99472"
            ]


@dataclass
class RevCodeConfig:
    """Revenue code configurations."""
    # Room & Board revenue code prefixes (011-017, 020)
    room_board_prefixes: List[str] = None

    # NICU level revenue codes (170-179)
    nicu_level_min: int = 170
    nicu_level_max: int = 179

    def __post_init__(self):
        if self.room_board_prefixes is None:
            self.room_board_prefixes = [
                "011", "012", "013", "014", "015", "016", "017", "020"
            ]


@dataclass
class DRGConfig:
    """DRG code configurations."""
    # DRG ranges for NICU filtering
    nicu_drg_ranges: List[tuple] = None

    def __post_init__(self):
        if self.nicu_drg_ranges is None:
            self.nicu_drg_ranges = [
                (580, 640),  # Primary NICU DRG range
                (789, 795)   # Secondary NICU DRG range
            ]

    def is_nicu_drg(self, drg_code: int) -> bool:
        """Check if DRG code is in NICU ranges."""
        for min_drg, max_drg in self.nicu_drg_ranges:
            if min_drg <= drg_code <= max_drg:
                return True
        return False


@dataclass
class DischargeStatusConfig:
    """Discharge status code priority configuration."""
    # Discharge status priority mapping
    priority_mapping: Dict[str, int] = None

    def __post_init__(self):
        if self.priority_mapping is None:
            # Lower number = higher priority
            self.priority_mapping = {
                "20": 0,  # Highest priority
                "07": 1,
                "02": 2, "05": 2, "66": 2, "43": 2, "62": 2, "63": 2, "65": 2,
                "30": 3,
                "01": 4, "06": 4,
                # Range 08-19, 21-29, 31-39, 44-49, 52-60, 67-69, 71-99
                # and specific codes 04, 41, 50, 51, 70, 03, 64
                # Default to priority 6
            }


# Global configuration instances
SNOWFLAKE_CONFIG = SnowflakeConfig()
TABLE_CONFIG = TableConfig()
REFERENCE_TABLE_CONFIG = ReferenceTableConfig()
BUSINESS_RULES = BusinessRulesConfig()
CPT_CONFIG = CPTConfig()
REV_CODE_CONFIG = RevCodeConfig()
DRG_CONFIG = DRGConfig()
DISCHARGE_STATUS_CONFIG = DischargeStatusConfig()


# Column name mappings for different source systems
MEMBERSHIP_COLUMN_MAP = {
    "INDV_ID": "INDV_ID",
    "YEARMO": "YEARMO",
    "GENDER": "GENDER",
    "BTH_DT": "BTH_DT",
    "BUS_LINE_CD": "BUS_LINE_CD",
    "PRODUCT_CD": "PRODUCT_CD",
    "STATE": "STATE"
}

MEDICAL_COLUMN_MAP = {
    "INDV_ID": "INDV_ID",
    "CLM_AUD_NBR": "CLM_AUD_NBR",
    "SRVC_FROM_DT": "SRVC_FROM_DT",
    "SRVC_THRU_DT": "SRVC_THRU_DT",
    "PROCESS_DT": "PROCESS_DT",
    "ADMIT_DT": "ADMIT_DT",
    "DSCHRG_DT": "DSCHRG_DT",
    "DIAG_1_CD": "DIAG_1_CD",
    "DIAG_2_CD": "DIAG_2_CD",
    "DIAG_3_CD": "DIAG_3_CD",
    "DIAG_4_CD": "DIAG_4_CD",
    "DIAG_5_CD": "DIAG_5_CD",
    "PROC_1_CD": "PROC_1_CD",
    "PROC_2_CD": "PROC_2_CD",
    "PROC_3_CD": "PROC_3_CD",
    "PROC_CD": "PROC_CD",
    "DSCHRG_STS": "DSCHRG_STS",
    "SBMT_CHRG_AMT": "SBMT_CHRG_AMT",
    "DRG": "DRG",
    "DRG_TYPE": "DRG_TYPE",
    "DRG_OTLR_FLG": "DRG_OTLR_FLG",
    "DRG_OTLR_COST": "DRG_OTLR_COST",
    "NET_PD_AMT": "NET_PD_AMT",
    "PL_OF_SRVC_CD": "PL_OF_SRVC_CD",
    "RVNU_CD": "RVNU_CD",
    "PROV_NPI": "PROV_NPI",
    "PROV_TIN": "PROV_TIN",
    "PROV_FULL_NM": "PROV_FULL_NM",
    "PROV_STATE": "PROV_STATE",
    "PROV_TYP_CD": "PROV_TYP_CD"
}
