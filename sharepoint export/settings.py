from pathlib import Path
import os

# Snowflake configuration
SNOWFLAKE_CONFIG = {
    'account': "uhgdwaas.east-us-2.azure",
    'user': os.getenv('SF_USERNAME'),
    'password': os.getenv('SF_PW'),
    'role': "AZU_SDRP_CSZNB_PRD_DEVELOPER_ROLE",
    'warehouse': "CSZNB_PRD_ANALYTICS_XS_WH",
    'database': 'CSZNB_PRD_OA_DEV_DB',
    'schema': 'BASE'
}

# Table names
SOURCE_TABLE = 'SHAREPOINT_ANALYTIC_REQUESTS'
TARGET_TABLE = 'FOCUSED_ANALYTIC_REQUESTS'
SALESFORCE_TABLE = 'SALESFORCE_INITIATIVES'

# File paths
SHAREPOINT_EXPORT_PATH = Path.home() / "Library/CloudStorage/OneDrive-UHG/Projects/SharePoint/exports/sharepoint_requests.csv"
SALESFORCE_EXPORT_PATH = Path.home() / "Library/CloudStorage/OneDrive-UHG/Projects/SharePoint/exports/salesforce_exports.xlsx"

# Business logic constants
OPEN_STATUS = ['Not Started', 'In Progress', 'Waiting']
DAYS_ON_STATUS_THRESHOLD = 14

# Client type mapping
CLIENT_TYPE_MAPPING = {
    '1': 'Optum Direct NBEA',
    '2': 'Optum/UHC Cross Carrier NBEA',
    '3': 'UHC NBEA',
    '4': 'Opum Direct',
    '5': 'UHC Cross Carrier',
    '6': 'Prospective',
    '7': 'N/A',
    '8': 'N/A'
}

# Boolean columns
BOOLEAN_COLUMNS = [
    "BARIATRIC", "BH", "CGP", "CSP", "DM", "KIDNEY", "TRANSPLANT", "CHD", "VAD",
    "NICU", "MATERNITY", "FERTILITY", "FOCUSED_ANALYTICS", "OUTPATIENT_REHAB",
    "OHS", "FCR_PROFESSIONAL", "CKS", "CKD", "CARDIOLOGY", "DME", "INPATIENT_REHAB",
    "SPINE_PAIN_JOINT", "SPECIALTY_REDIRECTION", "MEDICAL_REBATES_ONBOARDING",
    "BRS", "DATA_INTAKE", "DATA_QAVC", "SPECIALTY_FUSION", "MBO_IMPLEMENTATION",
    "MSPN_IMPLEMENTATION", "VARIABLE_COPAY", "ACCUMULATOR_ADJUSTMENT",
    "SMGP", "SGP", "SECOND_MD", "KAIA", "MBO_PRESALES", "MSPN_PRESALES",
    "MEDICAL_REBATES_PREDEAL", "MAVEN", "CAR_REPORT", "MSK_MSS",
    "FCR_FACILITY", "RADIATION_ONCOLOGY", "VIRTA_HEALTH", "SMO_PRESALES",
    "SMO_IMPLEMENTATION", "SBO_HEALTH_TRUST_PRESALES", "SBO_HEALTH_TRUST_IMPLEMENTATION",
    "CORE_SBO", "ENHANCE_SBO", "OPTUM_GUIDE", "CYLINDER_HEALTH", "RESOURCE_BRIDGE",
    "PHS", "CANCER", "PODIMETRICS"
]
