# NICU Empty DataFrame Investigation

## Problem
The `nicu_ident` DataFrame is returning empty, causing downstream errors in the NICU rollup step.

## Root Cause Analysis

### How NICU Babies are Identified

The pipeline identifies NICU babies through a multi-step process:

```
Step 1: Tag Claims with Reference Flags (pipeline.py:129)
   ├─> Match DIAG codes against SUPP_DATA.REF_NEWBORN_ICD → NEWBORN_ICD flag
   ├─> Match REV_CD against SUPP_DATA.REF_NEWBORN_REVCODE → NEWBORN_REV flag
   ├─> Match REV_CD against SUPP_DATA.REF_NICU_REVCODE → NICU_REV flag
   ├─> Match DRG against SUPP_DATA.REF_NICU_MSDRG → NICU_MSDRG flag
   └─> Match DRG against SUPP_DATA.REF_NICU_APRDRG → NICU_APRDRG flag

Step 2: Identify Newborns (nicu_analytics.py:66)
   └─> Filter claims where NEWBORN_ICD=True OR NEWBORN_REV=True

Step 3: Classify as NICU (nicu_analytics.py:97-103)
   └─> If NICU_REV=1 OR NICU_MSDRG=1 OR NICU_APRDRG=1 → BABY_TYPE="NICU"
   └─> Else → BABY_TYPE="Normal Newborn"

Step 4: Extract NICU Subset (nicu_analytics.py:358-362)
   └─> Filter newborn_ident_df where BABY_TYPE="NICU" → nicu_ident
```

### Where Records Can Be Lost

| Step | Filter Condition | Potential Issue |
|------|------------------|----------------|
| **Reference Tagging** | Must match reference tables | ✗ Reference tables empty or missing |
| | | ✗ Code format mismatch (ICD-10 with/without dots) |
| | | ✗ Claims missing DIAG/REV_CD/DRG columns |
| **Newborn Identification** | NEWBORN_ICD OR NEWBORN_REV | ✗ No claims tagged as newborns |
| | | ✗ Birth dates outside study period |
| **NICU Classification** | Has NICU codes | ✗ Newborns exist but none have NICU codes |
| | | ✗ Only "Normal Newborn" babies, no NICU babies |
| **Date Filtering** | Within birth window | ✗ Study period doesn't match data dates |

## Most Likely Issues (in order)

### 1. **Reference Tables Missing or Empty** (MOST COMMON)
   - **Symptom**: No claims tagged with newborn or NICU flags
   - **Check**: Run `diagnose_nicu_empty.py` to verify table existence
   - **Fix**: Load reference data into SUPP_DATA schema
   - **Tables needed**:
     - `SUPP_DATA.REF_NEWBORN_ICD` - ICD codes for newborn identification
     - `SUPP_DATA.REF_NEWBORN_REVCODE` - Revenue codes for newborn services
     - `SUPP_DATA.REF_NICU_REVCODE` - Revenue codes for NICU (e.g., 0170-0179)
     - `SUPP_DATA.REF_NICU_MSDRG` - MS-DRG codes for NICU
     - `SUPP_DATA.REF_NICU_APRDRG` - APR-DRG codes for NICU

### 2. **Study Period Misalignment**
   - **Symptom**: Tags working but no records after date filters
   - **Check**: Verify `birth_window_start`, `birth_window_end`, `runout_end` dates
   - **Fix**: Adjust dates to match your data's time period
   - **Example**: If testing with 2020 data, set `birth_window_start='2020-01-01'`

### 3. **Claims Data Schema Differences**
   - **Symptom**: Columns not found errors during tagging
   - **Check**: Verify `FA_HCLAIMS_{client}` has these columns:
     - `REV_CD` (revenue codes)
     - `DRG` (DRG codes)
     - `DIAG1`, `DIAG2`, `DIAG3`, `DIAG4`, `DIAG5` (diagnosis codes)
   - **Fix**: Update column names in tagging logic if different

### 4. **Code Format Mismatches**
   - **Symptom**: Reference tables populated but no matches
   - **Check**: Compare code formats in reference tables vs. claims
   - **Common issues**:
     - ICD-10: Some systems use dots (Z38.0), some don't (Z380)
     - Revenue codes: May be stored as strings vs. integers
     - DRG codes: Numeric vs. string with leading zeros
   - **Fix**: Standardize formats in reference tables or tagging logic

### 5. **No NICU Babies in Data**
   - **Symptom**: Newborns identified but all classified as "Normal Newborn"
   - **Check**: Review your data - does it contain NICU admissions?
   - **Note**: If data is from a small client or limited time period, there genuinely might be no NICU cases

## Diagnostic Steps

### Quick Check (5 minutes)
```python
from NRS.diagnose_nicu_empty import diagnose_nicu_empty
from NRS.config import get_snowflake_session

session = get_snowflake_session()
diagnose_nicu_empty(session, client="EMBLEM")
```

### Detailed Investigation (15 minutes)

1. **Add inline debugging to pipeline.py**
   - Copy code from `debug_inline_code.py`
   - Add after Steps 9, 11, and 13
   - Run pipeline and review debug logs

2. **Check specific claim**
   If you have a known NICU admission, trace it:
   ```python
   # Find a specific baby
   test_indv_id = "12345"  # Replace with real ID

   # Check if in claims
   session.table("FA_HCLAIMS_EMBLEM").filter(col("INDV_ID") == test_indv_id).show()

   # Check if tagged
   # (Run this after Step 9 in pipeline)
   claims_df.filter(col("INDV_ID") == test_indv_id).select(
       "INDV_ID", "FROMDATE", "REV_CD", "DRG", "DIAG1",
       "NEWBORN_ICD", "NEWBORN_REV", "NICU_REV", "NICU_MSDRG", "NICU_APRDRG"
   ).show()
   ```

## Solutions by Root Cause

### If Reference Tables Missing:
```sql
-- Create reference tables with sample NICU codes
CREATE TABLE SUPP_DATA.REF_NICU_REVCODE AS
SELECT * FROM (VALUES
    ('0170'), ('0171'), ('0172'), ('0173'), ('0174'),
    ('0175'), ('0176'), ('0177'), ('0178'), ('0179')
) AS t(CODE);

-- Add common NICU DRGs
CREATE TABLE SUPP_DATA.REF_NICU_MSDRG AS
SELECT * FROM (VALUES
    ('789'), ('790'), ('791'), ('792'), ('793'), ('794'), ('795')
) AS t(CODE);

-- Add newborn ICD codes
CREATE TABLE SUPP_DATA.REF_NEWBORN_ICD AS
SELECT * FROM (VALUES
    ('Z38.0'), ('Z38.00'), ('Z38.1'), ('Z38.2'),
    ('Z38.3'), ('Z38.4'), ('Z38.5'), ('Z38.6')
) AS t(CODE);
```

### If Date Range Issue:
```python
# In pipeline call, adjust dates to match your data
run_nicu_pipeline(
    client_data='EMBLEM',
    study_year=2023,
    birth_window_months=12,  # Use full year for testing
    auto_window=False
)
```

### If Column Names Different:
Update `NRS/tagging.py` functions to use your schema's column names.

## Next Steps

1. **Run diagnostic**: `python NRS/diagnose_nicu_empty.py`
2. **Review output**: Identify which check fails
3. **Apply appropriate fix** from solutions above
4. **Add inline debugging**: Use code from `debug_inline_code.py`
5. **Re-run pipeline**: Verify fixes worked

## Support

If issue persists after diagnostics:
1. Share output of `diagnose_nicu_empty.py`
2. Share debug logs from pipeline run
3. Share sample claim record (with NICU codes) that should match
4. Share reference table sample rows

This will help identify the exact mismatch.
