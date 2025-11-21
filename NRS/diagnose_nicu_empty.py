"""
Diagnostic script to identify why nicu_ident is empty.

Run this in a Jupyter notebook or Python script with Snowpark connection.
"""
import logging
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, count, sum as ssum

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def diagnose_nicu_empty(session: Session, client: str = "EMBLEM"):
    """
    Diagnose why nicu_ident DataFrame is empty.

    Args:
        session: Snowpark session
        client: Client name (default: EMBLEM)
    """
    print("\n" + "="*80)
    print("NICU IDENTIFICATION DIAGNOSTIC")
    print("="*80)

    # Check 1: Reference tables exist and have data
    print("\n1. Checking Reference Tables...")
    print("-" * 80)

    ref_tables = [
        'SUPP_DATA.REF_NEWBORN_ICD',
        'SUPP_DATA.REF_NEWBORN_REVCODE',
        'SUPP_DATA.REF_NICU_REVCODE',
        'SUPP_DATA.REF_NICU_MSDRG',
        'SUPP_DATA.REF_NICU_APRDRG',
        'SUPP_DATA.REF_SINGLETON_ICD',
        'SUPP_DATA.REF_TWIN_ICD',
        'SUPP_DATA.REF_MULTIPLE_ICD'
    ]

    for table_name in ref_tables:
        try:
            count_val = session.table(table_name).count()
            print(f"  ✓ {table_name}: {count_val} rows")

            # Show sample for NICU-specific tables
            if 'NICU' in table_name or 'NEWBORN' in table_name:
                sample = session.table(table_name).limit(5).collect()
                print(f"    Sample: {[row.asDict() for row in sample]}")
        except Exception as e:
            print(f"  ✗ {table_name}: ERROR - {str(e)}")

    # Check 2: Claims data columns
    print("\n2. Checking Claims Data Columns...")
    print("-" * 80)

    claims_table = f"FA_HCLAIMS_{client}"
    try:
        claims = session.table(claims_table)
        print(f"  Total claims: {claims.count()}")
        print(f"  Columns: {claims.columns}")

        # Check for required columns
        required_cols = ['REV_CD', 'DRG', 'DIAG1', 'DIAG2', 'DIAG3', 'DIAG4', 'DIAG5']
        for col_name in required_cols:
            if col_name in claims.columns:
                non_null = claims.filter(col(col_name).is_not_null()).count()
                print(f"  ✓ {col_name}: {non_null} non-null values")
            else:
                print(f"  ✗ {col_name}: MISSING")
    except Exception as e:
        print(f"  ✗ Error accessing {claims_table}: {str(e)}")

    # Check 3: Tagged claims
    print("\n3. Checking Tagged Claims (after reference matching)...")
    print("-" * 80)
    print("  Run the pipeline up to Step 9 (Reference Flag Tagging)")
    print("  Then check the following counts in claims_df:")
    print("    - claims_df.filter(col('NEWBORN_ICD')).count()")
    print("    - claims_df.filter(col('NEWBORN_REV')).count()")
    print("    - claims_df.filter(col('NICU_REV')).count()")
    print("    - claims_df.filter(col('NICU_MSDRG')).count()")
    print("    - claims_df.filter(col('NICU_APRDRG')).count()")

    # Check 4: Birth date filter
    print("\n4. Checking Birth Date Distribution...")
    print("-" * 80)

    member_table = f"FA_MEMBERSHIP_{client}"
    try:
        members = session.table(member_table)
        print(f"  Total members: {members.count()}")

        # Check BTH_DT distribution
        if 'BTH_DT' in members.columns:
            with_birth = members.filter(col("BTH_DT").is_not_null())
            print(f"  Members with BTH_DT: {with_birth.count()}")

            # Show recent births
            from snowflake.snowpark.functions import year, month
            recent = with_birth.filter(year(col("BTH_DT")) >= 2020)
            print(f"  Births since 2020: {recent.count()}")

            if recent.count() > 0:
                sample = recent.select("INDV_ID", "BTH_DT").limit(10).collect()
                print(f"  Sample recent births: {[row.asDict() for row in sample]}")
        else:
            print(f"  ✗ BTH_DT column missing from {member_table}")
    except Exception as e:
        print(f"  ✗ Error accessing {member_table}: {str(e)}")

    # Check 5: Study period alignment
    print("\n5. Study Period Alignment...")
    print("-" * 80)
    print("  Check your study period dates in pipeline parameters:")
    print("    - birth_window_start")
    print("    - birth_window_end")
    print("    - runout_end")
    print("  Ensure you have:")
    print("    a) Newborns born within the birth window")
    print("    b) Claims data covering the runout period after birth")

    # Recommendations
    print("\n" + "="*80)
    print("RECOMMENDATIONS")
    print("="*80)
    print("""
1. If reference tables are empty:
   - Run the reference data loading scripts
   - Check SUPP_DATA schema permissions

2. If claims lack required columns:
   - Verify FA_HCLAIMS_{client} table schema
   - Check if REV_CD, DRG columns exist with different names

3. If tags aren't matching:
   - Verify the code formats (ICD-10 with/without dots, DRG numbers)
   - Check if your data uses different coding systems

4. If no newborns in date range:
   - Adjust birth_window dates to match your data
   - Check if BTH_DT is populated in membership table

5. Add debugging to pipeline.py after Step 9:
   ```python
   logger.info(f"NEWBORN_ICD matches: {claims_df.filter(col('NEWBORN_ICD')).count()}")
   logger.info(f"NEWBORN_REV matches: {claims_df.filter(col('NEWBORN_REV')).count()}")
   logger.info(f"NICU_REV matches: {claims_df.filter(col('NICU_REV')).count()}")
   ```
    """)

    print("\n" + "="*80)


if __name__ == "__main__":
    # Example usage - replace with your session creation
    from NRS.config import get_snowflake_session

    session = get_snowflake_session()
    diagnose_nicu_empty(session, client="EMBLEM")
