# Option: Snowflake Temp Table Persistence for Debug Variables

## Implementation

Add this function to your notebook:

```python
def persist_debug_tables(debug_dict, client_data, table_suffix="_TST"):
    """
    Persist debug dataframes to Snowflake temp tables for inspection.

    Tables will be created in: CSZNB_PRD_PS_PFA_DB.STAGE.DEBUG_*
    """
    if not DEBUG_MODE or debug_dict is None:
        return

    import datetime
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')

    tables_created = []

    for var_name, df in debug_dict.items():
        if df is not None:
            table_name = f"DEBUG_{var_name.upper()}_{timestamp}"
            full_table = f"{DATABASE}.{SCHEMA}.{table_name}"

            logger.info(f"[DEBUG] Persisting {var_name} to {table_name}...")
            df.write.mode("overwrite").save_as_table(full_table)

            row_count = df.count()
            tables_created.append({
                'variable': var_name,
                'table': table_name,
                'full_path': full_table,
                'rows': row_count
            })

    # Print summary
    print("\n" + "="*80)
    print("DEBUG TABLES CREATED IN SNOWFLAKE")
    print("="*80)
    for info in tables_created:
        print(f"  {info['variable']:25s} → {info['table']:40s} ({info['rows']:,} rows)")
    print("="*80)
    print("\nQuery in Snowflake:")
    print("  SELECT * FROM CSZNB_PRD_PS_PFA_DB.STAGE.DEBUG_HOSP_ROLLUP_DF_{timestamp} LIMIT 100;")
    print("\nCleanup (run when done):")
    print("  DROP TABLE IF EXISTS CSZNB_PRD_PS_PFA_DB.STAGE.DEBUG_*_{timestamp};")
    print("="*80 + "\n")

    return tables_created
```

## Updated Execution Cell

```python
if __name__ == "__main__":
    result = main(auto_window=AUTO_WINDOW)

    if DEBUG_MODE and result is not None:
        # Option 1: Keep as Jupyter variables (for Jupyter/Snowflake Notebooks)
        newborn_ident_df = result['newborn_ident_df']
        nicu_ident = result['nicu_ident']
        hosp_rollup_df = result['hosp_rollup_df']
        # ... etc

        # Option 2: ALSO persist to Snowflake temp tables (for SQL inspection)
        tables_info = persist_debug_tables(result, CLIENT_DATA)
```

## Benefits of Snowflake Tables

✅ Inspectable from **any SQL tool** (Snowflake UI, SQL worksheets, DBeaver, etc.)
✅ **Share with team members** - they can query the same tables
✅ **Persist across sessions** - tables survive kernel restart
✅ **Use SQL** for analysis - more familiar for some users
✅ **Join with other tables** - can join debug tables with source tables

## Drawbacks

❌ Manual cleanup required (temp tables persist until explicitly dropped)
❌ Storage costs (though temp tables are usually free tier)
❌ Name collision if multiple runs in same minute

## Hybrid Approach (Recommended)

Do BOTH:
1. **Keep Jupyter variables** for interactive exploration in notebook
2. **Write to Snowflake tables** for SQL-based inspection and sharing

## Example Inspection Queries

Once persisted, you can query from Snowflake SQL:

```sql
-- Check if hosp_rollup_df is empty
SELECT COUNT(*) FROM CSZNB_PRD_PS_PFA_DB.STAGE.DEBUG_HOSP_ROLLUP_DF_20250126_143022;

-- Check claim type distribution
SELECT CLAIM_TYPE, COUNT(*) as cnt
FROM CSZNB_PRD_PS_PFA_DB.STAGE.DEBUG_CLAIMS_DF_TAGGED_20250126_143022
GROUP BY CLAIM_TYPE;

-- Check NICU revenue codes
SELECT REV_CD, CLAIM_TYPE, COUNT(*) as cnt
FROM CSZNB_PRD_PS_PFA_DB.STAGE.DEBUG_CLAIMS_DF_TAGGED_20250126_143022
WHERE REV_CD BETWEEN '0170' AND '0179'
GROUP BY REV_CD, CLAIM_TYPE;

-- Join debug tables together
SELECT h.*, n.BABY_TYPE
FROM CSZNB_PRD_PS_PFA_DB.STAGE.DEBUG_HOSP_ROLLUP_DF_20250126_143022 h
LEFT JOIN CSZNB_PRD_PS_PFA_DB.STAGE.DEBUG_NEWBORN_IDENT_DF_20250126_143022 n
  ON h.INDV_ID = n.INDV_ID
  AND h.DELIVERY_DT = n.DELIVERY_DT;
```

## Cleanup Script

```sql
-- List all debug tables
SHOW TABLES LIKE 'DEBUG_%' IN CSZNB_PRD_PS_PFA_DB.STAGE;

-- Drop all debug tables from a specific run
DROP TABLE IF EXISTS CSZNB_PRD_PS_PFA_DB.STAGE.DEBUG_CLAIMS_DF_TAGGED_20250126_143022;
DROP TABLE IF EXISTS CSZNB_PRD_PS_PFA_DB.STAGE.DEBUG_HOSP_ROLLUP_DF_20250126_143022;
-- etc...

-- Or drop all debug tables (careful!)
-- SELECT 'DROP TABLE ' || TABLE_CATALOG || '.' || TABLE_SCHEMA || '.' || TABLE_NAME || ';'
-- FROM INFORMATION_SCHEMA.TABLES
-- WHERE TABLE_SCHEMA = 'STAGE' AND TABLE_NAME LIKE 'DEBUG_%';
```
