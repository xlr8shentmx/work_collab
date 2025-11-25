# Debug Mode Implementation Notes

## Current Status

### Phase 1: Configuration Added ✓
- Added `DEBUG_MODE = True` parameter to configuration cell
- Updated configuration validation to display debug mode status
- This lays the groundwork for debug retention features

## Intermediate Dataframes for Debugging

The following key intermediate dataframes are created in `build_newborn_and_nicu_ids()` and assigned in `main()`:

1. **`newborn_ident_df`**: One row per newborn (INDV_ID + DELIVERY_DT) with aggregated metrics
2. **`nicu_ident`**: Subset of newborn_ident_df filtered to NICU cases only

## How to Access for Debugging

Since these are Snowpark DataFrames with lazy evaluation, they need to be explicitly materialized to be retained. Here are the options:

### Option 1: Return from main() (Recommended for Quick Testing)
Modify the last line of `main()` to return the dataframes:
```python
return newborn_ident_df, nicu_ident, newborns_df
```

Then call main() and capture the return values:
```python
newborn_ident_debug, nicu_ident_debug, newborns_final = main(auto_window=True)
```

### Option 2: Add Conditional Caching (For Production)
Add caching logic after the dataframes are assigned in `main()`:
```python
newborn_ident_df    = ids["newborn_ident_df"]
nicu_ident          = ids["nicu_ident"]

# Debug retention - cache intermediate dataframes
if DEBUG_MODE:
    logger.info("[DEBUG] Caching intermediate dataframes for inspection...")
    newborn_ident_df = newborn_ident_df.cache_result()
    nicu_ident = nicu_ident.cache_result()
    logger.info(f"[DEBUG] Cached newborn_ident_df ({newborn_ident_df.count():,} rows)")
    logger.info(f"[DEBUG] Cached nicu_ident ({nicu_ident.count():,} rows)")
```

### Option 3: Export to Temporary Tables
Add export logic when DEBUG_MODE=True:
```python
if DEBUG_MODE:
    debug_table_suffix = f"_DEBUG_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    newborn_ident_df.write.mode("overwrite").save_as_table(
        f"{DATABASE}.{SCHEMA}.DEBUG_NEWBORN_IDENT{debug_table_suffix}"
    )
    nicu_ident.write.mode("overwrite").save_as_table(
        f"{DATABASE}.{SCHEMA}.DEBUG_NICU_IDENT{debug_table_suffix}"
    )
    logger.info(f"[DEBUG] Exported to temporary tables with suffix: {debug_table_suffix}")
```

## Inspection Examples

Once retained (via any option above), you can inspect the dataframes:

```python
# View schema
newborn_ident_df.schema

# Count rows
print(f"Newborns: {newborn_ident_df.count():,}")
print(f"NICU: {nicu_ident.count():,}")

# Show sample
newborn_ident_df.show(10)
nicu_ident.show(10)

# Check specific metrics
nicu_ident.group_by("STUDY_YR").agg(
    count("*").alias("COUNT"),
    ssum("TOTAL_NICU_COST").alias("TOTAL_COST")
).show()

# Export sample to Pandas for detailed analysis
sample_df = nicu_ident.limit(1000).to_pandas()
```

## Next Steps

To complete debug retention implementation:
1. Choose one of the options above
2. Modify the `main()` function in `nrs_beta_2.ipynb` accordingly
3. Test with DEBUG_MODE=True
4. Verify dataframes are accessible for inspection

## Performance Considerations

- Caching adds overhead (time + temp storage space)
- Only enable DEBUG_MODE during development/testing
- Consider using `.limit()` when inspecting large dataframes
- Clean up debug tables periodically if using Option 3
