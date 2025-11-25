# Debug Tables Implementation Plan

## Current Debug Variables
Already returned when DEBUG_MODE=True:
- `newborn_ident_df` - One row per newborn (INDV_ID + DELIVERY_DT)
- `nicu_ident` - NICU subset
- `newborns_df` - Final output
- `nicu_rollup` - NICU aggregations

## Proposed Additional Debug Variables

### 1. Claims Processing Pipeline
**Variable:** `claims_df_tagged`
- **What:** Claims after all reference flags are tagged
- **Why:** Verify flag tagging logic (NEWBORN_ICD, NICU_REV, etc.)
- **When:** After `tag_all_reference_flags()`, before `newborn_rollup()`
- **Use Cases:**
  - Check how many claims have each flag
  - Verify ICD/revenue code matching
  - Debug missing newborns

### 2. Newborn Claims
**Variable:** `newborn_claims`
- **What:** Claims filtered to newborns with birth type hierarchy
- **Why:** See intermediate newborn identification step
- **When:** Returned from `newborn_rollup()`
- **Use Cases:**
  - Verify DELIVERY_DT assignment
  - Check birth type classification
  - See which claims qualify as newborn

### 3. Hospital Rollup
**Variable:** `hosp_rollup_df`
- **What:** Aggregated hospital stays per newborn
- **Why:** **CRITICAL** - This is likely empty causing your issue!
- **When:** After `build_hosp_rollup()`
- **Use Cases:**
  - **Diagnose why newborn_ident_df is empty**
  - Check if IP claims exist
  - Verify LOS calculations
  - See episode stitching

### 4. Newborn Hospital Claims
**Variable:** `newborn_hosp_clms`
- **What:** Claims bounded to hospital episodes
- **Why:** See claim-to-episode mapping
- **When:** From `build_newborn_and_nicu_ids()`
- **Use Cases:**
  - Check episode filtering
  - Verify de-duplication logic

### 5. NICU Claims
**Variable:** `nicu_claims_df`
- **What:** Claims for NICU episodes only
- **Why:** Detailed NICU claim-level analysis
- **When:** From `build_newborn_and_nicu_ids()`
- **Use Cases:**
  - Analyze NICU services
  - Check provider attribution
  - Verify discharge status logic

## Implementation Strategy

### Phase 1: Add Critical Debugging (IMMEDIATE)
Focus on diagnosing the empty newborn_ident_df issue:
```python
if DEBUG_MODE:
    return {
        # Current
        'newborn_ident_df': newborn_ident_df,
        'nicu_ident': nicu_ident,
        'newborns_df': newborns_df,
        'nicu_rollup': nicu_rollup,

        # NEW - Critical for debugging
        'claims_df_tagged': claims_df,  # After tagging, before rollup
        'hosp_rollup_df': hosp_rollup_df,  # MOST IMPORTANT
        'newborn_claims': newborn_claims,  # From newborn_rollup
    }
```

### Phase 2: Add Detailed Analysis (OPTIONAL)
After fixing the bug, add more detail:
```python
if DEBUG_MODE:
    return {
        # Phase 1 variables
        ...

        # Phase 2 - More detail
        'newborn_hosp_clms': newborn_hosp_clms,
        'nicu_claims_df': nicu_claims_df,
        'nicu_dischg_provider': nicu_dischg_provider,
    }
```

## Diagnostic Workflow

With these variables, you can diagnose the issue:

```python
# 1. Check if claims have the right flags
claims_df_tagged.select("NEWBORN_ICD", "NEWBORN_REV").show()
claims_df_tagged.filter(col("NEWBORN_ICD") | col("NEWBORN_REV")).count()

# 2. Check if newborn_claims is populated
print(f"Newborn claims: {newborn_claims.count():,}")

# 3. CHECK THE CULPRIT - Is hosp_rollup_df empty?
print(f"Hospital rollups: {hosp_rollup_df.count():,}")  # ← Probably 0!

# 4. If hosp_rollup_df is empty, check why
# Check if we have IP claims
claims_df_tagged.group_by("CLAIM_TYPE").count().show()

# 5. Check if claims have DELIVERY_DT
newborn_claims.filter(col("DELIVERY_DT").is_null()).count()
```

## Expected Findings

Based on the revenue code bug we found:
- `claims_df_tagged`: Should have many rows
- `newborn_claims`: Should have many rows
- `hosp_rollup_df`: **EMPTY** ← Because NICU rev codes 170-179 not classified as "IP"
- `newborn_ident_df`: **EMPTY** ← Because INNER JOIN with empty hosp_rollup_df

## Solution Priority

1. **First:** Implement Phase 1 debug returns
2. **Second:** Run pipeline and inspect `hosp_rollup_df.count()`
3. **Third:** Fix the revenue code classification bug
4. **Fourth:** Re-run and verify newborn_ident_df is populated
