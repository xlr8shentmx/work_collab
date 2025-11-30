# NRS Analytics Metrics History Table - Documentation

## Overview

The `nrs_analytics_history` table is designed to capture and track key performance indicators (KPIs) and data quality metrics from the NRS (Newborn Risk Stratification) analytics pipeline across multiple client runs over time.

## Purpose

This table enables:
- **Trend Analysis**: Track NICU rates, costs, and utilization patterns over time
- **Period-over-Period Comparison**: Compare Previous vs Current study periods within each run
- **Data Quality Monitoring**: Identify data quality issues and pipeline anomalies
- **Client Benchmarking**: Compare metrics across different clients
- **Audit Trail**: Maintain historical record of pipeline executions and results
- **Business Reporting**: Generate executive dashboards and KPI reports

---

## Table Structure

### 1. Primary Keys & Run Identifiers

| Field | Type | Description |
|-------|------|-------------|
| `run_id` | VARCHAR(100) | Unique identifier for each pipeline execution (PRIMARY KEY) |
| `run_date` | TIMESTAMP | Date/time when the pipeline was executed |
| `client_name` | VARCHAR(100) | Client identifier (part of PRIMARY KEY) |
| `etl_timestamp` | TIMESTAMP | When this record was inserted into the history table |

**Usage Notes:**
- `run_id` should be generated as a UUID or timestamp-based identifier (e.g., `20250130_CLIENT_ABC_v1`)
- Each client run creates exactly one row in this table
- Composite primary key allows multiple runs per client over time

---

### 2. Time Period Metadata

| Field | Type | Description |
|-------|------|-------------|
| `birth_window_start` | DATE | Earliest service date in the claims dataset |
| `birth_window_mid` | DATE | Midpoint date that separates Previous/Current periods |
| `birth_window_end` | DATE | Latest service date in the claims dataset |
| `runout_end_date` | DATE | Latest paid date (allows for claims run-out) |
| `data_cutoff_date` | DATE | Data extraction cutoff date |

**Usage Notes:**
- These dates define the temporal boundaries of the analysis
- `birth_window_mid` is critical for stratifying newborns into Previous vs Current cohorts
- Typically: `birth_window_mid = (birth_window_start + birth_window_end) / 2`

---

### 3. Membership Metrics

Captures enrollment and demographic information from the `PS_MEMBERSHIP` table.

#### Overall Metrics

| Field | Calculation | Business Meaning |
|-------|-------------|------------------|
| `total_members_enrolled` | COUNT(DISTINCT INDV_ID) | Total unique members in the eligibility file |
| `total_member_months` | SUM(MEMBER_MONTHS) | Total enrollment duration across all members |
| `avg_member_months_per_member` | total_member_months / total_members | Average enrollment duration per member |

#### Period Stratification

| Field | Description |
|-------|-------------|
| `members_previous_period` | Members with dates < birth_window_mid |
| `members_current_period` | Members with dates >= birth_window_mid |
| `member_months_previous` | Member months for Previous period |
| `member_months_current` | Member months for Current period |

#### Demographic Breakdowns

| Field | Description |
|-------|-------------|
| `members_by_gender_m` | Male members count |
| `members_by_gender_f` | Female members count |
| `members_by_gender_unknown` | Members with null/unknown gender |

#### Business Attribute Breakdowns

| Field | Description |
|-------|-------------|
| `member_count_medicaid` | Members with LOB = Medicaid |
| `member_count_commercial` | Members with LOB = Commercial |
| `member_count_other_lob` | Members with other/unknown LOB |
| `unique_states_count` | COUNT(DISTINCT STATE) |
| `unique_business_lines_count` | COUNT(DISTINCT BUS_LINE_CD) |
| `unique_product_codes_count` | COUNT(DISTINCT PRDCT_CD) |

---

### 4. Newborn Metrics (Overall)

Captures aggregate statistics for all newborns identified in the `PS_NEWBORNS` table.

#### Overall Counts

| Field | Calculation | Business Meaning |
|-------|-------------|------------------|
| `total_newborns_identified` | COUNT(DISTINCT INDV_ID, DELIVERY_DT) | Total unique newborns in the analysis |
| `newborns_previous_period` | COUNT WHERE STUDY_YR = 'Previous' | Newborns born in earlier period |
| `newborns_current_period` | COUNT WHERE STUDY_YR = 'Current' | Newborns born in later period |

#### Birth Type Distribution

| Field | Description |
|-------|-------------|
| `newborns_single_birth` | Single births (most common) |
| `newborns_twin_birth` | Twin births (multiplicity = 2) |
| `newborns_multiple_birth` | Triplets or higher multiples |

**Note:** Birth type is determined from ICD diagnosis codes and revenue code patterns.

#### Baby Type Classification

| Field | Calculation | Business Meaning |
|-------|-------------|------------------|
| `newborns_normal` | COUNT WHERE ANY_NICU = 0 | Healthy newborns without NICU admission |
| `newborns_with_any_nicu` | COUNT WHERE ANY_NICU = 1 | Newborns with at least one NICU episode |
| `nicu_rate_pct` | (newborns_with_any_nicu / total_newborns) × 100 | **KEY METRIC:** Percentage of newborns requiring NICU |

#### Hospital Stay Metrics

| Field | Description |
|-------|-------------|
| `newborns_with_hospital_stay` | Newborns with ≥1 inpatient episode |
| `total_hospital_episodes` | Total hospital stays (including readmissions) |
| `avg_episodes_per_newborn` | Average number of stays per newborn |
| `newborns_admitted_in_init_days` | Admitted within `init_hosp_threshold_days` (typically 4 days) |

#### Length of Stay (LOS)

| Field | Calculation | Business Meaning |
|-------|-------------|------------------|
| `total_newborn_los_days` | SUM(LOS) | Cumulative hospital days across all newborns |
| `avg_newborn_los_days` | total_los / total_newborns | Average hospital stay length |
| `newborns_short_stay` | COUNT WHERE LOS < 3 | Newborns with brief stays (< 3 days) |
| `newborns_long_stay` | COUNT WHERE LOS >= 3 | Newborns with extended stays |

#### Financial Metrics

| Field | Calculation | Business Meaning |
|-------|-------------|------------------|
| `total_newborn_paid_amt` | SUM(NET_PD_AMT) | Total paid amount for all newborn care |
| `avg_newborn_paid_amt` | total_paid / total_newborns | Average cost per newborn |

#### Readmission Metrics

| Field | Calculation | Business Meaning |
|-------|-------------|------------------|
| `newborns_with_readmission` | COUNT(newborns with READMIT > 0) | Newborns readmitted within 30 days |
| `total_readmissions_count` | SUM(READMIT) | Total readmission events |
| `total_readmission_paid_amt` | SUM(READMIT_PAID_AMT) | Total cost of readmissions |
| `total_readmission_los_days` | SUM(READMIT_LOS) | Total days for readmissions |

---

### 5. NICU Metrics (Detailed)

NICU-specific metrics for episodes identified by revenue codes, MS-DRG, or APR-DRG criteria.

#### NICU Episode Counts

| Field | Description |
|-------|-------------|
| `total_nicu_episodes` | Total NICU hospital episodes across all newborns |
| `nicu_episodes_previous` | NICU episodes in Previous period |
| `nicu_episodes_current` | NICU episodes in Current period |

#### NICU Identification Methods

| Field | Criteria | Description |
|-------|----------|-------------|
| `nicu_identified_by_rev` | REV 170-179 | Episodes with NICU revenue codes |
| `nicu_identified_by_msdrg` | MS-DRG 580-640 | Episodes with neonate MS-DRG codes |
| `nicu_identified_by_aprdrg` | APR-DRG 789-795 | Episodes with extreme neonate APR-DRG codes |

**Note:** Episodes may be counted in multiple categories if they meet multiple criteria.

#### NICU by Birth Type & Contract

| Field | Description |
|-------|-------------|
| `nicu_single_birth` | NICU episodes for single births |
| `nicu_twin_birth` | NICU episodes for twins |
| `nicu_multiple_birth` | NICU episodes for multiples |
| `nicu_drg_contract` | DRG-based contract episodes |
| `nicu_per_diem_contract` | Per-diem contract episodes |

#### NICU Length of Stay

| Field | Calculation | Business Meaning |
|-------|-------------|------------------|
| `total_nicu_los_days` | SUM(LOS) across NICU episodes | Total NICU days across all episodes |
| `avg_nicu_los_days` | total_los / total_episodes | Average NICU stay length |
| `median_nicu_los_days` | MEDIAN(LOS) | Median NICU stay (less affected by outliers) |
| `max_nicu_los_days` | MAX(LOS) | Longest NICU stay |
| `min_nicu_los_days` | MIN(LOS) | Shortest NICU stay (should be ≥ 1) |

#### NICU Cost Metrics - Total

| Field | Calculation | Business Meaning |
|-------|-------------|------------------|
| `total_nicu_cost` | SUM(TOTAL_NICU_COST) | **KEY METRIC:** Total NICU expenditure |
| `avg_nicu_cost_per_episode` | total_cost / total_episodes | Average cost per NICU episode |
| `avg_nicu_cost_per_day` | total_cost / total_los | Average daily NICU cost (benchmark: ~$150+/day) |
| `median_nicu_cost_per_episode` | MEDIAN(TOTAL_NICU_COST) | Median cost per episode |
| `max_nicu_cost_per_episode` | MAX(TOTAL_NICU_COST) | Most expensive NICU episode |

#### NICU Cost Breakdown - Professional Fees

| Field | Source | Description |
|-------|--------|-------------|
| `total_professional_fees` | SUM(ALL_PROFFEE) | All professional service fees |
| `avg_professional_fees` | total / episodes | Average professional fees per episode |
| `pct_professional_fees` | (prof_fees / total_cost) × 100 | Professional fees as % of total cost |
| `total_manageable_proffee` | SUM(MANAGEABLE_PROFFEE) | Fees for manageable procedures (has CPT code) |
| `total_manageable_service_days` | COUNT(unique service-days) | Unique service days for manageable procedures |
| `total_critical_care_proffee` | SUM(CRITICAL_CARE_PROFFEE) | Critical care professional fees |
| `total_critical_care_service_days` | COUNT(unique service-days) | Unique critical care service days |

#### NICU Cost Breakdown - Facility

| Field | Calculation | Description |
|-------|-------------|-------------|
| `total_facility_cost` | TOTAL_NICU_COST - ALL_PROFFEE | Facility/hospital charges |
| `avg_facility_cost` | total / episodes | Average facility cost per episode |
| `pct_facility_cost` | (facility / total_cost) × 100 | Facility as % of total cost |
| `total_facility_room_cost` | SUM(FACILITY_RM_COST) | Room & board costs (REV 011-017, 020) |
| `avg_facility_room_cost` | total / episodes | Average room cost per episode |
| `pct_room_of_facility` | (room / facility) × 100 | Room cost as % of facility cost |

#### NICU Revenue Code Leveling

| Field | Calculation | Business Meaning |
|-------|-------------|------------------|
| `nicu_episodes_with_rev_leveling` | COUNT WHERE REV_LEVELING = TRUE | Episodes with multiple NICU revenue levels |
| `pct_episodes_with_rev_leveling` | (with_leveling / total) × 100 | % of episodes with level changes |
| `nicu_rev_level_1` | REV 170-173 | Level I/II NICU (basic) |
| `nicu_rev_level_2` | REV 174-175 | Level II NICU (intermediate) |
| `nicu_rev_level_3` | REV 176-177 | Level III NICU (intensive) |
| `nicu_rev_level_4` | REV 178-179 | Level IV NICU (highest acuity) |

**Clinical Significance:**
- Higher revenue codes indicate higher acuity care
- Revenue leveling suggests patient condition changes during stay

#### NICU Discharge Status

| Field | Status Codes | Description |
|-------|--------------|-------------|
| `nicu_discharge_routine` | 20 | Routine discharge to home (best outcome) |
| `nicu_discharge_transfer` | 02,05,66,43,62,63,65 | Transfer to other facility/care level |
| `nicu_discharge_expired` | 01,06 | Patient expired |
| `nicu_discharge_other` | Various | Other discharge dispositions |
| `nicu_discharge_invalid` | N/A | Invalid/unmapped status codes (data quality issue) |

#### NICU Readmissions

| Field | Description |
|-------|-------------|
| `nicu_episodes_with_readmit` | NICU episodes followed by readmission within 30 days |
| `total_nicu_readmissions` | Total readmission events |
| `total_nicu_readmit_paid_amt` | Total readmission costs |
| `total_nicu_readmit_los_days` | Total readmission days |

---

### 6. Data Quality Metrics

Critical for monitoring pipeline health and identifying data issues.

#### Claim Processing

| Field | Description |
|-------|-------------|
| `total_claims_processed` | Total claims analyzed by pipeline |
| `total_ip_claims` | Inpatient claims (has ADMIT_DT/DSCHRG_STS/DRG) |
| `total_op_claims` | Outpatient claims |
| `claims_excluded_high_cost` | Claims excluded due to high cost threshold |

#### Diagnosis Data Quality

| Field | Calculation | Acceptable Range |
|-------|-------------|------------------|
| `claims_with_null_diagnosis` | COUNT(claims with all DIAG_CD null) | Should be < 10% |
| `pct_claims_null_diagnosis` | (null_diag / total_claims) × 100 | Target: < 10% |

#### Newborn/NICU Matching Rates

| Field | Description | Expected Range |
|-------|-------------|----------------|
| `claims_matched_newborn_icd` | Claims matched to REF_NEWBORN_ICD | Varies by dataset |
| `claims_matched_newborn_rev` | Claims matched to REF_NEWBORN_REVCODE | Varies by dataset |
| `claims_matched_nicu_rev` | Claims matched to NICU REV 170-179 | Subset of newborn claims |
| `claims_matched_nicu_msdrg` | Claims matched to MS-DRG 580-640 | Subset of newborn claims |
| `claims_matched_nicu_aprdrg` | Claims matched to APR-DRG 789-795 | Subset of newborn claims |

#### Episode Grouping Quality

| Field | Calculation | Business Meaning |
|-------|-------------|------------------|
| `total_hospital_episodes_created` | COUNT(DISTINCT INDV_ID, DELIVERY_DT, HOSP_STAY) | Hospital stays created by grouping logic |
| `avg_claims_per_episode` | total_claims / total_episodes | Typical: 5-20 claims per episode |
| `episodes_with_admit_gap` | COUNT(episodes with ADMIT_GAP calculated) | Episodes where admission gap was measured |

#### Date Validation

| Field | Calculation | Acceptable Range |
|-------|-------------|------------------|
| `episodes_discharge_clipped` | COUNT WHERE DSCHRG > runout_end | Should be minimal (< 5%) |
| `pct_episodes_discharge_clipped` | (clipped / total) × 100 | Target: < 5% |

**Note:** High clipping rates indicate inadequate run-out period in data.

#### Provider Data Quality

| Field | Calculation | Target |
|-------|-------------|--------|
| `nicu_episodes_with_npi` | COUNT(episodes with PROV_NPI not null) | > 90% |
| `nicu_episodes_with_tin` | COUNT(episodes with PROV_TIN not null) | > 90% |
| `pct_nicu_episodes_with_provider` | (with_npi OR with_tin) / total × 100 | > 95% |
| `unique_nicu_providers_npi` | COUNT(DISTINCT PROV_NPI) | Measure of network breadth |
| `unique_nicu_providers_tin` | COUNT(DISTINCT PROV_TIN) | Measure of provider organizations |
| `unique_nicu_provider_states` | COUNT(DISTINCT PROV_STATE) | Geographic spread |

#### NICU Appropriateness Flags

**Low-Paid NICU Cases:**

| Field | Criteria | Business Meaning |
|-------|----------|------------------|
| `nicu_low_paid_cases` | TOTAL_NICU_COST / LOS < $150/day | Suspiciously low cost per day |
| `pct_nicu_low_paid` | (low_paid / total_nicu) × 100 | Should be < 5% |
| `total_low_paid_nicu_cost` | SUM(cost for flagged episodes) | Financial impact |

**Inappropriate NICU Admissions:**

| Field | Criteria | Business Meaning |
|-------|----------|------------------|
| `nicu_inappropriate_cases` | DRG contract AND LOS ≤ 5 days | Potentially avoidable NICU admissions |
| `pct_nicu_inappropriate` | (inappropriate / total_nicu) × 100 | Target for reduction |
| `total_inappropriate_nicu_cost` | SUM(cost for flagged episodes) | Potential savings opportunity |

**Clinical Rationale:**
- NICU admissions with very short stays (≤ 5 days) may not require NICU-level care
- DRG-based contracts make these high-cost events
- Opportunity for care management intervention

#### LOS Validation

| Field | Description | Expected Value |
|-------|-------------|----------------|
| `episodes_with_los_lt_1` | COUNT WHERE LOS < 1 | Should be 0 (data quality check) |
| `avg_los_all_episodes` | Average LOS across all episodes | Benchmark varies by type |

#### Membership Quality

| Field | Description | Acceptable Range |
|-------|-------------|------------------|
| `members_with_null_birth_date` | Members missing BTH_DT | Should be minimal |
| `members_with_null_gender` | Members missing GENDER | Should be < 1% |
| `members_with_missing_lob` | Members missing LOB/BUS_LINE_CD | Should be < 5% |

#### Study Year Distribution

| Field | Calculation | Expected Range |
|-------|-------------|----------------|
| `pct_previous_period` | (previous / total) × 100 | ~50% (depends on birth_window_mid) |
| `pct_current_period` | (current / total) × 100 | ~50% (depends on birth_window_mid) |

**Note:** Unbalanced periods may indicate data completeness issues or shifting birth windows.

---

### 7. Clinical Metrics (Optional)

If birth weight and gestational age data are available in the dataset:

#### Birth Weight Categories

| Field | Category | Clinical Definition |
|-------|----------|---------------------|
| `nicu_birth_weight_unknown` | Unknown | Missing birth weight data |
| `nicu_birth_weight_normal` | Normal | ≥ 2500g |
| `nicu_birth_weight_low` | LBW | 1500-2499g |
| `nicu_birth_weight_very_low` | VLBW | 1000-1499g |
| `nicu_birth_weight_extremely_low` | ELBW | < 1000g |

#### Gestational Age Categories

| Field | Category | Clinical Definition |
|-------|----------|---------------------|
| `nicu_gest_age_unknown` | Unknown | Missing gestational age data |
| `nicu_gest_age_term` | Term | ≥ 37 weeks |
| `nicu_gest_age_preterm` | Preterm | 32-36 weeks |
| `nicu_gest_age_very_preterm` | Very Preterm | 28-31 weeks |
| `nicu_gest_age_extremely_preterm` | Extremely Preterm | < 28 weeks |

**Clinical Significance:**
- Lower birth weight and earlier gestational age correlate with higher NICU costs and longer stays
- Use these metrics to risk-stratify and case-manage high-risk newborns

---

### 8. Metadata & Versioning

Critical for reproducibility and troubleshooting.

| Field | Description | Example |
|-------|-------------|---------|
| `pipeline_version` | Version of NRS pipeline code | "v2.3.1" |
| `data_source_version` | Version/extract date of source data | "2025-01-15" |
| `sql_engine` | Database engine used | "databricks", "snowflake" |
| `config_hash` | MD5/SHA hash of configuration parameters | "a3b2c1d4..." |
| `processing_duration_seconds` | Time taken to run pipeline | 3600 (1 hour) |
| `data_quality_score` | Composite quality score (0-100) | 87.5 |
| `notes` | Free-text notes about the run | "Rerun due to data refresh" |
| `created_by` | User/process that ran pipeline | "analytics_team" |

**Best Practice:**
- Store the actual configuration file or parameters separately (referenced by `config_hash`)
- Use `config_hash` to ensure identical runs produce identical results
- Use `notes` to document anomalies or special circumstances

---

## How to Populate This Table

### Option 1: Direct Insertion from Pipeline

After your NRS pipeline completes, calculate the aggregate metrics and insert:

```sql
INSERT INTO nrs_analytics_history
SELECT
    -- Run identifiers
    '2025-01-30_CLIENT_ABC_v1' AS run_id,
    CURRENT_TIMESTAMP AS run_date,
    'CLIENT_ABC' AS client_name,
    CURRENT_TIMESTAMP AS etl_timestamp,

    -- Time periods
    '2023-01-01'::DATE AS birth_window_start,
    '2024-06-15'::DATE AS birth_window_mid,
    '2024-12-31'::DATE AS birth_window_end,
    '2025-01-15'::DATE AS runout_end_date,
    '2025-01-30'::DATE AS data_cutoff_date,

    -- Membership metrics
    (SELECT COUNT(DISTINCT INDV_ID) FROM PS_MEMBERSHIP_CLIENT_ABC) AS total_members_enrolled,
    (SELECT SUM(MEMBER_MONTHS) FROM PS_MEMBERSHIP_CLIENT_ABC) AS total_member_months,
    -- ... continue with all metrics ...

FROM dual; -- or SELECT without FROM depending on SQL dialect
```

### Option 2: Create Summary View First

Create intermediate aggregation views/CTEs, then insert:

```sql
WITH membership_summary AS (
    SELECT
        COUNT(DISTINCT INDV_ID) AS total_members,
        SUM(MEMBER_MONTHS) AS total_mm,
        COUNT(DISTINCT CASE WHEN STUDY_YR = 'Previous' THEN INDV_ID END) AS members_prev,
        COUNT(DISTINCT CASE WHEN STUDY_YR = 'Current' THEN INDV_ID END) AS members_curr
    FROM PS_MEMBERSHIP_CLIENT_ABC
),
newborn_summary AS (
    SELECT
        COUNT(*) AS total_newborns,
        COUNT(CASE WHEN ANY_NICU = 1 THEN 1 END) AS newborns_with_nicu,
        SUM(NET_PD_AMT) AS total_paid,
        AVG(LOS) AS avg_los
    FROM PS_NEWBORNS_CLIENT_ABC
),
nicu_summary AS (
    SELECT
        COUNT(*) AS total_nicu_episodes,
        SUM(TOTAL_NICU_COST) AS total_cost,
        AVG(LOS) AS avg_los,
        COUNT(CASE WHEN LOW_PAID_NICU = 1 THEN 1 END) AS low_paid_cases
    FROM NICU_ROLLUP_CLIENT_ABC
)
INSERT INTO nrs_analytics_history (...)
SELECT
    '2025-01-30_CLIENT_ABC_v1',
    CURRENT_TIMESTAMP,
    'CLIENT_ABC',
    -- Pull from CTEs
    m.total_members,
    m.total_mm,
    n.total_newborns,
    n.newborns_with_nicu,
    nicu.total_nicu_episodes,
    -- ... etc ...
FROM membership_summary m
CROSS JOIN newborn_summary n
CROSS JOIN nicu_summary nicu;
```

### Option 3: Post-Processing Script

Use a Python/R script to:
1. Query the final PS_MEMBERSHIP, PS_NEWBORNS, and NICU tables
2. Calculate all aggregate metrics
3. Insert row into nrs_analytics_history

```python
# Example pseudocode
import pandas as pd
from datetime import datetime

# Calculate metrics
total_newborns = len(newborns_df)
nicu_rate = (newborns_df['ANY_NICU'].sum() / total_newborns) * 100
avg_nicu_cost = nicu_df['TOTAL_NICU_COST'].mean()

# Build history record
history_record = {
    'run_id': f"{datetime.now().strftime('%Y%m%d')}_{client_name}_v1",
    'run_date': datetime.now(),
    'client_name': client_name,
    'total_newborns_identified': total_newborns,
    'nicu_rate_pct': nicu_rate,
    'avg_nicu_cost_per_episode': avg_nicu_cost,
    # ... all other metrics ...
}

# Insert to database
history_df = pd.DataFrame([history_record])
history_df.to_sql('nrs_analytics_history', con=engine, if_exists='append', index=False)
```

---

## Key Metrics Dashboard

### Executive Summary Metrics (Top 5)

| Metric | Field Name | Business Importance |
|--------|------------|---------------------|
| **NICU Rate** | `nicu_rate_pct` | Primary outcome measure; target varies by population |
| **Avg NICU Cost** | `avg_nicu_cost_per_episode` | Cost per episode; benchmark against industry standards |
| **Total NICU Spend** | `total_nicu_cost` | Total financial exposure for NICU care |
| **Inappropriate NICU %** | `pct_nicu_inappropriate` | Opportunity for utilization management |
| **Data Quality Score** | `data_quality_score` | Overall confidence in results |

### Trending Reports

**Month-over-Month NICU Rate:**
```sql
SELECT
    DATE_TRUNC('month', run_date) AS month,
    AVG(nicu_rate_pct) AS avg_nicu_rate,
    AVG(avg_nicu_cost_per_episode) AS avg_cost
FROM nrs_analytics_history
WHERE client_name = 'CLIENT_XYZ'
    AND run_date >= CURRENT_DATE - INTERVAL '12 months'
GROUP BY 1
ORDER BY 1;
```

**Period-over-Period Comparison:**
```sql
SELECT
    run_id,
    newborns_previous_period,
    newborns_current_period,
    nicu_episodes_previous,
    nicu_episodes_current,
    ROUND((nicu_episodes_previous::DECIMAL / NULLIF(newborns_previous_period,0)) * 100, 2) AS nicu_rate_prev,
    ROUND((nicu_episodes_current::DECIMAL / NULLIF(newborns_current_period,0)) * 100, 2) AS nicu_rate_curr
FROM nrs_analytics_history
WHERE client_name = 'CLIENT_XYZ'
ORDER BY run_date DESC
LIMIT 10;
```

---

## Data Quality Thresholds

Recommended thresholds for monitoring data quality:

| Metric | Warning Threshold | Critical Threshold |
|--------|-------------------|-------------------|
| `pct_claims_null_diagnosis` | > 10% | > 20% |
| `pct_episodes_discharge_clipped` | > 5% | > 10% |
| `pct_nicu_episodes_with_provider` | < 90% | < 80% |
| `pct_nicu_low_paid` | > 5% | > 10% |
| `pct_nicu_inappropriate` | > 15% | > 25% |
| `members_with_null_birth_date` | > 100 | > 500 |
| `nicu_rate_pct` (sudden change) | ±20% from baseline | ±50% from baseline |

**Alert Logic:**
- If any metric crosses warning threshold → investigate
- If any metric crosses critical threshold → halt reporting until resolved
- Track thresholds in separate configuration table

---

## Schema Evolution & Maintenance

### Adding New Metrics

When new metrics are identified:

1. **Add column with ALTER TABLE:**
```sql
ALTER TABLE nrs_analytics_history
ADD COLUMN new_metric_name DECIMAL(18,2);

COMMENT ON COLUMN nrs_analytics_history.new_metric_name IS
    'Description of new metric and calculation logic';
```

2. **Backfill historical data (if possible):**
```sql
UPDATE nrs_analytics_history
SET new_metric_name = <calculation>
WHERE new_metric_name IS NULL
    AND <data is available for recalculation>;
```

3. **Update documentation** in this file

### Versioning Strategy

- Use `pipeline_version` to track schema changes
- Major version change (v1 → v2): Breaking schema changes
- Minor version change (v2.1 → v2.2): Additive schema changes
- Patch version change (v2.2.1 → v2.2.2): No schema changes

### Archival Policy

For long-term storage efficiency:

- **Archive runs older than 3 years** to separate archive table
- **Partition table by year** for query performance
- **Aggregate monthly summaries** for historical trending

```sql
-- Example partitioning (Postgres)
CREATE TABLE nrs_analytics_history_2025 PARTITION OF nrs_analytics_history
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
```

---

## Related Tables

This history table references and summarizes these source tables:

| Source Table | Relationship | Description |
|--------------|--------------|-------------|
| `PS_MEMBERSHIP_{CLIENT}` | Aggregated | Member eligibility and demographics |
| `PS_NEWBORNS_{CLIENT}` | Aggregated | Newborn-level detail (1 row per baby) |
| `NICU_ROLLUP` | Aggregated | NICU episode-level detail |
| `FA_CLAIMS` | Indirect | Raw claims data (via transformations) |
| `REF_NEWBORN_ICD` | Reference | Newborn diagnosis code definitions |
| `REF_NICU_REVCODE` | Reference | NICU revenue code definitions |

---

## Appendix: Configuration Parameters

Store these in a separate configuration table or document:

| Parameter | Default Value | Description |
|-----------|---------------|-------------|
| `NEWBORN_SERVICE_WINDOW_DAYS` | 4 | Days after delivery to include claims |
| `HOSP_STAY_GAP_DAYS` | 4 | Days between episodes to consider separate stays |
| `init_hosp_threshold_days` | 4 | Days for initial admission window |
| `READMIT_THRESHOLD_DAYS` | 30 | Days for readmission window |
| `NICU_LOW_COST_PER_DAY_THRESHOLD` | $150 | Minimum expected cost per NICU day |
| `INAPPROPRIATE_NICU_MAX_LOS` | 5 | Max LOS for inappropriate NICU flag |
| `HIGH_COST_CLAIM_THRESHOLD` | Varies | Threshold to exclude outlier claims |

---

## Glossary

- **NICU**: Neonatal Intensive Care Unit
- **LOS**: Length of Stay (days)
- **DRG**: Diagnosis-Related Group (payment model)
- **MS-DRG**: Medicare Severity DRG
- **APR-DRG**: All Patient Refined DRG
- **LOB**: Line of Business (Medicaid, Commercial, etc.)
- **REV**: Revenue Code (facility billing code)
- **CPT**: Current Procedural Terminology (professional billing code)
- **NPI**: National Provider Identifier
- **TIN**: Tax Identification Number
- **Birth Window**: Time period of deliveries being analyzed
- **Runout Period**: Time allowed for claims to be submitted/paid after service
- **Study Year**: Classification of newborns into Previous vs Current periods for comparison

---

## Contact & Support

For questions about this schema or the NRS analytics pipeline:
- **Technical Lead**: [Your Name]
- **Business Owner**: [Analytics Team]
- **Documentation**: See `nrs_beta_2.ipynb` for detailed pipeline logic
- **Repository**: [Git repo link]

**Version History:**
- v1.0 (2025-01-30): Initial schema design
