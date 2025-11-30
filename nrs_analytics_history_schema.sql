-- =====================================================
-- NRS Analytics Metrics History Table Schema
-- =====================================================
-- Purpose: Track key metrics for membership, newborns, and NICU
--          across client runs for trending and data quality monitoring
-- =====================================================

CREATE TABLE nrs_analytics_history (

    -- ========================================
    -- PRIMARY KEYS & RUN IDENTIFIERS
    -- ========================================
    run_id                          VARCHAR(100) NOT NULL,
    run_date                        TIMESTAMP NOT NULL,
    client_name                     VARCHAR(100) NOT NULL,
    etl_timestamp                   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- ========================================
    -- TIME PERIOD METADATA
    -- ========================================
    birth_window_start              DATE,
    birth_window_mid                DATE,          -- Separates Previous vs Current periods
    birth_window_end                DATE,
    runout_end_date                 DATE,
    data_cutoff_date                DATE,

    -- ========================================
    -- MEMBERSHIP METRICS
    -- ========================================

    -- Overall Membership Counts
    total_members_enrolled          INTEGER,
    total_member_months             DECIMAL(18,2),
    avg_member_months_per_member    DECIMAL(10,2),

    -- Membership by Study Year
    members_previous_period         INTEGER,
    members_current_period          INTEGER,
    member_months_previous          DECIMAL(18,2),
    member_months_current           DECIMAL(18,2),

    -- Membership by Demographics
    members_by_gender_m             INTEGER,
    members_by_gender_f             INTEGER,
    members_by_gender_unknown       INTEGER,

    -- Membership by Business Attributes
    member_count_medicaid           INTEGER,
    member_count_commercial         INTEGER,
    member_count_other_lob          INTEGER,
    unique_states_count             INTEGER,
    unique_business_lines_count     INTEGER,
    unique_product_codes_count      INTEGER,

    -- ========================================
    -- NEWBORN METRICS (OVERALL)
    -- ========================================

    -- Total Newborn Counts
    total_newborns_identified       INTEGER,
    newborns_previous_period        INTEGER,
    newborns_current_period         INTEGER,

    -- Newborns by Birth Type
    newborns_single_birth           INTEGER,
    newborns_twin_birth             INTEGER,
    newborns_multiple_birth         INTEGER,

    -- Newborns by Baby Type
    newborns_normal                 INTEGER,
    newborns_with_any_nicu          INTEGER,
    nicu_rate_pct                   DECIMAL(5,2),  -- (NICU / Total Newborns) * 100

    -- Newborns by LOB
    newborns_medicaid               INTEGER,
    newborns_commercial             INTEGER,
    newborns_other_lob              INTEGER,

    -- Hospital Stay Metrics
    newborns_with_hospital_stay     INTEGER,
    total_hospital_episodes         INTEGER,
    avg_episodes_per_newborn        DECIMAL(10,2),

    -- Initial Admission Metrics
    newborns_admitted_in_init_days  INTEGER,       -- Admitted within init_hosp_threshold_days
    pct_admitted_in_init_days       DECIMAL(5,2),

    -- Length of Stay Metrics
    total_newborn_los_days          INTEGER,
    avg_newborn_los_days            DECIMAL(10,2),
    newborns_short_stay             INTEGER,       -- LOS < 3 days
    newborns_long_stay              INTEGER,       -- LOS >= 3 days

    -- Financial Metrics
    total_newborn_paid_amt          DECIMAL(18,2),
    avg_newborn_paid_amt            DECIMAL(18,2),

    -- Readmission Metrics
    newborns_with_readmission       INTEGER,
    total_readmissions_count        INTEGER,
    total_readmission_paid_amt      DECIMAL(18,2),
    total_readmission_los_days      INTEGER,
    avg_readmission_paid_amt        DECIMAL(18,2),

    -- Discharge Date Clipping
    newborns_discharge_clipped      INTEGER,       -- OUT_END_DATE flag count

    -- ========================================
    -- NICU METRICS (DETAILED)
    -- ========================================

    -- NICU Episode Counts
    total_nicu_episodes             INTEGER,
    nicu_episodes_previous          INTEGER,
    nicu_episodes_current           INTEGER,

    -- NICU by Identification Method
    nicu_identified_by_rev          INTEGER,       -- HAS_NICU_REV
    nicu_identified_by_msdrg        INTEGER,       -- HAS_NICU_MSDRG
    nicu_identified_by_aprdrg       INTEGER,       -- HAS_NICU_APRDRG

    -- NICU by Birth Type
    nicu_single_birth               INTEGER,
    nicu_twin_birth                 INTEGER,
    nicu_multiple_birth             INTEGER,

    -- NICU by Contract Type
    nicu_drg_contract               INTEGER,
    nicu_per_diem_contract          INTEGER,
    nicu_other_contract             INTEGER,

    -- NICU Length of Stay
    total_nicu_los_days             INTEGER,
    avg_nicu_los_days               DECIMAL(10,2),
    median_nicu_los_days            INTEGER,
    max_nicu_los_days               INTEGER,
    min_nicu_los_days               INTEGER,

    -- NICU Cost Metrics - Total
    total_nicu_cost                 DECIMAL(18,2),
    avg_nicu_cost_per_episode       DECIMAL(18,2),
    avg_nicu_cost_per_day           DECIMAL(18,2),
    median_nicu_cost_per_episode    DECIMAL(18,2),
    max_nicu_cost_per_episode       DECIMAL(18,2),

    -- NICU Cost Breakdown - Professional Fees
    total_professional_fees         DECIMAL(18,2),
    avg_professional_fees           DECIMAL(18,2),
    pct_professional_fees           DECIMAL(5,2),  -- (Prof Fees / Total Cost) * 100

    total_manageable_proffee        DECIMAL(18,2),
    avg_manageable_proffee          DECIMAL(18,2),
    total_manageable_service_days   INTEGER,

    total_critical_care_proffee     DECIMAL(18,2),
    avg_critical_care_proffee       DECIMAL(18,2),
    total_critical_care_service_days INTEGER,

    -- NICU Cost Breakdown - Facility
    total_facility_cost             DECIMAL(18,2),
    avg_facility_cost               DECIMAL(18,2),
    pct_facility_cost               DECIMAL(5,2),  -- (Facility / Total Cost) * 100

    total_facility_room_cost        DECIMAL(18,2),
    avg_facility_room_cost          DECIMAL(18,2),
    pct_room_of_facility            DECIMAL(5,2),  -- (Room / Facility) * 100

    -- NICU Revenue Code Leveling
    nicu_episodes_with_rev_leveling INTEGER,       -- REV_LEVELING = TRUE
    pct_episodes_with_rev_leveling  DECIMAL(5,2),

    -- NICU by Revenue Level (primary level)
    nicu_rev_level_1                INTEGER,       -- REV 170-173
    nicu_rev_level_2                INTEGER,       -- REV 174-175
    nicu_rev_level_3                INTEGER,       -- REV 176-177
    nicu_rev_level_4                INTEGER,       -- REV 178-179
    nicu_rev_unknown                INTEGER,

    -- NICU Discharge Status
    nicu_discharge_routine          INTEGER,       -- Status 20
    nicu_discharge_transfer         INTEGER,       -- Status 02,05,66,43,62,63,65
    nicu_discharge_expired          INTEGER,       -- Status 01,06
    nicu_discharge_other            INTEGER,
    nicu_discharge_invalid          INTEGER,       -- Invalid status codes

    -- NICU Readmissions
    nicu_episodes_with_readmit      INTEGER,
    total_nicu_readmissions         INTEGER,
    total_nicu_readmit_paid_amt     DECIMAL(18,2),
    total_nicu_readmit_los_days     INTEGER,
    avg_nicu_readmit_paid_amt       DECIMAL(18,2),

    -- ========================================
    -- DATA QUALITY METRICS
    -- ========================================

    -- Claim Processing Metrics
    total_claims_processed          INTEGER,
    total_ip_claims                 INTEGER,
    total_op_claims                 INTEGER,
    claims_excluded_high_cost       INTEGER,

    -- Diagnosis Data Quality
    claims_with_null_diagnosis      INTEGER,
    pct_claims_null_diagnosis       DECIMAL(5,2),

    -- Newborn/NICU Matching
    claims_matched_newborn_icd      INTEGER,
    claims_matched_newborn_rev      INTEGER,
    claims_matched_nicu_rev         INTEGER,
    claims_matched_nicu_msdrg       INTEGER,
    claims_matched_nicu_aprdrg      INTEGER,

    -- Episode Grouping Quality
    total_hospital_episodes_created INTEGER,
    avg_claims_per_episode          DECIMAL(10,2),
    episodes_with_admit_gap         INTEGER,       -- ADMIT_GAP calculated

    -- Date Validation
    episodes_discharge_clipped      INTEGER,       -- DSCHRG > runout_end
    pct_episodes_discharge_clipped  DECIMAL(5,2),

    -- Provider Data Quality
    nicu_episodes_with_npi          INTEGER,
    nicu_episodes_with_tin          INTEGER,
    pct_nicu_episodes_with_provider DECIMAL(5,2),
    unique_nicu_providers_npi       INTEGER,
    unique_nicu_providers_tin       INTEGER,
    unique_nicu_provider_states     INTEGER,

    -- NICU Appropriateness Flags
    nicu_low_paid_cases             INTEGER,       -- Cost/day < $150
    pct_nicu_low_paid               DECIMAL(5,2),
    total_low_paid_nicu_cost        DECIMAL(18,2),

    nicu_inappropriate_cases        INTEGER,       -- DRG + LOS <= 5 days
    pct_nicu_inappropriate          DECIMAL(5,2),
    total_inappropriate_nicu_cost   DECIMAL(18,2),

    -- LOS Validation
    episodes_with_los_lt_1          INTEGER,       -- Should be 0 after filtering
    avg_los_all_episodes            DECIMAL(10,2),

    -- Membership Quality
    members_with_null_birth_date    INTEGER,
    members_with_null_gender        INTEGER,
    members_with_missing_lob        INTEGER,

    -- Study Year Distribution
    pct_previous_period             DECIMAL(5,2),  -- Previous / Total * 100
    pct_current_period              DECIMAL(5,2),  -- Current / Total * 100

    -- ========================================
    -- CLINICAL METRICS (OPTIONAL)
    -- ========================================

    -- Birth Weight Categories (if available)
    nicu_birth_weight_unknown       INTEGER,
    nicu_birth_weight_normal        INTEGER,
    nicu_birth_weight_low           INTEGER,
    nicu_birth_weight_very_low      INTEGER,
    nicu_birth_weight_extremely_low INTEGER,

    -- Gestational Age Categories (if available)
    nicu_gest_age_unknown           INTEGER,
    nicu_gest_age_term              INTEGER,
    nicu_gest_age_preterm           INTEGER,
    nicu_gest_age_very_preterm      INTEGER,
    nicu_gest_age_extremely_preterm INTEGER,

    -- ========================================
    -- METADATA & VERSIONING
    -- ========================================
    pipeline_version                VARCHAR(50),
    data_source_version             VARCHAR(50),
    sql_engine                      VARCHAR(50),   -- e.g., 'databricks', 'snowflake'
    config_hash                     VARCHAR(100),  -- Hash of config params used
    processing_duration_seconds     INTEGER,
    data_quality_score              DECIMAL(5,2),  -- Composite quality score (optional)

    notes                           TEXT,          -- Free-text notes about run
    created_by                      VARCHAR(100),

    -- ========================================
    -- CONSTRAINTS
    -- ========================================
    CONSTRAINT pk_nrs_analytics_history PRIMARY KEY (run_id, client_name),
    CONSTRAINT chk_nicu_rate CHECK (nicu_rate_pct >= 0 AND nicu_rate_pct <= 100),
    CONSTRAINT chk_percentages CHECK (
        pct_professional_fees >= 0 AND pct_professional_fees <= 100 AND
        pct_facility_cost >= 0 AND pct_facility_cost <= 100
    )
);

-- ========================================
-- INDEXES FOR QUERY PERFORMANCE
-- ========================================

CREATE INDEX idx_nrs_history_client_date
    ON nrs_analytics_history(client_name, run_date);

CREATE INDEX idx_nrs_history_run_date
    ON nrs_analytics_history(run_date);

CREATE INDEX idx_nrs_history_birth_window
    ON nrs_analytics_history(birth_window_start, birth_window_end);

-- ========================================
-- COMMENTS ON KEY FIELDS
-- ========================================

COMMENT ON COLUMN nrs_analytics_history.run_id IS
    'Unique identifier for each pipeline run (e.g., UUID or timestamp-based)';

COMMENT ON COLUMN nrs_analytics_history.birth_window_mid IS
    'Midpoint date used to separate Previous vs Current study periods';

COMMENT ON COLUMN nrs_analytics_history.nicu_rate_pct IS
    'Calculated as (total_nicu_episodes / total_newborns_identified) * 100';

COMMENT ON COLUMN nrs_analytics_history.avg_nicu_cost_per_day IS
    'Calculated as total_nicu_cost / total_nicu_los_days';

COMMENT ON COLUMN nrs_analytics_history.nicu_low_paid_cases IS
    'NICU episodes with cost per day < $150 (NICU_LOW_COST_PER_DAY_THRESHOLD)';

COMMENT ON COLUMN nrs_analytics_history.nicu_inappropriate_cases IS
    'DRG-contract NICU episodes with LOS <= 5 days (potentially inappropriate admissions)';

COMMENT ON COLUMN nrs_analytics_history.config_hash IS
    'MD5/SHA hash of configuration parameters (thresholds, windows, etc.) for reproducibility';

-- ========================================
-- SAMPLE QUERY: Trend NICU Rate Over Time
-- ========================================

-- SELECT
--     client_name,
--     run_date,
--     birth_window_start,
--     birth_window_end,
--     total_newborns_identified,
--     total_nicu_episodes,
--     nicu_rate_pct,
--     avg_nicu_cost_per_episode,
--     avg_nicu_los_days,
--     pct_nicu_inappropriate,
--     pct_nicu_low_paid
-- FROM nrs_analytics_history
-- WHERE client_name = 'CLIENT_XYZ'
-- ORDER BY run_date DESC;

-- ========================================
-- SAMPLE QUERY: Compare Periods Within Run
-- ========================================

-- SELECT
--     run_id,
--     client_name,
--     'Previous Period' as period,
--     newborns_previous_period as newborns,
--     nicu_episodes_previous as nicu_episodes,
--     CASE
--         WHEN newborns_previous_period > 0
--         THEN ROUND((nicu_episodes_previous::DECIMAL / newborns_previous_period) * 100, 2)
--         ELSE 0
--     END as nicu_rate_pct
-- FROM nrs_analytics_history
-- UNION ALL
-- SELECT
--     run_id,
--     client_name,
--     'Current Period' as period,
--     newborns_current_period as newborns,
--     nicu_episodes_current as nicu_episodes,
--     CASE
--         WHEN newborns_current_period > 0
--         THEN ROUND((nicu_episodes_current::DECIMAL / newborns_current_period) * 100, 2)
--         ELSE 0
--     END as nicu_rate_pct
-- FROM nrs_analytics_history
-- ORDER BY run_id, period;
