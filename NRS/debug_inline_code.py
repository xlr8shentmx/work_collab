"""
Inline debugging code snippets to add to pipeline.py

Add these logging statements at key points to trace where records are lost.
"""

# ============================================================================
# Add after Step 9: Reference Flag Tagging (pipeline.py line ~129)
# ============================================================================
"""
# Step 9: Tag with reference flags
with Timer("Reference Flag Tagging"):
    claims_df = tag_all_reference_flags(session, claims_df, ref_manager).cache_result()

    # DEBUG: Check tagging results
    total_claims = claims_df.count()
    logger.info(f"[DEBUG] Total claims after tagging: {total_claims}")

    newborn_icd_count = claims_df.filter(col("NEWBORN_ICD")).count()
    newborn_rev_count = claims_df.filter(col("NEWBORN_REV")).count()
    newborn_total = claims_df.filter(col("NEWBORN_ICD") | col("NEWBORN_REV")).count()

    logger.info(f"[DEBUG] Claims with NEWBORN_ICD: {newborn_icd_count} ({100*newborn_icd_count/total_claims:.2f}%)")
    logger.info(f"[DEBUG] Claims with NEWBORN_REV: {newborn_rev_count} ({100*newborn_rev_count/total_claims:.2f}%)")
    logger.info(f"[DEBUG] Claims with NEWBORN_ICD OR NEWBORN_REV: {newborn_total} ({100*newborn_total/total_claims:.2f}%)")

    nicu_rev_count = claims_df.filter(col("NICU_REV")).count()
    nicu_msdrg_count = claims_df.filter(col("NICU_MSDRG")).count()
    nicu_aprdrg_count = claims_df.filter(col("NICU_APRDRG")).count()
    nicu_any = claims_df.filter(col("NICU_REV") | col("NICU_MSDRG") | col("NICU_APRDRG")).count()

    logger.info(f"[DEBUG] Claims with NICU_REV: {nicu_rev_count} ({100*nicu_rev_count/total_claims:.2f}%)")
    logger.info(f"[DEBUG] Claims with NICU_MSDRG: {nicu_msdrg_count} ({100*nicu_msdrg_count/total_claims:.2f}%)")
    logger.info(f"[DEBUG] Claims with NICU_APRDRG: {nicu_aprdrg_count} ({100*nicu_aprdrg_count/total_claims:.2f}%)")
    logger.info(f"[DEBUG] Claims with any NICU flag: {nicu_any} ({100*nicu_any/total_claims:.2f}%)")

    # Check overlap: newborn + NICU
    newborn_and_nicu = claims_df.filter(
        (col("NEWBORN_ICD") | col("NEWBORN_REV")) &
        (col("NICU_REV") | col("NICU_MSDRG") | col("NICU_APRDRG"))
    ).count()
    logger.info(f"[DEBUG] Claims with BOTH newborn AND NICU flags: {newborn_and_nicu}")

    if newborn_total == 0:
        logger.warning("[WARNING] No claims tagged as newborns! Check:")
        logger.warning("  1. Reference tables SUPP_DATA.REF_NEWBORN_ICD and REF_NEWBORN_REVCODE exist")
        logger.warning("  2. Claims have DIAG and REV_CD columns populated")
        logger.warning("  3. Code formats match (ICD-10 format, revenue code format)")
"""

# ============================================================================
# Add after Step 11: Newborn Rollup (pipeline.py line ~146)
# ============================================================================
"""
# Step 11: Apply newborn rollup logic
with Timer("Newborn Rollup"):
    newborns_df, claims_df = newborn_rollup(session, client_data, claims_df)

    # DEBUG: Check newborn identification
    newborn_count = newborns_df.count()
    logger.info(f"[DEBUG] Unique newborns identified: {newborn_count}")

    if newborn_count > 0:
        nicu_babies = newborns_df.filter(col("BABY_TYPE") == "NICU").count()
        normal_babies = newborns_df.filter(col("BABY_TYPE") == "Normal Newborn").count()

        logger.info(f"[DEBUG] NICU babies: {nicu_babies} ({100*nicu_babies/newborn_count:.2f}%)")
        logger.info(f"[DEBUG] Normal newborns: {normal_babies} ({100*normal_babies/newborn_count:.2f}%)")

        # Show sample
        sample = newborns_df.select("INDV_ID", "BTH_DT", "DELIVERY_DT", "BABY_TYPE",
                                     "HAS_NICU_REV", "HAS_NICU_MSDRG", "HAS_NICU_APRDRG").limit(10).show()
        logger.info(f"[DEBUG] Sample newborns:\\n{sample}")
    else:
        logger.warning("[WARNING] No newborns identified! This will result in empty nicu_ident.")
        logger.warning("  Check birth date filter and study period alignment.")
"""

# ============================================================================
# Add after Step 13: NICU Identification (pipeline.py line ~159)
# ============================================================================
"""
# Step 13: Build NICU identification and artifact tables
with Timer("NICU Identification"):
    ids = build_newborn_and_nicu_ids(
        claims_df,
        hosp_rollup_df,
        birth_window_start.date(),
        birth_window_mid.date()
    )

    newborn_hosp_clms = ids["newborn_hosp_clms"]
    newborn_ident_df = ids["newborn_ident_df"]
    nicu_ident = ids["nicu_ident"]
    nicu_claims_df = ids["nicu_claims_df"]
    nicu_dischg_provider = ids["nicu_dischg_provider"]
    rev_out = ids["rev_out"]
    drg_out = ids["drg_out"]

    # DEBUG: Check NICU identification results
    logger.info(f"[DEBUG] newborn_ident_df count: {newborn_ident_df.count()}")
    logger.info(f"[DEBUG] nicu_ident count: {nicu_ident.count()}")
    logger.info(f"[DEBUG] nicu_claims_df count: {nicu_claims_df.count()}")

    if nicu_ident.count() == 0:
        logger.error("[ERROR] nicu_ident is EMPTY!")
        logger.error("  This means no babies were classified as BABY_TYPE='NICU'")

        # Check if we have any newborns at all
        newborn_count = newborn_ident_df.count()
        if newborn_count == 0:
            logger.error("  Root cause: newborn_ident_df is also empty")
            logger.error("  Check hospital rollup and claim filtering logic")
        else:
            logger.error(f"  We have {newborn_count} newborns, but none classified as NICU")
            logger.error("  Check BABY_TYPE distribution:")
            baby_type_dist = newborn_ident_df.group_by("BABY_TYPE").count().collect()
            for row in baby_type_dist:
                logger.error(f"    {row['BABY_TYPE']}: {row['COUNT']}")
    else:
        logger.info(f"[DEBUG] ✓ NICU identification successful: {nicu_ident.count()} NICU babies")
        # Show sample
        nicu_ident.select("INDV_ID", "BTH_DT", "ADMIT", "DSCHRG", "LOS", "TOTAL_NICU_COST").limit(5).show()
"""

# ============================================================================
# Alternative: Add to nicu_analytics.py in build_newborn_and_nicu_ids function
# Add after line 355 (before line 357 where nicu_ident is created)
# ============================================================================
"""
    # DEBUG: Check BABY_TYPE distribution before filtering
    logger.info("[DEBUG] newborn_ident_df distribution:")
    logger.info(f"  Total newborns: {newborn_ident_df.count()}")

    baby_type_counts = (
        newborn_ident_df
        .group_by("BABY_TYPE")
        .agg(
            count("*").alias("COUNT"),
            ssum("AMTPAID").alias("TOTAL_COST")
        )
        .collect()
    )

    for row in baby_type_counts:
        logger.info(f"  {row['BABY_TYPE']}: {row['COUNT']} babies, ${row['TOTAL_COST']:,.2f}")

    # NICU subset
    nicu_ident = (
        newborn_ident_df
        .filter(col("BABY_TYPE") == lit("NICU"))
        .with_column_renamed("AMTPAID", "TOTAL_NICU_COST")
    )

    logger.info(f"[DEBUG] nicu_ident after filter: {nicu_ident.count()} records")
"""
