"""
NRS - NICU Analytics Pipeline

A modular, configurable pipeline for processing NICU (Neonatal Intensive Care Unit)
analytics from medical claims data.
"""

__version__ = "1.0.0"
__author__ = "Analytics Team"

# Main pipeline execution
from .pipeline import run_nicu_pipeline, main

# Configuration
from .config import (
    SNOWFLAKE_CONFIG,
    TABLE_CONFIG,
    REFERENCE_TABLE_CONFIG,
    BUSINESS_RULES,
    CPT_CONFIG,
    REV_CODE_CONFIG,
    DRG_CONFIG
)

# Data sources
from .data_sources import get_snowflake_session, export_to_snowflake, DataSourceManager

# Reference data management
from .reference_manager import ReferenceDataManager

# Utilities
from .utils import setup_logging, calculate_birth_window, Timer

__all__ = [
    # Main pipeline
    "run_nicu_pipeline",
    "main",

    # Configuration
    "SNOWFLAKE_CONFIG",
    "TABLE_CONFIG",
    "REFERENCE_TABLE_CONFIG",
    "BUSINESS_RULES",
    "CPT_CONFIG",
    "REV_CODE_CONFIG",
    "DRG_CONFIG",

    # Data sources
    "get_snowflake_session",
    "export_to_snowflake",
    "DataSourceManager",

    # Reference data
    "ReferenceDataManager",

    # Utilities
    "setup_logging",
    "calculate_birth_window",
    "Timer",
]
