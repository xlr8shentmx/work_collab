# NRS - NICU Analytics Pipeline

A modular, production-ready Python pipeline for processing and analyzing Neonatal Intensive Care Unit (NICU) medical claims data using Snowflake Snowpark.

## Overview

The NRS pipeline processes medical claims data to identify newborns and NICU cases, calculate costs, track readmissions, and generate comprehensive analytics for reporting. It replaces the monolithic notebook approach with a well-structured, maintainable codebase.

## Features

- **Modular Architecture**: Separated concerns with dedicated modules for each pipeline stage
- **Configurable**: Centralized configuration management for easy customization
- **Reference Data Management**: Efficient loading and caching of reference tables
- **Comprehensive Analytics**: Full NICU identification, cost analysis, and outcome tracking
- **Performance Optimized**: Strategic caching and parallel processing where applicable
- **Production Ready**: Logging, error handling, and timing instrumentation

## Project Structure

```
NRS/
├── __init__.py                  # Package initialization
├── config.py                    # Configuration and constants
├── utils.py                     # Helper utilities
├── data_sources.py              # Snowflake connection and data loading
├── reference_manager.py         # Reference data management
├── membership.py                # Membership processing
├── claims_processing.py         # Claims loading and type assignment
├── tagging.py                   # Reference flag tagging
├── nicu_analytics.py            # NICU-specific analytics and rollups
├── pipeline.py                  # Main pipeline orchestration
├── requirements.txt             # Python dependencies
├── nrs_pipeline_runner.ipynb    # Simplified notebook interface
└── README.md                    # This file
```

## Installation

### Prerequisites

- Python 3.8+
- Access to Snowflake with appropriate credentials
- Private key authentication set up for Snowflake

### Setup

1. **Clone or download the repository**

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Set environment variables**:
   ```bash
   export MY_SF_USER="your_snowflake_username"
   export MY_SF_PKEY="$(cat path/to/your/private_key.pem)"
   ```

## Usage

### Option 1: Run from Python

```python
from NRS import run_nicu_pipeline

# Run with automatic birth window calculation
run_nicu_pipeline(
    client_data='EMBLEM',
    auto_window=True
)
```

### Option 2: Run from Command Line

```bash
python -m NRS.pipeline
```

### Option 3: Run from Jupyter Notebook

Open and execute `nrs_pipeline_runner.ipynb` for an interactive experience.

### Option 4: Custom Execution

```python
from NRS import get_snowflake_session, ReferenceDataManager
from NRS.membership import process_membership
from NRS.utils import calculate_birth_window

# Initialize
session = get_snowflake_session()
ref_manager = ReferenceDataManager(session)

# Run individual components as needed
# ... custom workflow ...
```

## Configuration

All configuration is centralized in `config.py`:

### Snowflake Configuration
```python
from NRS import SNOWFLAKE_CONFIG

SNOWFLAKE_CONFIG.warehouse = "MY_WAREHOUSE"  # Change warehouse
```

### Business Rules
```python
from NRS import BUSINESS_RULES

BUSINESS_RULES.readmit_threshold_days = 30
BUSINESS_RULES.high_cost_threshold = 500000
```

### Table Configuration
```python
from NRS import TABLE_CONFIG

TABLE_CONFIG.database = "MY_DATABASE"
TABLE_CONFIG.stage_schema = "MY_SCHEMA"
```

## Pipeline Stages

The pipeline executes the following stages in order:

1. **Session Initialization**: Establish Snowflake connection and load reference data
2. **Birth Window Calculation**: Auto-calculate or use provided date ranges
3. **Membership Processing**: Process member demographics and coverage
4. **Eligibility Creation**: Create eligibility snapshot for claims merge
5. **Newborn Identification**: Identify newborns via ICD/REV/DRG codes
6. **Claims Loading**: Load claims for identified newborns
7. **Eligibility Merge**: Attach member demographics to claims
8. **Claim Type Assignment**: Classify claims as IP/ER/OP
9. **Reference Tagging**: Tag claims with newborn/NICU flags
10. **Newborn Rollup**: Apply birth type hierarchy and NICU identification
11. **Hospital Stay Rollup**: Stitch claims into continuous stays
12. **NICU Identification**: Build episode-level NICU artifacts
13. **NICU Rollup**: Calculate comprehensive NICU metrics
14. **Final Export**: Merge and export to Snowflake tables

## Output Tables

The pipeline produces two main output tables:

1. **PS_MEMBERSHIP_{CLIENT}_TST**: Member demographics and enrollment
2. **PS_NEWBORNS_{CLIENT}_TST**: Newborn and NICU analytics

## Key Metrics Calculated

- Birth type classification (Single/Twin/Multiple)
- NICU vs. Normal Newborn identification
- Contract type (DRG vs. Per-Diem)
- Hospital length of stay (LOS)
- Professional fees (all, manageable, critical care)
- Facility costs (room & board, total)
- Readmissions within 30 days
- Discharge status and provider attribution
- Revenue code and DRG features
- Birthweight and gestational age categories
- NAS (Neonatal Abstinence Syndrome) flags

## Performance Considerations

- Reference tables are preloaded and cached for efficiency
- Strategic use of `.cache_result()` on large intermediate DataFrames
- Column selection reduces data volume before heavy operations
- Timing instrumentation helps identify bottlenecks

## Logging

The pipeline uses Python's standard logging module:

```python
from NRS import run_nicu_pipeline
import logging

run_nicu_pipeline(
    client_data='EMBLEM',
    log_level=logging.DEBUG  # For detailed logging
)
```

## Error Handling

The pipeline includes comprehensive error handling and will log errors with full context before raising exceptions.

## Development

### Adding New Features

1. **New configuration**: Add to appropriate config class in `config.py`
2. **New reference table**: Add method to `ReferenceDataManager`
3. **New analytics**: Add to appropriate module (`membership.py`, `claims_processing.py`, etc.)
4. **New pipeline stage**: Add to `pipeline.py` and update `run_nicu_pipeline()`

### Testing

Individual modules can be imported and tested independently:

```python
from NRS.membership import process_membership
from NRS.utils import calculate_birth_window

# Test individual functions
```

## Maintenance

### Updating Business Rules

Edit values in `config.py`:

```python
BUSINESS_RULES.init_hosp_threshold_days = 5  # Change from 4 to 5
```

### Updating Reference Tables

Reference table names are in `ReferenceTableConfig`:

```python
REFERENCE_TABLE_CONFIG.newborn_icd = "REF_NEWBORN_ICD_V2"
```

### Updating Column Mappings

If source column names change, update mappings in `config.py`:

```python
MEDICAL_COLUMN_MAP["DRG"] = "NEW_DRG_FIELD"
```

## Comparison with Original Notebook

| Aspect | Original Notebook | NRS Package |
|--------|------------------|-------------|
| Lines of Code | ~1,230 in single cell | ~2,500 across 11 modules |
| Maintainability | Difficult | Easy |
| Testability | Hard to test | Each module testable |
| Reusability | Copy/paste only | Import and use |
| Configuration | Hardcoded values | Centralized config |
| Error Handling | Basic | Comprehensive |
| Logging | Print statements | Structured logging |
| Performance Monitoring | None | Built-in timing |
| Documentation | Inline comments | Docstrings + README |

## License

Internal use only.

## Support

For questions or issues, please contact the Analytics Team.
