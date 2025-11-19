# NRS Usage Guide

## Quick Fix for "no module named NRS" Error

If you're getting a "no module named NRS" error, use one of these solutions:

### Solution 1: Use the Jupyter Notebook (Recommended)

The `nrs_pipeline_runner.ipynb` notebook automatically handles the import path. Simply:

1. Open `NRS/nrs_pipeline_runner.ipynb`
2. Run the cells - the path is automatically configured

### Solution 2: Add to Python Path Manually

In your Python script or notebook, add these lines **before** importing NRS:

```python
import sys
import os

# Add the parent directory containing NRS to Python path
parent_dir = os.path.abspath(os.path.join(os.getcwd(), '..'))
sys.path.insert(0, parent_dir)

# Now you can import NRS
from NRS import run_nicu_pipeline
```

### Solution 3: Set PYTHONPATH Environment Variable

Before running your script:

```bash
export PYTHONPATH="/home/user/work_collab:$PYTHONPATH"
python your_script.py
```

### Solution 4: Run from the Correct Directory

Make sure you're running your script from the `/home/user/work_collab` directory:

```bash
cd /home/user/work_collab
python -m NRS.pipeline
```

## Basic Usage

### Option 1: Run Complete Pipeline

```python
from NRS import run_nicu_pipeline

run_nicu_pipeline(
    client_data='EMBLEM',  # Your client identifier
    auto_window=True       # Auto-calculate date ranges
)
```

### Option 2: Run from Command Line

```bash
cd /home/user/work_collab
python -m NRS.pipeline
```

### Option 3: Custom Date Ranges

```python
from NRS import run_nicu_pipeline
from datetime import datetime

run_nicu_pipeline(
    client_data='EMBLEM',
    auto_window=False,
    birth_window_start=datetime(2023, 1, 1),
    birth_window_end=datetime(2024, 12, 31),
    runout_end=datetime(2025, 3, 31)
)
```

### Option 4: Step-by-Step Execution

```python
from NRS import get_snowflake_session, ReferenceDataManager
from NRS.membership import process_membership
from NRS.utils import calculate_birth_window
from NRS.config import TABLE_CONFIG

# Initialize
session = get_snowflake_session()
ref_manager = ReferenceDataManager(session)

# Calculate windows
birth_start, birth_end, birth_mid, runout = calculate_birth_window(
    session, 'EMBLEM', TABLE_CONFIG
)

# Run individual steps
membership_df = process_membership(
    session, 'EMBLEM', birth_start, birth_mid, birth_end, 'EMBLEM', TABLE_CONFIG
)

# Continue with other steps...
```

## Configuration

### Change Business Rules

```python
from NRS import BUSINESS_RULES

# Modify threshold before running pipeline
BUSINESS_RULES.readmit_threshold_days = 45  # Change from 30 to 45
BUSINESS_RULES.high_cost_threshold = 600000  # Change threshold

# Now run pipeline
from NRS import run_nicu_pipeline
run_nicu_pipeline(client_data='EMBLEM')
```

### Change Snowflake Settings

```python
from NRS import get_snowflake_session

session = get_snowflake_session(
    warehouse="MY_CUSTOM_WAREHOUSE",
    database="MY_DATABASE"
)
```

## Environment Variables Required

Before running the pipeline, ensure these environment variables are set:

```bash
export MY_SF_USER="your_snowflake_username"
export MY_SF_PKEY="$(cat /path/to/your/private_key.pem)"
```

Or in Python:

```python
import os

os.environ['MY_SF_USER'] = 'your_username'
os.environ['MY_SF_PKEY'] = open('/path/to/private_key.pem').read()
```

## Output Tables

The pipeline creates these tables in Snowflake:

1. **PS_MEMBERSHIP_{CLIENT}_TST**: Member demographics and coverage
2. **PS_NEWBORNS_{CLIENT}_TST**: Comprehensive newborn and NICU analytics

## Troubleshooting

### "Cannot connect to Snowflake"
- Check that `MY_SF_USER` and `MY_SF_PKEY` environment variables are set
- Verify your private key is valid
- Confirm you have the necessary permissions

### "Table not found"
- Ensure source tables exist: `FA_MEMBERSHIP_{CLIENT}` and `FA_MEDICAL_{CLIENT}`
- Check that reference tables exist in `SUPP_DATA` schema

### "No newborn keys found"
- This may indicate no qualifying claims in the date range
- Check that reference tables contain appropriate codes

### Performance Issues
- The pipeline caches intermediate results with `.cache_result()`
- Large datasets may take 10-30 minutes to complete
- Monitor logs for timing information on each stage

## Advanced Examples

### Custom Reference Tables

```python
from NRS import ReferenceDataManager

# Load specific reference data
ref_manager = ReferenceDataManager(session)
newborn_icds = ref_manager.get_newborn_icd()
nicu_revcodes = ref_manager.get_nicu_revcode()
```

### Access Individual Components

```python
from NRS.claims_processing import fetch_newborn_keys, load_newborn_claims
from NRS.tagging import tag_all_reference_flags
from NRS.nicu_analytics import newborn_rollup, build_nicu_rollup

# Use any component independently
```

## Getting Help

- See `README.md` for complete documentation
- Check module docstrings: `help(run_nicu_pipeline)`
- Review `nrs_pipeline_runner.ipynb` for examples
