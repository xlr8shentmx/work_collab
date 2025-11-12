# SharePoint Export Pipeline

ETL pipeline for processing SharePoint analytic requests and loading them into Snowflake.

## Overview

This pipeline:
1. **Extracts** data from SharePoint CSV exports and Salesforce Excel files
2. **Transforms** data by cleaning, exploding products into separate records, and calculating metrics
3. **Loads** data into Snowflake with incremental or full refresh capabilities

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         EXTRACTION                              │
├─────────────────────────────────────────────────────────────────┤
│  sharepoint_extractor.py  →  Load SharePoint CSV               │
│  salesforce_extractor.py  →  Load Salesforce Excel             │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                       TRANSFORMATION                            │
├─────────────────────────────────────────────────────────────────┤
│  data_cleaner.py         →  Clean and normalize data           │
│  product_transformer.py  →  Explode products & calculate        │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                          LOADING                                │
├─────────────────────────────────────────────────────────────────┤
│  snowflake_connector.py  →  Manage Snowflake connections       │
│  snowflake_loader.py     →  Load data with MERGE operations    │
└─────────────────────────────────────────────────────────────────┘
```

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Environment Variables

Set the following environment variables:

```bash
export SF_USERNAME="your_snowflake_username"
export SF_PW="your_snowflake_password"

# Optional: For key-pair authentication
export MY_SF_PKEY="your_private_key_pem"
```

### 3. Configure File Paths

Edit `settings.py` to update the file paths for your environment:

```python
SHAREPOINT_EXPORT_PATH = Path("path/to/sharepoint_requests.csv")
SALESFORCE_EXPORT_PATH = Path("path/to/salesforce_exports.xlsx")
```

## Usage

### Basic Usage (Incremental Load)

```bash
python sp_export_pipeline.py
```

This performs an incremental load using MERGE operations to update existing records and insert new ones.

### Full Refresh

```bash
python sp_export_pipeline.py --full-refresh
```

Truncates the target tables and performs a complete reload.

### Dry Run

```bash
python sp_export_pipeline.py --dry-run
```

Runs the entire pipeline without uploading to Snowflake (useful for testing).

### Skip Raw Data Load

```bash
python sp_export_pipeline.py --skip-raw
```

Only loads the transformed data (skips raw SharePoint and Salesforce tables).

### Combined Options

```bash
python sp_export_pipeline.py --full-refresh --dry-run
```

## Pipeline Components

### Data Sources

- **SharePoint CSV**: Contains analytic request data with multiple products per request
- **Salesforce Excel**: Contains initiative data for enrichment (has_value field)

### Output Tables

| Table | Description |
|-------|-------------|
| `SHAREPOINT_ANALYTIC_REQUESTS` | Raw SharePoint data (one row per request) |
| `SALESFORCE_INITIATIVES` | Raw Salesforce data |
| `FOCUSED_ANALYTIC_REQUESTS` | Transformed product-level data (one row per product per request) |

### Key Features

#### Data Cleaning
- Maps client type codes to descriptions
- Detects "Spine Pain & Joint" products from text
- Normalizes boolean columns
- Populates product lists from boolean flags

#### Product Transformation
- Explodes 48 different products from wide format into individual records
- Enriches with Salesforce data
- Calculates metrics:
  - Days open
  - Product turnaround time (TAT)
  - Days on current status
  - Needs attention flag (open > 14 days)
  - Request type and year
  - SharePoint URLs

#### Incremental Loading
- Uses staging tables and MERGE operations
- Updates existing records based on ID (raw) or ID + PRODUCT (transformed)
- Inserts new records
- Maintains data consistency

## File Structure

```
sharepoint export/
├── sp_export_pipeline.py      # Main orchestration script
├── settings.py                 # Configuration and constants
├── sharepoint_extractor.py     # SharePoint CSV extraction
├── salesforce_extractor.py     # Salesforce Excel extraction
├── data_cleaner.py             # Data cleaning logic
├── product_transformer.py      # Product transformation logic
├── product_mappings.py         # Product configuration mappings
├── snowflake_connector.py      # Snowflake connection management
├── snowflake_loader.py         # Snowflake data loading
├── requirements.txt            # Python dependencies
└── README.md                   # This file
```

## Logging

The pipeline creates detailed logs:
- Console output (stdout)
- Log file: `pipeline_YYYYMMDD_HHMMSS.log`

Logs include:
- Extraction record counts
- Transformation statistics
- Data quality warnings
- Load operations and row counts
- Error details with stack traces

## Product Categories

The pipeline processes 48 different products across categories:
- Focused Analytics
- Cancer (CGP, CSP)
- Kidney & Transplant
- Behavioral Health
- Women's Health (Maternity, Fertility, NICU)
- Disease Management
- Musculoskeletal (MSK)
- Orthopedic Health Support
- OptumRx Programs
- And more...

See `product_mappings.py` for the complete list.

## Error Handling

- Missing SharePoint file: Pipeline fails with error
- Missing Salesforce file: Pipeline continues with warning (no enrichment)
- Database errors: Full stack trace logged, graceful exit
- File parsing errors: Logged with details

## Snowflake Configuration

Default configuration in `settings.py`:
- Account: `uhgdwaas.east-us-2.azure`
- Database: `CSZNB_PRD_OA_DEV_DB`
- Schema: `BASE`
- Warehouse: `CSZNB_PRD_ANALYTICS_XS_WH`
- Role: `AZU_SDRP_CSZNB_PRD_DEVELOPER_ROLE`

Modify these in `settings.py` as needed.

## Troubleshooting

### Import Errors
Ensure all dependencies are installed:
```bash
pip install -r requirements.txt
```

### File Not Found
Check that the file paths in `settings.py` are correct and files exist.

### Authentication Errors
Verify environment variables are set:
```bash
echo $SF_USERNAME
echo $SF_PW
```

### Network/Connection Issues
Check Snowflake connectivity and firewall rules.

## Scheduling

To run this pipeline on a schedule, use:

### Cron (Linux/Mac)
```bash
# Run daily at 6 AM
0 6 * * * cd /path/to/sharepoint\ export && /usr/bin/python3 sp_export_pipeline.py
```

### Task Scheduler (Windows)
Create a scheduled task to run:
```
python "C:\path\to\sharepoint export\sp_export_pipeline.py"
```

### Airflow DAG
```python
from airflow import DAG
from airflow.operators.bash import BashOperator

dag = DAG('sharepoint_export', schedule_interval='@daily')

run_pipeline = BashOperator(
    task_id='run_pipeline',
    bash_command='cd /path/to/sharepoint\ export && python sp_export_pipeline.py',
    dag=dag
)
```

## Support

For issues or questions, contact the data engineering team.
