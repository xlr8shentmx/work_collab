import pandas as pd
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class SalesforceExtractor:
    """Handles extraction of Salesforce data from Excel"""

    def __init__(self, file_path: Path):
        self.file_path = file_path

    def extract(self) -> pd.DataFrame:
        """Load Salesforce Excel export and return normalized DataFrame"""
        logger.info(f"Loading Salesforce export from {self.file_path}...")

        try:
            # Read Excel file (assuming first sheet)
            df = pd.read_excel(self.file_path, sheet_name=0)
        except FileNotFoundError:
            logger.warning(f"Salesforce file not found at {self.file_path}")
            # Return empty DataFrame with expected columns
            return pd.DataFrame(columns=['SALESFORCE_ID', 'HAS_VALUE'])
        except Exception as e:
            logger.error(f"Error reading Salesforce file: {e}")
            return pd.DataFrame(columns=['SALESFORCE_ID', 'HAS_VALUE'])

        # Normalize column names
        df.columns = df.columns.str.upper()

        logger.info(f"Loaded {len(df)} Salesforce records")
        return df
