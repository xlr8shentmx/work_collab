import pandas as pd
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class SharePointExtractor:
    """Handles extraction of SharePoint data"""
    
    def __init__(self, file_path: Path):
        self.file_path = file_path
    
    def extract(self) -> pd.DataFrame:
        """Load SharePoint CSV and return normalized DataFrame"""
        logger.info(f"Loading SharePoint export from {self.file_path}...")

        try:
            # Use low_memory=False to prevent mixed type warnings
            # This reads the entire file at once for better type inference
            df = pd.read_csv(self.file_path, low_memory=False)
        except FileNotFoundError:
            logger.error(f"SharePoint file not found at {self.file_path}")
            raise

        # Normalize column names
        df.columns = df.columns.str.upper()

        logger.info(f"Loaded {len(df)} SharePoint records")
        return df