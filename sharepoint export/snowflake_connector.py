import logging
import os
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

logger = logging.getLogger(__name__)


class SnowflakeConnector:
    """Manages Snowflake database connections"""
    
    def __init__(self, config: dict):
        self.config = config
        self._connection = None
    
    def connect(self):
        """Establish connection to Snowflake"""
        logger.info("Connecting to Snowflake...")
        
        # Optional: Load private key for key-pair authentication
        pkey_pem = os.getenv("MY_SF_PKEY")
        pkey = None
        if pkey_pem:
            pkey = serialization.load_pem_private_key(
                pkey_pem.encode("utf-8"),
                password=None,
                backend=default_backend()
            )
        
        self._connection = snowflake.connector.connect(**self.config)
        logger.info("Successfully connected to Snowflake")
        return self._connection
    
    def get_connection(self):
        """Get active connection or create new one"""
        if self._connection is None:
            return self.connect()
        return self._connection
    
    def close(self):
        """Close Snowflake connection"""
        if self._connection:
            self._connection.close()
            logger.info("Snowflake connection closed")
            self._connection = None