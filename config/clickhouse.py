import clickhouse_connect
from clickhouse_connect.driver import Client
import pandas as pd
import logging
from typing import Dict, List, Any, Optional
from core.config import settings

logger = logging.getLogger(__name__)

class ClickHouseClient:
    """ClickHouse Cloud client for log data operations"""
    
    def __init__(self):
        self.client: Optional[Client] = None
    
    async def connect(self):
        """Initialize connection to ClickHouse Cloud"""
        try:
            # Prepare connection settings for ClickHouse Cloud
            connection_settings = {
                'host': settings.clickhouse_host,
                'port': settings.clickhouse_port,
                'database': settings.clickhouse_database,
                'username': settings.clickhouse_username,
                'password': settings.clickhouse_password,
                'secure': settings.clickhouse_secure,
                'connect_timeout': settings.clickhouse_connect_timeout,
                'send_receive_timeout': settings.clickhouse_send_receive_timeout,
            }
            
            # Add SSL settings for ClickHouse Cloud
            if settings.clickhouse_secure:
                if not settings.clickhouse_verify_ssl:
                    # Disable SSL verification (not recommended for production)
                    connection_settings['verify'] = False
                    logger.warning("SSL certificate verification is disabled for ClickHouse Cloud connection")
                
                if settings.clickhouse_ca_cert:
                    connection_settings['ca_cert'] = settings.clickhouse_ca_cert
            
            # Try different connection approaches for ClickHouse Cloud
            try:
                # Method 1: Standard connection
                self.client = clickhouse_connect.get_client(**connection_settings)
            except Exception as e:
                logger.warning(f"Standard connection failed: {e}")
                
                # Method 2: Try with explicit port 9440 (ClickHouse Cloud native port)
                if settings.clickhouse_port == 8443:
                    logger.info("Trying connection with port 9440...")
                    connection_settings['port'] = 9440
                    self.client = clickhouse_connect.get_client(**connection_settings)
                else:
                    raise e
            
            # Test connection
            test_result = self.client.query('SELECT version()')
            logger.info(f"Connected to ClickHouse Cloud successfully. Version: {test_result.result_rows[0][0]}")
            logger.info(f"Connected to: {settings.clickhouse_host}:{connection_settings.get('port', settings.clickhouse_port)}")
            
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse Cloud: {e}")
            logger.error("Troubleshooting tips:")
            logger.error("1. Check your ClickHouse Cloud host URL")
            logger.error("2. Verify your username and password")
            logger.error("3. Try setting CLICKHOUSE_VERIFY_SSL=false")
            logger.error("4. Check if your IP is whitelisted in ClickHouse Cloud")
            raise
    
    async def disconnect(self):
        """Close connection"""
        try:
            if self.client:
                self.client.close()
            logger.info("Disconnected from ClickHouse Cloud")
        except Exception as e:
            logger.error(f"Error disconnecting from ClickHouse Cloud: {e}")
    
    async def fetch_log_data_by_integration_id(self, integration_id: str, last_id: str = None) -> list:
        """
        Fetch log data from ClickHouse by integration_id.
        If last_id is provided, only fetch rows after that id.
        Returns raw list of rows instead of DataFrame for speed.
        """
        try:
            if self.client is None:
                logger.info("ClickHouse client not connected. Connecting now...")
                await self.connect()

            # Base query
            query = """
            SELECT id, raw_data
            FROM queryinside.logs
            WHERE integration_id = {integration_id:String}
            {last_id_filter:sql}
            ORDER BY timestamp ASC
            LIMIT 100000
            """

            # Prepare filter only if last_id is provided
            last_id_filter = ""
            params = {"integration_id": integration_id}

            if last_id:
                last_id_filter = "AND id > {last_id:String}"
                params["last_id"] = last_id

            # Inject filter safely
            query = query.format(last_id_filter=last_id_filter)

            # Fetch as list of dicts (faster & lighter than DataFrame)
            result = self.client.query(query, params)
            return [row for row in result.result_rows]

        except Exception as e:
            logger.error(f"Failed to fetch log data for integration_id {integration_id}: {e}")
            return []

