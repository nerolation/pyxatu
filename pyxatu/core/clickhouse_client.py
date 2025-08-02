"""ClickHouse client for PyXatu."""

import asyncio
import logging
from typing import Any, Dict, List, Optional, Union
from datetime import datetime
import pandas as pd
from io import StringIO

import aiohttp
from aiohttp import BasicAuth, ClientTimeout
import backoff

from pyxatu.core.base import BaseClient
from pyxatu.config import ClickhouseConfig
from pyxatu.schema import get_schema_manager


class ClickHouseQueryBuilder:
    """SQL query builder for ClickHouse."""
    
    ALLOWED_TABLES = {
        'beacon_api_eth_v1_events_block',
        'canonical_beacon_elaborated_attestation',
        'beacon_api_eth_v1_events_attestation',
        'canonical_beacon_proposer_duty',
        'beacon_api_eth_v1_events_chain_reorg',
        'canonical_beacon_block',
        'beacon_api_eth_v1_beacon_committee',
        'beacon_api_eth_v2_beacon_block',
        'canonical_beacon_block_withdrawal',
        'beacon_api_eth_v1_events_blob_sidecar',
        'canonical_beacon_blob_sidecar',
        'canonical_beacon_block_execution_transaction',
        'canonical_execution_transaction',
        'mempool_transaction',
    }
    
    def __init__(self):
        self.reset()
        
    def reset(self) -> 'ClickHouseQueryBuilder':
        """Reset the builder to initial state."""
        self._select_columns: List[str] = []
        self._from_table: Optional[str] = None
        self._where_conditions: List[str] = []
        self._parameters: Dict[str, Any] = {}
        self._group_by: List[str] = []
        self._order_by: List[tuple[str, bool]] = []
        self._limit_count: Optional[int] = None
        self._use_final: bool = False
        self._param_counter = 0
        return self
        
    def select(self, columns: Union[str, List[str]]) -> 'ClickHouseQueryBuilder':
        """Add SELECT columns."""
        if isinstance(columns, str):
            if columns == "*":
                self._select_columns = ["*"]
            else:
                # Parse comma-separated columns
                self._select_columns = [col.strip() for col in columns.split(",")]
        else:
            self._select_columns = columns
        return self
        
    def from_table(self, table: str, use_final: bool = True) -> 'ClickHouseQueryBuilder':
        """Set FROM table with validation."""
        if table not in self.ALLOWED_TABLES:
            raise ValueError(f"Table '{table}' is not allowed")
        self._from_table = table
        self._use_final = use_final
        return self
        
    def where(self, column: str, operator: str, value: Any) -> 'ClickHouseQueryBuilder':
        """Add WHERE condition with parameter binding."""
        allowed_operators = ['=', '!=', '<', '>', '<=', '>=', 'IN', 'NOT IN', 'LIKE']
        if operator.upper() not in allowed_operators:
            raise ValueError(f"Operator '{operator}' is not allowed")
            
        param_name = f"param_{self._param_counter}"
        self._param_counter += 1
        
        if operator.upper() in ['IN', 'NOT IN']:
            if not isinstance(value, (list, tuple)):
                raise ValueError(f"Value for {operator} must be a list or tuple")
            placeholder = f"({', '.join(['%(' + param_name + f'_{i})s' for i in range(len(value))])})"
            for i, v in enumerate(value):
                # Add quotes around string values and escape single quotes
                if isinstance(v, str):
                    escaped_v = v.replace("'", "\\'")
                    self._parameters[f"{param_name}_{i}"] = f"'{escaped_v}'"
                else:
                    self._parameters[f"{param_name}_{i}"] = v
            condition = f"{column} {operator} {placeholder}"
        else:
            # Add quotes around string values and escape single quotes
            if isinstance(value, str):
                escaped_value = value.replace("'", "\\'")
                self._parameters[param_name] = f"'{escaped_value}'"
            else:
                self._parameters[param_name] = value
            condition = f"{column} {operator} %({param_name})s"
            
        self._where_conditions.append(condition)
        return self
        
    def where_between(self, column: str, start: Any, end: Any) -> 'ClickHouseQueryBuilder':
        """Add BETWEEN condition."""
        param_start = f"param_{self._param_counter}"
        param_end = f"param_{self._param_counter + 1}"
        self._param_counter += 2
        
        # Add quotes around string values and escape single quotes
        if isinstance(start, str):
            escaped_start = start.replace("'", "\\'")
            self._parameters[param_start] = f"'{escaped_start}'"
        else:
            self._parameters[param_start] = start
            
        if isinstance(end, str):
            escaped_end = end.replace("'", "\\'")
            self._parameters[param_end] = f"'{escaped_end}'"
        else:
            self._parameters[param_end] = end
        
        condition = f"{column} BETWEEN %({param_start})s AND %({param_end})s"
        self._where_conditions.append(condition)
        return self
        
    def where_slot_with_partition(self, slot_start: int, slot_end: Optional[int] = None) -> 'ClickHouseQueryBuilder':
        """Add slot filter with partition optimization."""
        from pyxatu.utils import slot_to_timestamp
        
        # Get partitioning info from schema if available
        schema_mgr = get_schema_manager()
        partition_col = 'slot_start_date_time'  # default
        
        if self._from_table:
            table_info = schema_mgr.get_table_info(self._from_table)
            if table_info:
                partition_col = table_info.partitioning_column
        
        # Only apply partition optimization if the table uses slot_start_date_time
        if partition_col != 'slot_start_date_time':
            # For tables like mempool_transaction that use event_date_time
            if slot_end is None:
                self.where('slot', '=', slot_start)
            else:
                self.where_between('slot', slot_start, slot_end - 1)
            return self
        
        # Single slot
        if slot_end is None:
            self.where('slot', '=', slot_start)
            # Add partition filter for single slot
            slot_time = slot_to_timestamp(slot_start)
            self.where_raw(
                f"{partition_col} >= toDateTime('{slot_time.strftime('%Y-%m-%d %H:%M:%S')}') - INTERVAL 1 MINUTE "
                f"AND {partition_col} <= toDateTime('{slot_time.strftime('%Y-%m-%d %H:%M:%S')}') + INTERVAL 1 MINUTE"
            )
        else:
            # Slot range
            self.where_between('slot', slot_start, slot_end - 1)
            # Add partition filter for range
            start_time = slot_to_timestamp(slot_start)
            end_time = slot_to_timestamp(slot_end)
            self.where_raw(
                f"{partition_col} >= toDateTime('{start_time.strftime('%Y-%m-%d %H:%M:%S')}') - INTERVAL 1 HOUR "
                f"AND {partition_col} <= toDateTime('{end_time.strftime('%Y-%m-%d %H:%M:%S')}') + INTERVAL 1 HOUR"
            )
        return self
        
    def where_raw(self, condition: str, params: Optional[Dict[str, Any]] = None) -> 'ClickHouseQueryBuilder':
        """Add raw WHERE condition (use with caution)."""
        # Basic validation to prevent obvious SQL injection
        forbidden = [';', '--', '/*', '*/', 'DROP', 'DELETE', 'TRUNCATE', 'ALTER', 'UNION', 'INSERT', 'UPDATE']
        if any(word in condition.upper() for word in forbidden):
            raise ValueError("Potentially unsafe SQL detected in condition")
            
        self._where_conditions.append(condition)
        if params:
            self._parameters.update(params)
        return self
        
    def group_by(self, columns: Union[str, List[str]]) -> 'ClickHouseQueryBuilder':
        """Add GROUP BY clause."""
        if isinstance(columns, str):
            self._group_by = [col.strip() for col in columns.split(",")]
        else:
            self._group_by = columns
        return self
        
    def order_by(self, column: str, desc: bool = False) -> 'ClickHouseQueryBuilder':
        """Add ORDER BY clause."""
        self._order_by.append((column, desc))
        return self
        
    def limit(self, count: int) -> 'ClickHouseQueryBuilder':
        """Add LIMIT clause."""
        if count <= 0:
            raise ValueError("LIMIT must be positive")
        self._limit_count = count
        return self
        
    def build(self) -> tuple[str, Dict[str, Any]]:
        """Build the query and return query string with parameters."""
        if not self._select_columns:
            raise ValueError("SELECT columns not specified")
        if not self._from_table:
            raise ValueError("FROM table not specified")
            
        # Build SELECT
        query_parts = [f"SELECT {', '.join(self._select_columns)}"]
        
        # Build FROM
        from_clause = f"FROM {self._from_table}"
        if self._use_final:
            from_clause += " FINAL"
        query_parts.append(from_clause)
        
        # Build WHERE
        if self._where_conditions:
            query_parts.append(f"WHERE {' AND '.join(self._where_conditions)}")
            
        # Build GROUP BY
        if self._group_by:
            query_parts.append(f"GROUP BY {', '.join(self._group_by)}")
            
        # Build ORDER BY
        if self._order_by:
            order_clauses = []
            for col, desc in self._order_by:
                order_clauses.append(f"{col} {'DESC' if desc else 'ASC'}")
            query_parts.append(f"ORDER BY {', '.join(order_clauses)}")
            
        # Build LIMIT
        if self._limit_count:
            query_parts.append(f"LIMIT {self._limit_count}")
            
        query = " ".join(query_parts)
        return query, self._parameters


class ClickHouseClient(BaseClient):
    """Async ClickHouse client."""
    
    def __init__(self, config: ClickhouseConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self._session: Optional[aiohttp.ClientSession] = None
        self._auth = BasicAuth(config.user, config.password.get_secret_value())
        self._timeout = ClientTimeout(total=config.timeout)
        
    def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session with connection pooling."""
        if self._session is None or self._session.closed:
            connector = aiohttp.TCPConnector(
                limit=self.config.pool_size,
                limit_per_host=self.config.pool_size
            )
            self._session = aiohttp.ClientSession(
                connector=connector,
                auth=self._auth,
                timeout=self._timeout
            )
        return self._session
        
    @backoff.on_exception(
        backoff.expo,
        (aiohttp.ClientError, asyncio.TimeoutError),
        max_tries=3,
        max_time=60
    )
    async def execute_query(
        self, 
        query: str, 
        params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Execute a query and return results as list of dicts."""
        # Create a new session for each request to avoid event loop issues
        connector = aiohttp.TCPConnector(force_close=True)
        timeout = ClientTimeout(total=self.config.timeout)
        
        async with aiohttp.ClientSession(
            connector=connector,
            auth=self._auth,
            timeout=timeout
        ) as session:
            # Format query with parameters if provided
            if params:
                formatted_query = query % params
            else:
                formatted_query = query
                
            self.logger.debug(f"Executing query: {formatted_query[:200]}...")
            
            async with session.get(
                f"{self.config.url}/",
                params={
                    'query': formatted_query,
                    'database': self.config.database,
                    'default_format': 'JSONEachRow'
                }
            ) as response:
                response.raise_for_status()
                text = await response.text()
                
                if not text.strip():
                    return []
                    
                # Parse JSON lines
                results = []
                for line in text.strip().split('\n'):
                    if line:
                        import json
                        results.append(json.loads(line))
                        
                return results
            
    async def execute_query_df(
        self, 
        query: str, 
        params: Optional[Dict[str, Any]] = None
    ) -> pd.DataFrame:
        """Execute a query and return results as pandas DataFrame."""
        # Create a new session for each request to avoid event loop issues
        connector = aiohttp.TCPConnector(force_close=True)
        timeout = ClientTimeout(total=self.config.timeout)
        
        async with aiohttp.ClientSession(
            connector=connector,
            auth=self._auth,
            timeout=timeout
        ) as session:
            # Format query with parameters if provided
            if params:
                formatted_query = query % params
            else:
                formatted_query = query
                
            self.logger.debug(f"Executing query for DataFrame: {formatted_query[:200]}...")
            
            async with session.get(
                f"{self.config.url}/",
                params={
                    'query': formatted_query,
                    'database': self.config.database,
                    'default_format': 'TSVWithNames'
                }
            ) as response:
                response.raise_for_status()
                text = await response.text()
                
                if not text.strip():
                    return pd.DataFrame()
                    
                # Parse TSV into DataFrame
                return pd.read_csv(StringIO(text), sep='\t')
            
    async def execute_query_stream(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        chunk_size: int = 1000
    ):
        """Execute a query and yield results in chunks."""
        session = self._get_session()
        
        if params:
            formatted_query = query % params
        else:
            formatted_query = query
            
        self.logger.debug(f"Executing streaming query: {formatted_query[:200]}...")
        
        async with session.get(
            f"{self.config.url}/",
            params={
                'query': formatted_query,
                'database': self.config.database,
                'default_format': 'JSONEachRow'
            }
        ) as response:
            response.raise_for_status()
            
            buffer = []
            async for line in response.content:
                if line.strip():
                    import json
                    buffer.append(json.loads(line))
                    
                    if len(buffer) >= chunk_size:
                        yield buffer
                        buffer = []
                        
            if buffer:
                yield buffer
                
    async def test_connection(self) -> bool:
        """Test the database connection."""
        try:
            result = await self.execute_query("SELECT 1 as test")
            return result[0]['test'] == 1
        except Exception as e:
            self.logger.error(f"Connection test failed: {e}")
            return False
            
    async def get_table_columns(self, table: str) -> List[str]:
        """Get column names for a table."""
        query = """
        SELECT name
        FROM system.columns
        WHERE table = %(table)s
          AND database = %(database)s
        ORDER BY position
        """
        
        results = await self.execute_query(
            query,
            {'table': table, 'database': self.config.database}
        )
        
        return [row['name'] for row in results]
        
    async def close(self) -> None:
        """Close the client connection."""
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None