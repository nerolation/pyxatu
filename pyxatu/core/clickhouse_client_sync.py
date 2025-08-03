"""Synchronous ClickHouse client - simplified and robust."""

import logging
from typing import Dict, Optional, Any, List
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from io import StringIO
import time

from ..config import ClickhouseConfig


class ClickHouseClient:
    """Simple synchronous ClickHouse client using requests."""
    
    def __init__(self, config: ClickhouseConfig):
        """Initialize the client with configuration."""
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Create a session for connection pooling
        self.session = requests.Session()
        self.session.auth = HTTPBasicAuth(config.user, config.password.get_secret_value())
        
        # Set reasonable timeouts and retry settings
        self.session.headers.update({
            'Accept-Encoding': 'gzip',
            'User-Agent': 'PyXatu/2.0'
        })
        
        # Configure retries
        from requests.adapters import HTTPAdapter
        from requests.packages.urllib3.util.retry import Retry
        
        retry_strategy = Retry(
            total=config.max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=config.pool_size,
            pool_maxsize=config.pool_size
        )
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute a query and return results as list of dictionaries."""
        # Format query with parameters if provided
        if params:
            formatted_query = query % params
        else:
            formatted_query = query
        
        self.logger.debug(f"Executing query: {formatted_query[:200]}...")
        
        try:
            response = self.session.get(
                f"{self.config.url}/",
                params={
                    'query': formatted_query,
                    'database': self.config.database,
                    'default_format': 'JSONEachRow'
                },
                timeout=self.config.timeout
            )
            response.raise_for_status()
            
            # Parse JSON lines
            results = []
            for line in response.text.strip().split('\n'):
                if line:
                    import json
                    results.append(json.loads(line))
            
            return results
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Query failed: {e}")
            raise
    
    def execute_query_df(self, query: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """Execute a query and return results as pandas DataFrame."""
        # Format query with parameters if provided
        if params:
            formatted_query = query % params
        else:
            formatted_query = query
        
        self.logger.debug(f"Executing query for DataFrame: {formatted_query[:200]}...")
        
        try:
            response = self.session.get(
                f"{self.config.url}/",
                params={
                    'query': formatted_query,
                    'database': self.config.database,
                    'default_format': 'TSVWithNames'
                },
                timeout=self.config.timeout
            )
            response.raise_for_status()
            
            if not response.text.strip():
                return pd.DataFrame()
            
            # Parse TSV into DataFrame
            return pd.read_csv(StringIO(response.text), sep='\t')
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Query failed: {e}")
            raise
    
    def test_connection(self) -> bool:
        """Test the database connection."""
        try:
            result = self.execute_query("SELECT 1 as test")
            return result[0]['test'] == 1
        except Exception as e:
            self.logger.error(f"Connection test failed: {e}")
            return False
    
    def get_table_columns(self, table: str) -> List[str]:
        """Get column names for a table."""
        query = """
        SELECT name
        FROM system.columns
        WHERE table = %(table)s
          AND database = %(database)s
        ORDER BY position
        """
        
        results = self.execute_query(
            query,
            {'table': table, 'database': self.config.database}
        )
        
        return [row['name'] for row in results]
    
    def close(self):
        """Close the session."""
        self.session.close()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


class ClickHouseQueryBuilder:
    """Simplified query builder for common patterns."""
    
    def __init__(self):
        self.reset()
    
    def reset(self):
        """Reset the query builder to initial state."""
        self._select_columns = []
        self._from_table = None
        self._where_conditions = []
        self._order_by = []
        self._limit = None
        self._parameters = {}
        return self
    
    def select(self, columns: List[str]) -> 'ClickHouseQueryBuilder':
        """Set SELECT columns."""
        if columns and columns != ['*']:
            self._select_columns = columns
        else:
            self._select_columns = ['*']
        return self
    
    def from_table(self, table: str) -> 'ClickHouseQueryBuilder':
        """Set FROM table."""
        self._from_table = table
        return self
    
    def where(self, column: str, operator: str, value: Any) -> 'ClickHouseQueryBuilder':
        """Add WHERE condition."""
        # Simple SQL injection prevention
        if operator.upper() not in ['=', '!=', '<', '>', '<=', '>=', 'IN', 'NOT IN', 'LIKE']:
            raise ValueError(f"Invalid operator: {operator}")
        
        # Use parameter placeholders
        param_name = f"param_{len(self._parameters)}"
        self._where_conditions.append(f"{column} {operator} %({param_name})s")
        self._parameters[param_name] = value
        return self
    
    def where_between(self, column: str, start: Any, end: Any) -> 'ClickHouseQueryBuilder':
        """Add BETWEEN condition."""
        param_start = f"param_{len(self._parameters)}"
        param_end = f"param_{len(self._parameters) + 1}"
        
        self._where_conditions.append(
            f"{column} BETWEEN %({param_start})s AND %({param_end})s"
        )
        self._parameters[param_start] = start
        self._parameters[param_end] = end
        return self
    
    def order_by(self, column: str, desc: bool = False) -> 'ClickHouseQueryBuilder':
        """Add ORDER BY clause."""
        self._order_by.append(f"{column} {'DESC' if desc else 'ASC'}")
        return self
    
    def limit(self, count: int) -> 'ClickHouseQueryBuilder':
        """Set LIMIT."""
        if count <= 0:
            raise ValueError("LIMIT must be positive")
        self._limit = count
        return self
    
    def build(self) -> tuple[str, Dict[str, Any]]:
        """Build the query and return query string with parameters."""
        if not self._select_columns:
            raise ValueError("SELECT columns not specified")
        if not self._from_table:
            raise ValueError("FROM table not specified")
        
        # Build query parts
        parts = [
            f"SELECT {', '.join(self._select_columns)}",
            f"FROM {self._from_table}"
        ]
        
        if self._where_conditions:
            parts.append(f"WHERE {' AND '.join(self._where_conditions)}")
        
        if self._order_by:
            parts.append(f"ORDER BY {', '.join(self._order_by)}")
        
        if self._limit:
            parts.append(f"LIMIT {self._limit}")
        
        query = " ".join(parts)
        return query, self._parameters