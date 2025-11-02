import time
import logging
import requests
import pandas as pd
from io import StringIO
from datetime import datetime, timezone
from typing import Optional, List, Any
from requests.auth import HTTPBasicAuth

from ..utils import retry_on_failure, CONSTANTS
from ..utils.helpers import PyXatuHelpers

class ClickhouseClient:
    """
    Client for executing queries against ClickHouse database.
    
    Handles authentication, query execution, and response parsing
    for the Xatu ClickHouse database.
    """
    
    def __init__(self, url: str, user: str, password: str, timeout: int = 1500, helper: Any = None) -> None:
        self.url = url
        self.auth = HTTPBasicAuth(user, password)
        self.timeout = timeout
        self.helpers = helper or PyXatuHelpers()

    @retry_on_failure()
    def execute_query(self, query: str, columns: Optional[str] = "*", handle_columns: bool = False) -> pd.DataFrame:
        """Execute a ClickHouse query and return the result as a DataFrame."""
        should_log = self._should_log_query(query)
        
        if should_log:
            logging.info(f"Executing query: {query}")
        
        if not self._has_network_filter(query):
            logging.warning("No network specified. You are requesting info across testnets and mainnet.")
            return None
        
        start_time = time.time()
        
        try:
            response = requests.get(
                self.url,
                params={'query': query},
                auth=self.auth,
                timeout=self.timeout
            )
            response.raise_for_status()
            
            if should_log:
                logging.info(f"Query executed in {time.time() - start_time:.2f} seconds")
            
            if not response.text.strip():
                if should_log:
                    logging.info("No data returned for query")
                return None
            
            potential_columns = self._extract_column_names(query) if handle_columns else None
            return self._parse_response(response.text, columns, potential_columns)
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Query execution failed: {e}")
            raise
    
    def _should_log_query(self, query: str) -> bool:
        """Determine if query execution should be logged."""
        return "FROM system.columns" not in query
    
    def _has_network_filter(self, query: str) -> bool:
        """Check if query includes network filtering."""
        return "meta_network_name" in query or "FROM system.columns" in query
    
    def _extract_column_names(self, query: str) -> Optional[str]:
        """Extract column names from SELECT clause."""
        try:
            if "DISTINCT" in query.upper():
                potential_columns = query.split("FROM")[0].split("DISTINCT")[1].strip()
            else:
                potential_columns = query.split("FROM")[0].split("SELECT")[1].strip()
            
            if potential_columns == "*":
                return None
            
            if "," in potential_columns:
                columns = [col.split("as ")[-1].strip() if "as " in col else col.strip() 
                          for col in potential_columns.split(",")]
                return ",".join(columns)
            else:
                return potential_columns.split("as ")[-1].strip() if "as " in potential_columns else potential_columns.strip()
        except (IndexError, AttributeError):
            return None

    def _parse_response(self, response_text: str, columns: Optional[str] = "*", potential_columns: Optional[str] = None) -> pd.DataFrame:
        """Converts response text to a Pandas DataFrame and assigns column names if provided."""
        df = pd.read_csv(StringIO(response_text), sep='\t', header=None)
        if columns and columns != "*":
            columns = [col.strip() for col in columns.split(',')]
            df.columns = [i.split("as ")[-1] if " as " in i else i for i in columns]

        elif potential_columns and potential_columns != "*":
            df.columns = [col.strip() for col in potential_columns.split(",")] 
            
        return df

    def fetch_data(self, data_table: str, slot: Optional[int] = None, columns: str = '*', where: Optional[str] = None,
                   time_interval: Optional[str] = None, network: str = "mainnet", groupby: Optional[str] = None,
                   orderby: Optional[str] = None, final_condition: Optional[str] = None, limit: int = None,
                   add_final_keyword_to_query: bool = True, time_column: str = "slot_start_date_time",
                   no_slot_filter: bool = False) -> pd.DataFrame:
        query = self._build_query(
            data_table = data_table, 
            slot = slot, 
            columns = columns, 
            where = where, 
            time_interval = time_interval, 
            network = network, 
            groupby = groupby, 
            orderby = orderby, 
            final_condition = final_condition,
            limit = limit,
            add_final_keyword_to_query=add_final_keyword_to_query,
            time_column=time_column,
            no_slot_filter=no_slot_filter
        )
        return self.execute_query(query, columns)

    def _build_query(self, data_table: str, slot: Optional[int], columns: str, where: Optional[str], 
                     time_interval: Optional[str], network: str,  groupby: Optional[str], orderby: Optional[str], 
                     final_condition: Optional[str], limit: int = None, add_final_keyword_to_query: bool = True,
                     time_column: str = "slot_start_date_time", no_slot_filter: bool = False) -> str:
        query = f"SELECT DISTINCT {columns} FROM {data_table}"
        if add_final_keyword_to_query:
            query += " FINAL"
        conditions = []
        
        if isinstance(slot, int):
            date_filter = self.helpers.get_sql_date_filter(slot=slot, time_column=time_column)
            conditions.append(date_filter)
            if not no_slot_filter:
                conditions.append(f"slot = {int(slot)}")
        elif slot and isinstance(slot, list) and len(slot) == 2:
            date_filter = self.helpers.get_sql_date_filter(slot=slot, time_column=time_column)
            conditions.append(date_filter)
            if not no_slot_filter: 
                conditions.append(f"slot >= {int(slot[0])} AND slot < {int(slot[1])}")
        
        if where: conditions.append(where)
        if time_interval: conditions.append(f"{time_column} > NOW() - INTERVAL '{time_interval}'")
        if network: conditions.append(f"meta_network_name = '{network}'")
        if final_condition: conditions.append(final_condition)
        
        query += f" WHERE {' AND '.join(filter(None, conditions))}"
        
        if groupby: query += f" GROUP BY {groupby}"
        if orderby: query += f" ORDER BY {orderby}"
            
        if limit: query += f" LIMIT {limit}"
        
        return query