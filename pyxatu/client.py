import logging
import requests
import pandas as pd
from io import StringIO
from datetime import datetime, timezone
from typing import Optional, List
from requests.auth import HTTPBasicAuth

from pyxatu.utils import retry_on_failure, CONSTANTS

class ClickhouseClient:
    def __init__(self, url: str, user: str, password: str, timeout: int = 1500) -> None:
        self.url = url
        self.auth = HTTPBasicAuth(user, password)
        self.timeout = timeout

    @retry_on_failure()
    def execute_query(self, query: str, columns: Optional[str] = "*") -> pd.DataFrame:
        logging.info(f"Executing query: {query}")
        response = requests.get(
            self.url,
            params={'query': query},
            auth=self.auth,
            timeout=self.timeout
        )
        response.raise_for_status()
        if "DISTINCT" in query.upper():
            potentials_columns = query.split("FROM")[0].split("DISTINCT")[1].strip()
        else:
            potentials_columns = query.split("FROM")[0].split("SELECT")[1].strip()

        return self._parse_response(response.text, columns, potentials_columns)

    def _parse_response(self, response_text: str, columns: Optional[str] = "*", potentials_columns: Optional[str] = None) -> pd.DataFrame:
        """Converts response text to a Pandas DataFrame and assigns column names if provided."""
        df = pd.read_csv(StringIO(response_text), sep='\t', header=None)
        if columns and columns != "*":
            df.columns = [col.strip() for col in columns.split(',')]

        elif potentials_columns and potentials_columns != "*":

            df.columns = [col.strip() for col in potentials_columns.split(",")]
            
        return df

    def fetch_data(self, table: str, slot: Optional[int] = None, columns: str = '*', where: Optional[str] = None,
                   time_interval: Optional[str] = None, network: str = "mainnet", orderby: Optional[str] = None,
                   final_condition: Optional[str] = None, limit: int = None) -> pd.DataFrame:
        query = self._build_query(
            table = table, 
            slot = slot, 
            columns = columns, 
            where = where, 
            time_interval = time_interval, 
            network = network, 
            orderby = orderby, 
            final_condition = final_condition,
            limit = limit
        )
        return self.execute_query(query, columns)

    def _build_query(self, table: str, slot: Optional[int], columns: str, where: Optional[str], 
                     time_interval: Optional[str], network: str, orderby: Optional[str], 
                     final_condition: Optional[str], limit: int = None) -> str:
        query = f"SELECT DISTINCT {columns} FROM {table}"
        
        conditions = []
        
        if isinstance(slot, int):
            date_filter = self._get_sql_date_filter(slot=slot)
            conditions.append(date_filter)
            conditions.append(f"slot = {int(slot)}")
        elif slot and isinstance(slot, list) and len(slot) == 2:
            date_filter = self._get_sql_date_filter(slot=slot)
            conditions.append(date_filter)
            conditions.append(f"slot >= {int(slot[0])} AND slot < {int(slot[1])}")
        
        if where: conditions.append(where)
        if time_interval: conditions.append(f"slot_start_date_time > NOW() - INTERVAL '{time_interval}'")
        if network: conditions.append(f"meta_network_name = '{network}'")
        if final_condition: conditions.append(final_condition)
        
        query += f" WHERE {' AND '.join(filter(None, conditions))}"
        
        if orderby: query += f" ORDER BY {orderby}"
            
        if limit: query += f" LIMIT {limit}"
        
        return query
    
    def get_slot_datetime(self, slot: int, as_type: str = "str") -> int:
        slot_timestamp = CONSTANTS["GENESIS_TIME_ETH_POS"] + (slot * CONSTANTS["SECONDS_PER_SLOT"])
        slot_datetime = datetime.fromtimestamp(slot_timestamp, tz=timezone.utc)
        if as_type == "str":
            return slot_datetime.strftime('%Y-%m-%d %H:%M:%S')
        elif as_type == "int":
            return int(slot_datetime.timestamp())
            

    def get_time_in_slot(self, slot: int, ts: int) -> int:
        return ts - get_slot_datetime(slot, as_type = "int")

    def _get_sql_date_filter(self, slot: Optional[int] = None) -> str:
        """
        Returns a SQL-compatible date filter for a given Ethereum PoS slot or a range of slots.
        This is useful to minimize the amount of requested resources on the Xatu backend.

        Args:
            slot (Optional[int]): A single slot number to calculate the date for.
            slots (Optional[List[int]]): A list of two slot numbers to create a range filter.

        Returns:
            str: A SQL date filter in the format 'YYYY-MM-DD HH:MM:SS' or a range of timestamps.

        Raises:
            ValueError: If neither a valid `slot` nor a valid list of `slots` is provided.
        """
        # Case 1: Single slot provided (it must be an integer)
        if isinstance(slot, int):
            slot_date_str = self.get_slot_datetime(slot)
            return f"slot_start_date_time >= '{slot_date_str}'"

        # Case 2: List of two slots provided (must be a list of exactly two integers)
        elif isinstance(slot, list) and len(slot) == 2 and all(isinstance(s, int) for s in slot):
            lower_slot_date_str = self.get_slot_datetime(slot[0])
            upper_slot_date_str = self.get_slot_datetime(slot[1])
            return f"slot_start_date_time >= '{lower_slot_date_str}' AND slot_start_date_time < '{upper_slot_date_str}'"

        else:
            raise ValueError("Invalid input: either a valid integer slot or a list of exactly two slots must be provided.")
