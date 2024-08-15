import os
import json
import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
from io import StringIO
import time
from pathlib import Path
import shutil
import logging
from functools import wraps
from typing import Optional, List, Dict, Any, Callable, TypeVar


TABLES = {
    "beaconchain_event_block": "default.beacon_api_eth_v1_events_block",
    "beaconchain_proposer": "default.canonical_beacon_proposer_duty",
    "beaconchain_reorgs": "default.beacon_api_eth_v1_events_chain_reorg",
    "beaconchain_canonical": "default.canonical_beacon_block",
    "beaconchain_attestations": "default.canonical_beacon_elaborated_attestation",
}

GENESIS_TIME_ETH_POS = 1606824023
SECONDS_PER_SLOT = 12

F = TypeVar('F', bound=Callable[..., Any])

def retry_on_failure(max_retries: int = 5, initial_wait: float = 1.0, backoff_factor: float = 2.0) -> Callable[[F], F]:
    """Decorator to retry a function if an exception occurs."""
    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            attempt = 0
            wait_time = initial_wait
            while attempt < max_retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    logging.warning(f"Attempt {attempt + 1}/{max_retries} failed: {e}. Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                    wait_time *= backoff_factor
                    attempt += 1
            logging.error(f"Max retries reached. Failed to complete operation.")
            return None
        return wrapper  # type: ignore
    return decorator


class ClickhouseClient:
    def __init__(self, url: str, user: str, password: str, timeout: int = 1500) -> None:
        self.url = url
        self.auth = HTTPBasicAuth(user, password)
        self.timeout = timeout

    @retry_on_failure()
    def execute_query(self, query: str, columns: Optional[str] = None) -> pd.DataFrame:
        logging.info(f"Executing query: {query}")
        response = requests.get(
            self.url,
            params={'query': query},
            auth=self.auth,
            timeout=self.timeout
        )
        response.raise_for_status()
        potentials_columns = query.split("FROM")[0].split("DISTINCT")[1].replace(" ")
        return self._parse_response(response.text, columns, potentials_columns)

    def _parse_response(self, response_text: str, columns: Optional[str] = None, potentials_columns: Optional[str] = None) -> pd.DataFrame:
        """Converts response text to a Pandas DataFrame and assigns column names if provided."""
        df = pd.read_csv(StringIO(response_text), sep='\t', header=None)
        if columns and columns != "*":
            df.columns = [col.strip() for col in columns.split(',')]

        elif potentials_columns and columns != "*":
            df.columns = potentials_columns.split(",")
            
        return df

    def fetch_data(self, table: str, slot: Optional[int] = None, columns: str = '*', where: Optional[str] = None,
                   time_interval: Optional[str] = None, network: str = "mainnet", orderby: Optional[str] = None,
                   final_condition: Optional[str] = None) -> pd.DataFrame:
        query = self._build_query(table, slot, columns, where, time_interval, network, orderby, final_condition)
        return self.execute_query(query, columns)

    def _build_query(self, table: str, slot: Optional[int], columns: str, where: Optional[str], 
                     time_interval: Optional[str], network: str, orderby: Optional[str], 
                     final_condition: Optional[str]) -> str:
        query = f"SELECT {columns} FROM {table}"
        
        if slot:
            date_filter = self._get_sql_date_filter(slot=slot)
            conditions.append(date_filter)
        elif slots and isinstance(slots, list) and len(slots) == 2:
            date_filter = self._get_sql_date_filter(slots=slots)
            conditions.append(date_filter)
        
        if where: conditions.append(where)
        if time_interval: conditions.append(f"slot_start_date_time > NOW() - INTERVAL '{time_interval}'")
        if network: conditions.append(f"meta_network_name = '{network}'")
        if final_condition: conditions.append(final_condition)
        
        query += f" WHERE {' AND '.join(filter(None, conditions))}"
        
        if orderby: query += f" ORDER BY {orderby}"
            
        if limit: query += f" LIMIT {limit}"
        
        return query
    
    
    def _get_sql_date_filter(self, slot: Optional[int] = None, slots: Optional[List[int]] = None) -> str:
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

        def get_slot_datetime(slot: int) -> str:
            slot_timestamp = GENESIS_TIME_ETH_POS + (slot * SECONDS_PER_SLOT)
            slot_datetime = datetime.utcfromtimestamp(slot_timestamp)
            return slot_datetime.strftime('%Y-%m-%d %H:%M:%S')

        # Case 1: Single slot provided (it must be an integer)
        if isinstance(slot, int):
            slot_date_str = get_slot_datetime(slot)
            return f"slot_start_date_time >= '{slot_date_str}'"

        # Case 2: List of two slots provided (must be a list of exactly two integers)
        elif isinstance(slots, list) and len(slots) == 2 and all(isinstance(s, int) for s in slots):
            lower_slot_date_str = get_slot_datetime(slots[0])
            upper_slot_date_str = get_slot_datetime(slots[1])
            return f"slot_start_date_time >= '{lower_slot_date_str}' AND slot_start_date_time < '{upper_slot_date_str}'"

        else:
            raise ValueError("Invalid input: either a valid integer slot or a list of exactly two slots must be provided.")


class BeaconChainDataRetriever:
    def __init__(self, client, tables):
        self.client = client
        self.tables = tables

    def get_data(self, data_type: str, slot: Optional[int] = None, columns: Optional[List[str]] = None, 
                 where: Optional[str] = None, time_interval: Optional[str] = None, network: str = "mainnet", 
                 orderby: Optional[str] = None, final_condition: Optional[str] = None, 
                 only_store_result_in_parquet: bool = None, custom_data_dir: str = None) -> Any:
        columns = columns or '*'
        table = self.tables.get(data_type)
        if not table:
            raise ValueError(
                f"Data type '{data_type}' is not valid. Please use one of the following: {', '.join(self.tables.keys())}"
            )
        result = self.client.fetch_data(table, slot, columns, where, time_interval, network, orderby, final_condition)
        if only_store_result_in_parquet:
            self.store_result_to_disk(custom_data_dir)
            return
        else:
            return result

class PyXatu:
    def __init__(self, config_path: Optional[str] = None, use_env_variables: bool = False, log_level: str = 'INFO') -> None:
        logging.basicConfig(level=getattr(logging, log_level.upper()), format='%(asctime)s - %(levelname)s - %(message)s')
        self.user_config_path = os.path.join(Path.home(), '.pyxatu_config.json')
        
        if config_path is None:
            config_path = self.user_config_path

        self.config_path = config_path
        
        if use_env_variables:
            config = self.read_clickhouse_config_from_env()
            self.clickhouse_url, self.clickhouse_user, self.clickhouse_password = config
        else:
            self.clickhouse_url, self.clickhouse_user, self.clickhouse_password = self.read_clickhouse_config()

        logging.info(f"Clickhouse URL: {self.clickhouse_url}, User: {self.clickhouse_user}")
        self.client = ClickhouseClient(self.clickhouse_url, self.clickhouse_user, self.clickhouse_password)
        
        self.beaconchain_retriever = BeaconChainDataRetriever(
            client=self.client,
            tables=TABLES
        )
        
    def read_clickhouse_config_from_env(self) -> Tuple[str, str, str]:
        """Reads Clickhouse configuration from environment variables."""
        clickhouse_user = os.getenv("CLICKHOUSE_USER", "default_user")
        clickhouse_password = os.getenv("CLICKHOUSE_PASSWORD", "default_password")
        url = os.getenv("CLICKHOUSE_URL", "http://localhost")

        logging.info("Clickhouse configs set")
        return url, clickhouse_user, clickhouse_password

    def read_clickhouse_config(self) -> Tuple[str, str, str]:
        """Reads Clickhouse configuration from the config file."""
        with open(self.config_path, 'r') as file:
            config = json.load(file)
        clickhouse_user = config.get("CLICKHOUSE_USER", "default_user")
        clickhouse_password = config.get("CLICKHOUSE_PASSWORD", "default_password")
        url = config.get("CLICKHOUSE_URL", "http://localhost")

        logging.info("Clickhouse configs set")
        return url, clickhouse_user, clickhouse_password

    def get_blockevent_of_slot(self, slot: Optional[int] = None, columns: Optional[str] = None, where: Optional[str] = None, 
                       time_interval: Optional[str] = None, network: str = "mainnet", max_retries: int = 5, 
                       orderby: Optional[str] = None, final_condition: Optional[str] = None, limit: int = None,
                       only_store_result_in_parquet: bool = None, custom_data_dir: str = None) -> Any:
        return self.beaconchain_retriever.get_data(
            'beaconchain_event_block',
            slot=slot, 
            columns=columns, 
            where=where, 
            time_interval=time_interval, 
            network=network, 
            orderby=orderby,
            final_condition=final_condition,
            limit=limit
        )
    
    def get_attestation_of_slot(self, slot: Optional[int] = None, columns: Optional[str] = None, where: Optional[str] = None, 
                         time_interval: Optional[str] = None, network: str = "mainnet", max_retries: int = 5, 
                         orderby: Optional[str] = None, final_condition: Optional[str] = None, limit: int = None,
                         only_store_result_in_parquet: bool = None, custom_data_dir: str = None) -> Any:
        return self.beaconchain_retriever.get_data(
            'beaconchain_attestations',
            slot=slot, 
            columns=columns, 
            where=where, 
            time_interval=time_interval, 
            network=network, 
            orderby=orderby,
            final_condition=final_condition,
            limit=limit,
            only_store_result_in_parquet=only_store_result_in_parquet
        )

    def get_proposer_of_slot(self, slot: Optional[int] = None, columns: Optional[str] = None, where: Optional[str] = None, 
                      time_interval: Optional[str] = None, network: str = "mainnet", max_retries: int = 5, 
                      orderby: Optional[str] = None, final_condition: Optional[str] = None, limit: int = None,
                      only_store_result_in_parquet: bool = None, custom_data_dir: str = None) -> Any:
        return self.beaconchain_retriever.get_data(
            'beaconchain_proposer',
            slot=slot, 
            columns=columns, 
            where=where, 
            time_interval=time_interval, 
            network=network, 
            orderby=orderby,
            final_condition=final_condition,
            limit=limit,
            only_store_result_in_parquet=only_store_result_in_parquet
        )
    
    def get_reorg_of_slot(self, slot: Optional[int] = None, columns: Optional[str] = None, where: Optional[str] = None, 
                   time_interval: Optional[str] = None, network: str = "mainnet", max_retries: int = 5, 
                   orderby: Optional[str] = None, final_condition: Optional[str] = None, limit: int = None,
                   only_store_result_in_parquet: bool = None, custom_data_dir: str = None) -> Any:
        return self.beaconchain_retriever.get_data(
            'beaconchain_reorgs',
            slot=slot, 
            columns=columns, 
            where=where, 
            time_interval=time_interval, 
            network=network, 
            orderby=orderby,
            final_condition=final_condition,
            limit=limit,
            only_store_result_in_parquet=only_store_result_in_parquet
        )
    
    def get_slots(self, slot: Optional[int] = None, columns: Optional[str] = None, where: Optional[str] = None, 
                  time_interval: Optional[str] = None, network: str = "mainnet", max_retries: int = 5, 
                  orderby: Optional[str] = None, final_condition: Optional[str] = None, limit: int = None,
                  only_store_result_in_parquet: bool = None, custom_data_dir: str = None) -> Any:
        return self.beaconchain_retriever.get_data(
            'beaconchain_canonical',
            slot=slot, 
            columns=columns, 
            where=where, 
            time_interval=time_interval, 
            network=network, 
            orderby=orderby,
            final_condition=final_condition,
            limit=limit,
            only_store_result_in_parquet=only_store_result_in_parquet
        )
    
    def execute_query(self, query: str, columns: Optional[str] = None) -> Any:
        return self.client.execute_query(query, columns)
    
    def preview_result(self, func: Callable[..., Any], limit: int = 100, **kwargs) -> Any:
        kwargs['limit'] = limit
        return func(**kwargs)
    
    def __str__(self) -> str:
        return f"PyXatu(config_path={self.config_path}, user={self.clickhouse_user}, url={self.clickhouse_url})"

    def __repr__(self) -> str:
        return self.__str__()