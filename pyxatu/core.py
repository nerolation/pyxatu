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

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def retry_on_failure(max_retries=5, initial_wait=1, backoff_factor=2):
    """Decorator to retry a function if an exception occurs."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
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
        return wrapper
    return decorator


class ClickhouseClient:
    def __init__(self, url, user, password, timeout=1500):
        self.url = url
        self.auth = HTTPBasicAuth(user, password)
        self.timeout = timeout

    @retry_on_failure()
    def execute_query(self, query, columns=None):
        logging.info(f"Executing query: {query}")
        response = requests.get(
            self.url,
            params={'query': query},
            auth=self.auth,
            timeout=self.timeout
        )
        response.raise_for_status()
        return self._parse_response(response.text, columns)

    def _parse_response(self, response_text, columns=None):
        """Converts response text to a Pandas DataFrame and assigns column names if provided."""
        df = pd.read_csv(StringIO(response_text), sep='\t', header=None)
        if columns and columns != "*":
            column_list = [col.strip() for col in columns.split(',')]
            df.columns = column_list
        return df

    def fetch_data(self, table, slot=None, columns='*', where=None, time_interval=None, network="mainnet", orderby=None):
        query = self._build_query(table, slot, columns, where, time_interval, network, orderby)
        return self.execute_query(query, columns)

    def _build_query(self, table, slot, columns, where, time_interval, network, orderby):
        """Builds an SQL query string based on the parameters."""
        query = f"SELECT {columns} FROM {table}"
        
        conditions = []
        if isinstance(slot, list):
            conditions.append(f"slot >= {slot[0]} AND slot < {slot[1]}")
        elif slot:
            conditions.append(f"slot = {slot}")
        
        if where:
            conditions.append(where)

        if time_interval:
            conditions.append(f"slot_start_date_time > NOW() - INTERVAL '{time_interval}'")

        if network:
            conditions.append(f"meta_network_name = '{network}'")

        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        if orderby:
            query += f" ORDER BY {orderby}"

        return query


class BeaconChainDataRetriever:
    def __init__(self, client, block_event_table, proposer_table, reorgs_table, canonical_table):
        self.client = client
        self.block_event_table = block_event_table
        self.proposer_table = proposer_table
        self.reorgs_table = reorgs_table
        self.canonical_table = canonical_table

    def get_block_event_data(self, slot=None, columns=None, where=None, time_interval=None, network="mainnet", orderby=None):
        columns = columns or '*'
        return self.client.fetch_data(self.slot_table, slot, columns, where, time_interval, network, orderby)

    def get_proposer_data(self, slot, columns=None, where=None, time_interval=None, network="mainnet", orderby=None):
        columns = columns or '*'
        return self.client.fetch_data(self.proposer_table, slot, columns, where, time_interval, network, orderby)
    
    def get_reorgs_data(self, slot, columns=None, where=None, time_interval=None, network="mainnet", orderby=None):
        columns = columns or '*'
        return self.client.fetch_data(self.reorgs_table, slot, columns, where, time_interval, network, orderby)
    
    def get_slot_data(self, slot, columns=None, where=None, time_interval=None, network="mainnet", orderby=None):
        columns = columns or '*'
        return self.client.fetch_data(self.canonical_table, slot, columns, where, time_interval, network, orderby)
    

class PyXatu:
    def __init__(self, config_path=None):
        self.user_config_path = os.path.join(Path.home(), '.pyxatu_config.json')
        
        if config_path is None:
            config_path = self.user_config_path

        self.config_path = config_path
        self.clickhouse_url, self.clickhouse_user, self.clickhouse_password = self.read_clickhouse_config()
        logging.info(f"Clickhouse URL: {self.clickhouse_url}, User: {self.clickhouse_user}")
        self.client = ClickhouseClient(self.clickhouse_url, self.clickhouse_user, self.clickhouse_password)
        
        self.read_tables()
        self.beaconchain_retriever = BeaconChainDataRetriever(
            client=self.client,
            block_event_table=self.beaconchain_event_block_table,
            proposer_table=self.beaconchain_proposer_table,
            reorgs_table=self.beaconchain_reorgs_table,
            canonical_table=self.beaconchain_canonical_table
        )

    def read_tables(self):
        self.beaconchain_event_block_table = "default.beacon_api_eth_v1_events_block"
        self.beaconchain_proposer_table = "default.canonical_beacon_proposer_duty"
        self.beaconchain_reorgs_table = "default.beacon_api_eth_v1_events_chain_reorg"
        self.beaconchain_canonical_table = "default.beacon_api_eth_v1_events_chain_reorg"
        
    def read_clickhouse_config(self):
        with open(self.config_path, 'r') as file:
            config = json.load(file)
        clickhouse_user = config.get("CLICKHOUSE_USER")
        clickhouse_password = config.get("CLICKHOUSE_PASSWORD")
        url = config.get("CLICKHOUSE_URL")
        logging.info("Clickhouse configs set")
        return url, clickhouse_user, clickhouse_password


    def get_blockevent(self, slot=None, columns=None, where=None, time_interval=None, network="mainnet", max_retries=5, orderby=None):
        return self.beaconchain_retriever.get_block_event_data (
            slot=slot, 
            columns=columns, 
            where=where, 
            time_interval=time_interval, 
            network=network, 
            orderby=orderby
        )

    def get_proposers(self, slot=None, columns=None, where=None, time_interval=None, network="mainnet", max_retries=5, orderby=None):
        return self.beaconchain_retriever.get_proposer_data (
            slot=slot, 
            columns=columns, 
            where=where, 
            time_interval=time_interval, 
            network=network, 
            orderby=orderby
        )
    
    def get_reorgs(self, slot=None, columns=None, where=None, time_interval=None, network="mainnet", max_retries=5, orderby=None):
        return self.beaconchain_retriever.get_reorgs_data(
            slot=slot, 
            columns=columns, 
            where=where, 
            time_interval=time_interval, 
            network=network, 
            orderby=orderby
        )
    
    def get_slots(self, slot=None, columns=None, where=None, time_interval=None, network="mainnet", max_retries=5, orderby=None):
        return self.beaconchain_retriever.get_slot_data (
            slot=slot, 
            columns=columns, 
            where=where, 
            time_interval=time_interval, 
            network=network, 
            orderby=orderby
        )
    
    def __str__(self):
        return f"PyXatu(config_path={self.config_path}, user={self.clickhouse_user}, url={self.clickhouse_url})"

    def __repr__(self):
        return self.__str__()