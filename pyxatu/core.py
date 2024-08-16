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
from typing import Optional, List, Dict, Any, Callable, TypeVar, Tuple

from pyxatu.utils import CONSTANTS
from pyxatu.client import ClickhouseClient
from pyxatu.retriever import DataRetriever


class PyXatu:
    def __init__(self, config_path: Optional[str] = None, use_env_variables: bool = False, log_level: str = 'INFO') -> None:
        if not logging.getLogger().hasHandlers():
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
        
        self.data_retriever = DataRetriever(
            client=self.client,
            tables=CONSTANTS["TABLES"]
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

    def get_blockevent_of_slot(self, slot: Optional[int] = None, columns: Optional[str] = "*", where: Optional[str] = None, 
                       time_interval: Optional[str] = None, network: str = "mainnet", max_retries: int = 1, 
                       orderby: Optional[str] = None, final_condition: Optional[str] = None, limit: int = None,
                       store_result_in_parquet: bool = None, custom_data_dir: str = None) -> Any:
        return self.data_retriever.get_data(
            'beaconchain_event_block',
            slot=slot, 
            columns=columns, 
            where=where, 
            time_interval=time_interval, 
            network=network, 
            orderby=orderby,
            final_condition=final_condition,
            limit=limit,
            store_result_in_parquet=store_result_in_parquet,
            custom_data_dir=custom_data_dir
        )
    
    def get_attestation_of_slot(self, slot: Optional[int] = None, columns: Optional[str] = "*", where: Optional[str] = None, 
                         time_interval: Optional[str] = None, network: str = "mainnet", max_retries: int = 1, 
                         orderby: Optional[str] = None, final_condition: Optional[str] = None, limit: int = None,
                         store_result_in_parquet: bool = None, custom_data_dir: str = None) -> Any:
        return self.data_retriever.get_data(
            'beaconchain_attestations',
            slot=slot, 
            columns=columns, 
            where=where, 
            time_interval=time_interval, 
            network=network, 
            orderby=orderby,
            final_condition=final_condition,
            limit=limit,
            store_result_in_parquet=store_result_in_parquet,
            custom_data_dir=custom_data_dir
        )

    def get_proposer_of_slot(self, slot: Optional[int] = None, columns: Optional[str] = "*", where: Optional[str] = None, 
                      time_interval: Optional[str] = None, network: str = "mainnet", max_retries: int = 1, 
                      orderby: Optional[str] = None, final_condition: Optional[str] = None, limit: int = None,
                      store_result_in_parquet: bool = None, custom_data_dir: str = None) -> Any:
        return self.data_retriever.get_data(
            'beaconchain_proposer',
            slot=slot, 
            columns=columns, 
            where=where, 
            time_interval=time_interval, 
            network=network, 
            orderby=orderby,
            final_condition=final_condition,
            limit=limit,
            store_result_in_parquet=store_result_in_parquet,
            custom_data_dir=custom_data_dir
        )
    
    def get_reorgs_in_slots(self, slot: Optional[int] = None, where: Optional[str] = None, 
                   time_interval: Optional[str] = None, network: str = "mainnet", max_retries: int = 1, 
                   orderby: Optional[str] = None, final_condition: Optional[str] = None, limit: int = None,
                   store_result_in_parquet: bool = None, custom_data_dir: str = None) -> Any:
       
        potential_reorgs = self.data_retriever.get_data(
            'beaconchain_reorgs',
            slot=slot, 
            columns="slot-depth", 
            where=where, 
            time_interval=time_interval, 
            network=network, 
            orderby=orderby,
            final_condition=final_condition,
            limit=limit,
            store_result_in_parquet=store_result_in_parquet,
            custom_data_dir=custom_data_dir
        )
        canonical = self.get_slots( 
            slot=slot, 
            columns=columns, 
            where=where, 
            time_interval=time_interval, 
            network=network, 
            orderby=orderby,
            final_condition=final_condition,
            limit=limit,
            store_result_in_parquet=False,
            custom_data_dir=None
        )
        missed = set(range(canonical.slot.min(), canonical.slot.max()+1)) - set(canonical.slot.tolist())
        reorgs = sorted(set(potential_reorgs["slot-depth"].tolist()).intersection(missed))
        return reorgs
    
    def get_slots(self, slot: Optional[int] = None, columns: Optional[str] = "*", where: Optional[str] = None, 
                  time_interval: Optional[str] = None, network: str = "mainnet", max_retries: int = 1, 
                  orderby: Optional[str] = None, final_condition: Optional[str] = None, limit: int = None,
                  store_result_in_parquet: bool = None, custom_data_dir: str = None) -> Any:
        return self.data_retriever.get_data(
            'beaconchain_canonical',
            slot=slot, 
            columns=columns, 
            where=where, 
            time_interval=time_interval, 
            network=network, 
            orderby=orderby,
            final_condition=final_condition,
            limit=limit,
            store_result_in_parquet=store_result_in_parquet,
            custom_data_dir=custom_data_dir
        )
    
    def execute_query(self, query: str, columns: Optional[str] = "*") -> Any:
        return self.client.execute_query(query, columns)
    
    def preview_result(self, func: Callable[..., Any], limit: int = 100, **kwargs) -> Any:
        kwargs['limit'] = limit
        return func(**kwargs)
    
    def __str__(self) -> str:
        return f"PyXatu(config_path={self.config_path}, user={self.clickhouse_user}, url={self.clickhouse_url})"

    def __repr__(self) -> str:
        return self.__str__()