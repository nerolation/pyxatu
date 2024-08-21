import os
import ast
import json
import time
import shutil
import logging
import inspect
import textwrap
from pathlib import Path
from functools import wraps
from typing import Optional, List, Dict, Any, Callable, TypeVar, Tuple
from io import StringIO

import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
from tqdm.auto import tqdm

from pyxatu.utils import CONSTANTS
from pyxatu.helpers import PyXatuHelpers
from pyxatu.docscraper import DocsScraper
from pyxatu.client import ClickhouseClient
from pyxatu.retriever import DataRetriever
from pyxatu.validators import ValidatorGadget
from pyxatu.relayendpoint import MevBoostCaller



def column_check_decorator(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        # Extract columns and table name from the function arguments
        columns = kwargs.get('columns', "*")
        table = kwargs.get('data_table')

        if table is None:
            raise ValueError("Table name must be provided to verify columns.")
        
        # Perform column verification using the verify_columns method
        if not self.verify_columns(columns, table):
            raise ValueError(f"One or more columns in '{columns}' are not valid for table '{table}'")

        # Call the original function if columns are valid
        return func(self, *args, **kwargs)

    return wrapper

class PyXatu:
    def __init__(self, config_path: Optional[str] = None, use_env_variables: bool = False, log_level: str = 'INFO', relay: str = None) -> None:
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
            self.clickhouse_url, self.clickhouse_user, self.clickhouse_password = self.read_clickhouse_config_locally()

        logging.info(f"Clickhouse URL: {self.clickhouse_url}, User: {self.clickhouse_user}")
        self.client = ClickhouseClient(self.clickhouse_url, self.clickhouse_user, self.clickhouse_password)
        
        self.data_retriever = DataRetriever(
            client=self.client,
            tables=CONSTANTS["TABLES"]
        )
        
        self.mevboost = MevBoostCaller()
        self.helpers = PyXatuHelpers()
        self.validators = ValidatorGadget()
        self.docs = DocsScraper()
        
        self.method_table_mapping = self.create_method_table_mapping()
        
        self.update_all_column_docs()
        
        
    def read_clickhouse_config_from_env(self) -> Tuple[str, str, str]:
        """Reads Clickhouse configuration from environment variables."""
        clickhouse_user = os.getenv("CLICKHOUSE_USER", "default_user")
        clickhouse_password = os.getenv("CLICKHOUSE_PASSWORD", "default_password")
        url = os.getenv("CLICKHOUSE_URL", "http://localhost")

        logging.info("Clickhouse configs set")
        return url, clickhouse_user, clickhouse_password

    def read_clickhouse_config_locally(self) -> Tuple[str, str, str]:
        """Reads Clickhouse configuration from the config file."""
        with open(self.config_path, 'r') as file:
            config = json.load(file)
        clickhouse_user = config.get("CLICKHOUSE_USER", "default_user")
        clickhouse_password = config.get("CLICKHOUSE_PASSWORD", "default_password")
        url = config.get("CLICKHOUSE_URL", "http://localhost")

        logging.info("Clickhouse configs set")
        return url, clickhouse_user, clickhouse_password
    
    @column_check_decorator
    def _get_data(self, *args, **kwargs):
         return self.data_retriever.get_data(*args, **kwargs)

    def get_blockevent_of_slot(self, slot: Optional[int] = None, columns: Optional[str] = "*", where: Optional[str] = None, 
                       time_interval: Optional[str] = None, network: str = "mainnet", max_retries: int = 1, 
                       groupby: str = None, orderby: Optional[str] = None, final_condition: Optional[str] = None, limit: int = None,
                       store_result_in_parquet: bool = None, custom_data_dir: str = None) -> Any:
        return self._get_data(
            data_table='beacon_api_eth_v1_events_block',
            slot=slot, 
            columns=columns, 
            where=where, 
            time_interval=time_interval, 
            network=network, 
            groupby=groupby,
            orderby=orderby,
            final_condition=final_condition,
            limit=limit,
            store_result_in_parquet=store_result_in_parquet,
            custom_data_dir=custom_data_dir
        )
     
    def get_attestation_of_slot(self, slot: Optional[int] = None, columns: Optional[str] = "*", where: Optional[str] = None, 
                 time_interval: Optional[str] = None, network: str = "mainnet", max_retries: int = 1, 
                 groupby: str = None, orderby: Optional[str] = None, final_condition: Optional[str] = None, 
                 limit: int = None, store_result_in_parquet: bool = None, custom_data_dir: str = None) -> Any:
        res = self._get_data(
            data_table='canonical_beacon_elaborated_attestation',
            slot=slot, 
            columns=columns, 
            where=where, 
            time_interval=time_interval, 
            network=network, 
            groupby=groupby,
            orderby=orderby,
            final_condition=final_condition,
            limit=limit,
            store_result_in_parquet=store_result_in_parquet,
            custom_data_dir=custom_data_dir
        )
        print("validators")
        print( res.columns)
        if "validators" in set(res.columns):
            res["validators"] = res["validators"].apply(lambda x: eval(x))
            res = res.explode("validators").reset_index(drop=True)
        
        return res    
 
    def get_attestation_event_of_slot(self, slot: Optional[int] = None, columns: Optional[str] = "*", 
                where: Optional[str] = None, time_interval: Optional[str] = None, network: str = "mainnet", 
                max_retries: int = 1, groupby: str = None, orderby: Optional[str] = None, 
                final_condition: Optional[str] = None, limit: int = None, 
                store_result_in_parquet: bool = None, custom_data_dir: str = None,
                add_final_keyword_to_query: bool = False) -> Any:
        
        res = self._get_data(
            data_table='beacon_api_eth_v1_events_attestation',
            slot=slot, 
            columns=columns, 
            where=where, 
            time_interval=time_interval, 
            network=network, 
            groupby=groupby,
            orderby=orderby,
            final_condition=final_condition,
            limit=limit,
            store_result_in_parquet=store_result_in_parquet,
            custom_data_dir=custom_data_dir,
            add_final_keyword_to_query=add_final_keyword_to_query
        )
        return res
 
    def get_proposer_of_slot(self, slot: Optional[int] = None, columns: Optional[str] = "*", where: Optional[str] = None, 
                      time_interval: Optional[str] = None, network: str = "mainnet", max_retries: int = 1, 
                      groupby: str = None, orderby: Optional[str] = None, final_condition: Optional[str] = None, limit: int = None,
                      store_result_in_parquet: bool = None, custom_data_dir: str = None) -> Any:
        return self._get_data(
            data_table='canonical_beacon_proposer_duty',
            slot=slot, 
            columns=columns, 
            where=where, 
            time_interval=time_interval, 
            network=network, 
            groupby=groupby,
            orderby=orderby,
            final_condition=final_condition,
            limit=limit,
            store_result_in_parquet=store_result_in_parquet,
            custom_data_dir=custom_data_dir
        )
    
    def get_reorgs_in_slots(self, slots: List[int] = None, where: Optional[str] = None, 
                   time_interval: Optional[str] = None, network: str = "mainnet", max_retries: int = 1, 
                   groupby: str = None, orderby: Optional[str] = None, final_condition: Optional[str] = None, limit: int = None,
                   store_result_in_parquet: bool = None, custom_data_dir: str = None) -> Any:
       
        potential_reorgs = self.data_retriever.get_data(
            data_table='beacon_api_eth_v1_events_chain_reorg',
            slot=slots, 
            columns="slot-depth", 
            where=where, 
            time_interval=time_interval, 
            network=network, 
            groupby=groupby,
            orderby=orderby,
            final_condition=final_condition,
            limit=limit,
            store_result_in_parquet=store_result_in_parquet,
            custom_data_dir=custom_data_dir
        )
        missed = self.get_missed_slots( 
            slots=[slots[0]-32, slots[-1]+31] if isinstance(slots, list) else None, 
            columns="slot", 
            where=where, 
            time_interval=time_interval, 
            network=network, 
            groupby=groupby,
            orderby=orderby,
            final_condition=final_condition,
            limit=limit,
            store_result_in_parquet=False,
            custom_data_dir=None,
            canonical=None
        )
        reorgs = sorted(set(potential_reorgs["slot-depth"].tolist()).intersection(missed))
        return reorgs
    
    def get_slots(self, slot: List[int] = None, columns: Optional[str] = "*", where: Optional[str] = None, 
                  time_interval: Optional[str] = None, network: str = "mainnet", max_retries: int = 1, 
                  groupby: str = None, orderby: Optional[str] = None, final_condition: Optional[str] = None, limit: int = None,
                  store_result_in_parquet: bool = None, custom_data_dir: str = None, add_missed: bool = True) -> Any:
        
        df = self._get_data(
            data_table='canonical_beacon_block',
            slot=slot, 
            columns=columns, 
            where=where, 
            time_interval=time_interval,
            network=network, 
            groupby=groupby,
            orderby=orderby,
            final_condition=final_condition,
            limit=limit,
            store_result_in_parquet=store_result_in_parquet,
            custom_data_dir=custom_data_dir
        )
        
        if add_missed:
            missed = self.get_missed_slots(canonical=df)
            missed_df = pd.DataFrame(missed, columns=['slot'])

            _d = 999999999
            for col, dtype in df.dtypes.items():
                if col != 'slot':  
                    if pd.api.types.is_numeric_dtype(dtype):
                        missed_df[col] = _d if "index" in col else 0
                    else:
                        missed_df[col] = "missed"

            df = pd.concat([df, missed_df], ignore_index=True)
            
            # Add proposer_index for slots that were missed
            if "proposer_index" in df.columns:
                _c = "proposer_validator_index"
                _c1 = "proposer_index"
                _p = self.get_proposer_of_slot(slot=[int(df.slot.min()), int(df.slot.max()+1)], columns=f"slot,{_c}")
                df[_c1] = df.apply(lambda x: _p[_p["slot"] == x["slot"]][_c].values[0] if x[_c1] == _d else x[_c1], axis=1)
            if orderby and "," not in orderby:
                df.sort_values(orderby, inplace=True)
        return df 
 
    def get_missed_slots(self, slots: List[int] = None, columns: Optional[str] = "*", 
            where: Optional[str] = None, time_interval: Optional[str] = None, 
            network: str = "mainnet", max_retries: int = 1, groupby: str = None, orderby: Optional[str] = None,
            final_condition: Optional[str] = None, limit: int = None, 
            store_result_in_parquet: bool = None, custom_data_dir: str = None, 
            canonical: Optional = None
        ) -> Any:
        if canonical is None:
            canonical = self.get_slots( 
                slot=[slots[0], slots[-1]] if isinstance(slots, list) else slots, 
                columns="slot", 
                where=where, 
                time_interval=time_interval, 
                network=network, 
                groupby=groupby,
                orderby=orderby,
                final_condition=final_condition,
                limit=limit,
                store_result_in_parquet=store_result_in_parquet,
                custom_data_dir=custom_data_dir,
                add_missed=False
            )      
        missed = set(range(canonical.slot.min(), canonical.slot.max()+1)) - set(canonical.slot.unique().tolist())
        return missed
    
    def get_duties_for_slots(self, slot: Optional[int] = None, columns: Optional[str] = "*", 
                            where: Optional[str] = None, time_interval: Optional[str] = None, 
                            network: str = "mainnet", max_retries: int = 1, groupby: str = None, orderby: Optional[str] = None,
                            final_condition: Optional[str] = None, limit: int = None, 
                            store_result_in_parquet: bool = None, custom_data_dir: str = None) -> Any:
        required_columns = ["slot", "validators"]
        committee = self._get_data(
            data_table='beacon_api_eth_v1_beacon_committee',
            slot=slot, 
            columns=",".join(list(dict.fromkeys(
                    [i.strip() for i in columns.split(",")] + required_columns 
            ))), 
            where=where, 
            time_interval=time_interval, 
            network=network, 
            groupby=groupby,
            orderby=orderby,
            final_condition=final_condition,
            limit=limit,
            store_result_in_parquet=store_result_in_parquet,
            custom_data_dir=custom_data_dir
        )
        committee["validators"] = committee["validators"].apply(lambda x: eval(x))
        duties = pd.DataFrame(columns=["slot", "validators"])
        for i in committee.slot.unique():
            _committee = committee[committee["slot"] == i]
            all_validators = sorted([item for sublist in _committee["validators"] for item in sublist])
            temp_df = pd.DataFrame({"slot": [i] * len(all_validators), "validators": all_validators})
            duties = pd.concat([duties, temp_df], ignore_index=True).drop_duplicates()
        return duties.reset_index(drop=True)
    
    def get_checkpoints_for_slot(self, slot: int):
        epoch_start_slot = int(slot // 32 * 32)
        last_epoch_start_slot = int(epoch_start_slot - 32)
        slots = self.get_slots(
            slot=[last_epoch_start_slot - 32, epoch_start_slot + 32], 
            columns="slot,block_root", 
            orderby="slot",
            add_missed=False
        )
        head, target, source = [None]*3
        
        _slot = slot
        while head == None:
            if len(slots[slots["slot"] == _slot]) > 0:
                head = slots[slots["slot"] == _slot]["block_root"].values[0]
            else:
                _slot -= 1
        
        _slot = epoch_start_slot
        while target == None:
            if len(slots[slots["slot"] == _slot]) > 0:
                target = slots[slots["slot"] == _slot]["block_root"].values[0]
            else:
                _slot -= 1
                
        _slot = last_epoch_start_slot
        while source == None:
            if len(slots[slots["slot"] == _slot]) > 0:
                source = slots[slots["slot"] == _slot]["block_root"].values[0]
            else:
                _slot -= 1
                
        return head, target, source            
    
    def get_elaborated_attestations(self, slot: Optional[int] = None, what: str = "source,target,head", 
                columns: Optional[str] = "*", where: Optional[str] = None, 
                time_interval: Optional[str] = None, network: str = "mainnet", max_retries: int = 1, 
                groupby: str = None, orderby: Optional[str] = None, final_condition: Optional[str] = None,
                limit: int = None, store_result_in_parquet: bool = None, 
                custom_data_dir: str = None, only_status="correct,failed,offline") -> Any:

        if not isinstance(slot, list):
            slot = [slot, slot + 1]
            
        required_columns = ["source_root", "target_root", "validators", "beacon_block_root"]
        attestations = self.get_attestation_of_slot(
            slot=[slot[0]//32 * 32, slot[-1]//32 * 32 + 32], 
            columns=",".join(list(
                dict.fromkeys(
                    [i.strip() for i in columns.split(",")] + required_columns
                )
            )),
            where=where, 
            time_interval=time_interval, 
            network=network, 
            groupby=groupby,
            orderby=orderby,
            final_condition=final_condition,
            limit=limit,
            store_result_in_parquet=store_result_in_parquet,
            custom_data_dir=custom_data_dir
        )

        duties = self.get_duties_for_slots(
            slot=[int(slot[0]//32 * 32), int(slot[-1]//32 * 32 + 32)], 
            columns="slot, validators", 
            where=where, 
            time_interval=time_interval, 
            network=network, 
            groupby=groupby,
            orderby="slot",
            final_condition=final_condition,
            limit=limit,
            store_result_in_parquet=store_result_in_parquet,
            custom_data_dir=custom_data_dir
        ) 

        # Initialize empty list to store all status data
        status_data = []

        # Process each slot
        for _slot in tqdm(sorted(attestations.slot.unique()), desc="Processing slots"):
            head, target, source = self.get_checkpoints_for_slot(_slot)
            _attestations = attestations[attestations["slot"] == _slot]
            _duties = duties[duties["slot"] == _slot]
            _all = set(_duties.validators.tolist())
            voting_validators = set(_attestations.validators.tolist())

            def process_vote(vote_type: str, root_value: str) -> None:
                correct = set(_attestations[_attestations[f"{vote_type}_root"] == root_value].validators.tolist())
                correct = correct.intersection(_all)
                failing_validators = _all.intersection(voting_validators) - correct
                offline_validators = _all - voting_validators
                
                if "correct" in only_status:
                    status_data.extend([(_slot, v, "correct", vote_type) for v in correct])
                if "failed" in only_status:
                    status_data.extend([(_slot, v, "failed", vote_type) for v in failing_validators])
                if "offline" in only_status:
                    status_data.extend([(_slot, v, "offline", vote_type) for v in offline_validators])

            if "source" in what:
                process_vote("source", source)
            if "target" in what:
                process_vote("target", target)
            if "head" in what:
                process_vote("beacon_block", head)

        final_df = pd.DataFrame(status_data, columns=["slot", "validator", "status", "vote_type"])
        final_df = final_df.drop_duplicates().reset_index(drop=True)

        return final_df  
 
    def get_beacon_block_v2(self, slots: List[int] = None, columns: Optional[str] = "*", 
                where: Optional[str] = None, time_interval: Optional[str] = None, 
                network: str = "mainnet", max_retries: int = 1, groupby: str = None, orderby: Optional[str] = None,
                final_condition: Optional[str] = None, limit: int = None, 
                store_result_in_parquet: bool = None, custom_data_dir: str = None) -> Any:
        block = self._get_data(
            data_table='beacon_api_eth_v2_beacon_block',
            slot=slots, 
            columns=columns,
            where=where, 
            time_interval=time_interval, 
            network=network, 
            groupby=groupby,
            orderby=orderby,
            final_condition=final_condition,
            limit=limit,
            store_result_in_parquet=store_result_in_parquet,
            custom_data_dir=custom_data_dir
        )
        return block

    def get_block_size(self, slots: List[int], columns: Optional[str] = "*", where: Optional[str] = None, 
                time_interval: Optional[str] = None, network: str = "mainnet", max_retries: int = 1, 
                groupby: str = None, orderby: Optional[str] = None, final_condition: Optional[str] = None, limit: int = None, 
                store_result_in_parquet: bool = None, custom_data_dir: str = None, add_missed: bool = True):
        if isinstance(slots, int):
            slots = [slots, slots+1]
        if columns == None:
            columns = []
        sizes = self.get_slots( 
            slot=[slots[0], slots[-1]] if isinstance(slots, list) else slots, 
            columns= ",".join(list(dict.fromkeys(columns.split(",") + ["block_total_bytes_compressed", "block_total_bytes"]))),
            where=where, #" AND ".join(where + ["meta_client_geo_country = 'Finland'", "meta_client_name = 'utility-xatu-cannon'"]), 
            time_interval=time_interval, 
            network=network, 
            orderby="slot",
            groupby=groupby,
            final_condition=final_condition,
            limit=limit,
            store_result_in_parquet=store_result_in_parquet,
            custom_data_dir=custom_data_dir,
            add_missed=add_missed
        ) 
        if "execution_payload_blob_gas_used" in sizes.columns:
            sizes["blobs"] = sizes["execution_payload_blob_gas_used"] // 131072
            sizes.drop("execution_payload_blob_gas_used", axis=1, inplace=True)
        return sizes
    
    def get_blob_events_of_slot(self, slot: Optional[int] = None, columns: Optional[str] = "*", 
            where: Optional[str] = None, time_interval: Optional[str] = None, 
            network: str = "mainnet", max_retries: int = 1, groupby: str = None, orderby: Optional[str] = None,
            final_condition: Optional[str] = None, limit: int = None, 
            store_result_in_parquet: bool = None, custom_data_dir: str = None) -> Any:
        return self._get_data(
            data_table='beacon_api_eth_v1_events_blob_sidecar',
            slot=slot, 
            columns=columns, 
            where=where, 
            time_interval=time_interval,
            network=network, 
            groupby=groupby,
            orderby=orderby,
            final_condition=final_condition,
            limit=limit,
            store_result_in_parquet=store_result_in_parquet,
            custom_data_dir=custom_data_dir
        )
    
    def get_blobs_of_slot(self, slot: Optional[int] = None, columns: Optional[str] = "*", 
            where: Optional[str] = None, time_interval: Optional[str] = None, 
            network: str = "mainnet", max_retries: int = 1, groupby: str = None, orderby: Optional[str] = None,
            final_condition: Optional[str] = None, limit: int = None, 
            store_result_in_parquet: bool = None, custom_data_dir: str = None) -> Any:
        return self._get_data(
            data_table='canonical_beacon_blob_sidecar',
            slot=slot, 
            columns=columns, 
            where=where, 
            time_interval=time_interval,
            network=network, 
            groupby=groupby,
            orderby=orderby,
            final_condition=final_condition,
            limit=limit,
            store_result_in_parquet=store_result_in_parquet,
            custom_data_dir=custom_data_dir
        )
 
    def get_withdrawals_of_slot(self, slot: Optional[int] = None, columns: Optional[str] = "*", 
            where: Optional[str] = None, time_interval: Optional[str] = None, 
            network: str = "mainnet", max_retries: int = 1, groupby: str = None, orderby: Optional[str] = None,
            final_condition: Optional[str] = None, limit: int = None, 
            store_result_in_parquet: bool = None, custom_data_dir: str = None) -> Any:
        """
        Retrieve withdrawal data for a given slot.

        Parameters:
        -----------
        slot : Optional[int]
            Slot number to retrieve data for.
        columns : Optional[str]
            Columns to retrieve, default is "*".
        where : Optional[str]
            Optional condition to filter data.
        time_interval : Optional[str]
            Time range to filter data.
        network : str
            Network name, default is "mainnet".
        max_retries : int
            Number of retry attempts, default is 1.
        groupby : str
            Grouping condition.
        orderby : Optional[str]
            Column to order by.
        final_condition : Optional[str]
            Final filter condition.
        limit : int
            Limit the number of rows retrieved.
        store_result_in_parquet : bool
            If true, store result in a Parquet file.
        custom_data_dir : str
            Directory to store Parquet data.

        Available columns (documentation only):
        """
        return self._get_data(
            data_table='canonical_beacon_block_withdrawal',
            slot=slot, 
            columns=columns, 
            where=where, 
            time_interval=time_interval,
            network=network, 
            groupby=groupby,
            orderby=orderby,
            final_condition=final_condition,
            limit=limit,
            store_result_in_parquet=store_result_in_parquet,
            custom_data_dir=custom_data_dir
        )
    
    def execute_query(self, query: str, columns: Optional[str] = "*", time_interval: Optional[str] = None) -> Any:
        return self.client.execute_query(query, columns)
    
    def get_docs(self, table_name: str = None, print_loading: bool = True):
        """
        Retrieves table information, such as available columns, from the DocsScraper.
        """
        if table_name in self.method_table_mapping.keys():
            table_name = self.method_table_mapping[table_name]
        if print_loading:
            print(f"Retrieving schema for table {table_name}")
        return self.docs.get_table_info(table_name)
    
    def create_method_table_mapping(self):
        """
        Dynamically creates the method-to-table mapping by inspecting each method
        and extracting the table name passed to self._get_data().
        """
        method_table_mapping = {}

        # Get all the methods in the class
        for method_name, method in inspect.getmembers(self, predicate=inspect.ismethod):
            # Inspect the method's source code
            source = inspect.getsource(method)
            
            # Dedent the source code to remove unnecessary indentation
            dedented_source = textwrap.dedent(source)
            
            # Extract the table name from the source code
            table_name = self.extract_table_name_from_source(dedented_source)
            if table_name:
                method_table_mapping[method_name] = table_name

        return method_table_mapping
    
    def extract_table_name_from_source(self, source: str, current_func_name: str = None, depth: int = 0, max_depth: int = 5) -> Optional[str]:
        """
        Extracts the table name from the method's source code by looking for
        the first argument or keyword argument `data_table` in the call to 
        self._get_data() or self.get_data(), with recursion to resolve nested calls.

        Recurses up to `max_depth` times when encountering function calls.
        """
        try:
            # Check if we've exceeded the maximum recursion depth
            if depth > max_depth:
                return None

            # Parse the source code into an abstract syntax tree (AST)
            tree = ast.parse(source)

            # Iterate over all nodes in the AST
            for node in ast.walk(tree):
                # Look for calls to self._get_data() or self.get_data()
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):

                    if node.func.attr in ['get_data', '_get_data']:
                        # Check if the table name is passed as a keyword argument
                        for keyword in node.keywords:
                            if keyword.arg == 'data_table':
                                if isinstance(keyword.value, ast.Constant):
                                    table_name = keyword.value.value
                                    return table_name
                            
                        # Handle positional arguments
                        if node.args and len(node.args) > 0:
                            first_arg = node.args[0]

                            if isinstance(first_arg, ast.Constant):
                                table_name = first_arg.value
                                return table_name
                          
                    # If it's not a get_data or _get_data call, check which function is being called
                    elif isinstance(node.func.value, ast.Name):
                        called_function = node.func.attr

                        # Load the source of the called function and recurse
                        try:
                            called_func_source = inspect.getsource(getattr(self, called_function))
                            # Clean up the source to handle any potential indentation issues
                            cleaned_source = textwrap.dedent(called_func_source).strip()
                            return self.extract_table_name_from_source(cleaned_source, called_function, depth + 1, max_depth)
                        except (AttributeError, OSError) as e:
                            pass
               
        except Exception as e:
            pass
        
        return None


    def update_all_column_docs(self):
        """
        Updates the docstrings of all high-level methods by using the table information stored in DocsScraper.
        """
        # Fetch the table info from DocsScraper (already stored during initialization)
        all_table_info = {table: self.get_docs(table, False) for table in self.method_table_mapping.values()}

        # Iterate through methods and update their docstrings
        for method_name, table_name in self.method_table_mapping.items():
            method = getattr(self, method_name, None)
            table_info = all_table_info.get(table_name)

            if table_info is not None and not table_info.empty:
                columns_doc = "\n".join([f"  - {col}" for col in table_info['Column']])
            else:
                columns_doc = "  No columns available."

            # Use a helper function to avoid closure issues
            self._wrap_method_with_columns(method_name, method, table_name, columns_doc)

    def _wrap_method_with_columns(self, method_name, method, table_name, columns_doc):
        """
        Helper function to wrap a method with a new docstring that includes the available columns.
        """
        # Define a wrapper function that maintains the original method behavior
        def method_wrapper(*args, **kwargs):
            return method(*args, **kwargs)

        # Update the docstring of the wrapper function
        method_wrapper.__doc__ = (method.__doc__ or "") + f"\nAvailable columns for {table_name}:\n{columns_doc}\n"

        # Replace the original method with the wrapped version
        setattr(self, method_name, method_wrapper)       
        
    def verify_columns(self, columns: str = None, table: str = None):
        if columns is None or table is None:
            return True
        assert type(columns) == str
        if columns == "*":
            return True
        if "," not in columns:
            columns += ","
        
        existing_columns = self.get_docs(table, False)
        if existing_columns is None:
            return True
        if "Column" in existing_columns.columns:
            existing_columns = existing_columns["Column"].tolist()
        else:
            return True
        for c in [i for i in columns.split(",") if i != ""]:
            if c.strip() not in existing_columns:
                if c.strip == "" or c.strip == " ":
                    continue
                print("\n" + f"{c.strip()} not in {table} with columns:" + '\n'.join(existing_columns))
                print("\nExisting columns: " + '\n'.join(self.get_docs(table, False)['Column'].to_list()))
                return False
        return True

    def preview_result(self, func: Callable[..., Any], limit: int = 100, **kwargs) -> Any:
        kwargs['limit'] = limit
        return func(**kwargs)
    
    def __str__(self) -> str:
        return f"PyXatu(config_path={self.config_path}, user={self.clickhouse_user}, url={self.clickhouse_url})"

    def __repr__(self) -> str:
        return self.__str__()