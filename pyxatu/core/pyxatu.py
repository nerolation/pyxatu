import os
import ast
import json
import time
import logging
import inspect
import textwrap
from io import StringIO
from pathlib import Path
from functools import wraps
from typing import Optional, List, Dict, Any, Callable, TypeVar, Tuple, Union

import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
from tqdm.auto import tqdm

from ..utils import CONSTANTS
from ..utils.helpers import PyXatuHelpers
from .client import ClickhouseClient
from ..integrations.mempool import MempoolConnector
from .retriever import DataRetriever
from ..integrations.validators import ValidatorGadget
from ..integrations.relay import MevBoostCaller


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
    def __init__(
        self, 
        config_path: Optional[str] = None, 
        use_env_variables: bool = False, 
        log_level: str = 'INFO', 
        relay: Optional[str] = None,
        no_validator_gadget: bool = False
    ) -> None:
        self._setup_logging(log_level)
        
        self.config_path = self._resolve_config_path(config_path)
        
        if use_env_variables:
            self.clickhouse_url, self.clickhouse_user, self.clickhouse_password = self._read_config_from_env()
        else:
            logging.info(f"Using config file: {self.config_path}")
            self.clickhouse_url, self.clickhouse_user, self.clickhouse_password = self._read_config_from_file()

        logging.info(f"Clickhouse URL: {self.clickhouse_url}, User: {self.clickhouse_user}")
        self.client = ClickhouseClient(self.clickhouse_url, self.clickhouse_user, self.clickhouse_password)
        
        self.data_retriever = DataRetriever(
            client=self.client,
            tables=CONSTANTS["TABLES"]
        )
        
        self.mevboost = MevBoostCaller()
        self.helpers = PyXatuHelpers()
        
        self.method_table_mapping = self.create_method_table_mapping()

        self.update_all_column_docs()
        
        self.no_validator_gadget = no_validator_gadget
    
    def _setup_logging(self, log_level: str) -> None:
        """Setup logging configuration if not already configured."""
        if not logging.getLogger().hasHandlers():
            logging.basicConfig(
                level=getattr(logging, log_level.upper()), 
                format='%(asctime)s - %(levelname)s - %(message)s'
            )
    
    def _resolve_config_path(self, config_path: Optional[str]) -> str:
        """Resolve and validate the configuration file path."""
        default_path = os.path.join(Path.home(), '.pyxatu_config.json')
        
        if config_path is None:
            if not os.path.isfile(default_path):
                raise ValueError(
                    "~/.pyxatu_config.json file not found\n"
                    "Run `xatu setup` to copy the default config file to your HOME directory "
                    "and then add your credentials to it. Alternatively you can use environment variables."
                )
            return default_path
        
        if not os.path.isfile(config_path):
            raise ValueError(f"Config file not found: {config_path}")
        
        return config_path
        
        
    def _read_config_from_env(self) -> Tuple[str, str, str]:
        """Reads Clickhouse configuration from environment variables."""
        clickhouse_user = os.getenv("CLICKHOUSE_USER", "default_user")
        clickhouse_password = os.getenv("CLICKHOUSE_PASSWORD", "default_password")
        url = os.getenv("CLICKHOUSE_URL", "http://localhost")

        logging.info("Clickhouse configs set")
        return url, clickhouse_user, clickhouse_password

    def _read_config_from_file(self) -> Tuple[str, str, str]:
        """Reads Clickhouse configuration from the config file."""
        with open(self.config_path, 'r') as file:
            config = json.load(file)
        clickhouse_user = config.get("CLICKHOUSE_USER", "default_user")
        clickhouse_password = config.get("CLICKHOUSE_PASSWORD", "default_password")
        url = config.get("CLICKHOUSE_URL", "http://localhost")

        logging.info("Clickhouse configs set")
        return url, clickhouse_user, clickhouse_password
    
    def execute_query(self, query: str, columns: Optional[str] = "*", time_interval: Optional[str] = None, handle_columns: bool = False) -> Any:
        return self.client.execute_query(query, columns)
    
    @property
    def validators(self):
        if self.no_validator_gadget == True:
            self._validators = None
        elif not hasattr(self, '_validators'):
            self._validators = ValidatorGadget(client=self.client)
        return self._validators
    
    @property
    def mempool(self):
        if not hasattr(self, '_mempool'):
            self._mempool = MempoolConnector()
        return self._mempool
    
        
    #@property
    #def docs(self):
    #    if not hasattr(self, '_docs'):
    #        self._docs = DocsScraper()
    #    return self._docs
    
    def get_docs(self, table_name: str = None, print_loading: bool = True):
        """
        Retrieves table information, such as available columns, from the DocsScraper.
        """
        if table_name in self.method_table_mapping.keys():
            table_name = self.method_table_mapping[table_name]
        if print_loading:
            logging.info(f"Retrieving schema for table {table_name}")
        return self.get_columns(table_name)
    

    def get_columns(self, table_name: str = None):
        return self.execute_query(f"""
            SELECT name
            FROM system.columns
            WHERE table = '{table_name}'
              AND database = 'default'
        """)
    
    def _get_types(self, arguments: List[str]) -> List[type]:
        """
        Returns a list of types corresponding to the given argument names based on predefined type hints.

        :param arguments: List of argument names as strings.
        :return: List of types corresponding to the given arguments.
        :raises ValueError: If an argument does not match the predefined list.
        """

        # Define a dictionary mapping argument names to their expected types
        argument_types = {
            "data_table": str,
            "slot": [list, int, type(None)],
            "columns": [str, type(None)],
            "where": [str, type(None)],
            "time_interval": [str, type(None)],
            "network": str,
            "max_retries": int,
            "groupby": [str, type(None)],
            "orderby": [str, type(None)],
            "final_condition": [str, type(None)],
            "limit": [int, type(None)],
            "store_result_in_parquet": [bool, type(None)],
            "custom_data_dir": [str, type(None)],
            "add_final_keyword_to_query": [bool, type(None)],
            "time_column": [str, type(None)],
            "no_slot_filter": [bool, type(None)]
        }

        types_list = []

        # Iterate over the provided arguments and get the corresponding type
        for arg in arguments:
            if arg in argument_types:
                types_list.append(argument_types[arg])
            else:
                logging.warning(f"Argument '{arg}' is not recognized.")

        return types_list

    
    @column_check_decorator
    def _get_data(self, *args, **kwargs):
        self.helpers.check_types(kwargs.values(), self._get_types(kwargs.keys()))
        return self.data_retriever.get_data(*args, **kwargs)

    def _generic_getter(self, table_name: str, **kwargs) -> Any:
        return self._get_data(data_table=table_name, **kwargs)

    def get_blockevent(self, **kwargs):
        return self._generic_getter('beacon_api_eth_v1_events_block', **kwargs)
     
    def get_attestation(self, **kwargs) -> Any:
        res = self._generic_getter('canonical_beacon_elaborated_attestation', **kwargs)
        if "validators" in set(res.columns):
            res["validators"] = res["validators"].apply(lambda x: eval(x))
            res = res.explode("validators").reset_index(drop=True)
        return res    
 
    def get_attestation_event(self, add_final_keyword_to_query: bool = False, **kwargs) -> Any:
        kwargs["add_final_keyword_to_query"] = add_final_keyword_to_query
        return self._generic_getter('beacon_api_eth_v1_events_attestation', **kwargs)
 
    def get_proposer(self, **kwargs) -> Any:
        return self._generic_getter('canonical_beacon_proposer_duty', **kwargs)
    
    def get_reorgs(self, **kwargs) -> Any:
        if not "columns" in kwargs:
            kwargs["columns"] = "(slot-depth) as reorged_slot"
        potential_reorgs = self.data_retriever.get_data(
            data_table='beacon_api_eth_v1_events_chain_reorg',
            **kwargs
        )
        if "reorged_slot" not in potential_reorgs.columns:
            potential_reorgs.columns = ["reorged_slot"]
            
        if "slot" in kwargs:
            slot = kwargs["slot"]
            kwargs["slot"] = [slot[0]-32, slot[-1]+31] if isinstance(slot, list) else slot
        else:
            kwargs["slot"] = None
        
        kwargs = {i: j for i, j in kwargs.items() if i != "columns"}
        kwargs["columns"] = "slot"
        kwargs["limit"] = None
        missed = self.get_missed_slots( 
            **kwargs
        )
        reorgs = sorted(set(potential_reorgs["reorged_slot"].tolist()).intersection(missed))
        return pd.DataFrame(reorgs, columns=["slot"])
    
    def get_slots(self, add_missed: bool = True, **kwargs) -> Any:
                
        df = self._generic_getter('canonical_beacon_block', **kwargs)
          
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
                _p = self.get_proposer(slot=[int(df.slot.min()), int(df.slot.max()+1)], columns=f"slot,{_c}")
                df[_c1] = df.apply(
                    lambda x: _p[_p["slot"] == x["slot"]][_c].values[0] if x[_c1] == _d else x[_c1], 
                    axis=1
                )
        if "orderby" in kwargs and "," not in kwargs["orderby"]:
            df.sort_values(kwargs["orderby"], inplace=True)
        return df 
 
    def get_missed_slots(self, canonical: Optional = None, **kwargs) -> Any:
        slot = kwargs.get("slot")
        if not slot is None:
            kwargs["slot"] = [slot[0], slot[-1]] if isinstance(slot, list) else slot
        
        if not "add_missed" in kwargs:
            kwargs["add_missed"] = False
        if canonical is None:
            canonical = self.get_slots( 
                **kwargs
            )      
        missed = set(range(canonical.slot.min(), canonical.slot.max()+1)) - set(canonical.slot.unique().tolist())
        return missed
    
    def get_duties(
        self, 
        columns: Optional[str] = "*",
        **kwargs
    ) -> Any:
        required_columns = ["slot", "validators"]            
        kwargs["columns"] = self.clean_columns(columns, required_columns)
        committee = self._generic_getter('beacon_api_eth_v1_beacon_committee', **kwargs)
        committee["validators"] = committee["validators"].apply(lambda x: eval(x))
        duties = pd.DataFrame(columns=["slot", "validators"])
        for i in committee.slot.unique():
            _committee = committee[committee["slot"] == i]
            all_validators = sorted([item for sublist in _committee["validators"] for item in sublist])
            temp_df = pd.DataFrame({"slot": [i] * len(all_validators), "validators": all_validators})
            duties = pd.concat([duties, temp_df], ignore_index=True).drop_duplicates()
        return duties.reset_index(drop=True)
    
    def get_checkpoints(self, slot: int):
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
    
    def get_elaborated_attestations(
        self, 
        slot: Optional[Union[int, List[int]]] = None, 
        columns: str = "*",
        what: str = "source,target,head", 
        orderby: Optional[str] = "slot", 
        only_status: Optional[str] = "correct,failed,offline",
        add_inclusion_delay: bool = True,
        **kwargs
    ) -> Any:
        """Get elaborated attestation data with validator status information."""
        slot_range = self._normalize_slot_range(slot)
        attestations, duties = self._fetch_attestation_data(slot_range, columns, orderby, **kwargs)
        
        status_data = []
        vote_types = [vt.strip() for vt in what.split(",")]
        status_types = [st.strip() for st in only_status.split(",")]
        
        for _slot in tqdm(sorted(attestations.slot.unique()), desc="Processing slots"):
            slot_data = self._process_slot_attestations(
                _slot, attestations, duties, vote_types, status_types, add_inclusion_delay
            )
            status_data.extend(slot_data)
        
        columns = ["slot", "validator", "status", "vote_type"]
        if add_inclusion_delay:
            columns.append("inclusion_delay")
            
        return pd.DataFrame(status_data, columns=columns).sort_values("slot").drop_duplicates().reset_index(drop=True)
    
    def _normalize_slot_range(self, slot: Optional[Union[int, List[int]]]) -> List[int]:
        """Convert slot input to a normalized range."""
        if not isinstance(slot, list):
            return [slot, slot + 1]
        return slot
    
    def _fetch_attestation_data(self, slot_range: List[int], columns: str, orderby: Optional[str], **kwargs):
        """Fetch attestation and duty data for the given slot range."""
        required_columns = ["slot", "block_slot", "source_root", "target_root", "validators", "beacon_block_root"]
        
        kwargs["slot"] = [slot_range[0]//32 * 32, slot_range[-1]//32 * 32 + 32]
        kwargs["columns"] = self.clean_columns(columns, required_columns)
        kwargs["orderby"] = orderby
        
        attestations = self.get_attestation(**kwargs)

        kwargs["columns"] = "slot, validators"
        kwargs["limit"] = None
        kwargs["orderby"] = None
        
        duties = self.get_duties(**kwargs)
        
        return attestations, duties
    
    def _process_slot_attestations(
        self, 
        slot: int, 
        attestations: pd.DataFrame, 
        duties: pd.DataFrame, 
        vote_types: List[str], 
        status_types: List[str],
        add_inclusion_delay: bool
    ) -> List[tuple]:
        """Process attestations for a single slot."""
        head, target, source = self.get_checkpoints(slot)
        slot_attestations = attestations[attestations["slot"] == slot]
        slot_duties = duties[duties["slot"] == slot]
        
        if len(slot_duties) == 0:
            raise ValueError(f"No duties found for slot {slot}")
        
        all_validators = set(slot_duties.validators.tolist())
        voting_validators = set(slot_attestations.validators.tolist())
        
        delay_map = self._build_delay_map(slot_attestations) if add_inclusion_delay else {}
        
        slot_data = []
        vote_map = {"source": source, "target": target, "head": head}
        
        for vote_type in vote_types:
            if vote_type in vote_map:
                root_col = "beacon_block_root" if vote_type == "head" else f"{vote_type}_root"
                slot_data.extend(
                    self._process_vote_type(
                        slot, slot_attestations, all_validators, voting_validators,
                        vote_type, root_col, vote_map[vote_type], status_types, delay_map
                    )
                )
        
        return slot_data
    
    def _build_delay_map(self, attestations: pd.DataFrame) -> dict:
        """Build inclusion delay mapping for validators."""
        delay_df = attestations[["validators", "slot", "block_slot"]].drop_duplicates().dropna()
        delay_df["delay"] = delay_df["block_slot"] - delay_df["slot"]
        return delay_df.set_index("validators")["delay"].to_dict()
    
    def _process_vote_type(
        self, 
        slot: int, 
        attestations: pd.DataFrame, 
        all_validators: set, 
        voting_validators: set,
        vote_type: str, 
        root_column: str, 
        expected_root: str, 
        status_types: List[str],
        delay_map: dict
    ) -> List[tuple]:
        """Process a specific vote type for validator status."""
        correct = set(attestations.loc[attestations[root_column] == expected_root, 'validators'])
        correct = correct.intersection(all_validators)
        failed = all_validators.intersection(voting_validators) - correct
        offline = all_validators - voting_validators
        
        results = []
        if "correct" in status_types:
            results.extend([(slot, v, "correct", vote_type, delay_map.get(v)) for v in correct])
        if "failed" in status_types:
            results.extend([(slot, v, "failed", vote_type, delay_map.get(v)) for v in failed])
        if "offline" in status_types:
            results.extend([(slot, v, "offline", vote_type, delay_map.get(v)) for v in offline])
        
        return results  
 
    def get_beacon_block_v2(self, **kwargs) -> Any:
        block = self._generic_getter('beacon_api_eth_v2_beacon_block', **kwargs)
        return block

    def get_block_size(self, orderby: Optional[str] = "slot", **kwargs) -> Any:
        if isinstance(kwargs["slot"], int):
            kwargs["slot"] = [kwargs["slot"], kwargs["slot"]+1]
        if not "columns" in kwargs:
            kwargs["columns"] = []
            
        required_columns = ["slot", "block_total_bytes_compressed", "block_total_bytes", "execution_payload_blob_gas_used", "execution_payload_transactions_total_bytes", "execution_payload_transactions_total_bytes_compressed"]
        kwargs["slot"] = [kwargs["slot"][0], kwargs["slot"][-1]] if isinstance(kwargs["slot"], list) else kwargs["slot"]
        kwargs["columns"] = self.clean_columns(kwargs["columns"], required_columns)
        
        sizes = self.get_slots(**kwargs) 
        if "execution_payload_blob_gas_used" in sizes.columns:
            sizes = sizes[sizes["execution_payload_blob_gas_used"] != "\\N"]
            sizes = sizes[sizes["execution_payload_blob_gas_used"] != "missed"]
            sizes["execution_payload_blob_gas_used"] = sizes["execution_payload_blob_gas_used"].astype(int)
            sizes["blobs"] = sizes["execution_payload_blob_gas_used"] // 131072
            sizes.drop("execution_payload_blob_gas_used", axis=1, inplace=True)
        return sizes
    
    def get_blob_events(self, **kwargs) -> Any:
        return self._generic_getter('beacon_api_eth_v1_events_blob_sidecar', **kwargs)
    
    def get_blobs(self, **kwargs) -> Any:
        return self._generic_getter('canonical_beacon_blob_sidecar', **kwargs)
    
    def get_transactions(self, **kwargs) -> Any:
        return self._generic_getter('canonical_beacon_block_execution_transaction', **kwargs)
    
    def get_el_transactions(self, **kwargs) -> Any:
        return self._generic_getter('canonical_execution_transaction', **kwargs)
    
    def get_mempool(self, **kwargs) -> Any:
        if isinstance(kwargs["slot"], list):
            kwargs["slot"] = [kwargs["slot"][0] - 32*100, kwargs["slot"][-1]]
        elif isinstance(kwargs["slot"], int):
            kwargs["slot"] = [kwargs["slot"] - 32*100, kwargs["slot"]]
        required_columns = ["event_date_time"]
        kwargs["columns"] = self.clean_columns(kwargs["columns"], required_columns)
        
        kwargs["time_column"] = "event_date_time"
        kwargs["no_slot_filter"] = True
        return self._generic_getter('mempool_transaction', **kwargs)
                              
    def get_elaborated_transactions(self, **kwargs) -> Any:
        transactions = self.get_transactions(**kwargs)
        kwargs["columns"] = ["hash"]
        
        if isinstance(kwargs["slot"], int):
            kwargs["slot"] = [kwargs["slot"]]
        
        slots = kwargs["slot"]
        mempool_hash_set = set()
        
        for slot in slots:
            kwargs["slot"] = slot
            xatu_data = self.get_mempool(**kwargs)
            xatu_data = set(xatu_data["hash"].str.lower())
            
            blocknative_data = self.mempool.download_blocknative_mempool_data(self.helpers.slot_to_time(kwargs["slot"]))
            flashbots_data = self.mempool.download_flashbots_mempool_data(self.helpers.slot_to_time(kwargs["slot"]))

            blocknative_data = set(blocknative_data["hash"])
            flashbots_data = set(flashbots_data["hash"])

            logging.info(f"Transactions found in Xatu mempool: {len(xatu_data.intersection(set(transactions['hash'])))}")
            logging.info(f"Transactions found in Blocknative data: {len(blocknative_data.intersection(set(transactions['hash'])))}")
            logging.info(f"Transactions found in Flashbots data: {len(flashbots_data.intersection(set(transactions['hash'])))}")

            mempool_hash_set = mempool_hash_set.union(xatu_data)
            mempool_hash_set = mempool_hash_set.union(blocknative_data)
            mempool_hash_set = mempool_hash_set.union(flashbots_data)

            logging.info(f"Total transactions found: {len(mempool_hash_set)}")
        transactions["private"] = ~transactions["hash"].str.lower().isin(mempool_hash_set)
        return transactions
 
    def get_withdrawals(self, **kwargs) -> Any:
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
        return self._generic_getter('canonical_beacon_block_withdrawal', **kwargs)

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
    
    def extract_table_name_from_source(
        self, 
        source: str, 
        current_func_name: str = None, 
        depth: int = 0, 
        max_depth: int = 5
    ) -> Optional[str]:
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

                    if node.func.attr in ['_generic_getter', 'get_data', '_get_data']:
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
                            return self.extract_table_name_from_source(
                                cleaned_source, called_function, 
                                depth + 1, 
                                max_depth
                            )
                        except (AttributeError, OSError) as e:
                            pass
               
        except Exception as e:
            pass
        
        return None


    def update_all_column_docs(self):
        """
        Updates the docstrings of all high-level methods.
        """
        self.all_table_info = {table: self.get_columns(table)for table in self.method_table_mapping.values()}

        # Iterate through methods and update their docstrings
        for method_name, columns in self.method_table_mapping.items():
            method = getattr(self, method_name, None)
            table_info = self.all_table_info.get(columns)

            if table_info is not None and not table_info.empty:
                columns_doc = "\n".join([f"  - {col}" for col in table_info[0]])
            else:
                columns_doc = "  No columns available."

            # Use a helper function to avoid closure issues
            self._wrap_method_with_columns(method_name, method, columns, columns_doc)

    def _wrap_method_with_columns(
        self, 
        method_name: str, 
        method: Optional[Callable], 
        table_name: str, 
        columns_doc: str
    ) -> None:
        """
        Helper function to wrap a method with a new docstring that includes the available columns.

        :param method_name: The name of the method to wrap.
        :param method: The method to be wrapped.
        :param table_name: The name of the table associated with the method.
        :param columns_doc: The documentation string containing the available columns.
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
        
        existing_columns = self.all_table_info.get(table)
        if existing_columns is None:
            return True
        if 0 in existing_columns.columns:
            existing_columns = existing_columns[0].tolist()
        else:
            return True
        for c in [i for i in columns.split(",") if i != ""]:
            if " as " in c:
                c = c.split(" as ")[0].strip()
            _c = self.helpers.extract_inside_brackets(c.strip())
            if _c not in existing_columns:
                if _c == "" or _c == " ":
                    continue
                print("\n" + f"{_c.strip()} not in {table} with columns:" + '\n'.join(existing_columns))
                print("\nExisting columns: " + '\n'.join(self.all_table_info.get(table)[0].tolist()))
                return False
        return True
    
    def clean_columns(self, columns: str, required_columns: List[str]) -> str:
        if isinstance(columns, str):
            columns = [i.strip() for i in columns.split(",") if i != "*"]
        return ",".join(list(dict.fromkeys(columns + required_columns)))

    def preview_result(self, func: Callable[..., Any], limit: int = 100, **kwargs) -> Any:
        kwargs['limit'] = limit
        return func(**kwargs)
    
    def __str__(self) -> str:
        return f"PyXatu(config_path={self.config_path}, user={self.clickhouse_user}, url={self.clickhouse_url})"

    def __repr__(self) -> str:
        return self.__str__()