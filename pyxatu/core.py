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
    def __init__(
        self, 
        config_path: Optional[str] = None, 
        use_env_variables: bool = False, 
        log_level: str = 'INFO', 
        relay: str = None
    ) -> None:
        if not logging.getLogger().hasHandlers():
            logging.basicConfig(level=getattr(logging, log_level.upper()), format='%(asctime)s - %(levelname)s - %(message)s')
        
        default_path = os.path.join(Path.home(), '.pyxatu_config.json')
        if not config_path is None and not os.path.isfile(default_path):
            raise ValueError("~/.pyxatu_config.json file not found\nRun `xatu setup` to copy the default config file to your HOME directory and then add your credentials to it. Alternatively you can use environment variables.")
        
        if config_path is None:
            self.config_path = default_path
        else:
            self.config_path = config_path
        
        if use_env_variables:
            config = self.read_clickhouse_config_from_env()
            self.clickhouse_url, self.clickhouse_user, self.clickhouse_password = config
        else:
            print("Config Path: ", self.config_path)
            print(os.path.isfile(self.config_path))
            print(os.listdir("/home/devops/"))
            assert os.path.isfile(self.config_path) == True, "Config file not found."
            config = self.read_clickhouse_config_locally()
            self.clickhouse_url, self.clickhouse_user, self.clickhouse_password = config

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
    
    def execute_query(self, query: str, columns: Optional[str] = "*", time_interval: Optional[str] = None) -> Any:
        return self.client.execute_query(query, columns)
    
    @property
    def validators(self):
        if not hasattr(self, '_validators'):
            self._validators = ValidatorGadget()
        return self._validators
    
    @property
    def docs(self):
        if not hasattr(self, '_docs'):
            self._docs = DocsScraper()
        return self._docs
    
    def get_docs(self, table_name: str = None, print_loading: bool = True):
        """
        Retrieves table information, such as available columns, from the DocsScraper.
        """
        if table_name in self.method_table_mapping.keys():
            table_name = self.method_table_mapping[table_name]
        if print_loading:
            print(f"Retrieving schema for table {table_name}")
        return self.docs.get_table_info(table_name)
    
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
            "add_final_keyword_to_query": [bool, type(None)]
        }

        types_list = []

        # Iterate over the provided arguments and get the corresponding type
        for arg in arguments:
            if arg in argument_types:
                types_list.append(argument_types[arg])
            else:
                print(arg)
                (f"Argument '{arg}' is not recognized.")

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
        **kwargs
    ) -> Any:

        if not isinstance(slot, list):
            slot = [slot, slot + 1]
        
        required_columns = ["slot", "source_root", "target_root", "validators", "beacon_block_root"]
        
        kwargs["slot"] = [slot[0]//32 * 32, slot[-1]//32 * 32 + 32]
        kwargs["columns"] = self.clean_columns(columns, required_columns)
        kwargs["orderby"] = orderby
        attestations = self.get_attestation(**kwargs)

        kwargs["columns"] = "slot, validators"
        kwargs["limit"] = None
        kwargs["orderby"] = None
        duties = self.get_duties(**kwargs) 

        # Initialize empty list to store all status data
        status_data = []

        # Process each slot
        for _slot in tqdm(sorted(attestations.slot.unique()), desc="Processing slots"):
            head, target, source = self.get_checkpoints(_slot)
            _attestations = attestations[attestations["slot"] == _slot]
            _duties = duties[duties["slot"] == _slot]
            assert len(_duties) > 0, "Something wrong with retrieving duties."
            _all = set(_duties.validators.tolist())
            voting_validators = set(_attestations.validators.tolist())

            def process_vote(vote_type: str, root_value: str) -> None:
                correct = set(_attestations.loc[_attestations[f"{vote_type}_root"] == root_value, 'validators'])
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

        final_df = pd.DataFrame(status_data, columns=["slot", "validator", "status", "vote_type"]).sort_values("slot")
        final_df = final_df.drop_duplicates().reset_index(drop=True)

        return final_df  
 
    def get_beacon_block_v2(self, **kwargs) -> Any:
        block = self._generic_getter('beacon_api_eth_v2_beacon_block', **kwargs)
        return block

    def get_block_size(self, orderby: Optional[str] = "slot", **kwargs) -> Any:
        if isinstance(slots, int):
            slots = [slots, slots+1]
        if columns == None:
            columns = []
            
        required_columns = ["slot", "block_total_bytes_compressed", "block_total_bytes", "execution_payload_blob_gas_used"]
        kwargs["slot"] = [slots[0], slots[-1]] if isinstance(slots, list) else slots
        kwargs["columns"] = self.clean_columns(columns, required_columns)
        
        sizes = self.get_slots(**kwargs) 
        if "execution_payload_blob_gas_used" in sizes.columns:
            sizes["blobs"] = sizes["execution_payload_blob_gas_used"] // 131072
            sizes.drop("execution_payload_blob_gas_used", axis=1, inplace=True)
        return sizes
    
    def get_blob_events(self, **kwargs) -> Any:
        return self._generic_getter('beacon_api_eth_v1_events_blob_sidecar', **kwargs)
    
    def get_blobs(self, **kwargs) -> Any:
        return self._generic_getter('canonical_beacon_blob_sidecar', **kwargs)
 
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
        
        existing_columns = self.get_docs(table, False)
        if existing_columns is None:
            return True
        if "Column" in existing_columns.columns:
            existing_columns = existing_columns["Column"].tolist()
        else:
            return True
        for c in [i for i in columns.split(",") if i != ""]:
            _c = self.helpers.extract_inside_brackets(c.strip())
            if _c not in existing_columns:
                if _c == "" or _c == " ":
                    continue
                print("\n" + f"{_c.strip()} not in {table} with columns:" + '\n'.join(existing_columns))
                print("\nExisting columns: " + '\n'.join(self.get_docs(table, False)['Column'].to_list()))
                return False
        return True
    
    def clean_columns(self, columns: str, required_columns: List[str]) -> str:
        columns_list = [i.strip() for i in columns.split(",") if i != "*"]
        return ",".join(list(dict.fromkeys(columns_list + required_columns)))

    def preview_result(self, func: Callable[..., Any], limit: int = 100, **kwargs) -> Any:
        kwargs['limit'] = limit
        return func(**kwargs)
    
    def __str__(self) -> str:
        return f"PyXatu(config_path={self.config_path}, user={self.clickhouse_user}, url={self.clickhouse_url})"

    def __repr__(self) -> str:
        return self.__str__()