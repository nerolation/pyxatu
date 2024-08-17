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
            self.clickhouse_url, self.clickhouse_user, self.clickhouse_password = self.read_clickhouse_config_locally()

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

    def read_clickhouse_config_locally(self) -> Tuple[str, str, str]:
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
        res = self.data_retriever.get_data(
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
        if "validators" in res.columns.tolist():
            res["validators"] = res["validators"].apply(lambda x: eval(x))
            res = res.explode("validators").reset_index(drop=True)
        
        return res

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
    
    def get_reorgs_in_slots(self, slots: List[int] = None, where: Optional[str] = None, 
                   time_interval: Optional[str] = None, network: str = "mainnet", max_retries: int = 1, 
                   orderby: Optional[str] = None, final_condition: Optional[str] = None, limit: int = None,
                   store_result_in_parquet: bool = None, custom_data_dir: str = None) -> Any:
       
        potential_reorgs = self.data_retriever.get_data(
            'beaconchain_reorgs',
            slot=slots, 
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
        missed = self.get_missed_slots( 
            slots=[slots[0]-32, slots[-1]+31] if isinstance(slots, list) else None, 
            columns="slot", 
            where=where, 
            time_interval=time_interval, 
            network=network, 
            orderby=orderby,
            final_condition=final_condition,
            limit=limit,
            store_result_in_parquet=False,
            custom_data_dir=None
        )
        reorgs = sorted(set(potential_reorgs["slot-depth"].tolist()).intersection(missed))
        return reorgs
    
    def get_slots(self, slot: List[int] = None, columns: Optional[str] = "*", where: Optional[str] = None, 
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
    
    def get_missed_slots(self, slots: List[int] = None, columns: Optional[str] = "*", 
                            where: Optional[str] = None, time_interval: Optional[str] = None, 
                            network: str = "mainnet", max_retries: int = 1, orderby: Optional[str] = None,
                            final_condition: Optional[str] = None, limit: int = None, 
                            store_result_in_parquet: bool = None, custom_data_dir: str = None) -> Any:
        canonical = self.get_slots( 
            slot=[slots[0], slots[-1]] if isinstance(slots, list) else None, 
            columns="slot", 
            where=where, 
            time_interval=time_interval, 
            network=network, 
            orderby=orderby,
            final_condition=final_condition,
            limit=limit,
            store_result_in_parquet=store_result_in_parquet,
            custom_data_dir=custom_data_dir
        )      
        missed = set(range(canonical.slot.min(), canonical.slot.max()+1)) - set(canonical.slot.unique().tolist())
        return missed
    
    def get_duties_for_slots(self, slot: Optional[int] = None, columns: Optional[str] = "*", 
                            where: Optional[str] = None, time_interval: Optional[str] = None, 
                            network: str = "mainnet", max_retries: int = 1, orderby: Optional[str] = None,
                            final_condition: Optional[str] = None, limit: int = None, 
                            store_result_in_parquet: bool = None, custom_data_dir: str = None) -> Any:
        
        committee= self.data_retriever.get_data(
            'beaconchain_beacon_committee',
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
        committee["validators"] = committee["validators"].apply(lambda x: eval(x))
        duties = pd.DataFrame(columns=["slot", "validator"])
        for i in committee.slot.unique():
            _committee = committee[committee["slot"] == i]
            all_validators = sorted([item for sublist in _committee["validators"] for item in sublist])
            temp_df = pd.DataFrame({"slot": [i] * len(all_validators), "validator": all_validators})
            duties = pd.concat([duties, temp_df], ignore_index=True).drop_duplicates()
        return duties.reset_index(drop=True)
    
    def get_checkpoints_for_slot(self, slot: int):
        epoch_start_slot = slot // 32 * 32
        last_epoch_start_slot = epoch_start_slot - 32
        
        slots = self.get_slots([last_epoch_start_slot - 32, epoch_start_slot+32], columns="slot,block_root", orderby="slot")
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
              
    
    def get_elaborated_attestations(self, epoch: Optional[int] = None, what: str = "source,target,head", 
                            columns: Optional[str] = "*", where: Optional[str] = None, time_interval: Optional[str] = None, 
                            network: str = "mainnet", max_retries: int = 1, orderby: Optional[str] = None,
                            final_condition: Optional[str] = None, limit: int = None, 
                            store_result_in_parquet: bool = None, custom_data_dir: str = None) -> Any:
        if not isinstance(epoch, list):
            epoch = [epoch, epoch+1]
        
        for epoch in range(epoch[0], epoch[-1]):
            duties = self.get_duties_for_slots(
                slot=[epoch[0] * 32, epoch[-1]*32+32], 
                columns="slot, validator", 
                where=where, 
                time_interval=time_interval, 
                network=network, 
                orderby=orderby,
                final_condition=final_condition,
                limit=limit,
                store_result_in_parquet=store_result_in_parquet,
                custom_data_dir=custom_data_dir
            ) 
            attestations = self.get_attestation_of_slot(
                slot=[epoch[0] * 32, epoch[-1]*32+32], 
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
            final_df = pd.DataFrame(columns=["slot", "validator", "status"])
            for _slot in sorted(attestations.slot.unique()):
                head, target, source = self.get_checkpoints_for_slot(_slot)
                _attestations = attestations[attestations["slot"] == _slot]
                _duties = duties[duties["slot"] == _slot]
                _all = set(_duties.validators.tolist())
                voting_validators = set(_attestations.validators.tolist())
                correct = set()
                #wrong = set()
                if "source" in what:
                    correct = set(_attestations[_attestations["source_root"] == source].validators.tolist())
                    correct = correct.intersection(_all)
                    #wrong += (voting_validators - correct)
                    _attestations = _attestations[_attestations["validators"].isin(correct)]
                if "target" in what:
                    correct = set(_attestations[_attestations["target_root"] == target].validators.tolist())
                    correct = correct.intersection(_all)
                    #wrong += (voting_validators - correct)
                    _attestations = _attestations[_attestations["validators"].isin(correct)]
                if "head" in what:
                    correct = set(_attestations[_attestations["beacon_block_root"] == head].validators.tolist())
                    correct = correct.intersection(_all)
                    #wrong += (voting_validators - correct)
                    _attestations = _attestations[_attestations["validators"].isin(correct)]
                correct_validators = set(_attestations.validators.tolist())
                failing_validators = _all.intersection(voting_validators) - correct_validators
                offline_validators = _all - voting_validators
                
                status_data = []
                status_data.extend([(_slot, v, "correct") for v in correct_validators])
                status_data.extend([(_slot, v, "failed") for v in failing_validators])
                status_data.extend([(_slot, v, "offline") for v in offline_validators])

                temp_df = pd.DataFrame(status_data, columns=["slot", "validator", "status"])
                final_df = pd.concat([final_df, temp_df], ignore_index=True)

            final_df = final_df.drop_duplicates()

            return final_df = final_df.reset_index(drop=True)

                


    
    def execute_query(self, query: str, columns: Optional[str] = "*", time_interval: Optional[str] = None) -> Any:
        return self.client.execute_query(query, columns)
    
    def preview_result(self, func: Callable[..., Any], limit: int = 100, **kwargs) -> Any:
        kwargs['limit'] = limit
        return func(**kwargs)
    
    def __str__(self) -> str:
        return f"PyXatu(config_path={self.config_path}, user={self.clickhouse_user}, url={self.clickhouse_url})"

    def __repr__(self) -> str:
        return self.__str__()