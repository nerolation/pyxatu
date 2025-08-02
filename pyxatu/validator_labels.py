"""Validator label mapping for Ethereum validators."""

import json
import logging
import hashlib
from pathlib import Path
from typing import Dict, Optional, List, Set, Any, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
import codecs

import pandas as pd
import requests
import re

from .core.clickhouse_client import ClickHouseClient
from .config import ConfigManager


@dataclass
class EntityMapping:
    """Entity mapping configuration."""
    entity: str
    category: str
    depositor_addresses: Set[str] = field(default_factory=set)


class ValidatorLabelManager:
    """Manages validator labels and entity mappings."""
    
    BEACON_DEPOSIT_CONTRACT = "0x00000000219ab540356cBB839Cbe05303d7705Fa".lower()
    BEACON_DEPOSIT_CONTRACT_BLOCK = 11184524
    
    # Batch deposit contracts
    BATCH_CONTRACTS = {
        "0x9b8c989ff27e948f55b53bb19b3cc1947852e394",  # Kiln 1
        "0x1e68238ce926dec62b3fbc99ab06eb1d85ce0270",  # Kiln 2
        "0x1bdc639eabf1c5ebc020bb79e2dd069a8b6fe865",  # BatchDeposit
        "0xe8239b17034c372cdf8a5f8d3ccb7cf1795c4572",  # Batch Deposit
    }
    
    # Lido contract and event signatures
    LIDO_CONTRACT = "0x55032650b14df07b85bF18A3a3eC8E0Af2e028d5".lower()
    LIDO_NODE_OPERATOR_ADDED_SIG = "NodeOperatorAdded(uint256,string,address,uint64)"
    LIDO_SIGNING_KEY_ADDED_SIG = "SigningKeyAdded(uint256,bytes)"
    
    # Contract-to-entity direct mappings
    CONTRACT_ENTITIES = {
        "0xdcd51fc5cd918e0461b9b7fb75967fdfd10dae2f": ("Rocket Pool", "Liquid Staking"),
        "0x1cc9cf5586522c6f483e84a19c3c2b0b6d027bf0": ("Rocket Pool", "Liquid Staking"),
        "0x2fb42ffe2d7df8381853e96304300c6a5e846905": ("Rocket Pool", "Liquid Staking"),
        "0x9304b4ebfbe68932cf9af8de4d21d7e7621f701a": ("Rocket Pool", "Liquid Staking"),
        "0x672335b91b4f2096d897ca1b12ef4ec9346a5ff4": ("Rocket Pool", "Liquid Staking"),
        "0x2421a0af8badfae12e1c1700e369747d3db47b09": ("SenseiNode", "Staking Pool"),
        "0x10e02a656b5f9de2c44c687787c36a2c4801cc40": ("Tranchess", "Liquid Staking"),
        "0x447c3ee829a3b506ad0a66ff1089f30181c42637": ("HashKing", "Liquid Staking"),
        "0xa8f50a6c41d67685b820b4fe6bed7e549e54a949": ("Eth2Stake", "Staking Pool"),
        "0xf243a92eb7d4b4f6a00a57888b887bd01ec6fd12": ("MyEtherWallet", "Staking Pool"),
        "0x73fd39ba4fb23c9b080fca0fcbe4c8c7a2d630d0": ("MyEtherWallet", "Staking Pool"),
        "0xe7b385fb5d81259280b7d639df81513ab8b005e4": ("MyEtherWallet", "Staking Pool"),
        "0x82ce843130ff0ae069c54118dfbfa6a5ea17158e": ("Gemini", "CEX"),
        "0x24d729aae93a05a729e68504e5ccdfa3bb876491": ("Gemini", "CEX"),
        "0xcf5ea1b38380f6af39068375516daf40ed70d299": ("Stader", "Liquid Staking"),
        "0x4f4bfa0861f62309934a5551e0b2541ee82fdcf1": ("Stader", "Liquid Staking"),
        "0x09134c643a6b95d342bdaf081fa473338f066572": ("Stader", "Liquid Staking"),
        "0xd1a72bd052e0d65b7c26d3dd97a98b74acbbb6c5": ("Stader", "Liquid Staking"),
    }
    
    # Batch contract fund origins to entity mappings
    BATCH_FUND_ORIGINS = {
        # Transaction sender mappings
        "tx_from": {
            "0x617c8de5bde54ffbb8d92716cc947858ca38f582": ("MEV Protocol", "Liquid Staking"),
            "0xcdbf58a9a9b54a2c43800c50c7192946de858321": ("Bitpanda", "CEX"),
            "0xb10edd6fa6067dba8d4326f1c8f0d1c791594f13": ("Bitpanda", "CEX"),
            "0xf197c6f2ac14d25ee2789a73e4847732c7f16bc9": ("Bitpanda", "CEX"),
            "0xba1951df0c0a52af23857c5ab48b4c43a57e7ed1": ("Golem Foundation", "Staking Pool"),
            "0x70d5ccc14a1a264c05ff48b3ec6751b0959541aa": ("Binance US", "CEX"),
        }
    }
    
    # Dune Spellbook URLs
    SPELLBOOK_URL = "https://raw.githubusercontent.com/duneanalytics/spellbook/main/dbt_subprojects/hourly_spellbook/models/_sector/staking/ethereum/entities/staking_ethereum_entities_depositor_addresses.sql"
    CEX_URL = "https://raw.githubusercontent.com/duneanalytics/spellbook/main/dbt_subprojects/hourly_spellbook/models/_sector/cex/addresses/chains/cex_evms_addresses.sql"
    
    # Cache configuration
    CACHE_DIR = Path.home() / ".pyxatu" / "cache"
    CACHE_DURATION = timedelta(days=7)
    
    def __init__(self, client: Optional[ClickHouseClient] = None):
        """Initialize the validator label manager."""
        self.logger = logging.getLogger(__name__)
        self.client = client or self._create_client()
        
        self.CACHE_DIR.mkdir(parents=True, exist_ok=True)
        self.entity_cache = self.CACHE_DIR / "entity_mappings.json"
        self.deposits_cache = self.CACHE_DIR / "validator_deposits.parquet"
        self.labels_cache = self.CACHE_DIR / "validator_labels.parquet"
        self.lido_operators_cache = self.CACHE_DIR / "lido_operators.json"
        self.lido_keys_cache = self.CACHE_DIR / "lido_signing_keys.parquet"
        self.batch_deposits_cache = self.CACHE_DIR / "batch_deposits.parquet"
        
        self._entity_mappings: Optional[Dict[str, EntityMapping]] = None
        self._validator_labels: Optional[pd.DataFrame] = None
        self._deposits_df: Optional[pd.DataFrame] = None
    
    def _create_client(self) -> ClickHouseClient:
        """Create a ClickHouse client instance."""
        config_manager = ConfigManager()
        config = config_manager.load()
        return ClickHouseClient(config.clickhouse)
    
    @staticmethod
    def _get_event_signature(event_sig: str) -> str:
        """Calculate keccak256 hash of event signature."""
        try:
            from web3 import Web3
            return Web3.keccak(text=event_sig).hex()
        except ImportError:
            # Fallback to hashlib
            k = hashlib.sha3_256()
            k.update(event_sig.encode('utf-8'))
            return '0x' + k.hexdigest()
    
    async def initialize(self, force_refresh: bool = False) -> None:
        """Initialize the label manager."""
        if force_refresh or not self._is_cache_valid(self.entity_cache):
            await self._refresh_entity_mappings()
        else:
            self._load_entity_mappings()
        
        # Load validator labels
        if force_refresh or not self._is_cache_valid(self.labels_cache):
            await self._build_validator_labels()
        else:
            self._load_validator_labels()
    
    def _is_cache_valid(self, cache_path: Path) -> bool:
        """Check if a cache file is valid."""
        if not cache_path.exists():
            return False
        
        mtime = datetime.fromtimestamp(cache_path.stat().st_mtime)
        age = datetime.now() - mtime
        return age < self.CACHE_DURATION
    
    async def _refresh_entity_mappings(self) -> None:
        """Refresh entity mappings from Dune Spellbook."""
        self.logger.debug("Refreshing entity mappings")
        
        # Parse Spellbook
        entities = await self._parse_spellbook()
        
        # Add CEX addresses
        cex_addresses = await self._parse_cex_addresses()
        
        # Apply custom entity logic
        entities = self._apply_custom_entity_logic(entities, cex_addresses)
        
        # Save to cache
        self._save_entity_mappings(entities)
        self._entity_mappings = entities
    
    async def _parse_spellbook(self) -> Dict[str, EntityMapping]:
        """Parse the Dune Spellbook SQL file for entity mappings."""
        try:
            response = requests.get(self.SPELLBOOK_URL, timeout=30)
            response.raise_for_status()
            content = response.text
            
            entities = {}
            
            pattern = r"\((0x[a-fA-F0-9]+),\s*'([^']+)',\s*'[^']+',\s*'([^']+)'\)"
            
            for match in re.finditer(pattern, content):
                address = match.group(1).lower()
                entity_name = match.group(2).lower()
                category = match.group(3)
                
                if entity_name not in entities:
                    entities[entity_name] = EntityMapping(
                        entity=entity_name,
                        category=category,
                        depositor_addresses=set()
                    )
                
                entities[entity_name].depositor_addresses.add(address)
            
            self.logger.debug(f"Parsed {len(entities)} entities")
            return entities
            
        except Exception as e:
            self.logger.error(f"Error parsing Spellbook: {e}")
            return {}
    
    async def _parse_cex_addresses(self) -> Dict[str, Set[str]]:
        """Parse CEX addresses from Dune Spellbook."""
        try:
            response = requests.get(self.CEX_URL, timeout=30)
            response.raise_for_status()
            content = response.text
            
            cex_addresses = {}
            
            pattern = r"\('([^']+)',\s*'ethereum',\s*'([^']+)',\s*'(0x[a-fA-F0-9]+)'"
            
            for match in re.finditer(pattern, content):
                cex_name = match.group(1).lower()
                address = match.group(3).lower()
                
                if cex_name not in cex_addresses:
                    cex_addresses[cex_name] = set()
                
                cex_addresses[cex_name].add(address)
            
            self.logger.debug(f"Parsed {len(cex_addresses)} CEX entities")
            return cex_addresses
            
        except Exception as e:
            self.logger.error(f"Error parsing CEX addresses: {e}")
            return {}
    
    def _categorize_entity(self, entity_name: str) -> str:
        """Categorize entity based on name."""
        name_lower = entity_name.lower()
        
        if any(x in name_lower for x in ['liquid', 'staked', 'rocket', 'lido', 'stader']):
            return "Liquid Staking"
        elif any(x in name_lower for x in ['binance', 'coinbase', 'kraken', 'huobi', 'okx']):
            return "CEX"
        elif 'pool' in name_lower:
            return "Staking Pool"
        else:
            return "Other"
    
    def _apply_custom_entity_logic(
        self,
        entities: Dict[str, EntityMapping],
        cex_addresses: Dict[str, Set[str]]
    ) -> Dict[str, EntityMapping]:
        """Apply custom entity logic."""
        # Add CEX addresses to entities
        for cex_name, addresses in cex_addresses.items():
            if cex_name in entities:
                entities[cex_name].depositor_addresses.update(addresses)
            else:
                entities[cex_name] = EntityMapping(
                    entity=cex_name,
                    category="CEX",
                    depositor_addresses=addresses
                )
        
        # Add contract entities
        for address, (entity_name, category) in self.CONTRACT_ENTITIES.items():
            entity_name = entity_name.lower()
            if entity_name not in entities:
                entities[entity_name] = EntityMapping(
                    entity=entity_name,
                    category=category,
                    depositor_addresses=set()
                )
            entities[entity_name].depositor_addresses.add(address.lower())
        
        return entities
    
    def _save_entity_mappings(self, entities: Dict[str, EntityMapping]) -> None:
        """Save entity mappings to cache."""
        cache_data = {
            name: {
                'entity': mapping.entity,
                'category': mapping.category,
                'depositor_addresses': list(mapping.depositor_addresses)
            }
            for name, mapping in entities.items()
        }
        
        with open(self.entity_cache, 'w') as f:
            json.dump(cache_data, f, indent=2)
        
        self.logger.debug(f"Saved {len(entities)} entity mappings")
    
    def _load_entity_mappings(self) -> None:
        """Load entity mappings from cache."""
        try:
            with open(self.entity_cache, 'r') as f:
                cache_data = json.load(f)
            
            self._entity_mappings = {
                name: EntityMapping(
                    entity=data['entity'],
                    category=data['category'],
                    depositor_addresses=set(data['depositor_addresses'])
                )
                for name, data in cache_data.items()
            }
            
            self.logger.debug(f"Loaded {len(self._entity_mappings)} entity mappings")
            
        except Exception as e:
            self.logger.error(f"Error loading entity mappings: {e}")
            self._entity_mappings = {}
    
    def _load_validator_labels(self) -> None:
        """Load validator labels from cache."""
        try:
            self._validator_labels = pd.read_parquet(self.labels_cache)
            self.logger.debug(f"Loaded {len(self._validator_labels)} validator labels")
        except Exception as e:
            self.logger.error(f"Error loading validator labels: {e}")
            self._validator_labels = pd.DataFrame()
    
    async def _build_validator_labels(self) -> None:
        """Build validator labels from scratch."""
        self.logger.debug("Building validator labels")
        
        # Get deposits
        deposits_df = await self._get_deposits()
        if deposits_df.empty:
            self.logger.warning("No deposits found")
            self._validator_labels = pd.DataFrame()
            return
        
        validators_df = await self._get_validators()
        if validators_df.empty:
            self.logger.warning("No validators found")
            self._validator_labels = pd.DataFrame()
            return
        
        merged_df = self._merge_deposits_validators(deposits_df, validators_df)
        merged_df = await self._apply_entity_labels(merged_df)
        merged_df = await self._apply_custom_logic(merged_df)
        merged_df = await self._apply_exit_information(merged_df)
        self._validator_labels = merged_df
        merged_df.to_parquet(self.labels_cache, index=False)
        
        self.logger.debug(f"Built labels for {len(merged_df)} validators")
    
    async def _get_deposits(self) -> pd.DataFrame:
        """Get validator deposits."""
        if self._is_cache_valid(self.deposits_cache):
            return pd.read_parquet(self.deposits_cache)
        
        # Query deposits from the proper table
        query = """
        SELECT DISTINCT
            d.pubkey as pubkey,
            t.from_address as from_address,
            d.amount as amount,
            d.block_number as block_number,
            t.hash as tx_hash
        FROM canonical_beacon_block_deposit d
        INNER JOIN canonical_execution_transaction t
            ON d.block_number = t.block_number
        WHERE d.meta_network_name = 'mainnet'
          AND t.meta_network_name = 'mainnet'
        ORDER BY d.block_number, d.index
        """
        
        deposits_df = await self.client.execute_query_df(query)
        
        # Cache results
        if not deposits_df.empty:
            deposits_df.to_parquet(self.deposits_cache, index=False)
        
        self.logger.debug(f"Found {len(deposits_df)} deposits")
        return deposits_df
    
    async def _get_validators(self) -> pd.DataFrame:
        """Get all validators with their pubkeys."""
        query = """
        SELECT DISTINCT
            v.index as validator_index,
            p.pubkey as validator_pubkey
        FROM canonical_beacon_validators v
        INNER JOIN canonical_beacon_validators_pubkeys p
            ON v.index = p.validator_index
        WHERE v.meta_network_name = 'mainnet'
            AND p.meta_network_name = 'mainnet'
        ORDER BY v.index
        """
        
        validators_df = await self.client.execute_query_df(query)
        self.logger.debug(f"Found {len(validators_df)} validators")
        return validators_df
    
    async def _get_latest_block(self) -> int:
        """Get the latest block number."""
        query = """
        SELECT max(execution_payload_block_number) as latest
        FROM canonical_beacon_block
        WHERE meta_network_name = 'mainnet'
        """
        
        result = await self.client.execute_query_df(query)
        return int(result.iloc[0]['latest'])
    
    def _calldata_to_pubkey(self, calldata: str) -> str:
        """Extract pubkey from deposit calldata."""
        try:
            if not calldata or len(calldata) < 4:
                return '0x'
            
            calldata_bytes = codecs.decode(calldata[2:], "hex")
            
            # Skip function selector (4 bytes)
            offset_pubkey = int.from_bytes(calldata_bytes[4:36], "big")
            pubkey_length = int.from_bytes(
                calldata_bytes[offset_pubkey+4:offset_pubkey+36], "big"
            )
            pubkey_start = offset_pubkey + 36
            pubkey_end = pubkey_start + pubkey_length
            pubkey = calldata_bytes[pubkey_start:pubkey_end]
            
            return '0x' + pubkey.hex()
        except Exception:
            return '0x'
    
    def _merge_deposits_validators(
        self,
        deposits: pd.DataFrame,
        validators: pd.DataFrame
    ) -> pd.DataFrame:
        """Merge deposits with validator data."""
        # Merge validators with deposits on pubkey
        merged = pd.merge(
            validators,
            deposits,
            left_on='validator_pubkey',
            right_on='pubkey',
            how='left'
        )
        
        # Rename columns for consistency
        merged = merged.rename(columns={
            'validator_pubkey': 'pubkey',
            'from_address': 'depositor_address'
        })
        
        return merged
    
    async def _apply_entity_labels(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply entity labels based on depositor addresses."""
        if not self._entity_mappings:
            return df
        
        df['entity'] = None
        
        # Only apply labels where we have depositor addresses
        has_depositor = df['depositor_address'].notna()
        
        for entity_name, mapping in self._entity_mappings.items():
            mask = has_depositor & df['depositor_address'].isin(mapping.depositor_addresses)
            df.loc[mask, 'entity'] = entity_name
        
        return df
    
    async def _apply_custom_logic(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply custom logic for specific entities."""
        # Apply batch contract logic (for Kiln and others)
        df = await self._apply_batch_contract_logic(df)
        
        # Coinbase: Check for transactions to Coinbase addresses
        if 'coinbase' in self._entity_mappings:
            df = await self._apply_coinbase_logic(df)
        
        # Binance: Similar to Coinbase
        if 'binance' in self._entity_mappings:
            df = await self._apply_binance_logic(df)
        
        # Apply Lido node operator logic
        if 'lido' in self._entity_mappings:
            df = await self._apply_lido_logic(df)
        
        return df
    
    async def _apply_batch_contract_logic(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply batch contract logic to identify validators deposited through intermediaries."""
        # Get batch contract deposits
        batch_deposits = await self._get_batch_contract_deposits()
        
        if batch_deposits.empty:
            return df
        
        # Apply tx_from-based mappings
        for tx_from, (entity_name, category) in self.BATCH_FUND_ORIGINS['tx_from'].items():
            mask = batch_deposits['tx_from'] == tx_from.lower()
            pubkeys = batch_deposits.loc[mask, 'pubkey'].unique()
            
            validator_mask = df['pubkey'].isin(pubkeys) & df['entity'].isna()
            df.loc[validator_mask, 'entity'] = entity_name.lower()
        
        kiln_contracts = {
            "0x9b8c989ff27e948f55b53bb19b3cc1947852e394",
            "0x1e68238ce926dec62b3fbc99ab06eb1d85ce0270"
        }
        kiln_query = f"""
        SELECT DISTINCT d.pubkey as pubkey
        FROM canonical_beacon_block_deposit d
        INNER JOIN canonical_execution_traces dep 
            ON dep.to_address = '{self.BEACON_DEPOSIT_CONTRACT}'
            AND dep.block_number = d.block_number
            AND dep.from_address IN ('{"','".join(kiln_contracts)}')
            AND dep.action_value > 0
            AND dep.error IS NULL
        WHERE d.meta_network_name = 'mainnet'
        """
        
        try:
            kiln_result = await self.client.execute_query_df(kiln_query)
            if not kiln_result.empty:
                kiln_pubkeys = kiln_result['pubkey'].unique()
                validator_mask = df['pubkey'].isin(kiln_pubkeys) & df['entity'].isna()
                df.loc[validator_mask, 'entity'] = 'kiln'
                self.logger.debug(f"Labeled {validator_mask.sum()} validators as Kiln")
        except Exception as e:
            self.logger.error(f"Error identifying Kiln validators: {e}")
        
        return df
    
    async def _get_batch_contract_deposits(self) -> pd.DataFrame:
        """Get deposits made through batch contracts."""
        if self._is_cache_valid(self.batch_deposits_cache):
            return pd.read_parquet(self.batch_deposits_cache)
        
        # Query batch contract deposits with fund origins
        batch_contracts_str = "','".join(self.BATCH_CONTRACTS)
        
        query = f"""
        WITH batch_deposits AS (
            SELECT DISTINCT
                d.block_number as block_number,
                t.hash as transaction_hash,
                d.pubkey as pubkey,
                traces.from_address as funds_origin,
                txs.from_address as tx_from
            FROM canonical_beacon_block_deposit d
            INNER JOIN canonical_execution_transaction t
                ON t.block_number = d.block_number
            INNER JOIN canonical_execution_traces dep 
                ON dep.to_address = '{self.BEACON_DEPOSIT_CONTRACT}'
                AND dep.transaction_hash = t.hash
                AND dep.block_number = d.block_number
                AND dep.from_address IN ('{batch_contracts_str}')
                AND dep.action_value > 0
                AND dep.error IS NULL
            INNER JOIN canonical_execution_traces traces 
                ON traces.block_number = d.block_number
                AND traces.transaction_hash = t.hash
                AND traces.to_address = dep.from_address
                AND traces.action_value > 0
                AND traces.error IS NULL
            INNER JOIN canonical_execution_transaction txs
                ON txs.block_number = traces.block_number
                AND txs.hash = traces.transaction_hash
            WHERE d.meta_network_name = 'mainnet'
        )
        SELECT DISTINCT * FROM batch_deposits
        """
        
        result = await self.client.execute_query_df(query)
        
        # Cache results
        if not result.empty:
            result.to_parquet(self.batch_deposits_cache, index=False)
        
        return result
    
    async def _apply_coinbase_logic(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply Coinbase-specific logic to identify validators."""
        entity = self._entity_mappings.get('coinbase')
        if not entity or not entity.depositor_addresses:
            return df
        
        # Get addresses that sent to Coinbase
        cb_senders = await self._get_cex_senders('coinbase', entity.depositor_addresses)
        
        # Mark validators from those addresses
        mask = df['depositor_address'].isin(cb_senders)
        df.loc[mask & df['entity'].isna(), 'entity'] = 'coinbase'
        
        return df
    
    async def _apply_binance_logic(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply Binance-specific logic to identify validators."""
        entity = self._entity_mappings.get('binance')
        if not entity or not entity.depositor_addresses:
            return df
        
        # Get addresses that sent to Binance
        bi_senders = await self._get_cex_senders('binance', entity.depositor_addresses)
        
        # Mark validators from those addresses
        mask = df['depositor_address'].isin(bi_senders)
        df.loc[mask & df['entity'].isna(), 'entity'] = 'binance'
        
        return df
    
    async def _get_cex_senders(
        self,
        cex_name: str,
        cex_addresses: Set[str]
    ) -> Set[str]:
        """Get addresses that sent funds to a CEX."""
        cache_file = self.CACHE_DIR / f"{cex_name}_senders.parquet"
        
        # Check cache
        if cache_file.exists() and self._is_cache_valid(cache_file):
            senders_df = pd.read_parquet(cache_file)
            return set(senders_df['from_address'].unique())
        
        # Query senders
        addresses_str = "','".join(cex_addresses)
        query = f"""
        SELECT DISTINCT from_address
        FROM canonical_execution_transaction
        WHERE to_address IN ('{addresses_str}')
            AND meta_network_name = 'mainnet'
            AND block_number >= {self.BEACON_DEPOSIT_CONTRACT_BLOCK}
        
        UNION DISTINCT
        
        SELECT DISTINCT action_from as from_address
        FROM canonical_execution_traces
        WHERE action_to IN ('{addresses_str}')
            AND meta_network_name = 'mainnet'
            AND block_number >= {self.BEACON_DEPOSIT_CONTRACT_BLOCK}
        """
        
        senders_df = await self.client.execute_query_df(query)
        
        # Cache results
        if not senders_df.empty:
            senders_df.to_parquet(cache_file, index=False)
        
        return set(senders_df['from_address'].unique())
    
    async def _apply_lido_logic(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply Lido-specific logic to map validators to node operators."""
        # Get Lido validators
        lido_mask = df['entity'] == 'lido'
        if not lido_mask.any():
            return df
        
        # Get Lido node operators
        operators = await self._get_lido_operators()
        if not operators:
            return df
        
        # Get signing keys
        signing_keys = await self._get_lido_signing_keys()
        if signing_keys.empty:
            return df
        
        # Map validators to node operators
        lido_validators = df[lido_mask].copy()
        
        for _, key_row in signing_keys.iterrows():
            operator_id = key_row['operator_id']
            pubkey = key_row['pubkey']
            
            if operator_id in operators:
                operator_name = operators[operator_id]['name']
                entity_name = f"lido - {operator_name}"
                
                # Update entity for this pubkey
                mask = df['pubkey'] == pubkey
                df.loc[mask, 'entity'] = entity_name.lower()
        
        return df
    
    async def _get_lido_operators(self) -> Dict[int, Dict[str, Any]]:
        """Get Lido node operators."""
        # Check cache
        if self.lido_operators_cache.exists() and self._is_cache_valid(self.lido_operators_cache):
            with open(self.lido_operators_cache, 'r') as f:
                return json.load(f)
        
        # Query NodeOperatorAdded events
        event_hash = self._get_event_signature(self.LIDO_NODE_OPERATOR_ADDED_SIG)
        
        query = f"""
        SELECT
            topic1,
            data,
            block_number
        FROM canonical_execution_logs
        WHERE address = '{self.LIDO_CONTRACT}'
            AND topic0 = '{event_hash}'
            AND meta_network_name = 'mainnet'
        ORDER BY block_number
        """
        
        events_df = await self.client.execute_query_df(query)
        
        operators = {}
        for _, event in events_df.iterrows():
            try:
                # Parse operator ID from topic1
                operator_id = int(event['topic1'], 16)
                
                # Parse data (name, reward address, staking limit)
                data = event['data'][2:]  # Remove 0x
                
                # Extract name (offset, length, data)
                name_offset = int(data[0:64], 16) * 2
                name_length = int(data[name_offset:name_offset+64], 16)
                name_start = name_offset + 64
                name_end = name_start + name_length * 2
                name_hex = data[name_start:name_end]
                name = bytes.fromhex(name_hex).decode('utf-8', errors='ignore').strip()
                
                operators[operator_id] = {
                    'name': name,
                    'block_number': event['block_number']
                }
            except Exception as e:
                self.logger.warning(f"Error parsing operator event: {e}")
        
        # Save to cache
        with open(self.lido_operators_cache, 'w') as f:
            json.dump(operators, f, indent=2)
        
        return operators
    
    async def _get_lido_signing_keys(self) -> pd.DataFrame:
        """Get Lido signing keys."""
        # Check cache
        if self.lido_keys_cache.exists() and self._is_cache_valid(self.lido_keys_cache):
            return pd.read_parquet(self.lido_keys_cache)
        
        # Query SigningKeyAdded events
        event_hash = self._get_event_signature(self.LIDO_SIGNING_KEY_ADDED_SIG)
        
        query = f"""
        SELECT
            topic1,
            data,
            block_number
        FROM canonical_execution_logs
        WHERE address = '{self.LIDO_CONTRACT}'
            AND topic0 = '{event_hash}'
            AND meta_network_name = 'mainnet'
        ORDER BY block_number
        """
        
        events_df = await self.client.execute_query_df(query)
        
        keys = []
        for _, event in events_df.iterrows():
            try:
                # Parse operator ID from topic1
                operator_id = int(event['topic1'], 16)
                
                # Parse pubkey from data
                data = event['data'][2:]  # Remove 0x
                pubkey_offset = int(data[0:64], 16) * 2
                pubkey = '0x' + data[pubkey_offset+64:pubkey_offset+160]
                
                keys.append({
                    'operator_id': operator_id,
                    'pubkey': pubkey,
                    'block_number': event['block_number']
                })
            except Exception as e:
                self.logger.warning(f"Error parsing signing key event: {e}")
        
        keys_df = pd.DataFrame(keys)
        
        # Save to cache
        if not keys_df.empty:
            keys_df.to_parquet(self.lido_keys_cache, index=False)
        
        return keys_df
    
    async def _apply_exit_information(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add exit information to validators."""
        self.logger.info("Adding exit information...")
        
        # Initialize exited column
        df['exited'] = False
        df['exit_type'] = None
        df['exit_epoch'] = None
        
        # Get voluntary exits
        voluntary_exits = await self._get_voluntary_exits()
        if not voluntary_exits.empty:
            # Mark validators with voluntary exits
            exit_mask = df['validator_index'].isin(voluntary_exits['validator_index'])
            df.loc[exit_mask, 'exited'] = True
            df.loc[exit_mask, 'exit_type'] = 'voluntary'
            
            # Add exit epoch information
            exit_map = dict(zip(voluntary_exits['validator_index'], voluntary_exits['exit_epoch']))
            df.loc[exit_mask, 'exit_epoch'] = df.loc[exit_mask, 'validator_index'].map(exit_map)
            
            self.logger.info(f"Marked {exit_mask.sum()} validators with voluntary exits")
        
        # Get attester slashings
        attester_slashings = await self._get_attester_slashings()
        if not attester_slashings.empty:
            # Mark validators with attester slashings
            slash_mask = df['validator_index'].isin(attester_slashings['validator_index'])
            df.loc[slash_mask, 'exited'] = True
            df.loc[slash_mask, 'exit_type'] = 'attester_slashing'
            
            # Add slashing epoch information
            slash_map = dict(zip(attester_slashings['validator_index'], attester_slashings['epoch']))
            df.loc[slash_mask & df['exit_epoch'].isna(), 'exit_epoch'] = df.loc[slash_mask, 'validator_index'].map(slash_map)
            
            self.logger.info(f"Marked {slash_mask.sum()} validators with attester slashings")
        
        # Get proposer slashings
        proposer_slashings = await self._get_proposer_slashings()
        if not proposer_slashings.empty:
            # Mark validators with proposer slashings
            prop_mask = df['validator_index'].isin(proposer_slashings['validator_index'])
            df.loc[prop_mask, 'exited'] = True
            df.loc[prop_mask, 'exit_type'] = 'proposer_slashing'
            
            # Add slashing epoch information
            prop_map = dict(zip(proposer_slashings['validator_index'], proposer_slashings['epoch']))
            df.loc[prop_mask & df['exit_epoch'].isna(), 'exit_epoch'] = df.loc[prop_mask, 'validator_index'].map(prop_map)
            
            self.logger.info(f"Marked {prop_mask.sum()} validators with proposer slashings")
        
        # Summary
        total_exited = df['exited'].sum()
        self.logger.info(f"Total exited validators: {total_exited} ({total_exited/len(df)*100:.2f}%)")
        
        return df
    
    async def _get_voluntary_exits(self) -> pd.DataFrame:
        """Get voluntary exit information."""
        query = """
        SELECT DISTINCT
            voluntary_exit_message_validator_index as validator_index,
            voluntary_exit_message_epoch as exit_epoch,
            epoch,
            slot
        FROM canonical_beacon_block_voluntary_exit
        WHERE meta_network_name = 'mainnet'
        ORDER BY validator_index
        """
        
        try:
            exits_df = await self.client.execute_query_df(query)
            self.logger.info(f"Found {len(exits_df)} voluntary exits")
            return exits_df
        except Exception as e:
            self.logger.error(f"Error getting voluntary exits: {e}")
            return pd.DataFrame()
    
    async def _get_attester_slashings(self) -> pd.DataFrame:
        """Get attester slashing information."""
        query = """
        WITH slashed_validators AS (
            SELECT DISTINCT
                arrayJoin(attestation_1_attesting_indices) as validator_index,
                epoch,
                slot
            FROM canonical_beacon_block_attester_slashing
            WHERE meta_network_name = 'mainnet'
            
            UNION DISTINCT
            
            SELECT DISTINCT
                arrayJoin(attestation_2_attesting_indices) as validator_index,
                epoch,
                slot
            FROM canonical_beacon_block_attester_slashing
            WHERE meta_network_name = 'mainnet'
        )
        SELECT DISTINCT
            validator_index,
            min(epoch) as epoch,
            min(slot) as slot
        FROM slashed_validators
        GROUP BY validator_index
        ORDER BY validator_index
        """
        
        try:
            slashings_df = await self.client.execute_query_df(query)
            self.logger.info(f"Found {len(slashings_df)} attester slashings")
            return slashings_df
        except Exception as e:
            self.logger.error(f"Error getting attester slashings: {e}")
            return pd.DataFrame()
    
    async def _get_proposer_slashings(self) -> pd.DataFrame:
        """Get proposer slashing information."""
        query = """
        SELECT DISTINCT
            signed_header_1_message_proposer_index as validator_index,
            epoch,
            slot
        FROM canonical_beacon_block_proposer_slashing
        WHERE meta_network_name = 'mainnet'
        ORDER BY validator_index
        """
        
        try:
            slashings_df = await self.client.execute_query_df(query)
            self.logger.info(f"Found {len(slashings_df)} proposer slashings")
            return slashings_df
        except Exception as e:
            self.logger.error(f"Error getting proposer slashings: {e}")
            return pd.DataFrame()
    
    # Public API methods
    
    def get_validator_label(self, validator_id: int) -> Optional[str]:
        """Get entity label for a validator by ID."""
        if self._validator_labels is None or self._validator_labels.empty:
            return None
        
        mask = self._validator_labels['validator_index'] == validator_id
        if mask.any():
            return self._validator_labels.loc[mask, 'entity'].iloc[0]
        return None
    
    def get_validator_labels_bulk(self, validator_ids: List[int]) -> Dict[int, Optional[str]]:
        """Get entity labels for multiple validators."""
        if self._validator_labels is None or self._validator_labels.empty:
            return {vid: None for vid in validator_ids}
        
        mask = self._validator_labels['validator_index'].isin(validator_ids)
        results = self._validator_labels[mask].set_index('validator_index')['entity'].to_dict()
        
        return {vid: results.get(vid) for vid in validator_ids}
    
    def label_dataframe(
        self,
        df: pd.DataFrame,
        index_column: str = 'proposer_index',
        label_column: str = 'entity'
    ) -> pd.DataFrame:
        """Add entity labels to a DataFrame with validator indices."""
        if index_column not in df.columns:
            self.logger.warning(f"Column '{index_column}' not found")
            return df
        
        if self._validator_labels is None or self._validator_labels.empty:
            df[label_column] = None
            return df
        
        # Create mapping
        label_map = dict(
            zip(
                self._validator_labels['validator_index'],
                self._validator_labels['entity']
            )
        )
        
        # Apply mapping
        result = df.copy()
        result[label_column] = result[index_column].map(label_map)
        
        return result
    
    def get_validators_by_entity(self, entity_name: str) -> List[int]:
        """Get all validator indices for an entity."""
        if self._validator_labels is None or self._validator_labels.empty:
            return []
        
        mask = self._validator_labels['entity'].str.lower() == entity_name.lower()
        return self._validator_labels.loc[mask, 'validator_index'].tolist()
    
    def get_entity_statistics(self) -> pd.DataFrame:
        """Get statistics about entities."""
        if self._validator_labels is None or self._validator_labels.empty:
            return pd.DataFrame()
        
        # Get entity counts
        entity_stats = self._validator_labels.groupby('entity').agg({
            'validator_index': 'count',
            'exited': 'sum'
        }).reset_index()
        
        entity_stats.columns = ['entity', 'validator_count', 'exited_count']
        entity_stats['active_count'] = entity_stats['validator_count'] - entity_stats['exited_count']
        
        total = len(self._validator_labels)
        entity_stats['percentage'] = (entity_stats['validator_count'] / total * 100).round(2)
        entity_stats['exit_rate'] = (entity_stats['exited_count'] / entity_stats['validator_count'] * 100).round(2)
        
        # Sort by validator count
        entity_stats = entity_stats.sort_values('validator_count', ascending=False)
        
        return entity_stats
    
    def get_entity_list(self) -> List[str]:
        """Get list of all known entities."""
        if self._entity_mappings:
            return sorted(self._entity_mappings.keys())
        return []
    
    async def get_lido_operators(self) -> List[Dict[str, Any]]:
        """Get list of Lido node operators with their validator counts."""
        operators = await self._get_lido_operators()
        
        # Get validator counts for each operator
        result = []
        for op_id, op_info in operators.items():
            entity_name = f"lido - {op_info['name']}"
            validators = self.get_validators_by_entity(entity_name)
            
            result.append({
                'id': op_id,
                'name': op_info['name'],
                'entity_name': entity_name,
                'validator_count': len(validators),
                'block_number': op_info['block_number']
            })
        
        return sorted(result, key=lambda x: x['validator_count'], reverse=True)
    
    def get_exit_statistics(self) -> Dict[str, Any]:
        """Get statistics about validator exits."""
        if self._validator_labels is None or self._validator_labels.empty:
            return {}
        
        total = len(self._validator_labels)
        exited = self._validator_labels['exited'].sum()
        active = total - exited
        
        # Exit type breakdown
        exit_types = self._validator_labels[self._validator_labels['exited']]['exit_type'].value_counts().to_dict()
        
        return {
            'total_validators': total,
            'active_validators': active,
            'exited_validators': exited,
            'exit_rate': round(exited / total * 100, 2),
            'exit_types': exit_types,
            'voluntary_exits': exit_types.get('voluntary', 0),
            'attester_slashings': exit_types.get('attester_slashing', 0),
            'proposer_slashings': exit_types.get('proposer_slashing', 0)
        }
    
    def get_active_validators_by_entity(self, entity_name: str) -> List[int]:
        """Get all active (non-exited) validator indices for an entity."""
        if self._validator_labels is None or self._validator_labels.empty:
            return []
        
        mask = (
            (self._validator_labels['entity'].str.lower() == entity_name.lower()) &
            (~self._validator_labels['exited'])
        )
        return self._validator_labels.loc[mask, 'validator_index'].tolist()
    
    async def refresh(self) -> None:
        """Refresh all data from sources."""
        await self.initialize(force_refresh=True)


# Convenience function
async def create_label_manager(client: Optional[ClickHouseClient] = None) -> ValidatorLabelManager:
    """Create and initialize a ValidatorLabelManager."""
    manager = ValidatorLabelManager(client)
    await manager.initialize()
    return manager