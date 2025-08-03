"""Fixed validator label mapping for Ethereum validators."""

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
        self.labels_cache = self.CACHE_DIR / "validator_labels.parquet"
        self.lido_operators_cache = self.CACHE_DIR / "lido_operators.json"
        self.batch_deposits_cache = self.CACHE_DIR / "batch_deposits.parquet"
        
        self._entity_mappings: Optional[Dict[str, EntityMapping]] = None
        self._validator_labels: Optional[pd.DataFrame] = None
        self._label_index: Optional[Dict[int, str]] = None
    
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
        try:
            # Load or refresh entity mappings
            if force_refresh or not self._is_cache_valid(self.entity_cache):
                self.logger.info("Refreshing entity mappings from Dune Spellbook...")
                await self._refresh_entity_mappings()
            else:
                self.logger.info("Loading entity mappings from cache...")
                self._load_entity_mappings()
            
            # Build or load validator labels
            if force_refresh or not self._is_cache_valid(self.labels_cache):
                self.logger.info("Building validator labels...")
                await self._build_validator_labels()
            else:
                self.logger.info("Loading validator labels from cache...")
                self._load_validator_labels()
                
            self.logger.info(f"Initialized with {len(self._entity_mappings)} entities")
            if self._validator_labels is not None:
                labeled = self._validator_labels['entity'].notna().sum()
                total = len(self._validator_labels)
                self.logger.info(f"Labeled {labeled:,}/{total:,} validators ({labeled/total*100:.1f}%)")
        except Exception as e:
            self.logger.error(f"Error initializing label manager: {e}")
            # Initialize with empty data
            self._entity_mappings = {}
            self._validator_labels = pd.DataFrame(columns=['validator_index', 'entity'])
    
    def _is_cache_valid(self, cache_path: Path) -> bool:
        """Check if a cache file is valid."""
        if not cache_path.exists():
            return False
        
        mtime = datetime.fromtimestamp(cache_path.stat().st_mtime)
        age = datetime.now() - mtime
        return age < self.CACHE_DURATION
    
    async def _refresh_entity_mappings(self) -> None:
        """Refresh entity mappings from Dune Spellbook."""
        try:
            # Parse Spellbook data
            entities = await self._parse_spellbook()
            
            # Parse CEX addresses
            cex_addresses = await self._parse_cex_addresses()
            
            # Apply CEX mappings
            for entity_name, addresses in cex_addresses.items():
                if entity_name in entities:
                    entities[entity_name].depositor_addresses.update(addresses)
            
            # Apply custom logic
            entities = self._apply_custom_entity_logic(entities)
            
            # Save to cache
            self._save_entity_mappings(entities)
            self._entity_mappings = entities
            
        except Exception as e:
            self.logger.error(f"Failed to refresh entity mappings: {e}")
            # Fallback to empty mappings
            self._entity_mappings = {}
    
    async def _parse_spellbook(self) -> Dict[str, EntityMapping]:
        """Parse entity mappings from Dune Spellbook."""
        entities = {}
        
        try:
            response = requests.get(self.SPELLBOOK_URL, timeout=30)
            response.raise_for_status()
            content = response.text
            
            # Extract entity to depositor mappings using regex
            pattern = r"\('(0x[a-fA-F0-9]+)',\s*'([^']+)'\)"
            matches = re.findall(pattern, content)
            
            for depositor, entity_name in matches:
                entity_name_lower = entity_name.lower()
                
                if entity_name_lower not in entities:
                    entities[entity_name_lower] = EntityMapping(
                        entity=entity_name_lower,
                        category=self._categorize_entity(entity_name)
                    )
                
                entities[entity_name_lower].depositor_addresses.add(depositor.lower())
            
            self.logger.info(f"Parsed {len(entities)} entities from Spellbook")
            
        except Exception as e:
            self.logger.error(f"Failed to parse Spellbook: {e}")
        
        return entities
    
    async def _parse_cex_addresses(self) -> Dict[str, Set[str]]:
        """Parse CEX addresses from Spellbook."""
        cex_addresses = {}
        
        try:
            response = requests.get(self.CEX_URL, timeout=30)
            response.raise_for_status()
            content = response.text
            
            # Extract CEX addresses
            pattern = r"\('ethereum',\s*'([^']+)',\s*'(0x[a-fA-F0-9]+)'"
            matches = re.findall(pattern, content)
            
            for exchange_name, address in matches:
                exchange_lower = exchange_name.lower()
                if exchange_lower not in cex_addresses:
                    cex_addresses[exchange_lower] = set()
                cex_addresses[exchange_lower].add(address.lower())
            
            self.logger.info(f"Parsed {len(cex_addresses)} CEX entities")
            
        except Exception as e:
            self.logger.error(f"Failed to parse CEX addresses: {e}")
        
        return cex_addresses
    
    def _categorize_entity(self, entity_name: str) -> str:
        """Categorize an entity based on its name."""
        name_lower = entity_name.lower()
        
        if any(x in name_lower for x in ['liquid', 'staked', 'rocket', 'lido', 'stader']):
            return "Liquid Staking"
        elif any(x in name_lower for x in ['binance', 'coinbase', 'kraken', 'huobi', 'okx', 'gemini', 'bitpanda']):
            return "CEX"
        elif 'pool' in name_lower:
            return "Staking Pool"
        else:
            return "Other"
    
    def _apply_custom_entity_logic(
        self,
        entities: Dict[str, EntityMapping]
    ) -> Dict[str, EntityMapping]:
        """Apply custom entity logic and contract mappings."""
        # Add contract-based entities
        for contract_addr, (entity_name, category) in self.CONTRACT_ENTITIES.items():
            entity_name_lower = entity_name.lower()
            
            if entity_name_lower not in entities:
                entities[entity_name_lower] = EntityMapping(
                    entity=entity_name_lower,
                    category=category
                )
            
            entities[entity_name_lower].depositor_addresses.add(contract_addr)
        
        return entities
    
    def _save_entity_mappings(self, entities: Dict[str, EntityMapping]) -> None:
        """Save entity mappings to cache."""
        cache_data = {
            'timestamp': datetime.now().isoformat(),
            'entities': {
                name: {
                    'entity': mapping.entity,
                    'category': mapping.category,
                    'depositor_addresses': list(mapping.depositor_addresses)
                }
                for name, mapping in entities.items()
            }
        }
        
        with open(self.entity_cache, 'w') as f:
            json.dump(cache_data, f, indent=2)
    
    def _load_entity_mappings(self) -> None:
        """Load entity mappings from cache."""
        try:
            with open(self.entity_cache, 'r') as f:
                cache_data = json.load(f)
            
            self._entity_mappings = {}
            for name, data in cache_data['entities'].items():
                self._entity_mappings[name] = EntityMapping(
                    entity=data['entity'],
                    category=data['category'],
                    depositor_addresses=set(data['depositor_addresses'])
                )
        except Exception as e:
            self.logger.error(f"Failed to load entity mappings: {e}")
            self._entity_mappings = {}
    
    def _load_validator_labels(self) -> None:
        """Load validator labels from cache."""
        try:
            self._validator_labels = pd.read_parquet(self.labels_cache)
            self.logger.info(f"Loaded {len(self._validator_labels)} validator labels from cache")
        except Exception as e:
            self.logger.error(f"Failed to load validator labels: {e}")
            self._validator_labels = pd.DataFrame()
    
    async def _build_validator_labels(self) -> None:
        """Build validator labels using a simplified approach."""
        try:
            # Since we can't directly map deposits to validators without proper pubkey data,
            # we'll use a simplified approach that focuses on what we can determine
            
            # Try to use ethseer data as base if available
            base_labels = await self._get_ethseer_labels()
            
            if base_labels.empty:
                self.logger.warning("No base labels available from ethseer_validator_entity")
                # Create empty DataFrame with proper structure
                self._validator_labels = pd.DataFrame(columns=['validator_index', 'entity', 'exited'])
                return
            
            # Apply custom enhancements
            self._validator_labels = base_labels
            
            # Add exit information
            self._validator_labels = await self._apply_exit_information(self._validator_labels)
            
            # Apply Lido operator logic
            self._validator_labels = await self._apply_lido_operator_labels(self._validator_labels)
            
            # Save to cache
            if not self._validator_labels.empty:
                self._validator_labels.to_parquet(self.labels_cache, index=False)
                
            self.logger.info(f"Built labels for {len(self._validator_labels)} validators")
            
        except Exception as e:
            self.logger.error(f"Error building validator labels: {e}")
            self._validator_labels = pd.DataFrame(columns=['validator_index', 'entity', 'exited'])
    
    async def _get_ethseer_labels(self) -> pd.DataFrame:
        """Get base labels from ethseer_validator_entity table."""
        try:
            query = """
            SELECT DISTINCT
                index as validator_index,
                entity,
                false as exited
            FROM ethseer_validator_entity
            WHERE meta_network_name = 'mainnet'
            ORDER BY index
            """
            
            result = await self.client.execute_query_df(query)
            self.logger.info(f"Retrieved {len(result)} labels from ethseer_validator_entity")
            
            # Normalize entity names
            result['entity'] = result['entity'].str.lower()
            
            return result
            
        except Exception as e:
            self.logger.error(f"Failed to get ethseer labels: {e}")
            return pd.DataFrame()
    
    async def _apply_exit_information(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply exit information to validators."""
        try:
            # Get voluntary exits
            voluntary_query = """
            SELECT DISTINCT
                voluntary_exit_data_validator_index as validator_index,
                'voluntary' as exit_type
            FROM canonical_beacon_block_voluntary_exit
            WHERE meta_network_name = 'mainnet'
            """
            
            voluntary_exits = await self.client.execute_query_df(voluntary_query)
            
            # Get attester slashings
            attester_query = """
            SELECT DISTINCT
                arrayJoin(attestation_1_attesting_indices) as validator_index,
                'attester_slashing' as exit_type
            FROM canonical_beacon_block_attester_slashing
            WHERE meta_network_name = 'mainnet'
            """
            
            attester_slashings = await self.client.execute_query_df(attester_query)
            
            # Combine exit data
            all_exits = pd.concat([voluntary_exits, attester_slashings], ignore_index=True)
            
            # Mark exited validators
            df['exited'] = df['validator_index'].isin(all_exits['validator_index'])
            
            self.logger.info(f"Marked {df['exited'].sum()} validators as exited")
            
        except Exception as e:
            self.logger.error(f"Failed to apply exit information: {e}")
            df['exited'] = False
        
        return df
    
    async def _apply_lido_operator_labels(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply Lido node operator labels."""
        try:
            # Get Lido operators
            operators = await self._get_lido_operators()
            
            # Get Lido signing keys
            signing_keys = await self._get_lido_signing_keys()
            
            if signing_keys.empty:
                return df
            
            # For each operator, update the entity label
            for op_id, op_info in operators.items():
                operator_keys = signing_keys[signing_keys['operator_id'] == op_id]['validator_index'].tolist()
                
                if operator_keys:
                    mask = df['validator_index'].isin(operator_keys) & (df['entity'] == 'lido')
                    df.loc[mask, 'entity'] = f"lido - {op_info['name']}"
                    self.logger.debug(f"Updated {mask.sum()} validators for Lido operator {op_info['name']}")
            
        except Exception as e:
            self.logger.error(f"Failed to apply Lido operator labels: {e}")
        
        return df
    
    async def _get_lido_operators(self) -> Dict[int, Dict[str, Any]]:
        """Get Lido node operators from events."""
        if self._is_cache_valid(self.lido_operators_cache):
            with open(self.lido_operators_cache, 'r') as f:
                return json.load(f)
        
        operators = {}
        
        try:
            # Query NodeOperatorAdded events
            sig_hash = self._get_event_signature(self.LIDO_NODE_OPERATOR_ADDED_SIG)
            
            query = f"""
            SELECT
                topic_1,
                topic_2,
                data,
                block_number
            FROM canonical_execution_logs
            WHERE address = '{self.LIDO_CONTRACT}'
              AND topic_0 = '{sig_hash}'
              AND meta_network_name = 'mainnet'
            ORDER BY block_number
            """
            
            result = await self.client.execute_query_df(query)
            
            for _, row in result.iterrows():
                # Parse operator ID from topic_1
                operator_id = int(row['topic_1'], 16)
                
                # Parse operator name from data
                # The data contains: name (string), rewardAddress (address), stakingLimit (uint64)
                data = row['data']
                if data.startswith('0x'):
                    data = data[2:]
                
                # Skip offset parsing and extract name directly
                # Name typically starts at position 128 (64 bytes offset)
                try:
                    name_length = int(data[128:192], 16)
                    name_hex = data[192:192 + name_length * 2]
                    name = codecs.decode(name_hex, 'hex').decode('utf-8').strip()
                    
                    operators[operator_id] = {
                        'name': name,
                        'block_number': row['block_number']
                    }
                except Exception as e:
                    self.logger.debug(f"Failed to parse operator name for ID {operator_id}: {e}")
            
            # Cache results
            with open(self.lido_operators_cache, 'w') as f:
                json.dump(operators, f)
            
            self.logger.info(f"Found {len(operators)} Lido operators")
            
        except Exception as e:
            self.logger.error(f"Failed to get Lido operators: {e}")
        
        return operators
    
    async def _get_lido_signing_keys(self) -> pd.DataFrame:
        """Get Lido signing keys to validator index mapping."""
        # For now, return empty DataFrame since we can't map pubkeys to validator indices
        # without the proper validator pubkey data
        return pd.DataFrame(columns=['operator_id', 'validator_index'])
    
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
        
        # For large queries, create a set for O(1) lookups
        if len(validator_ids) > 1000:
            # Use pre-indexed DataFrame for faster lookups
            if not hasattr(self, '_label_index'):
                self._label_index = self._validator_labels.set_index('validator_index')['entity'].to_dict()
            
            return {vid: self._label_index.get(vid) for vid in validator_ids}
        else:
            # For smaller queries, use the original method
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
    
    async def refresh(self) -> None:
        """Refresh all data from sources."""
        self.logger.info("Starting validator label refresh...")
        start_time = datetime.now()
        await self.initialize(force_refresh=True)
        duration = (datetime.now() - start_time).total_seconds()
        self.logger.info(f"Validator label refresh completed in {duration:.1f} seconds")


# Convenience function
async def create_label_manager(client: Optional[ClickHouseClient] = None) -> ValidatorLabelManager:
    """Create and initialize a ValidatorLabelManager."""
    manager = ValidatorLabelManager(client)
    await manager.initialize()
    return manager