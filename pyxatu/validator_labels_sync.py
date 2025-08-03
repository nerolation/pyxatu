"""Synchronous validator label management for PyXatu."""

import logging
from typing import Dict, List, Optional, Set
import pandas as pd
import requests
from datetime import datetime
import csv
from io import StringIO

from pyxatu.core.clickhouse_client_sync import ClickHouseClient


class ValidatorLabelManager:
    """Synchronous manager for validator labels using ClickHouse ethseer_labels table."""
    
    # URLs for entity data
    SPELLBOOK_URL = "https://raw.githubusercontent.com/duneanalytics/spellbook/main/models/labels/addresses/ethereum/labels_addresses_ethereum.csv"
    CEX_URL = "https://raw.githubusercontent.com/duneanalytics/spellbook/main/models/labels/addresses/institution/identifier/cex/labels_cex_ethereum.csv"
    
    def __init__(self, client: ClickHouseClient):
        """Initialize the validator label manager.
        
        Args:
            client: Synchronous ClickHouse client instance
        """
        self.client = client
        self.logger = logging.getLogger(__name__)
        self._initialized = False
        self._entity_mappings = {}
        self._validator_labels = pd.DataFrame()
        
    def initialize(self, force_refresh: bool = False) -> None:
        """Initialize the label manager by loading data.
        
        Args:
            force_refresh: Force refresh entity mappings
        """
        if self._initialized and not force_refresh:
            return
            
        try:
            # Refresh entity mappings if needed
            if force_refresh or not self._entity_mappings:
                self._refresh_entity_mappings()
            
            # Build validator labels
            self._build_validator_labels()
            
            self._initialized = True
            self.logger.info("Validator label manager initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize validator labels: {e}")
            # Continue with partial data if available
            if not self._validator_labels.empty:
                self._initialized = True
    
    def _refresh_entity_mappings(self) -> None:
        """Refresh entity mappings from Dune Spellbook."""
        try:
            # Parse Spellbook data
            entities = self._parse_spellbook()
            
            # Parse CEX addresses
            cex_addresses = self._parse_cex_addresses()
            
            # Apply CEX mappings
            for entity_name, addresses in cex_addresses.items():
                if entity_name not in entities:
                    entities[entity_name] = {
                        'name': entity_name,
                        'category': 'institution',
                        'addresses': set()
                    }
                entities[entity_name]['addresses'].update(addresses)
            
            self._entity_mappings = entities
            self.logger.info(f"Loaded {len(entities)} entities from Spellbook")
            
        except Exception as e:
            self.logger.warning(f"Failed to refresh entity mappings: {e}")
            # Fallback to empty mappings
            self._entity_mappings = {}
    
    def _parse_spellbook(self) -> Dict[str, Dict]:
        """Parse entity mappings from Dune Spellbook."""
        entities = {}
        
        try:
            response = requests.get(self.SPELLBOOK_URL, timeout=30)
            response.raise_for_status()
            content = response.text
            
            reader = csv.DictReader(StringIO(content))
            for row in reader:
                entity_name = row.get('name', '').strip()
                address = row.get('address', '').strip().lower()
                category = row.get('category', '').strip()
                
                if entity_name and address:
                    if entity_name not in entities:
                        entities[entity_name] = {
                            'name': entity_name,
                            'category': category,
                            'addresses': set()
                        }
                    entities[entity_name]['addresses'].add(address)
                    
        except Exception as e:
            self.logger.error(f"Failed to parse Spellbook data: {e}")
        
        return entities
    
    def _parse_cex_addresses(self) -> Dict[str, Set[str]]:
        """Parse CEX addresses from Spellbook."""
        cex_addresses = {}
        
        try:
            response = requests.get(self.CEX_URL, timeout=30)
            response.raise_for_status()
            content = response.text
            
            reader = csv.DictReader(StringIO(content))
            for row in reader:
                cex_name = row.get('cex_name', '').strip()
                address = row.get('address', '').strip().lower()
                
                if cex_name and address:
                    if cex_name not in cex_addresses:
                        cex_addresses[cex_name] = set()
                    cex_addresses[cex_name].add(address)
                    
        except Exception as e:
            self.logger.error(f"Failed to parse CEX addresses: {e}")
            
        return cex_addresses
    
    def _build_validator_labels(self) -> None:
        """Build validator labels from ethseer_labels table."""
        try:
            # Get base labels from ethseer_labels
            base_labels = self._get_ethseer_labels()
            
            if base_labels.empty:
                self.logger.warning("No data found in ethseer_labels table")
                self._validator_labels = pd.DataFrame()
                return
            
            # Apply additional information
            self._validator_labels = self._apply_exit_information(base_labels)
            self._validator_labels = self._apply_lido_operator_labels(self._validator_labels)
            
            # Log statistics
            label_counts = self._validator_labels['entity'].value_counts()
            self.logger.info(f"Built labels for {len(self._validator_labels)} validators")
            self.logger.info(f"Top entities: {label_counts.head(10).to_dict()}")
            
        except Exception as e:
            self.logger.error(f"Failed to build validator labels: {e}")
            self._validator_labels = pd.DataFrame()
    
    def _get_ethseer_labels(self) -> pd.DataFrame:
        """Get base labels from ethseer_labels table."""
        query = """
        SELECT 
            validator_index,
            label as entity,
            on_chain_name,
            deposit_tx_from,
            deposit_tx_to,
            withdrawal_address
        FROM ethseer_labels
        WHERE label != ''
        """
        
        try:
            result = self.client.execute_query_df(query)
            if not result.empty:
                # Clean entity names
                result['entity'] = result['entity'].str.strip()
                # Remove empty labels
                result = result[result['entity'] != '']
            return result
        except Exception as e:
            self.logger.error(f"Failed to query ethseer_labels: {e}")
            return pd.DataFrame()
    
    def _apply_exit_information(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply exit information to validator labels."""
        try:
            # Query voluntary exits
            voluntary_query = """
            SELECT DISTINCT validator_index, 'voluntary_exit' as exit_type
            FROM beacon_api_eth_v1_events_voluntary_exit FINAL
            """
            voluntary_exits = self.client.execute_query_df(voluntary_query)
            
            # Query attester slashings
            attester_query = """
            SELECT DISTINCT 
                arrayJoin(attestation_1_indices) as validator_index,
                'attester_slashing' as exit_type
            FROM beacon_api_eth_v1_events_attester_slashing FINAL
            """
            attester_slashings = self.client.execute_query_df(attester_query)
            
            # Combine exit information
            all_exits = pd.concat([voluntary_exits, attester_slashings], ignore_index=True)
            
            # Merge with main dataframe
            if not all_exits.empty:
                df = df.merge(all_exits, on='validator_index', how='left')
            else:
                df['exit_type'] = None
                
            return df
            
        except Exception as e:
            self.logger.warning(f"Failed to apply exit information: {e}")
            df['exit_type'] = None
            return df
    
    def _apply_lido_operator_labels(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply Lido operator labels to validators."""
        try:
            operators = self._get_lido_operators()
            
            if operators.empty:
                return df
            
            # Create a mapping of validator indices to operators
            lido_validators = {}
            for _, op in operators.iterrows():
                op_name = op['operator_name']
                indices = op['validator_indices']
                if isinstance(indices, list):
                    for idx in indices:
                        lido_validators[idx] = f"Lido - {op_name}"
            
            # Apply Lido labels
            def update_label(row):
                idx = row['validator_index']
                if idx in lido_validators and row['entity'] == 'Lido':
                    return lido_validators[idx]
                return row['entity']
            
            df['entity'] = df.apply(update_label, axis=1)
            
            return df
            
        except Exception as e:
            self.logger.warning(f"Failed to apply Lido operator labels: {e}")
            return df
    
    def _get_lido_operators(self) -> pd.DataFrame:
        """Get Lido operator information."""
        query = """
        SELECT 
            operator_name,
            groupArray(validator_index) as validator_indices
        FROM (
            SELECT DISTINCT
                validator_index,
                JSONExtractString(meta_client_additional_data, 'lido_node_operator') as operator_name
            FROM ethseer_labels
            WHERE operator_name != ''
        )
        GROUP BY operator_name
        """
        
        try:
            return self.client.execute_query_df(query)
        except Exception as e:
            self.logger.error(f"Failed to get Lido operators: {e}")
            return pd.DataFrame()
    
    def get_validator_labels_bulk(self, validator_indices: List[int]) -> Dict[int, Optional[str]]:
        """Get labels for multiple validators.
        
        Args:
            validator_indices: List of validator indices
            
        Returns:
            Dictionary mapping validator index to label
        """
        if not self._initialized:
            self.initialize()
            
        if self._validator_labels.empty:
            return {idx: None for idx in validator_indices}
        
        # Filter for requested validators
        mask = self._validator_labels['validator_index'].isin(validator_indices)
        filtered = self._validator_labels[mask]
        
        # Create result dictionary
        result = {idx: None for idx in validator_indices}
        for _, row in filtered.iterrows():
            result[row['validator_index']] = row['entity']
            
        return result
    
    def get_validators_by_entity(self, entity_name: str) -> List[int]:
        """Get all validator indices for a given entity.
        
        Args:
            entity_name: Name of the entity
            
        Returns:
            List of validator indices
        """
        if not self._initialized:
            self.initialize()
            
        if self._validator_labels.empty:
            return []
        
        mask = self._validator_labels['entity'] == entity_name
        return self._validator_labels[mask]['validator_index'].tolist()
    
    def get_entity_statistics(self) -> pd.DataFrame:
        """Get statistics about entities.
        
        Returns:
            DataFrame with entity statistics
        """
        if not self._initialized:
            self.initialize()
            
        if self._validator_labels.empty:
            return pd.DataFrame()
        
        # Calculate statistics
        stats = self._validator_labels.groupby('entity').agg({
            'validator_index': 'count',
            'exit_type': lambda x: (x.notna()).sum()
        }).rename(columns={
            'validator_index': 'validator_count',
            'exit_type': 'exited_count'
        })
        
        # Add exit rate
        stats['exit_rate'] = stats['exited_count'] / stats['validator_count']
        
        # Sort by validator count
        return stats.sort_values('validator_count', ascending=False)
    
    def refresh(self) -> None:
        """Refresh all validator labels."""
        self.initialize(force_refresh=True)