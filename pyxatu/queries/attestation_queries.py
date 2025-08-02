"""Attestation-related queries for PyXatu."""

import json
import logging
from typing import List, Optional, Set, Dict, Any
import pandas as pd
from tqdm.auto import tqdm

from pyxatu.core.base import BaseDataFetcher
from pyxatu.models import (
    Attestation, ElaboratedAttestation, SlotQueryParams, 
    VoteType, AttestationStatus
)
from pyxatu.core.clickhouse_client import ClickHouseQueryBuilder
from pyxatu.queries.slot_queries import SlotDataFetcher


class AttestationDataFetcher(BaseDataFetcher[Attestation]):
    """Fetcher for attestation data."""
    
    def __init__(self, client, slot_fetcher: Optional[SlotDataFetcher] = None):
        super().__init__(client)
        self.slot_fetcher = slot_fetcher or SlotDataFetcher(client)
        
    def get_table_name(self) -> str:
        """Return the primary table name."""
        return 'canonical_beacon_elaborated_attestation'
        
    async def fetch(self, params: SlotQueryParams) -> pd.DataFrame:
        """Fetch attestation data."""
        builder = ClickHouseQueryBuilder()
        builder.select(params.columns).from_table(self.get_table_name())
        
        # Add slot filter with partition optimization
        if params.slot is not None:
            if isinstance(params.slot, int):
                builder.where_slot_with_partition(params.slot)
            else:
                builder.where_slot_with_partition(params.slot[0], params.slot[1])
                
        # Add network filter
        builder.where('meta_network_name', '=', params.network.value)
        
        # Add custom conditions
        if params.where:
            builder.where_raw(params.where)
            
        # Add grouping
        if params.groupby:
            builder.group_by(params.groupby)
            
        # Add ordering
        if params.orderby:
            desc = params.orderby.startswith('-')
            column = params.orderby.lstrip('-')
            builder.order_by(column, desc)
            
        # Add limit
        if params.limit:
            builder.limit(params.limit)
            
        query, query_params = builder.build()
        df = await self.client.execute_query_df(query, query_params)
        
        # Process validators column if present
        if 'validators' in df.columns:
            df['validators'] = df['validators'].apply(self._parse_validators)
            # Explode the list to have one row per validator
            df = df.explode('validators').reset_index(drop=True)
            
        return df
        
    def _parse_validators(self, validators_str: str) -> List[int]:
        """Safely parse validators JSON array."""
        if pd.isna(validators_str) or validators_str == '':
            return []
            
        try:
            # Parse JSON array
            validators = json.loads(validators_str)
            if not isinstance(validators, list):
                self.logger.warning(f"Validators not a list: {validators_str}")
                return []
            return [int(v) for v in validators]
        except (json.JSONDecodeError, ValueError) as e:
            self.logger.error(f"Failed to parse validators: {validators_str}, error: {e}")
            return []
            
    async def fetch_attestation_events(
        self, 
        params: SlotQueryParams,
        use_final: bool = False
    ) -> pd.DataFrame:
        """Fetch attestation events."""
        builder = ClickHouseQueryBuilder()
        builder.select(params.columns)
        builder.from_table('beacon_api_eth_v1_events_attestation', use_final=use_final)
        
        # Add filters
        if params.slot is not None:
            if isinstance(params.slot, int):
                builder.where('slot', '=', params.slot)
            else:
                builder.where_between('slot', params.slot[0], params.slot[1] - 1)
                
        builder.where('meta_network_name', '=', params.network.value)
        
        if params.where:
            builder.where_raw(params.where)
            
        if params.orderby:
            desc = params.orderby.startswith('-')
            builder.order_by(params.orderby.lstrip('-'), desc)
            
        if params.limit:
            builder.limit(params.limit)
            
        query, query_params = builder.build()
        return await self.client.execute_query_df(query, query_params)
        
    async def fetch_elaborated_attestations(
        self,
        slot_range: List[int],
        vote_types: List[VoteType] = None,
        status_filter: List[AttestationStatus] = None,
        include_delay: bool = True,
        network: str = 'mainnet'
    ) -> pd.DataFrame:
        """Fetch elaborated attestations with voting status."""
        if vote_types is None:
            vote_types = [VoteType.SOURCE, VoteType.TARGET, VoteType.HEAD]
        if status_filter is None:
            status_filter = list(AttestationStatus)
            
        # Fetch attestations for the slot range
        att_params = SlotQueryParams(
            slot=[slot_range[0] // 32 * 32, (slot_range[1] // 32 + 1) * 32],
            columns='slot,block_slot,source_root,target_root,validators,beacon_block_root',
            network=network,
            orderby='slot'
        )
        attestations = await self.fetch(att_params)
        
        # Fetch duties
        duties = await self._fetch_duties(att_params)
        
        # Process each slot
        results = []
        slots = sorted(attestations['slot'].unique())
        
        for slot in tqdm(slots, desc="Processing attestations"):
            if slot < slot_range[0] or slot >= slot_range[1]:
                continue
                
            # Get checkpoints for this slot
            try:
                head, target, source = await self.slot_fetcher.get_checkpoints(slot, network)
            except Exception as e:
                self.logger.warning(f"Failed to get checkpoints for slot {slot}: {e}")
                continue
                
            # Process attestations for this slot
            slot_results = await self._process_slot_attestations(
                slot, attestations, duties, 
                head, target, source,
                vote_types, status_filter, include_delay
            )
            results.extend(slot_results)
            
        return pd.DataFrame(results)
        
    async def _fetch_duties(self, params: SlotQueryParams) -> pd.DataFrame:
        """Fetch validator duties."""
        builder = ClickHouseQueryBuilder()
        builder.select('slot,validators')
        builder.from_table('beacon_api_eth_v1_beacon_committee')
        
        if params.slot:
            builder.where_between('slot', params.slot[0], params.slot[1] - 1)
        builder.where('meta_network_name', '=', params.network.value)
        
        query, query_params = builder.build()
        committee_df = await self.client.execute_query_df(query, query_params)
        
        # Process committee data
        committee_df['validators'] = committee_df['validators'].apply(self._parse_validators)
        
        # Flatten to get all validators per slot
        duties_data = []
        for _, row in committee_df.iterrows():
            slot = row['slot']
            all_validators = []
            for validators in row['validators']:
                if isinstance(validators, list):
                    all_validators.extend(validators)
                    
            # Create duty entries
            for validator in sorted(set(all_validators)):
                duties_data.append({'slot': slot, 'validator': validator})
                
        return pd.DataFrame(duties_data)
        
    async def _process_slot_attestations(
        self,
        slot: int,
        attestations: pd.DataFrame,
        duties: pd.DataFrame,
        head: str,
        target: str, 
        source: str,
        vote_types: List[VoteType],
        status_filter: List[AttestationStatus],
        include_delay: bool
    ) -> List[Dict[str, Any]]:
        """Process attestations for a single slot."""
        results = []
        
        # Get data for this slot
        slot_attestations = attestations[attestations['slot'] == slot]
        slot_duties = duties[duties['slot'] == slot]
        
        if slot_duties.empty:
            return results
            
        # Get all validators and voting validators
        all_validators = set(slot_duties['validator'].tolist())
        voting_validators = set(slot_attestations['validators'].tolist())
        
        # Calculate inclusion delays if needed
        delay_map = {}
        if include_delay and not slot_attestations.empty:
            delay_df = slot_attestations[['validators', 'slot', 'block_slot']].drop_duplicates()
            delay_df = delay_df.dropna()
            delay_df['delay'] = delay_df['block_slot'] - delay_df['slot']
            
            for _, row in delay_df.iterrows():
                delay_map[row['validators']] = int(row['delay'])
                
        # Process each vote type
        for vote_type in vote_types:
            if vote_type == VoteType.SOURCE:
                expected_root = source
                root_column = 'source_root'
            elif vote_type == VoteType.TARGET:
                expected_root = target
                root_column = 'target_root'
            else:  # HEAD
                expected_root = head
                root_column = 'beacon_block_root'
                
            # Find correct attestations
            correct_attestations = slot_attestations[
                slot_attestations[root_column] == expected_root
            ]
            correct_validators = set(correct_attestations['validators'].tolist())
            correct_validators = correct_validators.intersection(all_validators)
            
            # Calculate failed and offline
            failed_validators = all_validators.intersection(voting_validators) - correct_validators
            offline_validators = all_validators - voting_validators
            
            # Add results based on status filter
            if AttestationStatus.CORRECT in status_filter:
                for validator in correct_validators:
                    results.append({
                        'slot': slot,
                        'validator': validator,
                        'status': AttestationStatus.CORRECT.value,
                        'vote_type': vote_type.value,
                        'inclusion_delay': delay_map.get(validator) if include_delay else None
                    })
                    
            if AttestationStatus.FAILED in status_filter:
                for validator in failed_validators:
                    results.append({
                        'slot': slot,
                        'validator': validator,
                        'status': AttestationStatus.FAILED.value,
                        'vote_type': vote_type.value,
                        'inclusion_delay': delay_map.get(validator) if include_delay else None
                    })
                    
            if AttestationStatus.OFFLINE in status_filter:
                for validator in offline_validators:
                    results.append({
                        'slot': slot,
                        'validator': validator,
                        'status': AttestationStatus.OFFLINE.value,
                        'vote_type': vote_type.value,
                        'inclusion_delay': None
                    })
                    
        return results
    
    async def fetch_duties(self, params: SlotQueryParams) -> pd.DataFrame:
        """Fetch attester duty assignments (public wrapper)."""
        return await self._fetch_duties(params)