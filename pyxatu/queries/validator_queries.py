"""Validator-related queries for PyXatu."""

import logging
from typing import List, Optional, Dict, Any
import pandas as pd

from pyxatu.core.base import BaseDataFetcher
from pyxatu.models import ValidatorDuty, SlotQueryParams
from pyxatu.core.clickhouse_client import ClickHouseQueryBuilder


class ValidatorDataFetcher(BaseDataFetcher[ValidatorDuty]):
    """Fetcher for validator-related data."""
    
    def get_table_name(self) -> str:
        """Return the primary table name."""
        return 'canonical_beacon_proposer_duty'
        
    async def fetch(self, params: SlotQueryParams) -> pd.DataFrame:
        """Implementation of abstract fetch method."""
        return await self.fetch_proposer_duties(params)
        
    async def fetch_proposer_duties(self, params: SlotQueryParams) -> pd.DataFrame:
        """Fetch proposer duty assignments."""
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
            
        # Add ordering
        if params.orderby:
            desc = params.orderby.startswith('-')
            column = params.orderby.lstrip('-')
            builder.order_by(column, desc)
            
        # Add limit
        if params.limit:
            builder.limit(params.limit)
            
        query, query_params = builder.build()
        return await self.client.execute_query_df(query, query_params)
        
    async def fetch_block_events(self, params: SlotQueryParams) -> pd.DataFrame:
        """Fetch block event data."""
        builder = ClickHouseQueryBuilder()
        builder.select(params.columns)
        builder.from_table('beacon_api_eth_v1_events_block')
        
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
            
        # Add ordering
        if params.orderby:
            desc = params.orderby.startswith('-')
            column = params.orderby.lstrip('-')
            builder.order_by(column, desc)
            
        # Add limit
        if params.limit:
            builder.limit(params.limit)
            
        query, query_params = builder.build()
        return await self.client.execute_query_df(query, query_params)
        
    async def fetch_beacon_blocks_v2(self, params: SlotQueryParams) -> pd.DataFrame:
        """Fetch beacon block data from v2 endpoint."""
        builder = ClickHouseQueryBuilder()
        builder.select(params.columns)
        builder.from_table('beacon_api_eth_v2_beacon_block')
        
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
            
        # Add ordering
        if params.orderby:
            desc = params.orderby.startswith('-')
            column = params.orderby.lstrip('-')
            builder.order_by(column, desc)
            
        # Add limit
        if params.limit:
            builder.limit(params.limit)
            
        query, query_params = builder.build()
        return await self.client.execute_query_df(query, query_params)
        
    async def enrich_missed_slots_with_proposers(
        self,
        slots_df: pd.DataFrame,
        network: str = 'mainnet'
    ) -> pd.DataFrame:
        """Enrich missed slots with their assigned proposers."""
        if 'proposer_index' not in slots_df.columns:
            return slots_df
            
        # Find missed slots (where proposer_index is the sentinel value)
        missed_mask = slots_df['proposer_index'] == 999999999
        if not missed_mask.any():
            return slots_df
            
        # Get slot range
        slot_min = int(slots_df['slot'].min())
        slot_max = int(slots_df['slot'].max())
        
        # Fetch proposer duties for the range
        params = SlotQueryParams(
            slot=[slot_min, slot_max + 1],
            columns='slot,proposer_validator_index',
            network=network
        )
        proposer_duties = await self.fetch_proposer_duties(params)
        
        if proposer_duties.empty:
            return slots_df
            
        # Create a mapping of slot to proposer
        proposer_map = dict(
            zip(proposer_duties['slot'], proposer_duties['proposer_validator_index'])
        )
        
        # Update missed slots with actual proposer indices
        def update_proposer(row):
            if row['proposer_index'] == 999999999:
                return proposer_map.get(row['slot'], row['proposer_index'])
            return row['proposer_index']
            
        slots_df = slots_df.copy()
        slots_df['proposer_index'] = slots_df.apply(update_proposer, axis=1)
        
        return slots_df