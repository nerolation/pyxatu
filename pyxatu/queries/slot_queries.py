"""Slot and block-related queries for PyXatu."""

import logging
from typing import List, Optional, Set
import pandas as pd

from pyxatu.base import BaseDataFetcher
from pyxatu.models import Block, SlotQueryParams
from pyxatu.clickhouse_client import ClickHouseQueryBuilder


class SlotDataFetcher(BaseDataFetcher[Block]):
    """Fetcher for slot and block data."""
    
    def get_table_name(self) -> str:
        """Return the primary table name."""
        return 'canonical_beacon_block'
        
    async def fetch(self, params: SlotQueryParams) -> pd.DataFrame:
        """Fetch slot/block data based on parameters."""
        builder = ClickHouseQueryBuilder()
        builder.select(params.columns).from_table(self.get_table_name())
        
        # Add slot filter with partition optimization
        if params.slot is not None:
            if isinstance(params.slot, int):
                builder.where_slot_with_partition(params.slot)
            else:  # List of 2 ints
                builder.where_slot_with_partition(params.slot[0], params.slot[1])
                
        # Add network filter
        builder.where('meta_network_name', '=', params.network.value)
        
        # Add custom where conditions
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
        
    async def fetch_with_missed(self, params: SlotQueryParams) -> pd.DataFrame:
        """Fetch slots including missed slots."""
        # Get canonical blocks
        df = await self.fetch(params)
        
        if df.empty:
            return df
            
        # Calculate missed slots
        slot_min = df['slot'].min()
        slot_max = df['slot'].max()
        all_slots = set(range(slot_min, slot_max + 1))
        existing_slots = set(df['slot'].unique())
        missed_slots = all_slots - existing_slots
        
        if not missed_slots:
            return df
            
        # Create missed slots DataFrame
        missed_df = pd.DataFrame({'slot': sorted(missed_slots)})
        
        # Add default values for other columns
        for col in df.columns:
            if col != 'slot':
                dtype = df[col].dtype
                if pd.api.types.is_numeric_dtype(dtype):
                    missed_df[col] = 0 if 'index' not in col else 999999999
                else:
                    missed_df[col] = 'missed'
                    
        # Combine and sort
        result = pd.concat([df, missed_df], ignore_index=True)
        if params.orderby:
            result.sort_values(params.orderby.lstrip('-'), 
                             ascending=not params.orderby.startswith('-'), 
                             inplace=True)
        else:
            result.sort_values('slot', inplace=True)
            
        return result
        
    async def fetch_missed_slots(
        self, 
        slot_range: Optional[List[int]] = None,
        network: str = 'mainnet'
    ) -> Set[int]:
        """Get set of missed slots in a range."""
        params = SlotQueryParams(
            slot=slot_range,
            columns='slot',
            network=network,
            orderby='slot'
        )
        
        df = await self.fetch(params)
        
        if df.empty:
            return set()
            
        slot_min = slot_range[0] if slot_range else df['slot'].min()
        slot_max = slot_range[1] - 1 if slot_range else df['slot'].max()
        
        all_slots = set(range(slot_min, slot_max + 1))
        existing_slots = set(df['slot'].unique())
        
        return all_slots - existing_slots
        
    async def fetch_reorgs(self, params: SlotQueryParams) -> pd.DataFrame:
        """Fetch chain reorganization data."""
        # First get potential reorgs
        builder = ClickHouseQueryBuilder()
        builder.select('(slot-depth) as reorged_slot')
        builder.from_table('beacon_api_eth_v1_events_chain_reorg')
        
        if params.slot:
            if isinstance(params.slot, int):
                # For single slot, look at reorgs around it
                builder.where_between('slot', params.slot - 32, params.slot + 32)
            else:
                # For range, expand the search window
                builder.where_between('slot', params.slot[0] - 32, params.slot[1] + 31)
                
        builder.where('meta_network_name', '=', params.network.value)
        
        if params.where:
            builder.where_raw(params.where)
            
        query, query_params = builder.build()
        potential_reorgs = await self.client.execute_query_df(query, query_params)
        
        if potential_reorgs.empty:
            return pd.DataFrame(columns=['slot'])
            
        # Get missed slots in the range
        missed_slots = await self.fetch_missed_slots(params.slot, params.network.value)
        
        # Find intersection
        reorged_slots = set(potential_reorgs['reorged_slot'].tolist())
        actual_reorgs = sorted(reorged_slots.intersection(missed_slots))
        
        return pd.DataFrame({'slot': actual_reorgs})
        
    async def get_checkpoints(self, slot: int, network: str = 'mainnet') -> tuple[str, str, str]:
        """Get head, target, and source checkpoints for a slot."""
        epoch_start = (slot // 32) * 32
        last_epoch_start = epoch_start - 32
        
        # Fetch blocks around the checkpoints
        params = SlotQueryParams(
            slot=[last_epoch_start - 32, epoch_start + 32],
            columns='slot,block_root',
            network=network,
            orderby='slot'
        )
        
        slots_df = await self.fetch(params)
        
        if slots_df.empty:
            raise ValueError(f"No blocks found for slot {slot}")
            
        # Find head (last block at or before slot)
        head = None
        for check_slot in range(slot, -1, -1):
            matching = slots_df[slots_df['slot'] == check_slot]
            if not matching.empty:
                head = matching.iloc[0]['block_root']
                break
                
        # Find target (last block at or before epoch start)
        target = None
        for check_slot in range(epoch_start, -1, -1):
            matching = slots_df[slots_df['slot'] == check_slot]
            if not matching.empty:
                target = matching.iloc[0]['block_root']
                break
                
        # Find source (last block at or before previous epoch start)
        source = None
        for check_slot in range(last_epoch_start, -1, -1):
            matching = slots_df[slots_df['slot'] == check_slot]
            if not matching.empty:
                source = matching.iloc[0]['block_root']
                break
                
        if not all([head, target, source]):
            raise ValueError(f"Could not determine all checkpoints for slot {slot}")
            
        return head, target, source