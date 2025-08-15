"""Slot and block-related queries for PyXatu."""

import logging
from typing import List, Optional, Set
import pandas as pd

from pyxatu.core.base import BaseDataFetcher
from pyxatu.models import Block, SlotQueryParams
from pyxatu.core.clickhouse_client import ClickHouseQueryBuilder


class SlotDataFetcher(BaseDataFetcher[Block]):
    """Fetcher for slot and block data."""
    
    def get_table_name(self) -> str:
        """Return the primary table name."""
        return 'canonical_beacon_block'
        
    def fetch(self, params: SlotQueryParams) -> pd.DataFrame:
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
        return self.client.execute_query_df(query, query_params)
        
    def fetch_with_missed(self, params: SlotQueryParams) -> pd.DataFrame:
        """Fetch slots including missed slots."""
        # Get canonical blocks
        df = self.fetch(params)
        
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
        
    def fetch_missed_slots(
        self, 
        slot_range: Optional[List[int]] = None,
        network: str = 'mainnet'
    ) -> Set[int]:
        """Get set of missed slots in a range."""
        from pyxatu.models import Network
        
        # Convert string to Network enum if needed
        network_enum = Network(network) if isinstance(network, str) else network
        
        params = SlotQueryParams(
            slot=slot_range,
            columns='slot',
            network=network_enum,
            orderby='slot'
        )
        
        df = self.fetch(params)
        
        if df.empty:
            return set()
            
        slot_min = slot_range[0] if slot_range else df['slot'].min()
        slot_max = slot_range[1] - 1 if slot_range else df['slot'].max()
        
        all_slots = set(range(slot_min, slot_max + 1))
        existing_slots = set(df['slot'].unique())
        
        return all_slots - existing_slots
        
    def fetch_reorgs(self, params: SlotQueryParams) -> pd.DataFrame:
        """Fetch chain reorganization data.
        
        Returns slots that were reorged AND are missing (empty slots without execution payload).
        """
        # First get the reorg events to find affected slots
        builder = ClickHouseQueryBuilder()
        builder.select('slot, depth, slot - depth as reorged_slot, old_head_block, new_head_block')
        builder.from_table('beacon_api_eth_v1_events_chain_reorg')
        
        # Apply slot filter if provided
        if params.slot:
            if isinstance(params.slot, int):
                # For single slot, check if it was reorged
                builder.where_raw(f"(slot - depth) = {params.slot}")
            else:
                # For range, find reorgs affecting slots in that range
                builder.where_raw(f"(slot - depth) >= {params.slot[0]} AND (slot - depth) < {params.slot[1]}")
                
        builder.where('meta_network_name', '=', params.network.value)
        
        if params.where:
            builder.where_raw(params.where)
            
        # Get all reorg events
        query, query_params = builder.build()
        reorg_events = self.client.execute_query_df(query, query_params)
        
        if reorg_events.empty:
            return pd.DataFrame(columns=['slot', 'depth', 'old_head_block', 'new_head_block'])
            
        # Get unique reorged slots
        reorged_slots = set(reorg_events['reorged_slot'].unique())
        
        # If no reorged slots found, return early
        if not reorged_slots:
            return reorg_events[['slot', 'depth', 'old_head_block', 'new_head_block']] if not reorg_events.empty else reorg_events
        
        # Now check which of these slots are actually missing (empty blocks)
        # A slot is considered "missed" if it doesn't have an execution payload
        check_builder = ClickHouseQueryBuilder()
        check_builder.select('slot')
        check_builder.from_table('canonical_beacon_block')
        check_builder.where('meta_network_name', '=', params.network.value)
        check_builder.where_in('slot', list(reorged_slots))
        # Empty slots have null execution payload block hash
        check_builder.where_raw('body_execution_payload_block_hash IS NOT NULL')
        
        check_query, check_params = check_builder.build()
        slots_with_payload = self.client.execute_query_df(check_query, check_params)
        
        # Slots that exist and have execution payload are not truly "missed"
        if not slots_with_payload.empty:
            slots_with_payload_set = set(slots_with_payload['slot'].unique())
            reorged_slots = reorged_slots - slots_with_payload_set
        
        # Get the reorg event details for the truly missed slots
        result = reorg_events[reorg_events['reorged_slot'].isin(reorged_slots)].copy()
        
        # Clean up and format the result
        if not result.empty:
            result = result.drop_duplicates(subset=['reorged_slot']).sort_values('reorged_slot')
            result = result.rename(columns={'reorged_slot': 'slot'})
            
        return result
        
    def get_checkpoints(self, slot: int, network: str = 'mainnet') -> tuple[str, str, str]:
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
        
        slots_df = self.fetch(params)
        
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
    
    def fetch_checkpoints(self, params: SlotQueryParams) -> pd.DataFrame:
        """Fetch checkpoint data for slots."""
        builder = ClickHouseQueryBuilder()
        builder.select(params.columns).from_table('canonical_beacon_block')
        
        # Add slot filter with partition optimization
        if params.slot is not None:
            if isinstance(params.slot, int):
                builder.where_slot_with_partition(params.slot)
            else:
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
        return self.client.execute_query_df(query, query_params)
    
    def fetch_beacon_blocks_v2(self, params: SlotQueryParams) -> pd.DataFrame:
        """Fetch beacon block v2 data."""
        builder = ClickHouseQueryBuilder()
        builder.select(params.columns).from_table('canonical_beacon_block')
        
        # Add slot filter with partition optimization
        if params.slot is not None:
            if isinstance(params.slot, int):
                builder.where_slot_with_partition(params.slot)
            else:
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
        return self.client.execute_query_df(query, query_params)