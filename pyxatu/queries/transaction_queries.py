"""Transaction-related queries for PyXatu."""

import logging
from typing import List, Optional, Set, Dict, Any
from datetime import datetime, timedelta
import pandas as pd

from pyxatu.core.base import BaseDataFetcher, BaseConnector
from pyxatu.models import Transaction, SlotQueryParams
from pyxatu.core.clickhouse_client import ClickHouseQueryBuilder
from pyxatu.utils import slot_to_timestamp


class TransactionDataFetcher(BaseDataFetcher[Transaction]):
    """Fetcher for transaction data."""
    
    def __init__(self, client, mempool_connector: Optional[BaseConnector] = None):
        super().__init__(client)
        self.mempool_connector = mempool_connector
        
    def get_table_name(self) -> str:
        """Return the primary table name."""
        return 'canonical_beacon_block_execution_transaction'
        
    async def fetch(self, params: SlotQueryParams) -> pd.DataFrame:
        """Fetch transaction data."""
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
        
    async def fetch_el_transactions(self, params: SlotQueryParams) -> pd.DataFrame:
        """Fetch execution layer transaction data."""
        builder = ClickHouseQueryBuilder()
        builder.select(params.columns)
        builder.from_table('canonical_execution_transaction')
        
        # For EL transactions, we need to use block number instead of slot
        # This would require joining with canonical_beacon_block to map slots to blocks
        # For now, we'll use time-based filtering if slot is provided
        
        if params.slot is not None:
            # Convert slot to approximate timestamp range
            if isinstance(params.slot, int):
                start_time = slot_to_timestamp(params.slot)
                end_time = start_time + timedelta(seconds=12)
                builder.where_between('block_timestamp', start_time, end_time)
            else:
                start_time = slot_to_timestamp(params.slot[0])
                end_time = slot_to_timestamp(params.slot[1])
                builder.where_between('block_timestamp', start_time, end_time)
                
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
        
    async def fetch_mempool_transactions(
        self,
        slot_range: List[int],
        network: str = 'mainnet',
        lookback_slots: int = 100
    ) -> pd.DataFrame:
        """Fetch mempool transaction data."""
        # Expand slot range for mempool lookback
        expanded_start = slot_range[0] - lookback_slots
        
        # Convert to timestamp range
        start_time = slot_to_timestamp(expanded_start)
        end_time = slot_to_timestamp(slot_range[1] if isinstance(slot_range, list) else slot_range + 1)
        
        builder = ClickHouseQueryBuilder()
        builder.select('hash,event_date_time')
        builder.from_table('mempool_transaction')
        
        # Use event_date_time for mempool transactions
        builder.where_between('event_date_time', start_time, end_time)
        builder.where('meta_network_name', '=', network)
        
        query, query_params = builder.build()
        return await self.client.execute_query_df(query, query_params)
        
    async def fetch_elaborated_transactions(
        self,
        slots: List[int],
        network: str = 'mainnet',
        include_external_mempool: bool = True
    ) -> pd.DataFrame:
        """Fetch transactions with privacy analysis."""
        # Fetch transactions for the slots
        all_transactions = []
        
        for slot in slots:
            params = SlotQueryParams(
                slot=slot,
                columns='slot,position,hash,from_address,to_address,value,gas,gas_price',
                network=network
            )
            slot_txs = await self.fetch(params)
            all_transactions.append(slot_txs)
            
        if not all_transactions:
            return pd.DataFrame()
            
        transactions_df = pd.concat(all_transactions, ignore_index=True)
        
        # Normalize transaction hashes
        transactions_df['hash'] = transactions_df['hash'].str.lower()
        
        # Collect mempool data from multiple sources
        mempool_hashes = set()
        
        # 1. Xatu mempool data
        xatu_mempool = await self.fetch_mempool_transactions(
            [min(slots), max(slots) + 1],
            network
        )
        if not xatu_mempool.empty:
            mempool_hashes.update(xatu_mempool['hash'].str.lower())
            
        # 2. External mempool sources (if connector available and enabled)
        if include_external_mempool and self.mempool_connector:
            for slot in slots:
                try:
                    # Fetch from Flashbots
                    flashbots_data = await self.mempool_connector.fetch_data(
                        source='flashbots',
                        timestamp=slot_to_timestamp(slot)
                    )
                    if flashbots_data:
                        mempool_hashes.update(tx['hash'].lower() for tx in flashbots_data)
                        
                    # Fetch from Blocknative
                    blocknative_data = await self.mempool_connector.fetch_data(
                        source='blocknative',
                        timestamp=slot_to_timestamp(slot)
                    )
                    if blocknative_data:
                        mempool_hashes.update(tx['hash'].lower() for tx in blocknative_data)
                        
                except Exception as e:
                    self.logger.warning(f"Failed to fetch external mempool data: {e}")
                    
        # Mark private transactions
        transactions_df['private'] = ~transactions_df['hash'].isin(mempool_hashes)
        
        # Log statistics
        total_txs = len(transactions_df)
        private_txs = transactions_df['private'].sum()
        self.logger.info(
            f"Transaction privacy analysis: {private_txs}/{total_txs} "
            f"({private_txs/total_txs*100:.1f}%) marked as private"
        )
        
        return transactions_df
        
    async def fetch_withdrawals(self, params: SlotQueryParams) -> pd.DataFrame:
        """Fetch withdrawal data."""
        builder = ClickHouseQueryBuilder()
        builder.select(params.columns)
        builder.from_table('canonical_beacon_block_withdrawal')
        
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
        
    async def fetch_blob_sidecars(self, params: SlotQueryParams) -> pd.DataFrame:
        """Fetch blob sidecar data (EIP-4844)."""
        builder = ClickHouseQueryBuilder()
        builder.select(params.columns)
        builder.from_table('canonical_beacon_blob_sidecar')
        
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
        
    async def fetch_block_sizes(self, params: SlotQueryParams) -> pd.DataFrame:
        """Fetch block size metrics."""
        # Ensure we get the required columns
        required_columns = [
            'slot',
            'block_total_bytes_compressed',
            'block_total_bytes',
            'execution_payload_blob_gas_used',
            'execution_payload_transactions_total_bytes',
            'execution_payload_transactions_total_bytes_compressed'
        ]
        
        # Parse existing columns
        if params.columns == '*':
            columns = required_columns
        else:
            existing = [c.strip() for c in params.columns.split(',')]
            columns = list(set(existing + required_columns))
            
        # Update params with required columns
        params = SlotQueryParams(
            slot=params.slot,
            columns=','.join(columns),
            where=params.where,
            network=params.network,
            orderby=params.orderby,
            limit=params.limit
        )
        
        # Fetch slot data
        from pyxatu.queries.slot_queries import SlotDataFetcher
        slot_fetcher = SlotDataFetcher(self.client)
        df = await slot_fetcher.fetch_with_missed(params)
        
        # Process blob gas to blob count
        if 'execution_payload_blob_gas_used' in df.columns:
            # Filter out null and missed values
            df = df[
                (df['execution_payload_blob_gas_used'] != '\\N') &
                (df['execution_payload_blob_gas_used'] != 'missed')
            ].copy()
            
            # Convert to int and calculate blob count
            df['execution_payload_blob_gas_used'] = df['execution_payload_blob_gas_used'].astype(int)
            df['blobs'] = df['execution_payload_blob_gas_used'] // 131072  # Blob gas per blob
            df.drop('execution_payload_blob_gas_used', axis=1, inplace=True)
            
        return df
    
    async def fetch_blob_events(self, params: SlotQueryParams) -> pd.DataFrame:
        """Fetch blob sidecar events from beacon API."""
        query = f"""
        SELECT
            slot,
            slot_start_date_time,
            epoch,
            wallclock_slot,
            wallclock_epoch,
            propagation_slot_start_diff,
            block_root,
            blob_index,
            kzg_commitment,
            versioned_hash,
            meta_client_name,
            meta_client_version,
            meta_network_name
        FROM beacon_api_eth_v1_events_blob_sidecar
        WHERE {self._build_where_clause(params)}
        {self._build_order_clause(params)}
        {self._build_limit_clause(params)}
        """
        
        return await self.client.execute_query_df(query)