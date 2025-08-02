"""PyXatu client for querying Ethereum beacon chain data."""

import asyncio
import logging
from pathlib import Path
from typing import Optional, List, Union, Dict, Any
import pandas as pd
from functools import wraps

from pyxatu.config import ConfigManager, PyXatuConfig
from pyxatu.core.clickhouse_client import ClickHouseClient
from pyxatu.models import (
    SlotQueryParams, Network, VoteType, AttestationStatus
)
from pyxatu.queries import (
    SlotDataFetcher, AttestationDataFetcher,
    TransactionDataFetcher, ValidatorDataFetcher
)
from pyxatu.validator_labels import ValidatorLabelManager


def run_async(func):
    """Decorator to run async functions synchronously."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            # Try to get the current event loop
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # No loop running, we can use asyncio.run()
            return asyncio.run(func(*args, **kwargs))
        else:
            # Loop is already running (e.g., in Jupyter)
            # Create a task and run it in the existing loop
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                future = pool.submit(asyncio.run, func(*args, **kwargs))
                return future.result()
    return wrapper


class PyXatu:
    """Client for querying Ethereum beacon chain data from Xatu."""
    
    def __init__(
        self,
        config_path: Optional[Path] = None,
        use_env_vars: bool = True,
        log_level: str = 'INFO'
    ):
        """Initialize PyXatu.
        
        Args:
            config_path: Path to configuration file
            use_env_vars: Use environment variables for config
            log_level: Logging level
        """
        # Set up logging
        logging.basicConfig(
            level=getattr(logging, log_level.upper()),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        # Load configuration
        self.config_manager = ConfigManager(config_path, use_env_vars)
        self.config: PyXatuConfig = self.config_manager.load()
        
        # Initialize client and fetchers
        self._client: Optional[ClickHouseClient] = None
        self._slot_fetcher: Optional[SlotDataFetcher] = None
        self._attestation_fetcher: Optional[AttestationDataFetcher] = None
        self._transaction_fetcher: Optional[TransactionDataFetcher] = None
        self._validator_fetcher: Optional[ValidatorDataFetcher] = None
        self._label_manager: Optional[ValidatorLabelManager] = None
        
        # Don't auto-connect in __init__ to avoid event loop issues
        # Connection will happen on first use or explicit connect()
        
    @run_async
    async def _connect(self) -> None:
        """Connect to ClickHouse."""
        if self._client is None:
            self._client = ClickHouseClient(self.config.clickhouse)
            
            if not await self._client.test_connection():
                raise ConnectionError("Failed to connect to ClickHouse")
                
            self.logger.info("Connected to ClickHouse")
            self._slot_fetcher = SlotDataFetcher(self._client)
            self._attestation_fetcher = AttestationDataFetcher(
                self._client, self._slot_fetcher
            )
            self._transaction_fetcher = TransactionDataFetcher(self._client)
            self._validator_fetcher = ValidatorDataFetcher(self._client)
            
    @run_async
    async def _close(self) -> None:
        """Close connection."""
        if self._client:
            await self._client.close()
            self._client = None
            self.logger.info("Closed ClickHouse connection")
            
    def connect(self) -> None:
        """Connect to ClickHouse (synchronous wrapper)."""
        self._connect()
        
    def close(self) -> None:
        """Close connection."""
        self._close()
        
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        
    def _ensure_connected(self) -> None:
        """Ensure client is connected."""
        if self._client is None:
            # Try to connect if not already connected
            self.connect()
            # If still not connected after attempt, raise error
            if self._client is None:
                raise RuntimeError(
                    "Not connected to ClickHouse. Connection failed."
                )
                
    def _sort_and_reindex_df(self, df: pd.DataFrame, orderby: Optional[str] = None) -> pd.DataFrame:
        """Sort and reindex a DataFrame.
        
        Args:
            df: DataFrame to sort
            orderby: Column to sort by (prefix with - for DESC)
            
        Returns:
            Sorted and reindexed DataFrame
        """
        if df.empty:
            return df
            
        # Determine sort columns
        if orderby:
            # Handle DESC prefix
            desc = orderby.startswith('-')
            col = orderby.lstrip('-')
            if col in df.columns:
                df = df.sort_values(by=col, ascending=not desc)
        else:
            # Default: sort by first column (and second if exists)
            sort_cols = [df.columns[0]]
            if len(df.columns) > 1:
                sort_cols.append(df.columns[1])
            df = df.sort_values(by=sort_cols)
            
        # Reset index
        return df.reset_index(drop=True)
            
    # Slot/Block queries
    
    @run_async
    async def get_slots(
        self,
        slot: Optional[Union[int, List[int]]] = None,
        columns: Optional[Union[str, List[str]]] = None,
        network: Union[str, Network] = Network.MAINNET,
        include_missed: bool = False,
        limit: Optional[int] = None,
        orderby: Optional[str] = None
    ) -> pd.DataFrame:
        """Get slot/block data.
        
        Args:
            slot: Single slot or range [start, end)
            columns: Columns to retrieve (list of column names or "*" for all)
            network: Network name
            include_missed: Whether to include missed slots (default: False)
            limit: Maximum rows to return
            orderby: Column to order by (prefix with - for DESC)
            
        Returns:
            DataFrame with slot data, sorted and reindexed
        """
        self._ensure_connected()
        
        # Handle columns parameter
        if columns is None:
            columns_str = "*"
        elif isinstance(columns, list):
            columns_str = ", ".join(columns)
        else:
            columns_str = columns
        
        params = SlotQueryParams(
            slot=slot,
            columns=columns_str,
            network=network if isinstance(network, Network) else Network(network),
            limit=limit,
            orderby=orderby
        )
        
        if include_missed:
            df = await self._slot_fetcher.fetch_with_missed(params)
        else:
            df = await self._slot_fetcher.fetch(params)
            
        return self._sort_and_reindex_df(df, orderby)
            
    @run_async
    async def get_missed_slots(
        self,
        slot_range: Optional[List[int]] = None,
        network: Union[str, Network] = Network.MAINNET
    ) -> List[int]:
        """Get missed slots in a range."""
        self._ensure_connected()
        
        network_str = network.value if isinstance(network, Network) else network
        missed = await self._slot_fetcher.fetch_missed_slots(slot_range, network_str)
        return sorted(missed)
        
    @run_async
    async def get_reorgs(
        self,
        slot: Optional[Union[int, List[int]]] = None,
        network: Union[str, Network] = Network.MAINNET,
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """Get chain reorganizations.
        
        Returns:
            DataFrame with reorg data, sorted and reindexed
        """
        self._ensure_connected()
        
        params = SlotQueryParams(
            slot=slot,
            network=network if isinstance(network, Network) else Network(network),
            limit=limit
        )
        
        df = await self._slot_fetcher.fetch_reorgs(params)
        return self._sort_and_reindex_df(df)
        
    # Attestation queries
    
    @run_async
    async def get_attestations(
        self,
        slot: Optional[Union[int, List[int]]] = None,
        columns: Optional[Union[str, List[str]]] = None,
        network: Union[str, Network] = Network.MAINNET,
        limit: Optional[int] = None,
        orderby: Optional[str] = None
    ) -> pd.DataFrame:
        """Get attestation data.
        
        Args:
            slot: Single slot or range [start, end)
            columns: Columns to retrieve (list of column names or "*" for all)
            network: Network name
            limit: Maximum rows to return
            orderby: Column to order by
            
        Returns:
            DataFrame with attestation data, sorted and reindexed
        """
        self._ensure_connected()
        
        # Handle columns parameter
        if columns is None:
            columns_str = "*"
        elif isinstance(columns, list):
            columns_str = ", ".join(columns)
        else:
            columns_str = columns
        
        params = SlotQueryParams(
            slot=slot,
            columns=columns_str,
            network=network if isinstance(network, Network) else Network(network),
            limit=limit,
            orderby=orderby
        )
        
        df = await self._attestation_fetcher.fetch(params)
        return self._sort_and_reindex_df(df, orderby)
        
    @run_async
    async def get_elaborated_attestations(
        self,
        slot: Union[int, List[int]],
        vote_types: Optional[List[str]] = None,
        status_filter: Optional[List[str]] = None,
        include_delay: bool = True,
        network: Union[str, Network] = Network.MAINNET
    ) -> pd.DataFrame:
        """Get detailed attestation performance data.
        
        Returns:
            DataFrame with elaborated attestation data, sorted and reindexed
        """
        self._ensure_connected()
        
        # Convert slot to range
        if isinstance(slot, int):
            slot_range = [slot, slot + 1]
        else:
            slot_range = slot
            
        # Parse vote types
        if vote_types:
            vote_types_enum = [VoteType(vt) for vt in vote_types]
        else:
            vote_types_enum = None
            
        # Parse status filter
        if status_filter:
            status_enum = [AttestationStatus(s) for s in status_filter]
        else:
            status_enum = None
            
        network_str = network.value if isinstance(network, Network) else network
        
        df = await self._attestation_fetcher.fetch_elaborated_attestations(
            slot_range=slot_range,
            vote_types=vote_types_enum,
            status_filter=status_enum,
            include_delay=include_delay,
            network=network_str
        )
        return self._sort_and_reindex_df(df)
        
    # Transaction queries
    
    @run_async
    async def get_transactions(
        self,
        slot: Optional[Union[int, List[int]]] = None,
        columns: Optional[Union[str, List[str]]] = None,
        network: Union[str, Network] = Network.MAINNET,
        limit: Optional[int] = None,
        orderby: Optional[str] = None
    ) -> pd.DataFrame:
        """Get transaction data.
        
        Args:
            slot: Single slot or range [start, end)
            columns: Columns to retrieve (list of column names or "*" for all)
            network: Network name
            limit: Maximum rows to return
            orderby: Column to order by
            
        Returns:
            DataFrame with transaction data, sorted and reindexed
        """
        self._ensure_connected()
        
        # Handle columns parameter
        if columns is None:
            columns_str = "*"
        elif isinstance(columns, list):
            columns_str = ", ".join(columns)
        else:
            columns_str = columns
        
        params = SlotQueryParams(
            slot=slot,
            columns=columns_str,
            network=network if isinstance(network, Network) else Network(network),
            limit=limit,
            orderby=orderby
        )
        
        df = await self._transaction_fetcher.fetch(params)
        return self._sort_and_reindex_df(df, orderby)
        
    @run_async
    async def get_withdrawals(
        self,
        slot: Optional[Union[int, List[int]]] = None,
        columns: Optional[Union[str, List[str]]] = None,
        network: Union[str, Network] = Network.MAINNET,
        limit: Optional[int] = None,
        orderby: Optional[str] = None
    ) -> pd.DataFrame:
        """Get withdrawal data.
        
        Args:
            slot: Single slot or range [start, end)
            columns: Columns to retrieve (list of column names or "*" for all)
            network: Network name
            limit: Maximum rows to return
            orderby: Column to order by
            
        Returns:
            DataFrame with withdrawal data, sorted and reindexed
        """
        self._ensure_connected()
        
        # Handle columns parameter
        if columns is None:
            columns_str = "*"
        elif isinstance(columns, list):
            columns_str = ", ".join(columns)
        else:
            columns_str = columns
        
        params = SlotQueryParams(
            slot=slot,
            columns=columns_str,
            network=network if isinstance(network, Network) else Network(network),
            limit=limit,
            orderby=orderby
        )
        
        df = await self._transaction_fetcher.fetch_withdrawals(params)
        return self._sort_and_reindex_df(df, orderby)
        
    # Validator queries
    
    @run_async
    async def get_validators(
        self,
        validator_indices: Optional[Union[int, List[int]]] = None,
        columns: Optional[Union[str, List[str]]] = None,
        network: Union[str, Network] = Network.MAINNET,
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """Get validator data.
        
        Args:
            validator_indices: Single validator index or list of indices
            columns: Columns to retrieve (list of column names or "*" for all)
            network: Network name
            limit: Maximum rows to return
            
        Returns:
            DataFrame with validator data, sorted and reindexed
        """
        self._ensure_connected()
        
        # Handle columns parameter
        if columns is None:
            columns_str = "*"
        elif isinstance(columns, list):
            columns_str = ", ".join(columns)
        else:
            columns_str = columns
        
        network_str = network.value if isinstance(network, Network) else network
        
        df = await self._validator_fetcher.fetch_validators(
            validator_indices=validator_indices,
            columns=columns_str,
            network=network_str,
            limit=limit
        )
        return self._sort_and_reindex_df(df)
        
    async def _get_label_manager(self) -> ValidatorLabelManager:
        """Get or create label manager."""
        if self._label_manager is None:
            self._label_manager = ValidatorLabelManager()
        return self._label_manager
        
    @run_async
    async def get_validator_labels(
        self,
        indices: Optional[Union[int, List[int]]] = None,
        refresh: bool = False
    ) -> pd.DataFrame:
        """Get validator labels with entity mapping.
        
        Returns:
            DataFrame with validator labels, sorted and reindexed
        """
        manager = await self._get_label_manager()
        
        if refresh:
            await manager.refresh_labels()
            
        df = manager.get_labels(indices)
        return self._sort_and_reindex_df(df)
        
    @run_async
    async def get_validator_labels_bulk(
        self,
        validator_indices: List[int]
    ) -> Dict[int, Optional[str]]:
        """Get labels for multiple validators as a dictionary."""
        manager = await self._get_label_manager()
        return manager.get_validator_labels_bulk(validator_indices)
        
    def get_label_manager(self) -> ValidatorLabelManager:
        """Get the validator label manager instance."""
        if self._label_manager is None:
            self._label_manager = ValidatorLabelManager()
        return self._label_manager
        
    @run_async
    async def get_validators_by_entity(self, entity_name: str) -> List[int]:
        """Get all validator indices for a given entity."""
        manager = await self._get_label_manager()
        return manager.get_validators_by_entity(entity_name)
        
    @run_async
    async def get_entity_statistics(self) -> pd.DataFrame:
        """Get statistics about validator entities.
        
        Returns:
            DataFrame with entity statistics, sorted and reindexed
        """
        manager = await self._get_label_manager()
        df = manager.get_entity_statistics()
        return self._sort_and_reindex_df(df)
        
    @run_async
    async def refresh_validator_labels(self) -> None:
        """Refresh the validator labels cache."""
        manager = await self._get_label_manager()
        await manager.refresh_labels()
        
    # Custom queries
    
    @run_async
    async def execute_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None
    ) -> pd.DataFrame:
        """Execute raw SQL query.
        
        Args:
            query: SQL query to execute
            params: Query parameters for safe substitution
            
        Returns:
            DataFrame with query results, sorted and reindexed
        """
        self._ensure_connected()
        self.logger.warning("Executing raw SQL query - ensure it's properly sanitized")
        df = await self._client.execute_query_df(query, params)
        return self._sort_and_reindex_df(df)
        
    def raw_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None
    ) -> pd.DataFrame:
        """Execute raw SQL query (alias for execute_query).
        
        Returns:
            DataFrame with query results, sorted and reindexed
        """
        return self.execute_query(query, params)