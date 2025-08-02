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
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(func(*args, **kwargs))
        finally:
            # Clean up pending tasks
            pending = asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            loop.close()
            asyncio.set_event_loop(None)
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
        
        # Auto-connect on init
        self._connect()
        
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
            
    def close(self) -> None:
        """Close connection."""
        self._close()
        
    def __enter__(self):
        """Context manager entry."""
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        
    def _ensure_connected(self) -> None:
        """Ensure client is connected."""
        if self._client is None:
            raise RuntimeError(
                "Not connected to ClickHouse. Connection failed during initialization."
            )
            
    # Slot/Block queries
    
    @run_async
    async def get_slots(
        self,
        slot: Optional[Union[int, List[int]]] = None,
        columns: str = "*",
        network: Union[str, Network] = Network.MAINNET,
        include_missed: bool = True,
        limit: Optional[int] = None,
        orderby: Optional[str] = None
    ) -> pd.DataFrame:
        """Get slot/block data.
        
        Args:
            slot: Single slot or range [start, end)
            columns: Columns to retrieve
            network: Network name
            include_missed: Whether to include missed slots
            limit: Maximum rows to return
            orderby: Column to order by (prefix with - for DESC)
            
        Returns:
            DataFrame with slot data
        """
        self._ensure_connected()
        
        params = SlotQueryParams(
            slot=slot,
            columns=columns,
            network=network if isinstance(network, Network) else Network(network),
            limit=limit,
            orderby=orderby
        )
        
        if include_missed:
            return await self._slot_fetcher.fetch_with_missed(params)
        else:
            return await self._slot_fetcher.fetch(params)
            
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
        """Get chain reorganizations."""
        self._ensure_connected()
        
        params = SlotQueryParams(
            slot=slot,
            network=network if isinstance(network, Network) else Network(network),
            limit=limit
        )
        
        return await self._slot_fetcher.fetch_reorgs(params)
        
    # Attestation queries
    
    @run_async
    async def get_attestations(
        self,
        slot: Optional[Union[int, List[int]]] = None,
        columns: str = "*",
        network: Union[str, Network] = Network.MAINNET,
        limit: Optional[int] = None,
        orderby: Optional[str] = None
    ) -> pd.DataFrame:
        """Get attestation data.
        
        Args:
            slot: Single slot or range [start, end)
            columns: Columns to retrieve
            network: Network name
            limit: Maximum rows to return
            orderby: Column to order by
            
        Returns:
            DataFrame with attestation data
        """
        self._ensure_connected()
        
        params = SlotQueryParams(
            slot=slot,
            columns=columns,
            network=network if isinstance(network, Network) else Network(network),
            limit=limit,
            orderby=orderby
        )
        
        return await self._attestation_fetcher.fetch(params)
        
    @run_async
    async def get_elaborated_attestations(
        self,
        slot: Union[int, List[int]],
        vote_types: Optional[List[str]] = None,
        status_filter: Optional[List[str]] = None,
        include_delay: bool = True,
        network: Union[str, Network] = Network.MAINNET
    ) -> pd.DataFrame:
        """Get detailed attestation performance data."""
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
        
        return await self._attestation_fetcher.fetch_elaborated_attestations(
            slot_range=slot_range,
            vote_types=vote_types_enum,
            status_filter=status_enum,
            include_delay=include_delay,
            network=network_str
        )
        
    # Transaction queries
    
    @run_async
    async def get_transactions(
        self,
        slot: Optional[Union[int, List[int]]] = None,
        columns: str = "*",
        network: Union[str, Network] = Network.MAINNET,
        limit: Optional[int] = None,
        orderby: Optional[str] = None
    ) -> pd.DataFrame:
        """Get transaction data."""
        self._ensure_connected()
        
        params = SlotQueryParams(
            slot=slot,
            columns=columns,
            network=network if isinstance(network, Network) else Network(network),
            limit=limit,
            orderby=orderby
        )
        
        return await self._transaction_fetcher.fetch(params)
        
    @run_async
    async def get_withdrawals(
        self,
        slot: Optional[Union[int, List[int]]] = None,
        columns: str = "*",
        network: Union[str, Network] = Network.MAINNET,
        limit: Optional[int] = None,
        orderby: Optional[str] = None
    ) -> pd.DataFrame:
        """Get withdrawal data."""
        self._ensure_connected()
        
        params = SlotQueryParams(
            slot=slot,
            columns=columns,
            network=network if isinstance(network, Network) else Network(network),
            limit=limit,
            orderby=orderby
        )
        
        return await self._transaction_fetcher.fetch_withdrawals(params)
        
    # Validator queries
    
    @run_async
    async def get_validators(
        self,
        validator_indices: Optional[Union[int, List[int]]] = None,
        columns: str = "*",
        network: Union[str, Network] = Network.MAINNET,
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """Get validator data."""
        self._ensure_connected()
        
        network_str = network.value if isinstance(network, Network) else network
        
        return await self._validator_fetcher.fetch_validators(
            validator_indices=validator_indices,
            columns=columns,
            network=network_str,
            limit=limit
        )
        
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
        """Get validator labels with entity mapping."""
        manager = await self._get_label_manager()
        
        if refresh:
            await manager.refresh_labels()
            
        return manager.get_labels(indices)
        
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
        """Get statistics about validator entities."""
        manager = await self._get_label_manager()
        return manager.get_entity_statistics()
        
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
            DataFrame with query results
        """
        self._ensure_connected()
        self.logger.warning("Executing raw SQL query - ensure it's properly sanitized")
        return await self._client.execute_query_df(query, params)
        
    def raw_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None
    ) -> pd.DataFrame:
        """Execute raw SQL query (alias for execute_query)."""
        return self.execute_query(query, params)