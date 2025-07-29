"""PyXatu client for querying Ethereum beacon chain data."""

import asyncio
import logging
from pathlib import Path
from typing import Optional, List, Union, Dict, Any
import pandas as pd

from pyxatu.config import ConfigManager, PyXatuConfig
from pyxatu.clickhouse_client import ClickHouseClient
from pyxatu.models import (
    SlotQueryParams, Network, VoteType, AttestationStatus
)
from pyxatu.queries import (
    SlotDataFetcher, AttestationDataFetcher,
    TransactionDataFetcher, ValidatorDataFetcher
)
from pyxatu.validator_labels import ValidatorLabelManager


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
        
        # Track if we're in an async context
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        
    async def __aenter__(self):
        """Async context manager entry."""
        await self.connect()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
        
    async def connect(self) -> None:
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
            
    async def close(self) -> None:
        """Close connection."""
        if self._client:
            await self._client.close()
            self._client = None
            self.logger.info("Closed ClickHouse connection")
            
    def _ensure_connected(self) -> None:
        """Ensure client is connected."""
        if self._client is None:
            raise RuntimeError(
                "Not connected to ClickHouse. Use 'async with PyXatu() as xatu:' "
                "or call connect() first"
            )
            
    # Slot/Block queries
    
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
    
    async def get_transactions(
        self,
        slot: Optional[Union[int, List[int]]] = None,
        columns: str = "*",
        network: Union[str, Network] = Network.MAINNET,
        limit: Optional[int] = None,
        orderby: Optional[str] = None
    ) -> pd.DataFrame:
        """Get transaction data.
        
        Args:
            slot: Single slot or range [start, end)
            columns: Columns to retrieve
            network: Network name
            limit: Maximum rows to return
            orderby: Column to order by
            
        Returns:
            DataFrame with transaction data
        """
        self._ensure_connected()
        
        params = SlotQueryParams(
            slot=slot,
            columns=columns,
            network=network if isinstance(network, Network) else Network(network),
            limit=limit,
            orderby=orderby
        )
        
        return await self._transaction_fetcher.fetch(params)
        
    async def get_elaborated_transactions(
        self,
        slots: List[int],
        network: Union[str, Network] = Network.MAINNET,
        include_external_mempool: bool = True
    ) -> pd.DataFrame:
        """Get transactions with privacy analysis.
        
        Args:
            slots: List of slots to analyze
            network: Network name
            include_external_mempool: Whether to check external mempool sources
            
        Returns:
            DataFrame with transactions including 'private' column
        """
        self._ensure_connected()
        
        network_str = network.value if isinstance(network, Network) else network
        
        return await self._transaction_fetcher.fetch_elaborated_transactions(
            slots=slots,
            network=network_str,
            include_external_mempool=include_external_mempool
        )
        
    async def get_withdrawals(
        self,
        slot: Optional[Union[int, List[int]]] = None,
        columns: str = "*",
        network: Union[str, Network] = Network.MAINNET,
        limit: Optional[int] = None,
        orderby: Optional[str] = None
    ) -> pd.DataFrame:
        """Get withdrawal data.
        
        Args:
            slot: Single slot or range [start, end)
            columns: Columns to retrieve
            network: Network name
            limit: Maximum rows to return
            orderby: Column to order by
            
        Returns:
            DataFrame with withdrawal data
        """
        self._ensure_connected()
        
        params = SlotQueryParams(
            slot=slot,
            columns=columns,
            network=network if isinstance(network, Network) else Network(network),
            limit=limit,
            orderby=orderby
        )
        
        return await self._transaction_fetcher.fetch_withdrawals(params)
        
    async def get_block_sizes(
        self,
        slot: Optional[Union[int, List[int]]] = None,
        network: Union[str, Network] = Network.MAINNET,
        limit: Optional[int] = None,
        orderby: Optional[str] = "slot"
    ) -> pd.DataFrame:
        """Get block size metrics.
        
        Args:
            slot: Single slot or range [start, end)
            network: Network name
            limit: Maximum rows to return
            orderby: Column to order by
            
        Returns:
            DataFrame with block size data including blob count
        """
        self._ensure_connected()
        
        params = SlotQueryParams(
            slot=slot,
            network=network if isinstance(network, Network) else Network(network),
            limit=limit,
            orderby=orderby
        )
        
        return await self._transaction_fetcher.fetch_block_sizes(params)
        
    # Validator queries
    
    async def get_proposer_duties(
        self,
        slot: Optional[Union[int, List[int]]] = None,
        columns: str = "*",
        network: Union[str, Network] = Network.MAINNET,
        limit: Optional[int] = None,
        orderby: Optional[str] = None
    ) -> pd.DataFrame:
        """Get proposer duty assignments.
        
        Args:
            slot: Single slot or range [start, end)
            columns: Columns to retrieve
            network: Network name
            limit: Maximum rows to return
            orderby: Column to order by
            
        Returns:
            DataFrame with proposer duties
        """
        self._ensure_connected()
        
        params = SlotQueryParams(
            slot=slot,
            columns=columns,
            network=network if isinstance(network, Network) else Network(network),
            limit=limit,
            orderby=orderby
        )
        
        return await self._validator_fetcher.fetch_proposer_duties(params)
        
    # Utility methods
    
    async def get_table_columns(self, table: str) -> List[str]:
        """Get available columns for a table.
        
        Args:
            table: Table name
            
        Returns:
            List of column names
        """
        self._ensure_connected()
        return await self._client.get_table_columns(table)
        
    async def execute_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None
    ) -> pd.DataFrame:
        """Execute a raw SQL query (use with caution).
        
        Args:
            query: SQL query with parameter placeholders
            params: Parameter values
            
        Returns:
            Query results as DataFrame
        """
        self._ensure_connected()
        
        self.logger.warning("Executing raw SQL query - ensure it's properly sanitized")
        return await self._client.execute_query_df(query, params)
    
    # Validator label methods
    
    async def get_label_manager(self) -> ValidatorLabelManager:
        """Get or initialize the validator label manager.
        
        Returns:
            Initialized ValidatorLabelManager instance
        """
        self._ensure_connected()
        
        if self._label_manager is None:
            self._label_manager = ValidatorLabelManager(self._client)
            await self._label_manager.initialize()
        
        return self._label_manager
    
    async def get_validator_label(self, validator_index: int) -> Optional[str]:
        """Get entity label for a validator.
        
        Args:
            validator_index: Validator index
            
        Returns:
            Entity name or None
        """
        manager = await self.get_label_manager()
        return manager.get_validator_label(validator_index)
    
    async def get_validator_labels(
        self,
        validator_indices: List[int]
    ) -> Dict[int, Optional[str]]:
        """Get labels for multiple validators.
        
        Args:
            validator_indices: List of validator indices
            
        Returns:
            Dictionary mapping indices to entity names
        """
        manager = await self.get_label_manager()
        return manager.get_validator_labels_bulk(validator_indices)
    
    async def add_validator_labels(
        self,
        df: pd.DataFrame,
        index_column: str = 'validator_index',
        label_column: str = 'entity'
    ) -> pd.DataFrame:
        """Add entity labels to a DataFrame containing validator indices.
        
        Args:
            df: DataFrame with validator indices
            index_column: Column containing validator indices
            label_column: Name for the new label column
            
        Returns:
            DataFrame with added labels
        """
        manager = await self.get_label_manager()
        return manager.label_dataframe(df, index_column, label_column)
    
    async def get_validators_by_entity(self, entity_name: str) -> List[int]:
        """Get all validator indices for an entity.
        
        Args:
            entity_name: Entity name (e.g., 'coinbase', 'lido')
            
        Returns:
            List of validator indices
        """
        manager = await self.get_label_manager()
        return manager.get_validators_by_entity(entity_name)
    
    async def get_entity_statistics(self) -> pd.DataFrame:
        """Get statistics about validator entities.
        
        Returns:
            DataFrame with entity counts and percentages
        """
        manager = await self.get_label_manager()
        return manager.get_entity_statistics()
    
    async def refresh_validator_labels(self) -> None:
        """Refresh validator label data from sources."""
        manager = await self.get_label_manager()
        await manager.refresh()
    
    async def get_active_validators_by_entity(self, entity_name: str) -> List[int]:
        """Get active (non-exited) validator indices for an entity.
        
        Args:
            entity_name: Entity name (e.g., 'coinbase', 'lido')
            
        Returns:
            List of active validator indices
        """
        manager = await self.get_label_manager()
        return manager.get_active_validators_by_entity(entity_name)
    
    async def get_exit_statistics(self) -> Dict[str, Any]:
        """Get statistics about validator exits.
        
        Returns:
            Dictionary with exit statistics including total, active, exited counts
        """
        manager = await self.get_label_manager()
        return manager.get_exit_statistics()
    
    async def get_attestation(self, **kwargs) -> pd.DataFrame:
        """Alias for get_attestations."""
        return await self.get_attestations(**kwargs)
    
    async def get_attestation_event(
        self,
        slot: Optional[Union[int, List[int]]] = None,
        columns: str = "*",
        network: Union[str, Network] = Network.MAINNET,
        limit: Optional[int] = None,
        orderby: Optional[str] = None
    ) -> pd.DataFrame:
        """Get attestation event data from beacon API."""
        self._ensure_connected()
        
        params = SlotQueryParams(
            slot=slot,
            columns=columns,
            network=network if isinstance(network, Network) else Network(network),
            limit=limit,
            orderby=orderby
        )
        
        return await self._attestation_fetcher.fetch_attestation_events(params)
    
    async def get_beacon_block_v2(
        self,
        slot: Optional[Union[int, List[int]]] = None,
        columns: str = "*",
        network: Union[str, Network] = Network.MAINNET,
        limit: Optional[int] = None,
        orderby: Optional[str] = None
    ) -> pd.DataFrame:
        """Get beacon block v2 data."""
        self._ensure_connected()
        
        params = SlotQueryParams(
            slot=slot,
            columns=columns,
            network=network if isinstance(network, Network) else Network(network),
            limit=limit,
            orderby=orderby
        )
        
        return await self._slot_fetcher.fetch_beacon_blocks_v2(params)
    
    async def get_checkpoints(
        self,
        slot: Optional[Union[int, List[int]]] = None,
        columns: str = "*",
        network: Union[str, Network] = Network.MAINNET,
        limit: Optional[int] = None,
        orderby: Optional[str] = None
    ) -> pd.DataFrame:
        """Get checkpoint data."""
        self._ensure_connected()
        
        params = SlotQueryParams(
            slot=slot,
            columns=columns,
            network=network if isinstance(network, Network) else Network(network),
            limit=limit,
            orderby=orderby
        )
        
        return await self._slot_fetcher.fetch_checkpoints(params)
    
    async def get_duties(
        self,
        slot: Optional[Union[int, List[int]]] = None,
        columns: str = "*",
        network: Union[str, Network] = Network.MAINNET,
        limit: Optional[int] = None,
        orderby: Optional[str] = None
    ) -> pd.DataFrame:
        """Get attester duty assignments."""
        self._ensure_connected()
        
        params = SlotQueryParams(
            slot=slot,
            columns=columns,
            network=network if isinstance(network, Network) else Network(network),
            limit=limit,
            orderby=orderby
        )
        
        return await self._attestation_fetcher.fetch_duties(params)
    
    async def get_el_transactions(
        self,
        slot: Optional[Union[int, List[int]]] = None,
        columns: str = "*",
        network: Union[str, Network] = Network.MAINNET,
        limit: Optional[int] = None,
        orderby: Optional[str] = None
    ) -> pd.DataFrame:
        """Get execution layer transactions."""
        self._ensure_connected()
        
        params = SlotQueryParams(
            slot=slot,
            columns=columns,
            network=network if isinstance(network, Network) else Network(network),
            limit=limit,
            orderby=orderby
        )
        
        return await self._transaction_fetcher.fetch_el_transactions(params)
    
    async def get_mempool(
        self,
        time_interval: Optional[List[str]] = None,
        columns: str = "*",
        network: Union[str, Network] = Network.MAINNET,
        limit: Optional[int] = None,
        orderby: Optional[str] = None
    ) -> pd.DataFrame:
        """Get mempool transaction data."""
        self._ensure_connected()
        
        params = SlotQueryParams(
            time_interval=time_interval,
            columns=columns,
            network=network if isinstance(network, Network) else Network(network),
            limit=limit,
            orderby=orderby
        )
        
        return await self._transaction_fetcher.fetch_mempool_transactions(params)
    
    async def get_blobs(
        self,
        slot: Optional[Union[int, List[int]]] = None,
        columns: str = "*",
        network: Union[str, Network] = Network.MAINNET,
        limit: Optional[int] = None,
        orderby: Optional[str] = None
    ) -> pd.DataFrame:
        """Get blob sidecar data (EIP-4844)."""
        self._ensure_connected()
        
        params = SlotQueryParams(
            slot=slot,
            columns=columns,
            network=network if isinstance(network, Network) else Network(network),
            limit=limit,
            orderby=orderby
        )
        
        return await self._transaction_fetcher.fetch_blob_sidecars(params)
    
    async def get_blob_events(
        self,
        slot: Optional[Union[int, List[int]]] = None,
        columns: str = "*",
        network: Union[str, Network] = Network.MAINNET,
        limit: Optional[int] = None,
        orderby: Optional[str] = None
    ) -> pd.DataFrame:
        """Get blob event data."""
        self._ensure_connected()
        
        params = SlotQueryParams(
            slot=slot,
            columns=columns,
            network=network if isinstance(network, Network) else Network(network),
            limit=limit,
            orderby=orderby
        )
        
        return await self._transaction_fetcher.fetch_blob_events(params)
    
    async def get_proposer(self, **kwargs) -> pd.DataFrame:
        """Alias for get_proposer_duties."""
        return await self.get_proposer_duties(**kwargs)
    
    async def get_blockevent(
        self,
        slot: Optional[Union[int, List[int]]] = None,
        columns: str = "*",
        network: Union[str, Network] = Network.MAINNET,
        limit: Optional[int] = None,
        orderby: Optional[str] = None
    ) -> pd.DataFrame:
        """Get block event data."""
        self._ensure_connected()
        
        params = SlotQueryParams(
            slot=slot,
            columns=columns,
            network=network if isinstance(network, Network) else Network(network),
            limit=limit,
            orderby=orderby
        )
        
        return await self._validator_fetcher.fetch_block_events(params)
    
    async def get_block_size(self, **kwargs) -> pd.DataFrame:
        """Alias for get_block_sizes."""
        return await self.get_block_sizes(**kwargs)
        
    async def async_get_slots(self, **kwargs) -> pd.DataFrame:
        return await self.get_slots(**kwargs)
    
    async def async_get_attestations(self, **kwargs) -> pd.DataFrame:
        """Async alias for get_attestations."""
        return await self.get_attestations(**kwargs)
    
    async def async_get_transactions(self, **kwargs) -> pd.DataFrame:
        """Async alias for get_transactions."""
        return await self.get_transactions(**kwargs)
    
    def __repr__(self) -> str:
        """String representation."""
        return (
            f"PyXatu(url={self.config.clickhouse.url}, "
            f"user={self.config.clickhouse.user})"
        )