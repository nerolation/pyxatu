"""Main PyXatu class - simplified and secure interface for blockchain data queries."""

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


class PyXatu:
    """Main interface for querying Ethereum blockchain data from Xatu.
    
    This is a complete rewrite focusing on:
    - Security: No eval(), parameterized queries, input validation
    - Simplicity: Clean API, modular design
    - Performance: Async operations, connection pooling
    - Type safety: Full type hints with Pydantic models
    """
    
    def __init__(
        self,
        config_path: Optional[Path] = None,
        use_env_vars: bool = True,
        log_level: str = 'INFO'
    ):
        """Initialize PyXatu with configuration.
        
        Args:
            config_path: Path to configuration file (default: ~/.pyxatu_config.json)
            use_env_vars: Whether to use environment variables for config
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
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
        """Initialize connection to ClickHouse."""
        if self._client is None:
            self._client = ClickHouseClient(self.config.clickhouse)
            
            # Test connection
            if not await self._client.test_connection():
                raise ConnectionError("Failed to connect to ClickHouse")
                
            self.logger.info("Connected to ClickHouse")
            
            # Initialize fetchers
            self._slot_fetcher = SlotDataFetcher(self._client)
            self._attestation_fetcher = AttestationDataFetcher(
                self._client, self._slot_fetcher
            )
            self._transaction_fetcher = TransactionDataFetcher(self._client)
            self._validator_fetcher = ValidatorDataFetcher(self._client)
            
    async def close(self) -> None:
        """Close all connections."""
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
        """Get list of missed slots in a range.
        
        Args:
            slot_range: Range [start, end) to check
            network: Network name
            
        Returns:
            List of missed slot numbers
        """
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
        """Get chain reorganization data.
        
        Args:
            slot: Single slot or range to check
            network: Network name
            limit: Maximum rows to return
            
        Returns:
            DataFrame with reorg data
        """
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
        """Get detailed attestation performance data.
        
        Args:
            slot: Single slot or range [start, end)
            vote_types: Vote types to include (source, target, head)
            status_filter: Status types to include (correct, failed, offline)
            include_delay: Whether to calculate inclusion delay
            network: Network name
            
        Returns:
            DataFrame with elaborated attestation data
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
        
    def __repr__(self) -> str:
        """String representation."""
        return (
            f"PyXatu(url={self.config.clickhouse.url}, "
            f"user={self.config.clickhouse.user})"
        )