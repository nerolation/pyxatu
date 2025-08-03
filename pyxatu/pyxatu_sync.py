"""Simplified synchronous PyXatu client for querying Ethereum beacon chain data."""

import logging
from pathlib import Path
from typing import Optional, List, Union, Dict, Any
import pandas as pd

from .config import ConfigManager, PyXatuConfig
from .core.clickhouse_client_sync import ClickHouseClient
from .models import SlotQueryParams, Network
from .validator_labels_sync import ValidatorLabelManager


class PyXatu:
    """Simplified synchronous client for querying Ethereum beacon chain data from Xatu."""
    
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
        
        # Initialize client
        self._client: Optional[ClickHouseClient] = None
        self._label_manager: Optional[ValidatorLabelManager] = None
        
    def connect(self) -> None:
        """Connect to ClickHouse."""
        if self._client is None:
            self._client = ClickHouseClient(self.config.clickhouse)
            
            if not self._client.test_connection():
                raise ConnectionError("Failed to connect to ClickHouse")
                
            self.logger.info("Connected to ClickHouse")
            
    def close(self) -> None:
        """Close connection."""
        if self._client:
            self._client.close()
            self._client = None
            self.logger.info("Closed ClickHouse connection")
            
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
            self.connect()
            if self._client is None:
                raise RuntimeError("Not connected to ClickHouse. Connection failed.")
    
    def _sort_and_reindex_df(self, df: pd.DataFrame, orderby: Optional[str] = None) -> pd.DataFrame:
        """Sort and reindex a DataFrame."""
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
            # Default: sort by first column
            if len(df.columns) > 0:
                df = df.sort_values(by=df.columns[0])
            
        # Reset index
        return df.reset_index(drop=True)
    
    # Core query methods
    
    def query(self, sql: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """Execute a raw SQL query.
        
        Args:
            sql: SQL query to execute
            params: Query parameters for safe substitution
            
        Returns:
            Query results as DataFrame
        """
        self._ensure_connected()
        return self._client.execute_query_df(sql, params)
    
    def get_slots(
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
            columns: Columns to retrieve
            network: Network name
            include_missed: Whether to include missed slots
            limit: Maximum rows to return
            orderby: Column to order by (prefix with - for DESC)
            
        Returns:
            DataFrame with slot data
        """
        self._ensure_connected()
        
        # Build query
        if columns:
            if isinstance(columns, str):
                columns = [columns]
        else:
            columns = ['*']
            
        network_str = network.value if isinstance(network, Network) else network
        
        # Choose table based on include_missed
        table = 'beacon_api_slot' if include_missed else 'canonical_beacon_block'
        
        # Build WHERE conditions
        where_parts = [f"meta_network_name = '{network_str}'"]
        
        if isinstance(slot, int):
            where_parts.append(f"slot = {slot}")
        elif isinstance(slot, list) and len(slot) == 2:
            where_parts.append(f"slot >= {slot[0]} AND slot < {slot[1]}")
        
        # Build ORDER BY
        if orderby:
            desc = orderby.startswith('-')
            col = orderby.lstrip('-')
            order_clause = f" ORDER BY {col} {'DESC' if desc else 'ASC'}"
        else:
            order_clause = " ORDER BY slot DESC"
        
        # Build LIMIT
        limit_clause = f" LIMIT {limit}" if limit else " LIMIT 100"
        
        # Construct query
        query = f"""
        SELECT {', '.join(columns)}
        FROM {table}
        WHERE {' AND '.join(where_parts)}
        {order_clause}
        {limit_clause}
        """
        
        df = self._client.execute_query_df(query)
        return self._sort_and_reindex_df(df, orderby)
    
    def get_attestations(
        self,
        slot: Optional[Union[int, List[int]]] = None,
        validator_index: Optional[Union[int, List[int]]] = None,
        columns: Optional[Union[str, List[str]]] = None,
        network: Union[str, Network] = Network.MAINNET,
        limit: Optional[int] = None,
        orderby: Optional[str] = None
    ) -> pd.DataFrame:
        """Get attestation data.
        
        Args:
            slot: Single slot or range [start, end)
            validator_index: Single validator or list of validators to filter
            columns: Columns to retrieve
            network: Network name
            limit: Maximum rows to return
            orderby: Column to order by
            
        Returns:
            DataFrame with attestation data
        """
        self._ensure_connected()
        
        if columns:
            if isinstance(columns, str):
                columns = [columns]
        else:
            columns = ['*']
            
        network_str = network.value if isinstance(network, Network) else network
        
        # Build WHERE conditions
        where_parts = [f"meta_network_name = '{network_str}'"]
        
        if isinstance(slot, int):
            where_parts.append(f"slot = {slot}")
        elif isinstance(slot, list) and len(slot) == 2:
            where_parts.append(f"slot >= {slot[0]} AND slot < {slot[1]}")
            
        if validator_index is not None:
            if isinstance(validator_index, int):
                where_parts.append(f"validator_index = {validator_index}")
            elif isinstance(validator_index, list):
                validator_list = ', '.join(str(v) for v in validator_index)
                where_parts.append(f"validator_index IN ({validator_list})")
        
        # Build ORDER BY
        if orderby:
            desc = orderby.startswith('-')
            col = orderby.lstrip('-')
            order_clause = f" ORDER BY {col} {'DESC' if desc else 'ASC'}"
        else:
            order_clause = " ORDER BY slot DESC"
        
        # Build LIMIT
        limit_clause = f" LIMIT {limit}" if limit else " LIMIT 100"
        
        # Construct query
        query = f"""
        SELECT {', '.join(columns)}
        FROM canonical_beacon_elaborated_attestation
        WHERE {' AND '.join(where_parts)}
        {order_clause}
        {limit_clause}
        """
        
        df = self._client.execute_query_df(query)
        return self._sort_and_reindex_df(df, orderby)
    
    def get_missed_slots(
        self,
        slot_range: Optional[List[int]] = None,
        network: Union[str, Network] = Network.MAINNET,
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """Get missed slots in a range."""
        self._ensure_connected()
        
        network_str = network.value if isinstance(network, Network) else network
        
        # Query missed slots
        where_parts = [
            f"meta_network_name = '{network_str}'",
            "proposer_index = -1"  # Missed slots have proposer_index = -1
        ]
        
        if slot_range:
            where_parts.append(f"slot >= {slot_range[0]} AND slot < {slot_range[1]}")
        
        limit_clause = f" LIMIT {limit}" if limit else " LIMIT 100"
        
        query = f"""
        SELECT slot, slot_start_date_time
        FROM beacon_api_slot
        WHERE {' AND '.join(where_parts)}
        ORDER BY slot DESC
        {limit_clause}
        """
        
        return self._client.execute_query_df(query)
    
    def get_reorgs(
        self,
        slot: Optional[Union[int, List[int]]] = None,
        network: Union[str, Network] = Network.MAINNET,
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """Get chain reorganization events."""
        self._ensure_connected()
        
        network_str = network.value if isinstance(network, Network) else network
        
        where_parts = [f"meta_network_name = '{network_str}'"]
        
        if isinstance(slot, int):
            where_parts.append(f"slot = {slot}")
        elif isinstance(slot, list):
            where_parts.append(f"slot >= {slot[0]} AND slot < {slot[1]}")
        
        limit_clause = f" LIMIT {limit}" if limit else " LIMIT 100"
        
        query = f"""
        SELECT *
        FROM beacon_api_eth_v1_events_chain_reorg
        WHERE {' AND '.join(where_parts)}
        ORDER BY slot DESC
        {limit_clause}
        """
        
        return self._client.execute_query_df(query)
    
    # Validator label methods
    
    def get_label_manager(self) -> ValidatorLabelManager:
        """Get the validator label manager instance."""
        self._ensure_connected()
        if self._label_manager is None:
            self._label_manager = ValidatorLabelManager(client=self._client)
        return self._label_manager
    
    def get_validator_labels_bulk(self, validator_indices: List[int]) -> Dict[int, Optional[str]]:
        """Get labels for multiple validators as a dictionary."""
        manager = self.get_label_manager()
        # Initialize if needed
        if not hasattr(manager, '_validator_labels') or manager._validator_labels is None:
            import asyncio
            asyncio.run(manager.initialize())
        return manager.get_validator_labels_bulk(validator_indices)
    
    def get_validators_by_entity(self, entity_name: str) -> List[int]:
        """Get all validator indices for a given entity."""
        manager = self.get_label_manager()
        # Initialize if needed
        if not hasattr(manager, '_validator_labels') or manager._validator_labels is None:
            import asyncio
            asyncio.run(manager.initialize())
        return manager.get_validators_by_entity(entity_name)
    
    def get_entity_statistics(self) -> pd.DataFrame:
        """Get statistics about validator entities."""
        manager = self.get_label_manager()
        # Initialize if needed
        if not hasattr(manager, '_validator_labels') or manager._validator_labels is None:
            import asyncio
            asyncio.run(manager.initialize())
        return manager.get_entity_statistics()
    
    def refresh_validator_labels(self) -> None:
        """Refresh the validator labels cache."""
        manager = self.get_label_manager()
        import asyncio
        asyncio.run(manager.refresh())
    
    # Additional query methods can be added here following the same pattern
    
    def execute_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None
    ) -> pd.DataFrame:
        """Execute raw SQL query.
        
        Args:
            query: SQL query to execute
            params: Query parameters for safe substitution
            
        Returns:
            Query results as DataFrame
        """
        self._ensure_connected()
        return self._client.execute_query_df(query, params)