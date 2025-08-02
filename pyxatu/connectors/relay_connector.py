"""MEV-Boost relay connector with async support and retry logic."""

import asyncio
import json
import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
from urllib.parse import urljoin

import aiohttp
from aiohttp import ClientTimeout
import backoff

from pyxatu.core.base import BaseConnector
from pyxatu.config import RelayConfig


class RelayConnector(BaseConnector):
    """Connector for querying MEV-Boost relay endpoints."""
    
    def __init__(self, config: Optional[RelayConfig] = None):
        """Initialize relay connector.
        
        Args:
            config: Relay configuration (uses defaults if not provided)
        """
        self.config = config or RelayConfig()
        self.logger = logging.getLogger(__name__)
        self._session: Optional[aiohttp.ClientSession] = None
        
    async def connect(self) -> None:
        """Initialize HTTP session."""
        if self._session is None or self._session.closed:
            timeout = ClientTimeout(total=self.config.timeout)
            connector = aiohttp.TCPConnector(
                limit=100,
                limit_per_host=10
            )
            self._session = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector
            )
            self.logger.info("Relay connector initialized")
            
    async def disconnect(self) -> None:
        """Close HTTP session."""
        if self._session and not self._session.closed:
            await self._session.close()
            self.logger.info("Relay connector closed")
            
    async def __aenter__(self):
        """Async context manager entry."""
        await self.connect()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.disconnect()
        
    @backoff.on_exception(
        backoff.expo,
        (aiohttp.ClientError, asyncio.TimeoutError),
        max_tries=3,
        max_time=30
    )
    async def _make_request(
        self,
        relay_name: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """Make HTTP request to a relay endpoint with retry logic.
        
        Args:
            relay_name: Name of the relay
            endpoint: API endpoint path
            params: Query parameters
            
        Returns:
            JSON response data or None if failed
        """
        base_url = self.config.relay_endpoints.get(relay_name)
        if not base_url:
            self.logger.error(f"Unknown relay: {relay_name}")
            return None
            
        url = urljoin(base_url, endpoint)
        
        try:
            await self.connect()
            
            async with self._session.get(url, params=params) as response:
                if response.status == 404:
                    self.logger.debug(f"No data found at {url}")
                    return None
                    
                response.raise_for_status()
                
                # Parse JSON response safely
                text = await response.text()
                return json.loads(text)
                
        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON from {relay_name}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Request to {relay_name} failed: {e}")
            return None
            
    async def fetch_data(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        relays: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Fetch data from multiple relays concurrently.
        
        Args:
            endpoint: API endpoint to query
            params: Query parameters
            relays: List of relay names to query (default: all)
            
        Returns:
            Dictionary mapping relay names to their responses
        """
        if relays is None:
            relays = list(self.config.relay_endpoints.keys())
            
        # Create tasks for all relays
        tasks = {
            relay: self._make_request(relay, endpoint, params)
            for relay in relays
        }
        
        # Execute concurrently
        results = await asyncio.gather(
            *tasks.values(),
            return_exceptions=True
        )
        
        # Map results back to relay names
        relay_results = {}
        for relay, result in zip(tasks.keys(), results):
            if isinstance(result, Exception):
                self.logger.warning(f"Failed to fetch from {relay}: {result}")
                relay_results[relay] = None
            else:
                relay_results[relay] = result
                
        return relay_results
        
    async def get_proposer_payload_delivered(
        self,
        slot: Optional[int] = None,
        block_hash: Optional[str] = None,
        block_number: Optional[int] = None,
        proposer_pubkey: Optional[str] = None,
        builder_pubkey: Optional[str] = None,
        limit: int = 100,
        relays: Optional[List[str]] = None
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Get delivered payloads from relays.
        
        Args:
            slot: Filter by slot number
            block_hash: Filter by block hash
            block_number: Filter by block number
            proposer_pubkey: Filter by proposer public key
            builder_pubkey: Filter by builder public key
            limit: Maximum number of results
            relays: Specific relays to query
            
        Returns:
            Dictionary mapping relay names to payload data
        """
        params = {
            'limit': min(limit, 500)  # Most relays have a max limit
        }
        
        if slot is not None:
            params['slot'] = slot
        if block_hash:
            params['block_hash'] = block_hash
        if block_number is not None:
            params['block_number'] = block_number
        if proposer_pubkey:
            params['proposer_pubkey'] = proposer_pubkey
        if builder_pubkey:
            params['builder_pubkey'] = builder_pubkey
            
        results = await self.fetch_data(
            '/relay/v1/data/bidtraces/proposer_payload_delivered',
            params=params,
            relays=relays
        )
        
        # Ensure all results are lists
        for relay, data in results.items():
            if data is None:
                results[relay] = []
            elif isinstance(data, dict):
                results[relay] = [data]
            elif not isinstance(data, list):
                results[relay] = []
                
        return results
        
    async def get_builder_blocks_received(
        self,
        slot: Optional[int] = None,
        block_hash: Optional[str] = None,
        block_number: Optional[int] = None,
        builder_pubkey: Optional[str] = None,
        limit: int = 100,
        relays: Optional[List[str]] = None
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Get blocks received by relays from builders.
        
        Args:
            slot: Filter by slot number
            block_hash: Filter by block hash  
            block_number: Filter by block number
            builder_pubkey: Filter by builder public key
            limit: Maximum number of results
            relays: Specific relays to query
            
        Returns:
            Dictionary mapping relay names to block data
        """
        params = {
            'limit': min(limit, 500)
        }
        
        if slot is not None:
            params['slot'] = slot
        if block_hash:
            params['block_hash'] = block_hash
        if block_number is not None:
            params['block_number'] = block_number
        if builder_pubkey:
            params['builder_pubkey'] = builder_pubkey
            
        results = await self.fetch_data(
            '/relay/v1/data/bidtraces/builder_blocks_received',
            params=params,
            relays=relays
        )
        
        # Ensure all results are lists
        for relay, data in results.items():
            if data is None:
                results[relay] = []
            elif isinstance(data, dict):
                results[relay] = [data]
            elif not isinstance(data, list):
                results[relay] = []
                
        return results
        
    async def get_validator_registrations(
        self,
        pubkey: str,
        relays: Optional[List[str]] = None
    ) -> Dict[str, Optional[Dict[str, Any]]]:
        """Get validator registrations from relays.
        
        Args:
            pubkey: Validator public key
            relays: Specific relays to query
            
        Returns:
            Dictionary mapping relay names to registration data
        """
        return await self.fetch_data(
            f'/relay/v1/data/validator_registration?pubkey={pubkey}',
            relays=relays
        )
        
    async def get_aggregate_bid_stats(
        self,
        slot: int,
        relays: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Get aggregated bid statistics for a slot across relays.
        
        Args:
            slot: Slot number
            relays: Specific relays to query
            
        Returns:
            Aggregated statistics including max bid, winning relay, etc.
        """
        # Fetch delivered payloads for the slot
        payloads = await self.get_proposer_payload_delivered(
            slot=slot,
            relays=relays
        )
        
        stats = {
            'slot': slot,
            'relay_count': 0,
            'max_value_wei': '0',
            'winning_relay': None,
            'winning_builder': None,
            'block_hash': None,
            'proposer_pubkey': None,
            'relay_data': {}
        }
        
        max_value = 0
        
        for relay, data_list in payloads.items():
            if not data_list:
                continue
                
            # Assume first item is the delivered payload for this slot
            data = data_list[0]
            
            stats['relay_count'] += 1
            value_wei = int(data.get('value', 0))
            
            stats['relay_data'][relay] = {
                'value_wei': str(value_wei),
                'builder_pubkey': data.get('builder_pubkey'),
                'block_hash': data.get('block_hash')
            }
            
            if value_wei > max_value:
                max_value = value_wei
                stats['max_value_wei'] = str(value_wei)
                stats['winning_relay'] = relay
                stats['winning_builder'] = data.get('builder_pubkey')
                stats['block_hash'] = data.get('block_hash')
                stats['proposer_pubkey'] = data.get('proposer_pubkey')
                
        return stats
        
    async def check_relay_health(
        self,
        relays: Optional[List[str]] = None
    ) -> Dict[str, bool]:
        """Check health status of relays.
        
        Args:
            relays: Specific relays to check
            
        Returns:
            Dictionary mapping relay names to health status
        """
        if relays is None:
            relays = list(self.config.relay_endpoints.keys())
            
        health_status = {}
        
        # Try to fetch recent data as health check
        for relay in relays:
            try:
                result = await self._make_request(
                    relay,
                    '/relay/v1/data/bidtraces/proposer_payload_delivered',
                    params={'limit': 1}
                )
                health_status[relay] = result is not None
            except Exception:
                health_status[relay] = False
                
        return health_status