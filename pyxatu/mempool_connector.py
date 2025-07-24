"""Async mempool data connector for PyXatu."""

import asyncio
import json
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from urllib.parse import urljoin

import aiohttp
import aiofiles
from aiohttp import ClientTimeout
import backoff

from pyxatu.base import BaseConnector
from pyxatu.config import MempoolConfig


class MempoolConnector(BaseConnector):
    """Connector for fetching mempool data from multiple sources."""
    
    def __init__(self, config: Optional[MempoolConfig] = None):
        """Initialize mempool connector.
        
        Args:
            config: Mempool configuration (uses defaults if not provided)
        """
        self.config = config or MempoolConfig()
        self.logger = logging.getLogger(__name__)
        self._session: Optional[aiohttp.ClientSession] = None
        
        # Ensure cache directory exists
        self.config.cache_dir.mkdir(parents=True, exist_ok=True)
        
    async def connect(self) -> None:
        """Initialize HTTP session."""
        if self._session is None or self._session.closed:
            timeout = ClientTimeout(total=30)
            self._session = aiohttp.ClientSession(timeout=timeout)
            self.logger.info("Mempool connector initialized")
            
    async def disconnect(self) -> None:
        """Close HTTP session."""
        if self._session and not self._session.closed:
            await self._session.close()
            self.logger.info("Mempool connector closed")
            
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
        max_time=60
    )
    async def fetch_data(
        self,
        source: str,
        timestamp: datetime,
        use_cache: bool = True
    ) -> List[Dict[str, Any]]:
        """Fetch mempool data from specified source.
        
        Args:
            source: Data source ('flashbots' or 'blocknative')
            timestamp: Timestamp to fetch data for
            use_cache: Whether to use cached data if available
            
        Returns:
            List of transaction data dictionaries
            
        Raises:
            ValueError: If source is not supported
        """
        if source == 'flashbots':
            return await self._fetch_flashbots_data(timestamp, use_cache)
        elif source == 'blocknative':
            return await self._fetch_blocknative_data(timestamp, use_cache)
        else:
            raise ValueError(f"Unsupported mempool source: {source}")
            
    async def _fetch_flashbots_data(
        self,
        timestamp: datetime,
        use_cache: bool = True
    ) -> List[Dict[str, Any]]:
        """Fetch mempool data from Flashbots mempool dumpster.
        
        Args:
            timestamp: Timestamp to fetch data for
            use_cache: Whether to use cached data
            
        Returns:
            List of transaction hashes and metadata
        """
        # Round timestamp to minute
        rounded_time = timestamp.replace(second=0, microsecond=0)
        
        # Check cache first
        if use_cache:
            cached_data = await self._get_cached_data('flashbots', rounded_time)
            if cached_data is not None:
                return cached_data
                
        # Format URL for Flashbots mempool dumpster
        # Format: /2024/01/15/14/2024-01-15T14:15.json.gz
        date_path = rounded_time.strftime('%Y/%m/%d/%H')
        filename = rounded_time.strftime('%Y-%m-%dT%H:%M.json')
        url = urljoin(
            self.config.flashbots_url,
            f"{date_path}/{filename}"
        )
        
        try:
            await self.connect()
            self.logger.info(f"Fetching Flashbots mempool data from {url}")
            
            async with self._session.get(url) as response:
                if response.status == 404:
                    self.logger.warning(f"No Flashbots data available for {rounded_time}")
                    return []
                    
                response.raise_for_status()
                
                # Parse JSON response
                data = await response.json()
                
                # Extract transaction hashes
                transactions = []
                for entry in data:
                    if isinstance(entry, dict) and 'hash' in entry:
                        transactions.append({
                            'hash': entry['hash'].lower(),
                            'timestamp': rounded_time.isoformat(),
                            'source': 'flashbots'
                        })
                        
                # Cache the results
                if use_cache:
                    await self._cache_data('flashbots', rounded_time, transactions)
                    
                return transactions
                
        except Exception as e:
            self.logger.error(f"Failed to fetch Flashbots data: {e}")
            return []
            
    async def _fetch_blocknative_data(
        self,
        timestamp: datetime,
        use_cache: bool = True
    ) -> List[Dict[str, Any]]:
        """Fetch mempool data from Blocknative.
        
        Args:
            timestamp: Timestamp to fetch data for
            use_cache: Whether to use cached data
            
        Returns:
            List of transaction hashes and metadata
        """
        # Check cache first
        if use_cache:
            cached_data = await self._get_cached_data('blocknative', timestamp)
            if cached_data is not None:
                return cached_data
                
        try:
            await self.connect()
            
            # Blocknative API endpoint would go here
            # This is a placeholder as the actual API requires authentication
            self.logger.warning(
                "Blocknative API integration not implemented - requires API key"
            )
            return []
            
        except Exception as e:
            self.logger.error(f"Failed to fetch Blocknative data: {e}")
            return []
            
    async def _get_cached_data(
        self,
        source: str,
        timestamp: datetime
    ) -> Optional[List[Dict[str, Any]]]:
        """Retrieve cached mempool data.
        
        Args:
            source: Data source name
            timestamp: Timestamp of the data
            
        Returns:
            Cached data if available and not expired, None otherwise
        """
        cache_file = self._get_cache_path(source, timestamp)
        
        if not cache_file.exists():
            return None
            
        # Check if cache is expired
        cache_age = datetime.now() - datetime.fromtimestamp(cache_file.stat().st_mtime)
        if cache_age.total_seconds() > self.config.cache_ttl:
            self.logger.debug(f"Cache expired for {source} at {timestamp}")
            return None
            
        try:
            async with aiofiles.open(cache_file, 'r') as f:
                content = await f.read()
                data = json.loads(content)
                self.logger.debug(f"Using cached data for {source} at {timestamp}")
                return data
        except Exception as e:
            self.logger.warning(f"Failed to read cache file {cache_file}: {e}")
            return None
            
    async def _cache_data(
        self,
        source: str,
        timestamp: datetime,
        data: List[Dict[str, Any]]
    ) -> None:
        """Cache mempool data to disk.
        
        Args:
            source: Data source name
            timestamp: Timestamp of the data
            data: Data to cache
        """
        cache_file = self._get_cache_path(source, timestamp)
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            async with aiofiles.open(cache_file, 'w') as f:
                await f.write(json.dumps(data, separators=(',', ':')))
            self.logger.debug(f"Cached {len(data)} items for {source} at {timestamp}")
        except Exception as e:
            self.logger.warning(f"Failed to cache data: {e}")
            
    def _get_cache_path(self, source: str, timestamp: datetime) -> Path:
        """Get cache file path for given source and timestamp.
        
        Args:
            source: Data source name
            timestamp: Timestamp of the data
            
        Returns:
            Path to cache file
        """
        date_str = timestamp.strftime('%Y-%m-%d')
        time_str = timestamp.strftime('%H-%M')
        return self.config.cache_dir / source / date_str / f"{time_str}.json"
        
    async def fetch_multiple_timestamps(
        self,
        source: str,
        start_time: datetime,
        end_time: datetime,
        interval_minutes: int = 1
    ) -> List[Dict[str, Any]]:
        """Fetch mempool data for a time range.
        
        Args:
            source: Data source
            start_time: Start of time range
            end_time: End of time range
            interval_minutes: Interval between fetches in minutes
            
        Returns:
            Combined list of all transaction data
        """
        all_transactions = []
        current_time = start_time
        
        while current_time <= end_time:
            transactions = await self.fetch_data(source, current_time)
            all_transactions.extend(transactions)
            current_time += timedelta(minutes=interval_minutes)
            
        # Deduplicate by hash
        seen_hashes = set()
        unique_transactions = []
        
        for tx in all_transactions:
            if tx['hash'] not in seen_hashes:
                seen_hashes.add(tx['hash'])
                unique_transactions.append(tx)
                
        return unique_transactions
        
    async def clear_cache(self, older_than: Optional[timedelta] = None) -> int:
        """Clear cached data.
        
        Args:
            older_than: Only clear cache older than this duration
            
        Returns:
            Number of files deleted
        """
        deleted_count = 0
        cutoff_time = datetime.now() - older_than if older_than else None
        
        for cache_file in self.config.cache_dir.rglob('*.json'):
            if cutoff_time:
                file_time = datetime.fromtimestamp(cache_file.stat().st_mtime)
                if file_time > cutoff_time:
                    continue
                    
            try:
                cache_file.unlink()
                deleted_count += 1
            except Exception as e:
                self.logger.warning(f"Failed to delete cache file {cache_file}: {e}")
                
        self.logger.info(f"Cleared {deleted_count} cache files")
        return deleted_count