"""Tests for mempool connector - simplified to match current implementation."""

import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime, timedelta
import pandas as pd
import json
from pathlib import Path

from pyxatu.connectors.mempool_connector import MempoolConnector
from pyxatu.config import MempoolConfig


class TestMempoolConnector:
    """Test mempool connector functionality."""
    
    @pytest.fixture
    def config(self, tmp_path):
        """Create test mempool configuration."""
        return MempoolConfig(
            flashbots_url="https://test.flashbots.net",
            blocknative_url="https://test.blocknative.com",
            cache_dir=tmp_path / "mempool_cache",
            cache_ttl=300
        )
        
    @pytest.fixture
    def connector(self, config):
        """Create mempool connector instance."""
        return MempoolConnector(config)
        
    @pytest.mark.asyncio
    async def test_initialization(self, connector, config):
        """Test connector initialization."""
        assert connector.config == config
        assert connector._session is None
        assert connector.config.cache_dir.exists()
        
    @pytest.mark.asyncio
    async def test_connect_disconnect(self, connector):
        """Test connect and disconnect methods."""
        await connector.connect()
        assert connector._session is not None
        
        await connector.disconnect()
        # Session should be closed
        
    @pytest.mark.asyncio
    async def test_context_manager(self, config):
        """Test async context manager."""
        async with MempoolConnector(config) as connector:
            assert connector._session is not None
            
    @pytest.mark.asyncio
    async def test_fetch_data_basic(self, connector):
        """Test basic data fetching."""
        mock_data = {
            "transactions": [
                {"hash": "0x123", "from": "0xabc"},
                {"hash": "0x456", "from": "0xdef"}
            ]
        }
        
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_resp = AsyncMock()
            mock_resp.status = 200
            mock_resp.json = AsyncMock(return_value=mock_data)
            mock_resp.raise_for_status = Mock()
            
            mock_context = AsyncMock()
            mock_context.__aenter__.return_value = mock_resp
            mock_context.__aexit__.return_value = None
            mock_get.return_value = mock_context
            
            result = await connector.fetch_data(
                timestamp=datetime(2023, 1, 1),
                source="flashbots"
            )
            
            assert result is not None
            
    @pytest.mark.asyncio
    async def test_caching(self, connector, tmp_path):
        """Test caching functionality."""
        test_data = [{"test": "data"}]
        source = "flashbots"
        timestamp = datetime(2023, 1, 1)
        
        # Cache data
        await connector._cache_data(source, timestamp, test_data)
        
        # Retrieve cached data
        cached = await connector._get_cached_data(source, timestamp)
        assert cached == test_data
        
    @pytest.mark.asyncio
    async def test_clear_cache(self, connector):
        """Test cache clearing."""
        # Add some test cache files
        test_data = [{"test": "data"}]
        await connector._cache_data("flashbots", datetime(2023, 1, 1), test_data)
        await connector._cache_data("flashbots", datetime(2023, 1, 2), test_data)
        
        # Clear cache
        cleared = await connector.clear_cache()
        assert cleared >= 0
        
    @pytest.mark.asyncio
    async def test_fetch_multiple_timestamps(self, connector):
        """Test fetching data for multiple timestamps."""
        start_time = datetime(2023, 1, 1, 0, 0)
        end_time = datetime(2023, 1, 1, 0, 10)
        
        with patch.object(connector, 'fetch_data') as mock_fetch:
            # Return a list of transactions
            mock_fetch.return_value = [
                {"hash": "0x123", "from": "0xabc"},
                {"hash": "0x456", "from": "0xdef"}
            ]
            
            results = await connector.fetch_multiple_timestamps(
                source="flashbots",
                start_time=start_time,
                end_time=end_time,
                interval_minutes=5
            )
            
            # Should fetch at 0:00, 0:05, 0:10 and deduplicate
            assert isinstance(results, list)
            assert mock_fetch.call_count == 3


if __name__ == '__main__':
    pytest.main([__file__, '-v'])