"""Tests for the main PyXatu class."""

import pytest
import asyncio
from unittest.mock import Mock, patch, AsyncMock, MagicMock
from pathlib import Path
import pandas as pd

from pyxatu import PyXatu, Network
from pyxatu.config import ConfigManager, PyXatuConfig, ClickhouseConfig
from pyxatu.clickhouse_client import ClickHouseClient
from pydantic import SecretStr


@pytest.mark.asyncio
class TestPyXatuInitialization:
    """Test PyXatu initialization and configuration."""
    
    async def test_init_with_defaults(self):
        """Test initialization with default parameters."""
        with patch('pyxatu.pyxatu.ConfigManager') as mock_config_manager:
            mock_manager = Mock()
            mock_config = PyXatuConfig(
                clickhouse=ClickhouseConfig(
                    url="https://ch.example.com",
                    user="user",
                    password="pass"
                )
            )
            mock_manager.load.return_value = mock_config
            mock_config_manager.return_value = mock_manager
            
            xatu = PyXatu()
            
            assert xatu.config == mock_config
            assert xatu._client is None  # Not connected yet
            mock_config_manager.assert_called_once_with(None, True)
            
    async def test_init_with_custom_config(self):
        """Test initialization with custom configuration."""
        custom_path = Path("/custom/config.json")
        
        with patch('pyxatu.pyxatu.ConfigManager') as mock_config_manager:
            mock_manager = Mock()
            mock_config = PyXatuConfig(
                clickhouse=ClickhouseConfig(
                    url="https://ch.example.com",
                    user="user",
                    password="pass"
                )
            )
            mock_manager.load.return_value = mock_config
            mock_config_manager.return_value = mock_manager
            
            xatu = PyXatu(
                config_path=custom_path,
                use_env_vars=False,
                log_level='DEBUG'
            )
            
            mock_config_manager.assert_called_once_with(custom_path, False)
            
    async def test_context_manager(self):
        """Test using PyXatu as async context manager."""
        with patch('pyxatu.pyxatu.ConfigManager') as mock_config_manager:
            mock_manager = Mock()
            mock_config = PyXatuConfig(
                clickhouse=ClickhouseConfig(
                    url="https://ch.example.com",
                    user="user",
                    password="pass"
                )
            )
            mock_manager.load.return_value = mock_config
            mock_config_manager.return_value = mock_manager
            
            with patch('pyxatu.pyxatu.ClickHouseClient') as mock_client_class:
                mock_client = AsyncMock()
                mock_client.test_connection.return_value = True
                mock_client_class.return_value = mock_client
                
                async with PyXatu() as xatu:
                    # Should be connected
                    assert xatu._client is not None
                    mock_client.test_connection.assert_called_once()
                    
                # Should be closed after context
                mock_client.close.assert_called_once()
                
    async def test_connection_failure(self):
        """Test handling of connection failure."""
        with patch('pyxatu.pyxatu.ConfigManager') as mock_config_manager:
            mock_manager = Mock()
            mock_config = PyXatuConfig(
                clickhouse=ClickhouseConfig(
                    url="https://ch.example.com",
                    user="user",
                    password="pass"
                )
            )
            mock_manager.load.return_value = mock_config
            mock_config_manager.return_value = mock_manager
            
            with patch('pyxatu.pyxatu.ClickHouseClient') as mock_client_class:
                mock_client = AsyncMock()
                mock_client.test_connection.return_value = False
                mock_client_class.return_value = mock_client
                
                xatu = PyXatu()
                
                with pytest.raises(ConnectionError, match="Failed to connect"):
                    await xatu.connect()
                    
    def test_ensure_connected(self):
        """Test that operations fail when not connected."""
        xatu = PyXatu()
        
        with pytest.raises(RuntimeError, match="Not connected"):
            xatu._ensure_connected()


@pytest.mark.asyncio
class TestSlotQueries:
    """Test slot/block query methods."""
    
    @pytest.fixture
    async def xatu(self):
        """Create a connected PyXatu instance with mocks."""
        with patch('pyxatu.pyxatu.ConfigManager') as mock_config_manager:
            mock_manager = Mock()
            mock_config = PyXatuConfig(
                clickhouse=ClickhouseConfig(
                    url="https://ch.example.com",
                    user="user",
                    password="pass"
                )
            )
            mock_manager.load.return_value = mock_config
            mock_config_manager.return_value = mock_manager
            
            with patch('pyxatu.pyxatu.ClickHouseClient'):
                xatu = PyXatu()
                
                # Mock the client and fetchers
                xatu._client = AsyncMock()
                xatu._slot_fetcher = AsyncMock()
                xatu._attestation_fetcher = AsyncMock()
                xatu._transaction_fetcher = AsyncMock()
                xatu._validator_fetcher = AsyncMock()
                
                yield xatu
                
    async def test_get_slots_basic(self, xatu):
        """Test basic slot retrieval."""
        mock_df = pd.DataFrame({
            'slot': [1000, 1001, 1002],
            'proposer_index': [100, 101, 102]
        })
        xatu._slot_fetcher.fetch_with_missed.return_value = mock_df
        
        result = await xatu.get_slots(
            slot=[1000, 1003],
            columns="slot,proposer_index",
            network=Network.MAINNET
        )
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
        
        # Check call parameters
        call_args = xatu._slot_fetcher.fetch_with_missed.call_args[0][0]
        assert call_args.slot == [1000, 1003]
        assert call_args.columns == "slot,proposer_index"
        assert call_args.network == Network.MAINNET
        
    async def test_get_slots_without_missed(self, xatu):
        """Test slot retrieval without missed slots."""
        mock_df = pd.DataFrame({'slot': [1000, 1001]})
        xatu._slot_fetcher.fetch.return_value = mock_df
        
        result = await xatu.get_slots(
            slot=1000,
            include_missed=False
        )
        
        xatu._slot_fetcher.fetch.assert_called_once()
        xatu._slot_fetcher.fetch_with_missed.assert_not_called()
        
    async def test_get_missed_slots(self, xatu):
        """Test missed slot retrieval."""
        xatu._slot_fetcher.fetch_missed_slots.return_value = {1002, 1005, 1008}
        
        result = await xatu.get_missed_slots(
            slot_range=[1000, 1010],
            network="mainnet"
        )
        
        assert result == [1002, 1005, 1008]  # Should be sorted
        xatu._slot_fetcher.fetch_missed_slots.assert_called_once_with([1000, 1010], "mainnet")
        
    async def test_get_reorgs(self, xatu):
        """Test reorg data retrieval."""
        mock_df = pd.DataFrame({'slot': [1001, 1005]})
        xatu._slot_fetcher.fetch_reorgs.return_value = mock_df
        
        result = await xatu.get_reorgs(
            slot=[1000, 2000],
            network=Network.MAINNET,
            limit=10
        )
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        
        call_args = xatu._slot_fetcher.fetch_reorgs.call_args[0][0]
        assert call_args.slot == [1000, 2000]
        assert call_args.limit == 10


@pytest.mark.asyncio 
class TestAttestationQueries:
    """Test attestation query methods."""
    
    @pytest.fixture
    async def xatu(self):
        """Create a connected PyXatu instance with mocks."""
        with patch('pyxatu.pyxatu.ConfigManager'):
            xatu = PyXatu()
            xatu._client = AsyncMock()
            xatu._attestation_fetcher = AsyncMock()
            xatu._slot_fetcher = AsyncMock()
            yield xatu
            
    async def test_get_attestations(self, xatu):
        """Test basic attestation retrieval."""
        mock_df = pd.DataFrame({
            'slot': [1000, 1000],
            'committee_index': [0, 1],
            'validators': [[100, 101], [200, 201]]
        })
        xatu._attestation_fetcher.fetch.return_value = mock_df
        
        result = await xatu.get_attestations(
            slot=1000,
            columns="slot,committee_index,validators",
            limit=10
        )
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        
        call_args = xatu._attestation_fetcher.fetch.call_args[0][0]
        assert call_args.slot == 1000
        assert call_args.limit == 10
        
    async def test_get_elaborated_attestations(self, xatu):
        """Test elaborated attestation retrieval."""
        mock_df = pd.DataFrame({
            'slot': [1000, 1000],
            'validator': [100, 101],
            'status': ['correct', 'failed'],
            'vote_type': ['source', 'source'],
            'inclusion_delay': [1, 2]
        })
        xatu._attestation_fetcher.fetch_elaborated_attestations.return_value = mock_df
        
        result = await xatu.get_elaborated_attestations(
            slot=1000,
            vote_types=['source', 'target'],
            status_filter=['correct', 'failed'],
            include_delay=True
        )
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        
        # Check call was made correctly
        call_kwargs = xatu._attestation_fetcher.fetch_elaborated_attestations.call_args[1]
        assert call_kwargs['slot_range'] == [1000, 1001]
        assert len(call_kwargs['vote_types']) == 2
        assert len(call_kwargs['status_filter']) == 2
        assert call_kwargs['include_delay'] is True
        
    async def test_get_elaborated_attestations_range(self, xatu):
        """Test elaborated attestations with slot range."""
        mock_df = pd.DataFrame()
        xatu._attestation_fetcher.fetch_elaborated_attestations.return_value = mock_df
        
        await xatu.get_elaborated_attestations(
            slot=[1000, 1100]
        )
        
        call_kwargs = xatu._attestation_fetcher.fetch_elaborated_attestations.call_args[1]
        assert call_kwargs['slot_range'] == [1000, 1100]


@pytest.mark.asyncio
class TestTransactionQueries:
    """Test transaction query methods."""
    
    @pytest.fixture
    async def xatu(self):
        """Create a connected PyXatu instance with mocks."""
        with patch('pyxatu.pyxatu.ConfigManager'):
            xatu = PyXatu()
            xatu._client = AsyncMock()
            xatu._transaction_fetcher = AsyncMock()
            yield xatu
            
    async def test_get_transactions(self, xatu):
        """Test basic transaction retrieval."""
        mock_df = pd.DataFrame({
            'slot': [1000, 1000],
            'hash': ['0xabc', '0xdef'],
            'value': ['1000000', '2000000']
        })
        xatu._transaction_fetcher.fetch.return_value = mock_df
        
        result = await xatu.get_transactions(
            slot=1000,
            columns="slot,hash,value",
            orderby="-value"
        )
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        
        call_args = xatu._transaction_fetcher.fetch.call_args[0][0]
        assert call_args.slot == 1000
        assert call_args.orderby == "-value"
        
    async def test_get_elaborated_transactions(self, xatu):
        """Test elaborated transaction retrieval with privacy analysis."""
        mock_df = pd.DataFrame({
            'slot': [1000, 1001],
            'hash': ['0xabc', '0xdef'],
            'private': [True, False]
        })
        xatu._transaction_fetcher.fetch_elaborated_transactions.return_value = mock_df
        
        result = await xatu.get_elaborated_transactions(
            slots=[1000, 1001],
            include_external_mempool=True
        )
        
        assert isinstance(result, pd.DataFrame)
        assert 'private' in result.columns
        
        call_kwargs = xatu._transaction_fetcher.fetch_elaborated_transactions.call_args[1]
        assert call_kwargs['slots'] == [1000, 1001]
        assert call_kwargs['include_external_mempool'] is True
        
    async def test_get_withdrawals(self, xatu):
        """Test withdrawal data retrieval."""
        mock_df = pd.DataFrame({
            'slot': [1000],
            'validator_index': [12345],
            'amount': [1000000000]
        })
        xatu._transaction_fetcher.fetch_withdrawals.return_value = mock_df
        
        result = await xatu.get_withdrawals(
            slot=[1000, 1100],
            columns="slot,validator_index,amount"
        )
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        
    async def test_get_block_sizes(self, xatu):
        """Test block size metrics retrieval."""
        mock_df = pd.DataFrame({
            'slot': [1000, 1001],
            'block_total_bytes': [100000, 150000],
            'blobs': [0, 2]
        })
        xatu._transaction_fetcher.fetch_block_sizes.return_value = mock_df
        
        result = await xatu.get_block_sizes(
            slot=[1000, 1002],
            orderby="-blobs"
        )
        
        assert isinstance(result, pd.DataFrame)
        assert 'blobs' in result.columns


@pytest.mark.asyncio
class TestValidatorQueries:
    """Test validator query methods."""
    
    @pytest.fixture
    async def xatu(self):
        """Create a connected PyXatu instance with mocks."""
        with patch('pyxatu.pyxatu.ConfigManager'):
            xatu = PyXatu()
            xatu._client = AsyncMock()
            xatu._validator_fetcher = AsyncMock()
            yield xatu
            
    async def test_get_proposer_duties(self, xatu):
        """Test proposer duty retrieval."""
        mock_df = pd.DataFrame({
            'slot': [1000, 1001, 1002],
            'proposer_validator_index': [100, 101, 102]
        })
        xatu._validator_fetcher.fetch_proposer_duties.return_value = mock_df
        
        result = await xatu.get_proposer_duties(
            slot=[1000, 1003],
            columns="slot,proposer_validator_index"
        )
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
        
        call_args = xatu._validator_fetcher.fetch_proposer_duties.call_args[0][0]
        assert call_args.slot == [1000, 1003]


@pytest.mark.asyncio
class TestUtilityMethods:
    """Test utility methods."""
    
    @pytest.fixture
    async def xatu(self):
        """Create a connected PyXatu instance with mocks."""
        with patch('pyxatu.pyxatu.ConfigManager'):
            xatu = PyXatu()
            xatu._client = AsyncMock()
            yield xatu
            
    async def test_get_table_columns(self, xatu):
        """Test retrieving table columns."""
        xatu._client.get_table_columns.return_value = ['slot', 'proposer_index', 'block_root']
        
        columns = await xatu.get_table_columns('canonical_beacon_block')
        
        assert columns == ['slot', 'proposer_index', 'block_root']
        xatu._client.get_table_columns.assert_called_once_with('canonical_beacon_block')
        
    async def test_execute_query(self, xatu):
        """Test executing raw queries."""
        mock_df = pd.DataFrame({'count': [100]})
        xatu._client.execute_query_df.return_value = mock_df
        
        result = await xatu.execute_query(
            "SELECT COUNT(*) as count FROM test WHERE slot = %(slot)s",
            params={'slot': 1000}
        )
        
        assert isinstance(result, pd.DataFrame)
        xatu._client.execute_query_df.assert_called_once_with(
            "SELECT COUNT(*) as count FROM test WHERE slot = %(slot)s",
            {'slot': 1000}
        )
        
    def test_repr(self):
        """Test string representation."""
        with patch('pyxatu.pyxatu.ConfigManager'):
            xatu = PyXatu()
            xatu.config = PyXatuConfig(
                clickhouse=ClickhouseConfig(
                    url="https://ch.example.com",
                    user="testuser",
                    password="pass"
                )
            )
            
            repr_str = repr(xatu)
            assert "PyXatu" in repr_str
            assert "https://ch.example.com" in repr_str
            assert "testuser" in repr_str
            assert "pass" not in repr_str  # Password should not be exposed