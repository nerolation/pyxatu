"""Tests for main PyXatu functionality - testing synchronous API."""

import pytest
from unittest.mock import Mock, patch
import pandas as pd
from datetime import datetime

from pyxatu import PyXatu, Network
from pyxatu.config import PyXatuConfig, ClickhouseConfig


class TestPyXatuConnection:
    """Test PyXatu connection management."""
    
    def test_context_manager(self):
        """Test synchronous context manager."""
        with patch('pyxatu.pyxatu_sync.ClickHouseClient') as mock_client_class:
            mock_client = Mock()
            mock_client.test_connection.return_value = True
            mock_client_class.return_value = mock_client
            
            with PyXatu() as xatu:
                # Just verify that the context manager works
                pass
                
            mock_client.close.assert_called_once()
            
    def test_manual_close(self):
        """Test manual close."""
        with patch('pyxatu.pyxatu_sync.ClickHouseClient') as mock_client_class:
            mock_client = Mock()
            mock_client.test_connection.return_value = True
            mock_client.close = Mock()
            mock_client_class.return_value = mock_client
            
            xatu = PyXatu()
            # First connect, then close
            xatu.connect()
            xatu.close()
            mock_client.close.assert_called_once()


class TestPyXatuQueries:
    """Test PyXatu query methods."""
            
    def test_get_slots(self):
        """Test get_slots method."""
        with patch('pyxatu.pyxatu_sync.ClickHouseClient') as mock_client_class:
            mock_client = Mock()
            mock_client.test_connection.return_value = True
            mock_client.execute_query_df = Mock()
            mock_client_class.return_value = mock_client
            
            expected_df = pd.DataFrame({
                'slot': [8000000, 8000001, 8000002],
                'proposer_index': [12345, 67890, 11111]
            })
            mock_client.execute_query_df.return_value = expected_df
            
            with PyXatu() as xatu:
                result = xatu.get_slots(slot=[8000000, 8000003])
                
                assert isinstance(result, pd.DataFrame)
                assert len(result) == 3
                assert list(result.columns) == ['slot', 'proposer_index']
        
    def test_get_attestations(self):
        """Test get_attestations method."""
        with patch('pyxatu.pyxatu_sync.ClickHouseClient') as mock_client_class:
            mock_client = Mock()
            mock_client.test_connection.return_value = True
            mock_client.execute_query_df = Mock()
            mock_client_class.return_value = mock_client
            
            expected_df = pd.DataFrame({
                'slot': [8000000, 8000000, 8000001],
                'validator_index': [100, 200, 300],
                'source_vote': [True, True, False]
            })
            mock_client.execute_query_df.return_value = expected_df
            
            with PyXatu() as xatu:
                result = xatu.get_attestations(slot=8000000)
                
                assert isinstance(result, pd.DataFrame)
                assert len(result) == 3
    
    def test_get_attestations_with_validator_filter(self):
        """Test get_attestations with validator_index filter."""
        with patch('pyxatu.pyxatu_sync.ClickHouseClient') as mock_client_class:
            mock_client = Mock()
            mock_client.test_connection.return_value = True
            mock_client.execute_query_df = Mock()
            mock_client_class.return_value = mock_client
            
            # Test with single validator
            expected_df = pd.DataFrame({
                'slot': [8000000],
                'validator_index': [100],
                'source_vote': [True]
            })
            mock_client.execute_query_df.return_value = expected_df
            
            with PyXatu() as xatu:
                result = xatu.get_attestations(slot=8000000, validator_index=100)
                assert len(result) == 1
                assert result['validator_index'].iloc[0] == 100
                
                # Test with multiple validators
                expected_df_multi = pd.DataFrame({
                    'slot': [8000000, 8000000],
                    'validator_index': [100, 200],
                    'source_vote': [True, True]
                })
                mock_client.execute_query_df.return_value = expected_df_multi
                
                result = xatu.get_attestations(slot=8000000, validator_index=[100, 200])
                assert len(result) == 2
                assert set(result['validator_index']) == {100, 200}
        
    def test_execute_query(self):
        """Test query method."""
        with patch('pyxatu.pyxatu_sync.ClickHouseClient') as mock_client_class:
            mock_client = Mock()
            mock_client.test_connection.return_value = True
            mock_client.execute_query_df = Mock()
            mock_client_class.return_value = mock_client
            
            expected_df = pd.DataFrame({
                'count': [100]
            })
            mock_client.execute_query_df.return_value = expected_df
            
            with PyXatu() as xatu:
                result = xatu.query("SELECT count(*) as count FROM canonical_beacon_block")
                
                assert isinstance(result, pd.DataFrame)
                assert result.iloc[0]['count'] == 100
        
    def test_get_table_columns(self):
        """Test get_table_columns method."""
        with patch('pyxatu.pyxatu_sync.ClickHouseClient') as mock_client_class:
            mock_client = Mock()
            mock_client.test_connection.return_value = True
            mock_client.get_table_columns = Mock()
            mock_client_class.return_value = mock_client
            
            expected_columns = ['slot', 'proposer_index', 'block_root']
            mock_client.get_table_columns.return_value = expected_columns
            
            with PyXatu() as xatu:
                # Access the client directly since get_table_columns is not exposed
                xatu._ensure_connected()
                columns = xatu._client.get_table_columns('canonical_beacon_block')
                assert columns == expected_columns


class TestPyXatuValidatorLabels:
    """Test validator label functionality."""
            
    def test_get_validator_labels_bulk(self):
        """Test getting multiple validator labels."""
        with patch('pyxatu.pyxatu_sync.ClickHouseClient') as mock_client_class:
            mock_client = Mock()
            mock_client.test_connection.return_value = True
            mock_client_class.return_value = mock_client
            
            # Mock the validator label manager
            with patch('pyxatu.pyxatu_sync.ValidatorLabelManager') as mock_label_manager_class:
                mock_label_manager = Mock()
                mock_label_manager.initialize = Mock()
                mock_label_manager.get_validator_labels_bulk.return_value = {
                    100: 'lido',
                    200: 'coinbase',
                    300: None
                }
                mock_label_manager_class.return_value = mock_label_manager
                
                with PyXatu() as xatu:
                    labels = xatu.get_validator_labels_bulk([100, 200, 300])
                    assert labels == {
                        100: 'lido',
                        200: 'coinbase',
                        300: None
                    }


class TestPyXatuMissedSlots:
    """Test missed slots functionality."""
    
    def test_get_missed_slots(self):
        """Test getting missed slots."""
        with patch('pyxatu.pyxatu_sync.ClickHouseClient') as mock_client_class:
            mock_client = Mock()
            mock_client.test_connection.return_value = True
            mock_client.execute_query_df = Mock()
            mock_client_class.return_value = mock_client
            
            # Mock the query response
            expected_df = pd.DataFrame({
                'slot': [8000001, 8000002, 8000004],
                'epoch': [250000, 250000, 250000],
                'slot_start_date_time': ['2024-01-01', '2024-01-01', '2024-01-01']
            })
            mock_client.execute_query_df.return_value = expected_df
            
            with PyXatu() as xatu:
                result = xatu.get_missed_slots(slot_range=[8000000, 8000005])
                
                assert isinstance(result, pd.DataFrame)
                assert len(result) == 3
                assert list(result['slot']) == [8000001, 8000002, 8000004]


class TestPyXatuReorgs:
    """Test reorganization functionality."""
    
    def test_get_reorgs(self):
        """Test getting reorganizations."""
        with patch('pyxatu.pyxatu_sync.ClickHouseClient') as mock_client_class:
            mock_client = Mock()
            mock_client.test_connection.return_value = True
            mock_client.execute_query_df = Mock()
            mock_client_class.return_value = mock_client
            
            expected_df = pd.DataFrame({
                'slot': [8000100, 8000200],
                'depth': [1, 2],
                'old_head_block_root': ['0xabc', '0xdef'],
                'new_head_block_root': ['0x123', '0x456'],
                'epoch': [250003, 250006],
                'event_date_time': ['2024-01-01', '2024-01-01']
            })
            mock_client.execute_query_df.return_value = expected_df
            
            with PyXatu() as xatu:
                result = xatu.get_reorgs(limit=10)
                
                assert isinstance(result, pd.DataFrame)
                assert len(result) == 2
                assert 'depth' in result.columns


if __name__ == '__main__':
    pytest.main([__file__, '-v'])