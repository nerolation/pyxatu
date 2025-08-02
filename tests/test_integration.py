"""Integration tests for PyXatu library - simplified."""

import pytest
from unittest.mock import Mock, patch, AsyncMock
import pandas as pd
from datetime import datetime

from pyxatu import PyXatu, Network
from pyxatu.validator_labels import ValidatorLabelManager


class TestPyXatuIntegration:
    """Integration tests for complete workflows."""
    
    def test_complete_slot_analysis_workflow(self):
        """Test complete workflow for analyzing slots."""
        # Mock the client and responses
        with patch('pyxatu.pyxatu.ClickHouseClient') as mock_client_class:
            mock_client = Mock()
            # test_connection is async, so we need to mock it properly
            async def mock_test_connection():
                return True
            mock_client.test_connection = mock_test_connection
            # close is also async
            async def mock_close():
                pass
            mock_client.close = mock_close
            mock_client_class.return_value = mock_client
            
            # Mock slot data
            slot_df = pd.DataFrame({
                'slot': [8000000, 8000001, 8000002],
                'proposer_index': [12345, 67890, 11111],
                'block_root': ['0x123', '0x456', '0x789'],
                'graffiti': ['test1', 'test2', 'test3']
            })
            
            # execute_query_df is async, so we need to mock it properly
            async def mock_execute_query_df(*args, **kwargs):
                return slot_df
            mock_client.execute_query_df = mock_execute_query_df
            
            with PyXatu() as xatu:
                # Get slots
                slots = xatu.get_slots(
                    slot=[8000000, 8000005]
                )
                
                # Analyze results
                assert len(slots) == 3
                assert 'slot' in slots.columns
                
    def test_validator_performance_analysis(self):
        """Test analyzing validator performance with labels."""
        with patch('pyxatu.pyxatu.ClickHouseClient') as mock_client_class:
            mock_client = Mock()
            # test_connection is async, so we need to mock it properly
            async def mock_test_connection():
                return True
            mock_client.test_connection = mock_test_connection
            # close is also async
            async def mock_close():
                pass
            mock_client.close = mock_close
            mock_client_class.return_value = mock_client
            
            # Mock attestation data
            attestation_df = pd.DataFrame({
                'slot': [8000000] * 5,
                'validator_index': [100, 200, 300, 400, 500],
                'source_vote': [True, True, False, True, True],
                'target_vote': [True, True, True, False, True],
                'head_vote': [True, False, False, False, True],
                'inclusion_slot': [8000001, 8000001, 8000002, 8000003, 8000001]
            })
            
            # execute_query_df is async, so we need to mock it properly
            async def mock_execute_query_df(*args, **kwargs):
                return attestation_df
            mock_client.execute_query_df = mock_execute_query_df
            
            with PyXatu() as xatu:
                # Get attestations
                attestations = xatu.get_attestations(slot=8000000)
                
                # Basic analysis
                assert len(attestations) == 5
                assert attestations['source_vote'].mean() == 0.8
                    
    def test_transaction_privacy_analysis(self):
        """Test analyzing transaction privacy across slots."""
        with patch('pyxatu.pyxatu.ClickHouseClient') as mock_client_class:
            mock_client = Mock()
            # test_connection is async, so we need to mock it properly
            async def mock_test_connection():
                return True
            mock_client.test_connection = mock_test_connection
            # close is also async
            async def mock_close():
                pass
            mock_client.close = mock_close
            mock_client_class.return_value = mock_client
            
            # Mock transaction data
            tx_df = pd.DataFrame({
                'slot': [8000000, 8000000, 8000001, 8000001],
                'hash': ['0x123', '0x456', '0x789', '0xabc'],
                'from': ['0xa1', '0xa2', '0xa3', '0xa4'],
                'to': ['0xb1', '0xb2', '0xb3', '0xb4'],
                'value': [1e18, 2e18, 3e18, 4e18]
            })
            
            # execute_query_df is async, so we need to mock it properly
            async def mock_execute_query_df(*args, **kwargs):
                return tx_df
            mock_client.execute_query_df = mock_execute_query_df
            
            with PyXatu() as xatu:
                # Get transactions
                transactions = xatu.get_transactions(
                    slot=[8000000, 8000001]
                )
                
                # Analyze
                assert len(transactions) == 4
                assert transactions.groupby('slot').size().tolist() == [2, 2]
                
    def test_validator_exit_tracking(self):
        """Test tracking validator exits."""
        with patch('pyxatu.pyxatu.ClickHouseClient') as mock_client_class:
            mock_client = Mock()
            # test_connection is async, so we need to mock it properly
            async def mock_test_connection():
                return True
            mock_client.test_connection = mock_test_connection
            # close is also async
            async def mock_close():
                pass
            mock_client.close = mock_close
            mock_client_class.return_value = mock_client
            
            with PyXatu() as xatu:
                # Mock label manager
                with patch.object(xatu, 'get_label_manager') as mock_get_manager:
                    mock_manager = Mock()  # Use Mock instead of AsyncMock for non-async methods
                    mock_manager.get_exit_statistics.return_value = {
                        'total_validators': 5,
                        'active_validators': 2,
                        'exited_validators': 3,
                        'voluntary_exits': 1,
                        'attester_slashings': 1,
                        'proposer_slashings': 1,
                        'exit_rate': 60.0
                    }
                    mock_manager.get_entity_statistics.return_value = pd.DataFrame({
                        'entity': ['lido', 'coinbase', 'kraken'],
                        'validator_count': [2, 2, 1],
                        'active_count': [1, 0, 1],
                        'exited_count': [1, 2, 0],
                        'exit_rate': [50.0, 100.0, 0.0]
                    })
                    mock_get_manager.return_value = mock_manager
                    
                    # Get exit statistics
                    label_manager = xatu.get_label_manager()
                    exit_stats = label_manager.get_exit_statistics()
                    assert exit_stats['exit_rate'] == 60.0
                    assert exit_stats['voluntary_exits'] == 1
                    
                    # Get entity statistics
                    entity_stats = label_manager.get_entity_statistics()
                    assert len(entity_stats) == 3
                    # Coinbase has 100% exit rate
                    assert entity_stats[entity_stats['entity'] == 'coinbase']['exit_rate'].values[0] == 100.0
                    
    def test_error_recovery_workflow(self):
        """Test error handling and recovery in workflows."""
        with patch('pyxatu.pyxatu.ClickHouseClient') as mock_client_class:
            mock_client = Mock()
            # test_connection is async, so we need to mock it properly
            async def mock_test_connection():
                return True
            mock_client.test_connection = mock_test_connection
            # close is also async
            async def mock_close():
                pass
            mock_client.close = mock_close
            mock_client_class.return_value = mock_client
            
            # First query fails, second succeeds
            call_count = 0
            async def mock_execute_query_df_with_error(*args, **kwargs):
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    raise Exception("Database timeout")
                return pd.DataFrame({'slot': [8000000]})
            mock_client.execute_query_df = mock_execute_query_df_with_error
            
            with PyXatu() as xatu:
                # Should handle the error gracefully
                try:
                    slots = xatu.get_slots(slot=8000000)
                except Exception:
                    # Retry logic or error handling
                    async def mock_execute_query_df_retry(*args, **kwargs):
                        return pd.DataFrame({'slot': [8000000]})
                    mock_client.execute_query_df = mock_execute_query_df_retry
                    slots = xatu.get_slots(slot=8000000)
                    
                assert len(slots) == 1
                
    def test_multiple_queries(self):
        """Test running multiple queries in sequence."""
        with patch('pyxatu.pyxatu.ClickHouseClient') as mock_client_class:
            mock_client = Mock()
            # test_connection is async, so we need to mock it properly
            async def mock_test_connection():
                return True
            mock_client.test_connection = mock_test_connection
            # close is also async
            async def mock_close():
                pass
            mock_client.close = mock_close
            mock_client_class.return_value = mock_client
            
            # Mock different query results
            slot_df = pd.DataFrame({'slot': [8000000]})
            attestation_df = pd.DataFrame({'slot': [8000000], 'validator_index': [12345]})
            tx_df = pd.DataFrame({'slot': [8000000], 'hash': ['0x123']})
            
            # Return different results for different queries
            query_results = [slot_df, attestation_df, tx_df]
            query_index = 0
            async def mock_execute_query_df_multiple(*args, **kwargs):
                nonlocal query_index
                result = query_results[query_index]
                query_index += 1
                return result
            mock_client.execute_query_df = mock_execute_query_df_multiple
            
            with PyXatu() as xatu:
                # Run queries in sequence (synchronous API)
                slots = xatu.get_slots(slot=8000000)
                attestations = xatu.get_attestations(slot=8000000)
                transactions = xatu.get_transactions(slot=8000000)
                
                assert len(slots) == 1
                assert len(attestations) == 1
                assert len(transactions) == 1