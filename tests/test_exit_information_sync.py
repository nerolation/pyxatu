"""Test cases for validator exit information functionality - sync version."""

import pytest
import pandas as pd
from unittest.mock import Mock, patch
from pyxatu.validator_labels_sync import ValidatorLabelManager


class TestValidatorExitInformation:
    """Test suite for validator exit information using sync API."""
    
    @pytest.fixture
    def mock_client(self):
        """Create a mock ClickHouse client."""
        client = Mock()
        client.execute_query_df = Mock()
        return client
    
    @pytest.fixture
    def manager(self, mock_client):
        """Create a ValidatorLabelManager instance."""
        return ValidatorLabelManager(mock_client)
    
    def test_get_entity_statistics_with_exits(self, manager):
        """Test getting entity statistics including exit information."""
        # Mock internal data with various exit types
        test_data = pd.DataFrame({
            'validator_index': list(range(10)),
            'entity': ['lido', 'lido', 'lido', 'coinbase', 'coinbase',
                      'kiln', 'kiln', 'kraken', 'binance', 'binance'],
            'exit_type': ['voluntary_exit', None, 'attester_slashing', 
                         'voluntary_exit', None,
                         None, None, 
                         'voluntary_exit', 
                         'attester_slashing', 'attester_slashing']
        })
        manager._validator_labels = test_data
        manager._initialized = True
        
        # Get statistics
        stats = manager.get_entity_statistics()
        
        # Verify statistics
        assert not stats.empty
        assert 'validator_count' in stats.columns
        assert 'exited_count' in stats.columns
        assert 'exit_rate' in stats.columns
        
        # Check specific entities
        lido_stats = stats.loc['lido']
        assert lido_stats['validator_count'] == 3
        assert lido_stats['exited_count'] == 2
        assert lido_stats['exit_rate'] == 2/3
        
        coinbase_stats = stats.loc['coinbase']
        assert coinbase_stats['validator_count'] == 2
        assert coinbase_stats['exited_count'] == 1
        assert coinbase_stats['exit_rate'] == 0.5
        
        kiln_stats = stats.loc['kiln']
        assert kiln_stats['validator_count'] == 2
        assert kiln_stats['exited_count'] == 0
        assert kiln_stats['exit_rate'] == 0.0
    
    def test_apply_exit_information(self, manager, mock_client):
        """Test applying exit information to validator labels."""
        # Base dataframe
        base_df = pd.DataFrame({
            'validator_index': [1, 2, 3, 4, 5],
            'entity': ['lido', 'coinbase', 'kiln', 'kraken', 'binance']
        })
        
        # Mock voluntary exits query result
        voluntary_exits = pd.DataFrame({
            'validator_index': [1, 3],
            'exit_type': ['voluntary_exit', 'voluntary_exit']
        })
        
        # Mock attester slashings query result
        attester_slashings = pd.DataFrame({
            'validator_index': [2, 5],
            'exit_type': ['attester_slashing', 'attester_slashing']
        })
        
        # Set up mock to return these in sequence
        mock_client.execute_query_df.side_effect = [voluntary_exits, attester_slashings]
        
        # Apply exit information
        result = manager._apply_exit_information(base_df)
        
        # Verify results
        assert 'exit_type' in result.columns
        
        # Check specific validators
        assert result[result['validator_index'] == 1]['exit_type'].iloc[0] == 'voluntary_exit'
        assert result[result['validator_index'] == 2]['exit_type'].iloc[0] == 'attester_slashing'
        assert result[result['validator_index'] == 3]['exit_type'].iloc[0] == 'voluntary_exit'
        assert pd.isna(result[result['validator_index'] == 4]['exit_type'].iloc[0])
        assert result[result['validator_index'] == 5]['exit_type'].iloc[0] == 'attester_slashing'
    
    def test_get_validators_with_exit_info(self, manager):
        """Test getting validators for an entity with exit information."""
        # Mock internal data with exit information
        test_data = pd.DataFrame({
            'validator_index': [1, 2, 3, 4, 5, 6],
            'entity': ['lido', 'lido', 'lido', 'coinbase', 'coinbase', 'coinbase'],
            'exit_type': ['voluntary_exit', None, 'attester_slashing', None, None, 'voluntary_exit']
        })
        manager._validator_labels = test_data
        manager._initialized = True
        
        # Get all Lido validators
        lido_validators = manager.get_validators_by_entity('lido')
        assert sorted(lido_validators) == [1, 2, 3]
        
        # Get all Coinbase validators
        coinbase_validators = manager.get_validators_by_entity('coinbase')
        assert sorted(coinbase_validators) == [4, 5, 6]
        
        # To get active validators, we need to filter manually
        # since the sync version doesn't have get_active_validators_by_entity
        lido_active = test_data[
            (test_data['entity'] == 'lido') & 
            (test_data['exit_type'].isna())
        ]['validator_index'].tolist()
        assert lido_active == [2]
        
        coinbase_active = test_data[
            (test_data['entity'] == 'coinbase') & 
            (test_data['exit_type'].isna())
        ]['validator_index'].tolist()
        assert sorted(coinbase_active) == [4, 5]


if __name__ == '__main__':
    pytest.main([__file__, '-v'])