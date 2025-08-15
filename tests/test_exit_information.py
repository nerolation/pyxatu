"""Test cases for validator exit information functionality."""

import pytest
import pandas as pd
from unittest.mock import Mock, patch
from pyxatu.validator_labels import ValidatorLabelManager


class TestValidatorExitInformation:
    """Test suite for validator exit information."""
    
    @pytest.fixture
    def mock_client(self):
        """Create a mock ClickHouse client."""
        client = Mock()
        client.execute_query_df = Mock()
        return client
    
    @pytest.fixture
    def manager(self, mock_client, tmp_path):
        """Create a ValidatorLabelManager instance."""
        with patch.object(ValidatorLabelManager, 'CACHE_DIR', tmp_path / '.pyxatu' / 'cache'):
            manager = ValidatorLabelManager(mock_client)
            manager.CACHE_DIR.mkdir(parents=True, exist_ok=True)
            return manager
    
    @pytest.fixture
    def mock_validators(self):
        """Create mock validator data."""
        return pd.DataFrame({
            'validator_index': list(range(10)),
            'pubkey': [f'0x{i:040x}' for i in range(10)],
            'depositor_address': [f'0xaddr{i}' for i in range(10)],
            'entity': ['lido', 'coinbase', 'kiln', 'kraken', None,
                      'lido', 'coinbase', 'kiln', None, None]
        })
    
    def test_get_voluntary_exits(self, manager, mock_client):
        """Test getting voluntary exit information."""
        # Mock query result
        mock_exits = pd.DataFrame({
            'validator_index': [1, 3, 7],
            'exit_epoch': [100000, 100500, 101000],
            'epoch': [100000, 100500, 101000],
            'slot': [3200000, 3216000, 3232000]
        })
        mock_client.execute_query_df.return_value = mock_exits
        
        # Get voluntary exits
        exits = manager._get_voluntary_exits()
        
        # Verify
        assert len(exits) == 3
        assert 'validator_index' in exits.columns
        assert 'exit_epoch' in exits.columns
        assert mock_client.execute_query_df.called
    
    def test_get_attester_slashings(self, manager, mock_client):
        """Test getting attester slashing information."""
        # Mock query result
        mock_slashings = pd.DataFrame({
            'validator_index': [2, 5],
            'epoch': [95000, 96000],
            'slot': [3040000, 3072000]
        })
        mock_client.execute_query_df.return_value = mock_slashings
        
        # Get attester slashings
        slashings = manager._get_attester_slashings()
        
        # Verify
        assert len(slashings) == 2
        assert 'validator_index' in slashings.columns
        assert 'epoch' in slashings.columns
    
    def test_get_proposer_slashings(self, manager, mock_client):
        """Test getting proposer slashing information."""
        # Mock query result
        mock_slashings = pd.DataFrame({
            'validator_index': [8],
            'epoch': [97000],
            'slot': [3104000]
        })
        mock_client.execute_query_df.return_value = mock_slashings
        
        # Get proposer slashings
        slashings = manager._get_proposer_slashings()
        
        # Verify
        assert len(slashings) == 1
        assert slashings.iloc[0]['validator_index'] == 8
    
    def test_apply_exit_information(self, manager, mock_client, mock_validators):
        """Test applying exit information to validators."""
        # Mock exit data
        voluntary_exits = pd.DataFrame({
            'validator_index': [0, 3, 7],
            'exit_epoch': [100000, 100500, 101000],
            'epoch': [100000, 100500, 101000],
            'slot': [3200000, 3216000, 3232000]
        })
        
        attester_slashings = pd.DataFrame({
            'validator_index': [2],
            'epoch': [95000],
            'slot': [3040000]
        })
        
        proposer_slashings = pd.DataFrame({
            'validator_index': [8],
            'epoch': [97000],
            'slot': [3104000]
        })
        
        # Setup mock returns
        mock_client.execute_query_df.side_effect = [
            voluntary_exits,
            attester_slashings,
            proposer_slashings
        ]
        
        # Apply exit information
        result = manager._apply_exit_information(mock_validators.copy())
        
        # Verify columns added
        assert 'exited' in result.columns
        assert 'exit_type' in result.columns
        assert 'exit_epoch' in result.columns
        
        # Verify voluntary exits
        assert result.loc[result['validator_index'] == 0, 'exited'].iloc[0] == True
        assert result.loc[result['validator_index'] == 0, 'exit_type'].iloc[0] == 'voluntary'
        assert result.loc[result['validator_index'] == 0, 'exit_epoch'].iloc[0] == 100000
        
        # Verify attester slashing
        assert result.loc[result['validator_index'] == 2, 'exited'].iloc[0] == True
        assert result.loc[result['validator_index'] == 2, 'exit_type'].iloc[0] == 'attester_slashing'
        
        # Verify proposer slashing
        assert result.loc[result['validator_index'] == 8, 'exited'].iloc[0] == True
        assert result.loc[result['validator_index'] == 8, 'exit_type'].iloc[0] == 'proposer_slashing'
        
        # Verify non-exited validators
        assert result.loc[result['validator_index'] == 1, 'exited'].iloc[0] == False
        assert pd.isna(result.loc[result['validator_index'] == 1, 'exit_type'].iloc[0])
    
    def test_get_exit_statistics(self, manager):
        """Test getting exit statistics."""
        # Create mock labeled data with exits
        manager._validator_labels = pd.DataFrame({
            'validator_index': list(range(100)),
            'entity': ['lido'] * 30 + ['coinbase'] * 20 + ['kiln'] * 10 + [None] * 40,
            'exited': [True] * 15 + [False] * 85,
            'exit_type': ['voluntary'] * 10 + ['attester_slashing'] * 4 + ['proposer_slashing'] * 1 + [None] * 85
        })
        
        # Get statistics
        stats = manager.get_exit_statistics()
        
        # Verify
        assert stats['total_validators'] == 100
        assert stats['active_validators'] == 85
        assert stats['exited_validators'] == 15
        assert stats['exit_rate'] == 15.0
        assert stats['voluntary_exits'] == 10
        assert stats['attester_slashings'] == 4
        assert stats['proposer_slashings'] == 1
    
    def test_get_entity_statistics_with_exits(self, manager):
        """Test entity statistics including exit information."""
        # Create mock labeled data
        manager._validator_labels = pd.DataFrame({
            'validator_index': list(range(10)),
            'entity': ['lido', 'lido', 'lido', 'coinbase', 'coinbase', 
                      'kiln', 'kiln', 'kiln', 'kiln', None],
            'exited': [True, False, False, True, False, 
                      True, True, False, False, False]
        })
        
        # Get statistics
        stats = manager.get_entity_statistics()
        
        # Verify structure
        assert 'entity' in stats.columns
        assert 'validator_count' in stats.columns
        assert 'exited_count' in stats.columns
        assert 'active_count' in stats.columns
        assert 'exit_rate' in stats.columns
        
        # Verify Kiln stats
        kiln_stats = stats[stats['entity'] == 'kiln'].iloc[0]
        assert kiln_stats['validator_count'] == 4
        assert kiln_stats['exited_count'] == 2
        assert kiln_stats['active_count'] == 2
        assert kiln_stats['exit_rate'] == 50.0
    
    def test_get_active_validators_by_entity(self, manager):
        """Test getting only active validators for an entity."""
        # Create mock labeled data
        manager._validator_labels = pd.DataFrame({
            'validator_index': [1, 2, 3, 4, 5],
            'entity': ['kiln', 'kiln', 'kiln', 'kiln', 'lido'],
            'exited': [True, False, True, False, False]
        })
        
        # Get active Kiln validators
        active_kiln = manager.get_active_validators_by_entity('kiln')
        
        # Verify only active validators returned
        assert len(active_kiln) == 2
        assert 2 in active_kiln
        assert 4 in active_kiln
        assert 1 not in active_kiln  # exited
        assert 3 not in active_kiln  # exited
    
    def test_error_handling(self, manager, mock_client):
        """Test error handling in exit queries."""
        # Mock query error
        mock_client.execute_query_df.side_effect = Exception("Query timeout")
        
        # Should return empty DataFrame on error
        exits = manager._get_voluntary_exits()
        assert exits.empty
        
        slashings = manager._get_attester_slashings()
        assert slashings.empty
        
        prop_slashings = manager._get_proposer_slashings()
        assert prop_slashings.empty