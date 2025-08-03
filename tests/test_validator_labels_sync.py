"""Tests for the synchronous validator labels module."""

import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import Mock, patch
import json
from datetime import datetime

from pyxatu.validator_labels_sync import ValidatorLabelManager


class TestValidatorLabelManager:
    """Test suite for synchronous ValidatorLabelManager."""
    
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
    
    def test_initialization(self, manager):
        """Test manager initialization."""
        assert not manager._initialized
        assert manager._entity_mappings == {}
        assert manager._validator_labels.empty
    
    def test_get_validator_labels_bulk(self, manager):
        """Test getting labels for multiple validators."""
        # Mock the internal data
        test_data = pd.DataFrame({
            'validator_index': [1, 2, 3, 4, 5],
            'entity': ['lido', 'coinbase', 'kraken', 'binance', 'lido']
        })
        manager._validator_labels = test_data
        manager._initialized = True
        
        # Test bulk lookup
        labels = manager.get_validator_labels_bulk([1, 3, 5, 999])
        assert labels == {
            1: 'lido',
            3: 'kraken', 
            5: 'lido',
            999: None
        }
    
    def test_get_validators_by_entity(self, manager):
        """Test getting validators for a specific entity."""
        # Mock the internal data
        test_data = pd.DataFrame({
            'validator_index': [1, 2, 3, 4, 5],
            'entity': ['lido', 'coinbase', 'lido', 'binance', 'lido']
        })
        manager._validator_labels = test_data
        manager._initialized = True
        
        # Test getting Lido validators
        lido_validators = manager.get_validators_by_entity('lido')
        assert sorted(lido_validators) == [1, 3, 5]
        
        # Test getting validators for non-existent entity
        unknown_validators = manager.get_validators_by_entity('unknown')
        assert unknown_validators == []
    
    def test_get_entity_statistics(self, manager):
        """Test getting entity statistics."""
        # Mock the internal data with exit information
        test_data = pd.DataFrame({
            'validator_index': [1, 2, 3, 4, 5, 6],
            'entity': ['lido', 'coinbase', 'lido', 'binance', 'lido', 'coinbase'],
            'exit_type': ['voluntary_exit', None, None, 'attester_slashing', None, 'voluntary_exit']
        })
        manager._validator_labels = test_data
        manager._initialized = True
        
        # Get statistics
        stats = manager.get_entity_statistics()
        
        assert not stats.empty
        assert 'validator_count' in stats.columns
        assert 'exited_count' in stats.columns
        assert 'exit_rate' in stats.columns
        
        # Check Lido stats
        lido_stats = stats.loc['lido']
        assert lido_stats['validator_count'] == 3
        assert lido_stats['exited_count'] == 1
        assert lido_stats['exit_rate'] == 1/3
    
    @patch('requests.get')
    def test_parse_spellbook(self, mock_get, manager):
        """Test parsing Spellbook data."""
        # Mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = """blockchain,address,name,category,contributor,source,created_at,updated_at,model_name,label_type
ethereum,0x123,TestEntity,institution,contributor,source,2024-01-01,2024-01-01,model,identifier
ethereum,0x456,TestEntity,institution,contributor,source,2024-01-01,2024-01-01,model,identifier"""
        mock_get.return_value = mock_response
        
        entities = manager._parse_spellbook()
        
        assert 'TestEntity' in entities
        assert entities['TestEntity']['name'] == 'TestEntity'
        assert entities['TestEntity']['category'] == 'institution'
        assert '0x123' in entities['TestEntity']['addresses']
        assert '0x456' in entities['TestEntity']['addresses']
    
    @patch('requests.get')
    def test_parse_cex_addresses(self, mock_get, manager):
        """Test parsing CEX addresses."""
        # Mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = """blockchain,address,cex_name,contributor,source,created_at,updated_at,model_name,label_type
ethereum,0xabc,Binance,contributor,source,2024-01-01,2024-01-01,model,identifier
ethereum,0xdef,Binance,contributor,source,2024-01-01,2024-01-01,model,identifier"""
        mock_get.return_value = mock_response
        
        cex_addresses = manager._parse_cex_addresses()
        
        assert 'Binance' in cex_addresses
        assert '0xabc' in cex_addresses['Binance']
        assert '0xdef' in cex_addresses['Binance']
    
    def test_get_ethseer_labels(self, manager, mock_client):
        """Test querying ethseer labels."""
        # Mock query response
        expected_df = pd.DataFrame({
            'validator_index': [1, 2, 3],
            'entity': ['lido', 'coinbase', ''],
            'on_chain_name': ['Lido', 'Coinbase', ''],
            'deposit_tx_from': ['0x123', '0x456', '0x789'],
            'deposit_tx_to': ['0xabc', '0xdef', '0xghi'],
            'withdrawal_address': ['0x111', '0x222', '0x333']
        })
        mock_client.execute_query_df.return_value = expected_df
        
        result = manager._get_ethseer_labels()
        
        assert not result.empty
        assert len(result) == 2  # Empty labels should be filtered out
        assert 'lido' in result['entity'].values
        assert 'coinbase' in result['entity'].values
    
    def test_apply_exit_information(self, manager, mock_client):
        """Test applying exit information to labels."""
        # Base dataframe
        base_df = pd.DataFrame({
            'validator_index': [1, 2, 3, 4],
            'entity': ['lido', 'coinbase', 'kraken', 'binance']
        })
        
        # Mock voluntary exits
        voluntary_df = pd.DataFrame({
            'validator_index': [1, 3],
            'exit_type': ['voluntary_exit', 'voluntary_exit']
        })
        
        # Mock attester slashings
        slashing_df = pd.DataFrame({
            'validator_index': [2],
            'exit_type': ['attester_slashing']
        })
        
        # Mock the execute_query_df calls
        mock_client.execute_query_df.side_effect = [voluntary_df, slashing_df]
        
        result = manager._apply_exit_information(base_df)
        
        assert 'exit_type' in result.columns
        assert result.loc[result['validator_index'] == 1, 'exit_type'].iloc[0] == 'voluntary_exit'
        assert result.loc[result['validator_index'] == 2, 'exit_type'].iloc[0] == 'attester_slashing'
        assert result.loc[result['validator_index'] == 3, 'exit_type'].iloc[0] == 'voluntary_exit'
        assert pd.isna(result.loc[result['validator_index'] == 4, 'exit_type'].iloc[0])
    
    def test_initialize_with_refresh(self, manager, mock_client):
        """Test full initialization with refresh."""
        # Mock ethseer labels
        ethseer_df = pd.DataFrame({
            'validator_index': [1, 2, 3],
            'entity': ['lido', 'coinbase', 'kraken'],
            'on_chain_name': ['Lido', 'Coinbase', 'Kraken'],
            'deposit_tx_from': ['0x123', '0x456', '0x789'],
            'deposit_tx_to': ['0xabc', '0xdef', '0xghi'],
            'withdrawal_address': ['0x111', '0x222', '0x333']
        })
        
        # Mock empty exit data
        empty_df = pd.DataFrame()
        
        # Mock responses
        mock_client.execute_query_df.side_effect = [
            ethseer_df,  # _get_ethseer_labels
            empty_df,    # voluntary exits
            empty_df,    # attester slashings
            empty_df     # lido operators
        ]
        
        # Mock external data fetching
        with patch.object(manager, '_refresh_entity_mappings'):
            manager.initialize(force_refresh=True)
        
        assert manager._initialized
        assert not manager._validator_labels.empty
        assert len(manager._validator_labels) == 3


if __name__ == '__main__':
    pytest.main([__file__, '-v'])