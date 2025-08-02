"""Tests for schema management - updated to match current API."""

import pytest
import json
from pathlib import Path
from unittest.mock import Mock, patch, mock_open

from pyxatu.schema import SchemaManager, TableInfo, ColumnInfo
from pyxatu.models import Network


class TestSchemaManager:
    """Test schema manager functionality."""
    
    @pytest.fixture
    def schema_data(self):
        """Create test schema data."""
        return {
            "canonical_beacon_block": {
                "description": "Beacon chain blocks",
                "table_name": "canonical_beacon_block",
                "partitioning_column": "slot_start_date_time",
                "networks_available": ["mainnet", "sepolia", "holesky"],
                "pyxatu_method": "get_slots",
                "data_characteristics": {
                    "update_frequency": "Real-time",
                    "retention": "Forever",
                    "data_quality": "Canonical"
                },
                "common_columns": [
                    {
                        "name": "slot",
                        "type": "UInt64",
                        "description": "The slot number"
                    },
                    {
                        "name": "proposer_index",
                        "type": "UInt64",
                        "description": "The proposer validator index"
                    },
                    {
                        "name": "block_root",
                        "type": "String",
                        "description": "The block root hash"
                    }
                ]
            },
            "canonical_beacon_attestation": {
                "description": "Beacon chain attestations",
                "table_name": "canonical_beacon_attestation",
                "partitioning_column": "slot_start_date_time",
                "networks_available": ["mainnet"],
                "pyxatu_method": "get_attestation",
                "data_characteristics": {
                    "update_frequency": "Real-time",
                    "retention": "Forever",
                    "data_quality": "Canonical"
                },
                "common_columns": [
                    {
                        "name": "slot",
                        "type": "UInt64",
                        "description": "The attestation slot"
                    },
                    {
                        "name": "attesting_validator_index",
                        "type": "UInt64",
                        "description": "The attesting validator index"
                    }
                ]
            }
        }
        
    @pytest.fixture
    def manager(self, schema_data):
        """Create a schema manager with test data."""
        with patch('pyxatu.schema.SchemaManager._load_schema'):
            manager = SchemaManager()
            # Clear any loaded data
            manager._tables = {}
            manager._method_to_table = {}
            
            # Directly set the internal attributes to match the implementation
            for table_name, table_data in schema_data.items():
                columns = [
                    ColumnInfo(
                        name=col['name'],
                        type=col['type'],
                        description=col['description']
                    )
                    for col in table_data.get('common_columns', [])
                ]
                
                table_info = TableInfo(
                    description=table_data['description'],
                    table_name=table_name,
                    partitioning_column=table_data['partitioning_column'],
                    networks_available=table_data['networks_available'],
                    pyxatu_method=table_data['pyxatu_method'],
                    data_characteristics=table_data['data_characteristics'],
                    common_columns=columns
                )
                
                manager._tables[table_name] = table_info
                manager._method_to_table[table_data['pyxatu_method']] = table_name
            
            return manager
            
    def test_load_schema(self, manager, schema_data):
        """Test schema loading."""
        assert manager._tables is not None
        assert len(manager._tables) == 2
        assert "canonical_beacon_block" in manager._tables
        assert "canonical_beacon_attestation" in manager._tables
        
    def test_get_table_info(self, manager):
        """Test getting table information."""
        info = manager.get_table_info("canonical_beacon_block")
        
        assert info is not None
        assert info.table_name == "canonical_beacon_block"
        assert info.description == "Beacon chain blocks"
        assert info.partitioning_column == "slot_start_date_time"
        assert len(info.common_columns) == 3
        assert info.common_columns[0].name == "slot"
        
    def test_get_table_info_not_found(self, manager):
        """Test getting info for non-existent table."""
        info = manager.get_table_info("non_existent_table")
        assert info is None
        
    def test_get_column_names(self, manager):
        """Test getting table column names."""
        info = manager.get_table_info("canonical_beacon_block")
        assert info is not None
        columns = info.get_column_names()
        
        assert columns == ["slot", "proposer_index", "block_root"]
        
    def test_get_column_names_not_found(self, manager):
        """Test getting columns for non-existent table."""
        info = manager.get_table_info("non_existent_table")
        assert info is None
        
    def test_validate_columns(self, manager):
        """Test column validation."""
        info = manager.get_table_info("canonical_beacon_block")
        assert info is not None
        
        # Valid columns
        invalid = info.validate_columns(["slot", "proposer_index"])
        assert invalid == []
        
        # Mixed valid and invalid
        invalid = info.validate_columns(["slot", "invalid_column", "proposer_index"])
        assert invalid == ["invalid_column"]
        
        # Empty columns
        invalid = info.validate_columns([])
        assert invalid == []
        
    def test_get_tables_for_network(self, manager):
        """Test getting tables available for a network."""
        # Mainnet should have both tables
        tables = manager.get_tables_for_network('mainnet')
        assert len(tables) == 2
        
        # Sepolia should only have canonical_beacon_block
        tables = manager.get_tables_for_network('sepolia')
        assert len(tables) == 1
        assert tables[0].table_name == "canonical_beacon_block"
        
    def test_get_table_by_method(self, manager):
        """Test getting table by PyXatu method."""
        info = manager.get_table_by_method("get_slots")
        assert info is not None
        assert info.table_name == "canonical_beacon_block"
        
        # Non-existent method
        info = manager.get_table_by_method("get_nonexistent")
        assert info is None
        
    def test_get_all_tables(self, manager):
        """Test getting all tables."""
        tables = manager.get_all_tables()
        assert len(tables) == 2
        assert "canonical_beacon_block" in tables
        assert isinstance(tables["canonical_beacon_block"], TableInfo)
        
    def test_validate_method_params(self, manager):
        """Test method parameter validation."""
        # Valid params
        result = manager.validate_method_params(
            "get_slots",
            columns=["slot", "proposer_index"],
            network='mainnet'
        )
        assert result['valid'] is True
        assert len(result['errors']) == 0
        
        # Invalid columns
        result = manager.validate_method_params(
            "get_slots",
            columns=["slot", "invalid_column"],
            network='mainnet'
        )
        assert result['valid'] is False
        assert len(result['errors']) == 1
        assert "invalid_column" in result['errors'][0]
        
        # Invalid network
        result = manager.validate_method_params(
            "get_attestation",
            network='holesky'
        )
        assert result['valid'] is False
        assert len(result['errors']) == 1
        assert "holesky" in result['errors'][0].lower()
        
    def test_deprecated_methods(self):
        """Test that deprecated methods don't exist."""
        manager = SchemaManager()
        # Verify old methods don't exist
        assert not hasattr(manager, 'get_table_schema')
        assert not hasattr(manager, 'validate_table_name')


class TestTableInfo:
    """Test TableInfo dataclass."""
    
    def test_column_names(self):
        """Test getting column names."""
        columns = [
            ColumnInfo("col1", "UInt64", "Column 1"),
            ColumnInfo("col2", "String", "Column 2")
        ]
        info = TableInfo(
            description="Test",
            table_name="test",
            partitioning_column="timestamp",
            networks_available=["mainnet"],
            pyxatu_method="get_test",
            data_characteristics={},
            common_columns=columns
        )
        
        assert info.get_column_names() == ["col1", "col2"]
        
    def test_validate_network(self):
        """Test network validation."""
        info = TableInfo(
            description="Test",
            table_name="test",
            partitioning_column="timestamp",
            networks_available=["mainnet", "sepolia"],
            pyxatu_method="get_test",
            data_characteristics={},
            common_columns=[]
        )
        
        assert info.validate_network("mainnet") is True
        assert info.validate_network("MAINNET") is True
        assert info.validate_network("sepolia") is True
        assert info.validate_network("holesky") is False


class TestSchemaValidation:
    """Test schema validation functionality."""
    
    @pytest.fixture
    def manager(self):
        """Create schema manager."""
        return SchemaManager()
        
    def test_validate_query_params(self, manager):
        """Test query parameter validation."""
        # This test is adapted to use validate_method_params instead
        result = manager.validate_method_params(
            method_name="get_slots",
            columns=["slot", "proposer_index"],
            network='mainnet'
        )
        # Result should be a dict with validation info
        assert isinstance(result, dict)
        assert 'valid' in result
        assert 'errors' in result


if __name__ == '__main__':
    pytest.main([__file__, '-v'])