"""Test that all modules can be imported successfully."""

import pytest


def test_import_main_module():
    """Test importing the main pyxatu module."""
    import pyxatu
    
    # Check main exports
    assert hasattr(pyxatu, 'PyXatu')
    assert hasattr(pyxatu, 'Network')
    assert hasattr(pyxatu, 'VoteType')
    assert hasattr(pyxatu, 'AttestationStatus')
    

def test_import_models():
    """Test importing models module."""
    from pyxatu import models
    
    # Check model classes
    assert hasattr(models, 'QueryParams')
    assert hasattr(models, 'SlotQueryParams')
    assert hasattr(models, 'Block')
    assert hasattr(models, 'Transaction')
    

def test_import_config():
    """Test importing config module."""
    from pyxatu import config
    
    assert hasattr(config, 'ConfigManager')
    assert hasattr(config, 'PyXatuConfig')
    assert hasattr(config, 'ClickhouseConfig')
    

def test_import_utils():
    """Test importing utils module."""
    from pyxatu import utils
    
    # Check constants
    assert utils.SECONDS_PER_SLOT == 12
    assert utils.SLOTS_PER_EPOCH == 32
    
    # Check functions
    assert hasattr(utils, 'slot_to_timestamp')
    assert hasattr(utils, 'timestamp_to_slot')
    

def test_import_base():
    """Test importing base module."""
    from pyxatu import base
    
    assert hasattr(base, 'BaseClient')
    assert hasattr(base, 'BaseDataFetcher')
    assert hasattr(base, 'BaseConnector')
    

def test_import_clickhouse_client():
    """Test importing clickhouse client."""
    from pyxatu import clickhouse_client
    
    assert hasattr(clickhouse_client, 'ClickHouseClient')
    assert hasattr(clickhouse_client, 'ClickHouseQueryBuilder')
    

def test_import_queries():
    """Test importing query modules."""
    from pyxatu import queries
    
    assert hasattr(queries, 'SlotDataFetcher')
    assert hasattr(queries, 'AttestationDataFetcher')
    assert hasattr(queries, 'TransactionDataFetcher')
    assert hasattr(queries, 'ValidatorDataFetcher')
    

def test_import_connectors():
    """Test importing connector modules."""
    # These might fail if dependencies are not installed
    try:
        from pyxatu import mempool_connector
        assert hasattr(mempool_connector, 'MempoolConnector')
    except ImportError as e:
        pytest.skip(f"Optional dependency not installed: {e}")
        
    try:
        from pyxatu import relay_connector
        assert hasattr(relay_connector, 'RelayConnector')
    except ImportError as e:
        pytest.skip(f"Optional dependency not installed: {e}")
        

def test_version():
    """Test that version is defined."""
    import pyxatu
    
    assert hasattr(pyxatu, '__version__')
    assert isinstance(pyxatu.__version__, str)
    assert pyxatu.__version__ == "1.9.1"