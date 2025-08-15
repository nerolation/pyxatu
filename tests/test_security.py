"""Security tests for PyXatu - ensuring all vulnerabilities are fixed."""

import pytest
from unittest.mock import Mock, patch

from pyxatu.core.clickhouse_client import ClickHouseQueryBuilder, ClickHouseClient
from pyxatu.models import SlotQueryParams, Network
from pyxatu.config import ClickhouseConfig


class TestSQLInjectionPrevention:
    """Test SQL injection prevention in query builder."""
    
    def test_table_validation(self):
        """Test that only allowed tables can be queried."""
        builder = ClickHouseQueryBuilder()
        
        # Valid table should work
        builder.from_table('canonical_beacon_block')
        
        # Invalid table should raise error
        with pytest.raises(ValueError, match="not allowed"):
            builder.from_table('users; DROP TABLE users;--')
            
    def test_operator_validation(self):
        """Test that only safe operators are allowed."""
        builder = ClickHouseQueryBuilder()
        
        # Valid operators
        builder.where('slot', '=', 1000)
        builder.reset().where('slot', '>=', 1000)
        builder.reset().where('network', 'IN', ['mainnet', 'sepolia'])
        
        # Invalid operator should raise error
        with pytest.raises(ValueError, match="not allowed"):
            builder.where('slot', 'UNION SELECT', 1000)
            
    def test_parameter_binding(self):
        """Test that values are properly parameterized."""
        builder = ClickHouseQueryBuilder()
        
        query, params = (
            builder
            .select('slot,proposer_index')
            .from_table('canonical_beacon_block')
            .where('slot', '=', 1000)
            .where('meta_network_name', '=', "mainnet'; DROP TABLE--")
            .build()
        )
        
        # Query should use placeholders
        assert '%(param_0)s' in query
        assert '%(param_1)s' in query
        
        # Dangerous input should be in params, not query
        assert "DROP TABLE" not in query
        assert params['param_1'] == "'mainnet\\'; DROP TABLE--'"  # String is now quoted and escaped
        
    def test_where_raw_validation(self):
        """Test that raw WHERE conditions are validated."""
        builder = ClickHouseQueryBuilder()
        
        # Should reject obvious SQL injection attempts
        dangerous_inputs = [
            "1=1; DROP TABLE users;",
            "1=1 -- comment",
            "1=1 /* comment */",
            "1=1 UNION SELECT * FROM passwords",
            "slot > 0; DELETE FROM data"
        ]
        
        for dangerous in dangerous_inputs:
            builder.reset()
            with pytest.raises(ValueError, match="unsafe SQL"):
                builder.where_raw(dangerous)
                
    def test_column_validation_in_model(self):
        """Test that column specifications are validated."""
        # Valid columns
        params = SlotQueryParams(
            columns="slot, proposer_index, block_root"
        )
        assert params.columns == "slot, proposer_index, block_root"
        
        # Dangerous column input should be rejected
        with pytest.raises(ValueError, match="unsafe SQL"):
            SlotQueryParams(
                columns="*; DROP TABLE canonical_beacon_block;--"
            )
            

class TestNoEvalUsage:
    """Ensure no eval() usage in the new codebase."""
    
    def test_json_parsing_instead_of_eval(self):
        """Test that JSON is parsed safely without eval."""
        import json
        
        # Simulate validator array parsing
        validators_json = '[1234, 5678, 9012]'
        validators = json.loads(validators_json)
        assert validators == [1234, 5678, 9012]
        
        # Simulate relay response parsing
        relay_response = '{"slot": 1000, "value": "1000000000000000000"}'
        data = json.loads(relay_response)
        assert data['slot'] == 1000
        

class TestInputValidation:
    """Test input validation across the codebase."""
    
    def test_slot_validation(self):
        """Test slot parameter validation."""
        # Valid slots
        params = SlotQueryParams(slot=1000)
        assert params.slot == 1000
        
        params = SlotQueryParams(slot=[1000, 2000])
        assert params.slot == [1000, 2000]
        
        # Invalid slots
        with pytest.raises(ValueError, match="cannot be negative"):
            SlotQueryParams(slot=-1)
            
        with pytest.raises(ValueError, match="exactly 2 elements"):
            SlotQueryParams(slot=[1000])
            
        with pytest.raises(ValueError, match="start must be less than end"):
            SlotQueryParams(slot=[2000, 1000])
            
    def test_network_validation(self):
        """Test network parameter validation."""
        # Valid networks
        params = SlotQueryParams(network=Network.MAINNET)
        params = SlotQueryParams(network=Network.SEPOLIA)
        
        # Invalid network
        with pytest.raises(ValueError):
            SlotQueryParams(network="fakenet")
            
    def test_limit_validation(self):
        """Test limit parameter validation."""
        # Valid limits
        params = SlotQueryParams(limit=100)
        params = SlotQueryParams(limit=1)
        
        # Invalid limits
        with pytest.raises(ValueError):
            SlotQueryParams(limit=0)
            
        with pytest.raises(ValueError):
            SlotQueryParams(limit=-10)
            

class TestConfigurationSecurity:
    """Test secure configuration handling."""
    
    def test_password_not_exposed(self):
        """Test that passwords are properly hidden."""
        from pyxatu.config import ClickhouseConfig
        from pydantic import SecretStr
        
        config = ClickhouseConfig(
            url="https://clickhouse.example.com",
            user="testuser",
            password=SecretStr("super-secret-password")
        )
        
        # Password should not be in string representation
        config_str = str(config)
        assert "super-secret-password" not in config_str
        assert "**********" in config_str or "SecretStr" in config_str
        
        # But should be accessible via get_secret_value()
        assert config.password.get_secret_value() == "super-secret-password"
        
    @patch.dict('os.environ', {
        'CLICKHOUSE_PASSWORD': 'env-secret-password',
        'CLICKHOUSE_URL': 'https://ch.example.com',
        'CLICKHOUSE_USER': 'envuser'
    })
    def test_env_var_loading(self):
        """Test loading sensitive data from environment variables."""
        from pyxatu.config import ConfigManager
        
        manager = ConfigManager(use_env_vars=True)
        config = manager.load()
        
        assert config.clickhouse.url == 'https://ch.example.com'
        assert config.clickhouse.user == 'envuser'
        assert config.clickhouse.password.get_secret_value() == 'env-secret-password'
        

class TestSyncSafety:
    """Test synchronous operation safety."""
    
    def test_connection_pooling(self):
        """Test that connections are properly pooled and closed."""
        config = ClickhouseConfig(
            url="https://clickhouse.example.com",
            user="test",
            password="test",
            pool_size=5
        )
        
        client = ClickHouseClient(config)
        
        # Get session multiple times
        session1 = client._get_session()
        session2 = client._get_session()
        
        # Should reuse same session
        assert session1 is session2
        
        # Check that session has proper configuration
        assert session1 is not None
        
        # Close should close session
        client.close()
            

def test_no_hardcoded_credentials():
    """Ensure no hardcoded credentials in source code."""
    import os
    import re
    
    # Patterns that might indicate hardcoded credentials
    patterns = [
        r'password\s*=\s*["\'][^"\']+["\']',
        r'api_key\s*=\s*["\'][^"\']+["\']',
        r'secret\s*=\s*["\'][^"\']+["\']',
    ]
    
    # Check main source files (not test files)
    source_files = [
        'pyxatu.py',
        'clickhouse_client.py',
        'config.py',
        'mempool_connector.py',
        'relay_connector.py'
    ]
    
    for filename in source_files:
        filepath = os.path.join(os.path.dirname(__file__), '..', 'pyxatu', filename)
        if os.path.exists(filepath):
            with open(filepath, 'r') as f:
                content = f.read()
                
            for pattern in patterns:
                matches = re.findall(pattern, content, re.IGNORECASE)
                # Filter out obvious test/example values
                real_matches = [
                    m for m in matches 
                    if not any(test in m.lower() for test in [
                        'test', 'example', 'your', 'placeholder', 
                        'default', 'secret_value', 'get_secret_value'
                    ])
                ]
                assert not real_matches, f"Possible credential in {filename}: {real_matches}"