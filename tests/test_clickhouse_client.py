"""Tests for the secure ClickHouse client."""

import pytest
from unittest.mock import Mock, patch, MagicMock
import json
import pandas as pd
from io import StringIO
import requests

from pyxatu.core.clickhouse_client import ClickHouseQueryBuilder, ClickHouseClient
from pyxatu.config import ClickhouseConfig
from pydantic import SecretStr


class TestClickHouseQueryBuilder:
    """Test the SQL query builder."""
    
    def test_basic_query_building(self):
        """Test building a basic query."""
        builder = ClickHouseQueryBuilder()
        query, params = (
            builder
            .select(['slot', 'proposer_index'])
            .from_table('canonical_beacon_block')
            .where('slot', '=', 1000)
            .build()
        )
        
        assert 'SELECT slot, proposer_index' in query
        assert 'FROM canonical_beacon_block FINAL' in query
        assert 'WHERE slot = %(param_0)s' in query
        assert params['param_0'] == 1000
        
    def test_table_validation(self):
        """Test that only allowed tables are accepted."""
        builder = ClickHouseQueryBuilder()
        
        # Valid table
        builder.from_table('canonical_beacon_block')
        
        # Invalid table
        with pytest.raises(ValueError, match="not allowed"):
            builder.from_table('invalid_table')
            
        # SQL injection attempt
        with pytest.raises(ValueError, match="not allowed"):
            builder.from_table('canonical_beacon_block; DROP TABLE users;')
            
    def test_operator_validation(self):
        """Test operator validation."""
        builder = ClickHouseQueryBuilder()
        
        # Valid operators
        valid_ops = ['=', '!=', '<', '>', '<=', '>=', 'IN', 'NOT IN', 'LIKE']
        for op in valid_ops:
            builder.reset()
            if op in ['IN', 'NOT IN']:
                builder.where('slot', op, [1, 2, 3])
            else:
                builder.where('slot', op, 1)
                
        # Invalid operators
        with pytest.raises(ValueError, match="not allowed"):
            builder.where('slot', 'UNION SELECT', 1)
            
    def test_in_operator_handling(self):
        """Test IN and NOT IN operator handling."""
        builder = ClickHouseQueryBuilder()
        
        query, params = (
            builder
            .select('*')
            .from_table('canonical_beacon_block')
            .where('slot', 'IN', [100, 200, 300])
            .build()
        )
        
        assert 'slot IN (%(param_0_0)s, %(param_0_1)s, %(param_0_2)s)' in query
        assert params['param_0_0'] == 100
        assert params['param_0_1'] == 200
        assert params['param_0_2'] == 300
        
    def test_between_handling(self):
        """Test BETWEEN clause."""
        builder = ClickHouseQueryBuilder()
        
        query, params = (
            builder
            .select('*')
            .from_table('canonical_beacon_block')
            .where_between('slot', 1000, 2000)
            .build()
        )
        
        assert 'slot BETWEEN %(param_0)s AND %(param_1)s' in query
        assert params['param_0'] == 1000
        assert params['param_1'] == 2000
        
    def test_complex_query(self):
        """Test building a complex query."""
        builder = ClickHouseQueryBuilder()
        
        query, params = (
            builder
            .select(['slot', 'COUNT(*) as count'])
            .from_table('canonical_beacon_block', use_final=False)
            .where('meta_network_name', '=', 'mainnet')
            .where_between('slot', 1000, 2000)
            .where('proposer_index', '>', 0)
            .group_by(['slot'])
            .order_by('count', desc=True)
            .limit(10)
            .build()
        )
        
        # Check all components are present
        assert 'SELECT slot, COUNT(*) as count' in query
        assert 'FROM canonical_beacon_block' in query
        assert 'FINAL' not in query  # use_final=False
        assert 'WHERE' in query
        assert 'GROUP BY slot' in query
        assert 'ORDER BY count DESC' in query
        assert 'LIMIT 10' in query
        
        # Check parameters
        assert params['param_0'] == "'mainnet'"  # String values are now quoted
        assert params['param_1'] == 1000
        assert params['param_2'] == 2000
        assert params['param_3'] == 0
        
    def test_where_raw_validation(self):
        """Test raw WHERE clause validation."""
        builder = ClickHouseQueryBuilder()
        
        # Safe raw conditions
        builder.where_raw('slot % 32 = 0')  # OK
        
        # Dangerous patterns should be rejected
        dangerous_patterns = [
            'slot = 1; DROP TABLE users;',
            'slot = 1 -- comment',
            'slot = 1 /* comment */',
            'slot = 1; DELETE FROM data;',
            'slot = 1 UNION SELECT password FROM users'
        ]
        
        for pattern in dangerous_patterns:
            builder.reset()
            with pytest.raises(ValueError, match="unsafe SQL"):
                builder.where_raw(pattern)
                
    def test_query_reset(self):
        """Test that reset clears the builder state."""
        builder = ClickHouseQueryBuilder()
        
        # Build a query
        builder.select('*').from_table('canonical_beacon_block').where('slot', '=', 1000)
        
        # Reset
        builder.reset()
        
        # Should fail without required components
        with pytest.raises(ValueError, match="SELECT columns not specified"):
            builder.build()


class TestClickHouseClient:
    """Test the synchronous ClickHouse client."""
    
    @pytest.fixture
    def config(self):
        """Create test configuration."""
        return ClickhouseConfig(
            url="https://clickhouse.example.com",
            user="testuser",
            password=SecretStr("testpass"),
            database="default",
            timeout=30,
            pool_size=5
        )
        
    @pytest.fixture
    def client(self, config):
        """Create test client."""
        return ClickHouseClient(config)
        
    def test_session_creation(self, config):
        """Test that session is created with proper configuration."""
        with patch('requests.Session') as mock_session_class:
            mock_session = Mock()
            mock_session_class.return_value = mock_session
            
            # Create client which should initialize session
            client = ClickHouseClient(config)
            
            # Check session was created
            mock_session_class.assert_called_once()
            
            # Check auth was set
            assert client._session.auth.username == 'testuser'
            assert client._session.auth.password == 'testpass'
            
    def test_execute_query_json(self, client):
        """Test executing a query with JSON response."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '{"slot":1000,"proposer_index":123}\n{"slot":1001,"proposer_index":456}'
        mock_response.raise_for_status = Mock()
        
        with patch.object(client, '_session') as mock_session:
            mock_session.get.return_value = mock_response
            
            result = client.execute_query("SELECT * FROM test")
            
            assert len(result) == 2
            assert result[0]['slot'] == 1000
            assert result[1]['proposer_index'] == 456
            
    def test_execute_query_df(self, client):
        """Test executing a query returning DataFrame."""
        tsv_data = "slot\tproposer_index\n1000\t123\n1001\t456"
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = tsv_data
        mock_response.raise_for_status = Mock()
        
        with patch.object(client, '_session') as mock_session:
            mock_session.get.return_value = mock_response
            
            df = client.execute_query_df("SELECT * FROM test")
            
            assert isinstance(df, pd.DataFrame)
            assert len(df) == 2
            assert list(df.columns) == ['slot', 'proposer_index']
            assert df.iloc[0]['slot'] == 1000
            
    def test_execute_query_with_params(self, client):
        """Test query execution with parameters."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '{"slot":1000}'
        mock_response.raise_for_status = Mock()
        
        with patch.object(client, '_session') as mock_session:
            mock_session.get.return_value = mock_response
            
            query = "SELECT * FROM blocks WHERE slot = %(slot)s"
            params = {'slot': 1000}
            
            result = client.execute_query(query, params)
            
            # Check that params were interpolated
            call_args = mock_session.get.call_args
            actual_query = call_args[1]['params']['query']
            assert '1000' in actual_query
            assert '%(slot)s' not in actual_query
            
    def test_empty_response_handling(self, client):
        """Test handling of empty responses."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = ''
        mock_response.raise_for_status = Mock()
        
        with patch.object(client, '_session') as mock_session:
            mock_session.get.return_value = mock_response
            
            # JSON format should return empty list
            result = client.execute_query("SELECT * FROM test")
            assert result == []
            
            # DataFrame format should return empty DataFrame
            df = client.execute_query_df("SELECT * FROM test")
            assert isinstance(df, pd.DataFrame)
            assert len(df) == 0
            
    def test_error_handling(self, client):
        """Test error response handling."""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.raise_for_status = Mock(side_effect=requests.HTTPError("500 Server Error"))
        mock_response.text = "Error response"
        
        with patch.object(client, '_session') as mock_session:
            mock_session.get.return_value = mock_response
            
            with pytest.raises(requests.HTTPError, match="500 Server Error"):
                client.execute_query("SELECT * FROM test")
                
    def test_connection_test(self, client):
        """Test connection testing."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '{"test":1}'
        mock_response.raise_for_status = Mock()
        
        with patch.object(client, '_session') as mock_session:
            mock_session.get.return_value = mock_response
            
            result = client.test_connection()
            assert result is True
            
    def test_get_table_columns(self, client):
        """Test fetching table columns."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '{"name":"slot"}\n{"name":"proposer_index"}\n{"name":"block_root"}'
        mock_response.raise_for_status = Mock()
        
        with patch.object(client, '_session') as mock_session:
            mock_session.get.return_value = mock_response
            
            columns = client.get_table_columns('canonical_beacon_block')
            
            assert columns == ['slot', 'proposer_index', 'block_root']
            
            # Check query includes proper parameters
            call_args = mock_session.get.call_args
            query = call_args[1]['params']['query']
            assert 'system.columns' in query
            assert 'canonical_beacon_block' in query
            
    def test_session_reuse(self, client):
        """Test that sessions are properly reused."""
        # The session is created once in __init__ and reused
        session = client._session
        
        # Make multiple requests to ensure session is reused
        with patch.object(client._session, 'get') as mock_get:
            mock_response = Mock()
            mock_response.text = '{\"test\": 1}'
            mock_response.raise_for_status = Mock()
            mock_get.return_value = mock_response
            
            client.execute_query("SELECT 1")
            client.execute_query("SELECT 2")
            client.execute_query("SELECT 3")
            
        # Session should still be the same instance
        assert client._session is session
            
    def test_close(self, client):
        """Test client closing."""
        mock_session = Mock()
        client._session = mock_session
        
        client.close()
        
        mock_session.close.assert_called_once()
        assert client._session is None
        
    def test_streaming_query(self, client):
        """Test streaming query execution."""
        # Create mock response that yields data in lines
        def mock_lines_iter():
            return ['{"slot":1000}', '{"slot":1001}', '{"slot":1002}']
            
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.iter_lines.return_value = mock_lines_iter()
        mock_response.raise_for_status = Mock()
        
        with patch.object(client, '_session') as mock_session:
            mock_session.get.return_value = mock_response
            
            chunks = []
            for chunk in client.execute_query_stream("SELECT * FROM test", chunk_size=2):
                chunks.append(chunk)
                
            assert len(chunks) == 2
            assert len(chunks[0]) == 2  # First chunk has 2 items
            assert len(chunks[1]) == 1  # Second chunk has 1 item
            assert chunks[0][0]['slot'] == 1000
            assert chunks[1][0]['slot'] == 1002