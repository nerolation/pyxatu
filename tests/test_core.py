import unittest
from unittest.mock import patch, MagicMock
from pyxatu.core import PyXatu
import os
import pandas as pd
from pathlib import Path


class TestPyXatu(unittest.TestCase):

    def setUp(self):
        # Patch the ClickhouseClient and DataRetriever
        self.patcher_client = patch('pyxatu.core.ClickhouseClient', autospec=True)
        self.patcher_retriever = patch('pyxatu.core.DataRetriever', autospec=True)

        # Start the patchers
        self.mock_client = self.patcher_client.start()
        self.mock_retriever = self.patcher_retriever.start()

        # Mock instances
        self.mock_client_instance = self.mock_client.return_value
        self.mock_retriever_instance = self.mock_retriever.return_value

        # Instantiate PyXatu with mocks
        self.pyxatu = PyXatu(config_path=None, use_env_variables=True)

    def tearDown(self):
        self.patcher_client.stop()
        self.patcher_retriever.stop()
    
    @patch.dict(os.environ, {
        "CLICKHOUSE_USER": "test_user",
        "CLICKHOUSE_PASSWORD": "test_password",
        "CLICKHOUSE_URL": "http://test-url"
    })
    def test_init_with_env_variables(self):
        # Reset mock before asserting to remove previous calls
        self.mock_client.reset_mock()

        # Initialize PyXatu with environment variables
        pyxatu_instance = PyXatu(config_path=None, use_env_variables=True)

        # Now assert the ClickhouseClient was called with the correct parameters
        self.mock_client.assert_called_once_with(
            "http://test-url", "test_user", "test_password"
        )

        # Ensure that DataRetriever is initialized
        self.assertIsNotNone(pyxatu_instance.data_retriever)

    @patch('pyxatu.core.PyXatu.read_clickhouse_config_locally', return_value=("http://test-url", "user", "pass"))
    def test_init_with_config_file(self, mock_read_config):
        
        default_path = os.path.join(Path.home(), '.pyxatu_config.json')
        
        # Reset the mock before asserting
        self.mock_client.reset_mock()

        # Initialize PyXatu with config file
        pyxatu_instance = PyXatu(config_path=default_path, use_env_variables=False)

        # Ensure the config file method was called
        mock_read_config.assert_called_once()

        # Now assert the ClickhouseClient was called with the config file values
        self.mock_client.assert_called_once_with(
            "http://test-url", "user", "pass"
        )

    def test_get_blockevent(self):
        # Set up the return value of get_data from DataRetriever
        self.mock_retriever_instance.get_data.return_value = 'mock_result'

        result = self.pyxatu.get_blockevent(slot=12345)

        # Assert that DataRetriever.get_data was called with correct arguments
        self.mock_retriever_instance.get_data.assert_called_once_with(
            'beacon_api_eth_v1_events_block',
            slot=12345,
        )
        self.assertEqual(result, 'mock_result')

    def test_get_reorgs(self):
        # Mock DataRetriever.get_data for reorgs and canonical slots
        self.mock_retriever_instance.get_data.side_effect = [
            pd.DataFrame({'reorged_slot': [9000000, 9000001]}),  # Mock reorgs data
            pd.DataFrame({'slot': [8999999, 9000000, 9000001, 9000002, 9000003]})  
        ]

        # Call the method under test
        result = self.pyxatu.get_reorgs(slot=[9000000, 9000001])

        # Ensure that get_data was called twice: once for reorgs and once for canonical slots
        self.assertEqual(self.mock_retriever_instance.get_data.call_count, 2)

        # Verify that the result contains the correct reorg slots
        self.assertEqual(result.to_string(), pd.DataFrame([], columns=["slot"]).to_string())


    def test_get_reorgs_no_reorgs(self):
        # Mock DataRetriever.get_data for reorgs (no reorgs in the data)
        self.mock_retriever_instance.get_data.side_effect = [
            pd.DataFrame({'reorged_slot': []}),  # No reorgs
            pd.DataFrame({'slot': [8999999, 9000000, 9000001, 9000002, 9000003]})  # Mock canonical slots data
        ]

        # Call the method under test with slots that do not have reorgs
        result = self.pyxatu.get_reorgs(slot=[9000000, 9000001])

        # Ensure that the result is an empty list, as there are no reorgs
        self.assertEqual(result.to_string(), pd.DataFrame([], columns=["slot"]).to_string())

    def test_get_reorgs_missing_canonical_slots(self):
        # Mock DataRetriever.get_data for reorgs and canonical slots
        # Simulate missing canonical slots
        self.mock_retriever_instance.get_data.side_effect = [
            pd.DataFrame({'reorged_slot': [9000000, 9000001]}),  # Mock reorgs data
            pd.DataFrame({'slot': [8999999, 9000002, 9000003]})  # Missing canonical slots 9000000 and 9000001
        ]

        # Call the method under test with slots
        result = self.pyxatu.get_reorgs(slot=[9000000, 9000001])

        # Ensure that the correct reorg slots are identified
        self.assertEqual(result.to_string(), pd.DataFrame([9000000, 9000001], columns=["slot"]).to_string())

    def test_execute_query(self):
        # Set up the return value of execute_query from ClickhouseClient
        self.mock_client_instance.execute_query.return_value = 'mock_query_result'

        result = self.pyxatu.execute_query("SELECT * FROM test_table")

        # Assert that ClickhouseClient.execute_query was called with correct arguments
        self.mock_client_instance.execute_query.assert_called_once_with(
            "SELECT * FROM test_table", "*"
        )
        self.assertEqual(result, 'mock_query_result')

if __name__ == '__main__':
    unittest.main()
