import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import os
import json
from pyxatu.client import ClickhouseClient


CONFIG_PATH = os.path.expanduser("~/.pyxatu_config.json")

print(CONFIG_PATH)
with open(CONFIG_PATH, 'r') as file:
    config = json.load(file)
    clickhouse_user = config.get("CLICKHOUSE_USER", "default_user")
    clickhouse_password = config.get("CLICKHOUSE_PASSWORD", "default_password")
    url = config.get("CLICKHOUSE_URL", "http://localhost")
        
os.environ["CLICKHOUSE_URL"] = url
os.environ["CLICKHOUSE_USER"] = clickhouse_user
os.environ["CLICKHOUSE_PASSWORD"] = clickhouse_password


class TestClickhouseClient(unittest.TestCase):
    
    @patch.dict(os.environ)
    def setUp(self):
        # Initialize the ClickhouseClient with the environment variables
        self.client = ClickhouseClient(
            url=os.getenv("CLICKHOUSE_URL"),
            user=os.getenv("CLICKHOUSE_USER"),
            password=os.getenv("CLICKHOUSE_PASSWORD")
        )

    @patch('requests.get')
    def test_execute_query_success(self, mock_get):
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_response.text = "value1\tvalue2"
        mock_get.return_value = mock_response
        
        result = self.client.execute_query("SELECT * FROM test_table")
        
        expected_df = pd.DataFrame([["value1", "value2"]], columns=[0, 1])
        pd.testing.assert_frame_equal(result, expected_df)

    @patch('requests.get')
    @patch('pyxatu.utils.logging')
    def test_execute_query_failure(self, mock_logging, mock_get):
        mock_get.side_effect = Exception("Request Failed")   
        result = self.client.execute_query("SELECT * FROM test_table")   
        self.assertIsNone(result)

    @patch('pyxatu.client.ClickhouseClient._build_query')
    @patch('pyxatu.client.ClickhouseClient.execute_query')
    def test_fetch_data(self, mock_execute_query, mock_build_query):
        mock_build_query.return_value = 'SELECT column1 FROM some_table'
        mock_execute_query.return_value = pd.DataFrame({'column1': [1, 2, 3]})

        # Call the real fetch_data method and test the result
        result = self.client.fetch_data(
            table='test_table', 
            slot=1234, 
            columns=['col1', 'col2'], 
            where=None, 
            time_interval=None, 
            network='mainnet', 
            orderby=None, 
            final_condition=None, 
            limit=None
        )

        # Assert that mock_build_query and mock_execute_query were called
        mock_build_query.assert_called_once()
        mock_execute_query.assert_called_once_with('SELECT column1 FROM some_table', ['col1', 'col2'])

        # Verify the result is as expected
        self.assertIsInstance(result, pd.DataFrame)
        self.assertFalse(result.empty)

    @patch('requests.get')
    def test_execute_query_with_slot(self, mock_get):
        # Mock the response
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_response.text = "9700000\t2024-08-09 17:20:23"
        mock_get.return_value = mock_response

        self.setUp()

        expected_query = "SELECT DISTINCT slot, slot_start_date_time FROM default.canonical_beacon_block WHERE slot = 9700000 AND slot_start_date_time = '2024-08-09 17:20:23'"

        result = self.client.execute_query(expected_query)
        mock_get.assert_called_once_with(
            self.client.url,
            params={'query': expected_query},
            auth=self.client.auth,
            timeout=1500
        )

        # Verify the result as expected
        expected_df = pd.DataFrame([[9700000, '2024-08-09 17:20:23']], columns=["slot", "slot_start_date_time"])
        
        pd.testing.assert_frame_equal(result, expected_df)


if __name__ == '__main__':
    unittest.main()
