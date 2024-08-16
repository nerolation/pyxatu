import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
import os
from pyxatu.retriever import DataRetriever

class TestDataRetriever(unittest.TestCase):

    def setUp(self):
        self.client = MagicMock()  # Mock the client
        self.tables = {
            "valid_data_type": "test_table"
        }
        self.retriever = DataRetriever(client=self.client, tables=self.tables)

    def test_get_data_success(self):
        # Mock fetch_data to return a valid DataFrame
        mock_result = pd.DataFrame({'col1': [1, 2], 'col2': ['A', 'B']})
        self.client.fetch_data.return_value = mock_result

        result = self.retriever.get_data(
            data_type="valid_data_type",  # Ensure this matches the keys in `tables`
            slot=1234,
            columns=["col1", "col2"]
        )

        # Use keyword arguments to match the actual call in `get_data`
        self.client.fetch_data.assert_called_once_with(
            table="test_table", 
            slot=1234, 
            columns=["col1", "col2"], 
            where=None, 
            time_interval=None, 
            network="mainnet", 
            orderby=None, 
            final_condition=None, 
            limit=None
        )

        # Ensure the result is as expected
        pd.testing.assert_frame_equal(result, mock_result)

    def test_get_data_invalid_data_type(self):
        # Test for invalid data type
        with self.assertRaises(ValueError) as context:
            self.retriever.get_data(data_type="invalid_data_type")
        
        self.assertIn("Data type 'invalid_data_type' is not valid", str(context.exception))

    @patch('pyxatu.retriever.os.path.exists', return_value=True)
    @patch('pyxatu.retriever.os.path.isdir', return_value=False)
    @patch('pyxatu.retriever.os.mkdir')
    @patch('pyxatu.retriever.time.time', return_value=1234567890)
    @patch('pyxatu.retriever.pd.DataFrame.to_parquet')
    def test_store_result_to_disk(self, mock_to_parquet, mock_time, mock_mkdir, mock_isdir, mock_exists):
        # Mock DataFrame
        mock_result = pd.DataFrame({'col1': [1, 2], 'col2': ['A', 'B']})

        # Test storing data when directory doesn't exist
        custom_dir = './test_output/output.parquet'
        
        self.retriever.store_result_to_disk(mock_result, custom_dir)
        
        # Assert mkdir was called to create directory
        mock_mkdir.assert_called_once_with('test_output')
        
        # Assert result is saved to the file
        expected_file_path = './test_output/output_1234567890.parquet'
        mock_to_parquet.assert_called_once_with(expected_file_path, index=True)

    @patch('pyxatu.retriever.os.path.exists',  side_effect=[True, False])  # Mock os.path.exists
    @patch('pyxatu.retriever.os.path.isdir', return_value=False)  # Mock os.path.isdir to return False
    @patch('pyxatu.retriever.os.mkdir')  # Mock os.mkdir
    @patch('pyxatu.retriever.pd.DataFrame.to_parquet')  # Mock to_parquet
    @patch('pyxatu.retriever.time.time', return_value=1234567890)  # Mock time to return a fixed timestamp
    def test_store_result_to_disk_without_existing_file(self, mock_time, mock_to_parquet, mock_mkdir, mock_isdir, mock_exists):
        # Mock DataFrame
        mock_result = pd.DataFrame({'col1': [1, 2], 'col2': ['A', 'B']})

        # Test storing data when the file doesn't exist
        custom_dir = 'test_output/output.parquet'

        self.retriever.store_result_to_disk(mock_result, custom_dir)

        # Assert mkdir was called to create the directory
        mock_mkdir.assert_called_once_with('test_output')  # Ensure mkdir was called with the correct directory

        # Assert the result is saved to the new file with a timestamp
        expected_file_path = 'test_output/output_1234567890.parquet'
        mock_to_parquet.assert_called_once_with(expected_file_path, index=True)
        
if __name__ == '__main__':
    unittest.main()
