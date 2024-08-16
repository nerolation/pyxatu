import unittest
from unittest.mock import MagicMock, patch
import time
from pyxatu.utils import retry_on_failure

class TestRetryOnFailure(unittest.TestCase):
    
    @patch('pyxatu.utils.logging')
    def test_successful_execution(self, mock_logging):
        @retry_on_failure()
        def mock_function():
            return "Success"
        
        self.assertEqual(mock_function(), "Success")
    
    @patch('pyxatu.utils.logging')
    def test_retry_on_failure(self, mock_logging):
        mock_func = MagicMock(side_effect=[Exception("Fail"), "Success"])
        
        @retry_on_failure(max_retries=2)
        def mock_function():
            return mock_func()
        
        self.assertEqual(mock_function(), "Success")
        self.assertEqual(mock_func.call_count, 2)
    
    @patch('time.sleep', return_value=None)
    @patch('pyxatu.utils.logging')
    def test_retry_max_attempts(self, mock_logging, mock_sleep):
        mock_func = MagicMock(side_effect=Exception("Fail"))
        
        @retry_on_failure(max_retries=3)
        def mock_function():
            return mock_func()
        
        self.assertIsNone(mock_function())
        self.assertEqual(mock_func.call_count, 3)

if __name__ == '__main__':
    unittest.main()