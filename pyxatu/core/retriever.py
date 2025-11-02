import os
import time
from typing import Optional, Any

import pandas as pd


class DataRetriever:
    """
    Data retrieval layer for PyXatu.
    
    Handles table validation, data fetching, and optional storage to disk.
    """
    
    def __init__(self, client, tables):
        self.client = client
        self.tables = tables

    def get_data(self, **kwargs) -> Optional[pd.DataFrame]:
        """
        Retrieve data from specified table with optional storage to disk.
        
        Args:
            **kwargs: Query parameters including data_table, columns, etc.
            
        Returns:
            DataFrame with query results or None if stored to disk
            
        Raises:
            ValueError: If data_table is invalid
        """
        self._normalize_columns(kwargs)
        self._validate_table(kwargs["data_table"])
        
        result = self.client.fetch_data(**kwargs)
        
        if result is not None:
            if kwargs.get("store_result_in_parquet", False):
                self.store_result_to_disk(result, kwargs.get("custom_data_dir"))
                return None
            return result
        
        return None
    
    def _normalize_columns(self, kwargs: dict) -> None:
        """Convert list of columns to comma-separated string."""
        if ("columns" in kwargs and 
            kwargs["columns"] != "*" and 
            isinstance(kwargs["columns"], list)):
            kwargs["columns"] = ",".join(kwargs["columns"])
    
    def _validate_table(self, table_name: str) -> None:
        """Validate that the table name exists in available tables."""
        if table_name not in self.tables:
            available_tables = ", ".join(self.tables.keys())
            raise ValueError(
                f"Data table '{table_name}' is not valid. "
                f"Please use one of the following: {available_tables}"
            )

        
        
    def store_result_to_disk(self, result: pd.DataFrame, custom_data_dir: Optional[str] = None) -> None:
        """
        Store DataFrame result to disk as a Parquet file.
        
        Args:
            result: DataFrame to store
            custom_data_dir: Optional custom directory path. Defaults to './output_data/output.parquet'
        """
        if custom_data_dir is None:
            custom_data_dir = './output_data/output.parquet'
        
        # Create directories if they don't exist
        self._create_directories(custom_data_dir)
        
        # Handle existing files by adding timestamp
        final_path = self._resolve_file_path(custom_data_dir)
        
        # Save the result
        result.to_parquet(final_path, index=True)
        print(f"File saved at '{final_path}'")
    
    def _create_directories(self, file_path: str) -> None:
        """Create directories in the file path if they don't exist."""
        directory = os.path.dirname(file_path)
        if directory and not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)
            print(f"Created directory: {directory}")
    
    def _resolve_file_path(self, file_path: str) -> str:
        """Resolve file path conflicts by adding timestamp if file exists."""
        if os.path.exists(file_path):
            timestamp = int(time.time())
            name, ext = os.path.splitext(file_path)
            new_path = f"{name}_{timestamp}{ext}"
            print(f"File '{file_path}' already exists. Using: '{new_path}'")
            return new_path
        return file_path
