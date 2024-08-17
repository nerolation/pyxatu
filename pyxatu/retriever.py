import os
import pandas as pd
from typing import Optional, List, Any
import time

class DataRetriever:
    def __init__(self, client, tables):
        self.client = client
        self.tables = tables

    def get_data(self, data_table: str, slot: Optional[int] = None, columns: str = "*", 
                 where: Optional[str] = None, time_interval: Optional[str] = None, network: str = "mainnet", 
                 orderby: Optional[str] = None, final_condition: Optional[str] = None, limit: int = None,
                 store_result_in_parquet: bool = None, custom_data_dir: str = None) -> Any:
        if columns != "*" and isinstance(columns, list):
            columns = ",".join(columns)
        table = self.tables.get(data_table)
        if not table:
            raise ValueError(
                f"Data table '{data_table}' is not valid. Please use one of the following: {', '.join(self.tables.keys())}"
            )
        result = self.client.fetch_data(
            table=table, 
            slot=slot, 
            columns=columns, 
            where=where, 
            time_interval=time_interval, 
            network=network, 
            orderby=orderby, 
            final_condition=final_condition,
            limit=limit
        )
        if store_result_in_parquet:
            self.store_result_to_disk(result, custom_data_dir)
            return
        else:
            return result
        
    def store_result_to_disk(self, result, custom_data_dir: str = None):
        if custom_data_dir is None:
            custom_data_dir = './output_data/output.parquet'
           
        directories = custom_data_dir.split("/")[:-1]
        for i in range(len(directories)):
            directory = directories[i]
            if directory == ".":
                continue
            if not os.path.isdir(directory):
                os.mkdir(directory)
            print(f"Created directory `{directory}`")

        if os.path.exists(custom_data_dir):
            timestamp = int(time.time())
            new_file_path = f"{os.path.splitext(custom_data_dir)[0]}_{timestamp}{os.path.splitext(custom_data_dir)[1]}"
            
            print(f"File '{custom_data_dir}' already exists. Using a new location: '{new_file_path}'")
            custom_data_dir = new_file_path  # Automatically suggest the new filename
        
        # Save the result to the specified or new file
        result.to_parquet(custom_data_dir, index=True)
        print(f"File saved at '{custom_data_dir}'")
