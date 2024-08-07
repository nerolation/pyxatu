import os
import json
import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
from io import StringIO
import time
from pathlib import Path
import shutil


class PyXatu:
    def __init__(self, config_path=None):
        self.user_config_path = os.path.join(Path.home(), '.pyxatu_config.json')
        
        if config_path is None:
            config_path = self.user_config_path

        self.config_path = config_path
        self.clickhouse_user, self.clickhouse_password, self.clickhouse_url = self.read_clickhouse_config()

    def read_clickhouse_config(self):
        with open(self.config_path, 'r') as file:
            config = json.load(file)
        clickhouse_user = config.get("CLICKHOUSE_USER")
        clickhouse_password = config.get("CLICKHOUSE_PASSWORD")
        url = config.get("CLICKHOUSE_URL")
        print("Clickhouse configs set")
        return clickhouse_user, clickhouse_password, url

    def request_query(self, _query, columns=None, max_retries=5):
        attempt = 0
        while attempt < max_retries:
            try:
                if attempt > 0:
                    print(f"Attempt {attempt + 1} of {max_retries}...")
                response = requests.get(
                    self.clickhouse_url,
                    params={'query': _query},
                    auth=HTTPBasicAuth(self.clickhouse_user, self.clickhouse_password),
                    timeout=1500
                )

                if response.status_code == 200:
                    data = StringIO(response.text)
                    df = pd.read_csv(data, sep='\t', header=None)
                    if columns:
                        df.columns = columns
                    return df
                else:
                    print(response.text)
                    print(f"Query failed with status code {response.status_code}. Retrying...")

            except requests.RequestException as e:
                print(f"An error occurred: {e}. Retrying...")

            attempt += 1
            wait_time = 2 ** attempt
            print(f"Waiting for {wait_time} seconds before next attempt...")
            time.sleep(wait_time)

        print("Max retries reached. Failed to retrieve data.")
        return None
    
    def __str__(self):
        return f"PyXatu(config_path={self.config_path}, user={self.clickhouse_user}, url={self.clickhouse_url})"

    def __repr__(self):
        return self.__str__()