import requests
from bs4 import BeautifulSoup
import pandas as pd

class DocsScraper:
    
    def __init__(self):
        # Dictionary to store table information for all tables
        self.tables = dict()
        self.scrape_all_tables()
        
    def _get_table_info(self, soup, table_header):
        """
        Extracts table information given the table header element.
        """
        table = table_header.find_next('table')
        if not table:
            return None
        
        columns = []
        types = []
        descriptions = []

        rows = table.find_all('tr')

        for row in rows[1:]:  # Skip the header row
            cells = row.find_all('td')
            if len(cells) == 3:
                columns.append(cells[0].get_text(strip=True))
                types.append(cells[1].get_text(strip=True))
                descriptions.append(cells[2].get_text(strip=True))

        # Creating a pandas DataFrame
        data = pd.DataFrame({
            'Column': columns,
            'Type': types,
            'Description': descriptions
        })

        return data
    
    def scrape_all_tables(self):
        """
        Scrapes all tables from the given URL and stores their dataframes in a dictionary.
        """
        url = "https://ethpandaops.io/data/xatu/schema/"
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')

        # Find all sections containing tables
        headers = soup.find_all('h2', class_='relative group')
        for header in headers:
            table_name = header.get_text(strip=True).replace("#", "").split()[0]
            if table_name.lower() != "tables":  # Skip the general 'Tables' header
                table_data = self._get_table_info(soup, header)
                if table_data is not None:
                    self.tables[table_name] = table_data

    def get_table_info(self, table_name: str = None):
        """
        Returns the table info as a pandas DataFrame for the specified table.
        If the table_name is None, it returns a list of all available tables.
        """
        if table_name is None:
            return list(self.tables.keys())  # Return all available table names

        if table_name in self.tables:
            return self.tables[table_name]
        
        print(f"Table {table_name} not found.")
        return None