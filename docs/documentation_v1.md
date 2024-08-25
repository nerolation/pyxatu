## Functions Documentation



 PyXatu accepts the following parameters for initialization:

- **config_path**: Path to the configuration file.
- **use_env_variables**: If set to `True`, Xatu configurations are read from environment variables.
- **log_level**: The log level for the application, default is `INFO`.
- **relay**: Optional parameter for mevboost relay configuration (can be used for mevboost related functions).

---

### `get_docs`

```
def get_docs(self, table_name: str = None):
```
#### Description:
This method retrieves the underlying table as a dataframe for the specified function or table. This is useful to check which columns can be parsed.

#### Parameters:
- **table_name**: `str`  
  The specific function or table name to retrieve the documentation for.

### `get_blockevent`

```
def get_blockevent(self, slot: Optional[int] = None, columns: Optional[str] = "*", where: Optional[str] = None, 
                       time_interval: Optional[str] = None, network: str = "mainnet", max_retries: int = 1, 
                       groupby: str = None, orderby: Optional[str] = None, final_condition: Optional[str] = None, limit: int = None,
                       store_result_in_parquet: bool = None, custom_data_dir: str = None) -> Any:
```

#### Description:
This method retrieves block event data for a specified slot from the `beacon_api_eth_v1_events_block` table. It offers flexibility to filter the data by different conditions, specify columns, and customize output.

#### Parameters:
- **slot**: `Optional[int]`  
  The specific slot number to retrieve data for. If not provided, retrieves data for all slots.
  
- **columns**: `Optional[str]`, default `"*"`  
  The columns to retrieve. Use `"*"` to retrieve all columns.

- **where**: `Optional[str]`  
  A condition to filter the rows. For example, you can filter rows where certain values meet specific criteria.

- **time_interval**: `Optional[str]`  
  Specify a time range to restrict the data retrieved.

- **network**: `str`, default `"mainnet"`  
  Specify the Ethereum network, e.g., `mainnet` or `testnet`.

- **max_retries**: `int`, default `1`  
  The maximum number of retries in case of failed data retrieval.

- **groupby**: `str`, default `None`  
  The column to group by in the results.

- **orderby**: `Optional[str]`  
  The column by which to order the results.

- **final_condition**: `Optional[str]`  
  Final SQL-like condition to be applied to the query.

- **limit**: `int`, default `None`  
  Limit the number of rows retrieved.

- **store_result_in_parquet**: `bool`, default `None`  
  If set to `True`, stores the result in a Parquet file.

- **custom_data_dir**: `Optional[str]`  
  Directory where the Parquet file will be stored, if applicable.

#### Returns:
- **`Any`**: A dataframe containing block event data for the specified slot.

---

### `get_attestation`

```
def get_attestation(self, slot: Optional[int] = None, columns: Optional[str] = "*", where: Optional[str] = None, 
                 time_interval: Optional[str] = None, network: str = "mainnet", max_retries: int = 1, 
                 groupby: str = None, orderby: Optional[str] = None, final_condition: Optional[str] = None, 
                 limit: int = None, store_result_in_parquet: bool = None, custom_data_dir: str = None) -> Any:
```

#### Description:
This method retrieves attestation data for a specified slot from the `canonical_beacon_elaborated_attestation` table. If the `validators` column is present, the method expands the validator data into individual rows for easier analysis.

#### Parameters:
- **slot**: `Optional[int]`  
  The slot number to retrieve attestation data for.

- **columns**: `Optional[str]`, default `"*"`  
  The columns to retrieve from the table.

- **where**: `Optional[str]`  
  Apply a condition to filter the results.

- **time_interval**: `Optional[str]`  
  Time range to filter the data.

- **network**: `str`, default `"mainnet"`  
  The Ethereum network, typically `mainnet`.

- **max_retries**: `int`, default `1`  
  Number of retry attempts for failed queries.

- **groupby**: `str`, default `None`  
  Group the results by the specified column.

- **orderby**: `Optional[str]`  
  Specify the column by which to order the results.

- **final_condition**: `Optional[str]`  
  Apply a final condition to the query before retrieving results.

- **limit**: `int`, default `None`  
  Limit the number of rows returned.

- **store_result_in_parquet**: `bool`, default `None`  
  Store the results in a Parquet file if set to `True`.

- **custom_data_dir**: `Optional[str]`  
  Directory where Parquet files will be stored.

#### Returns:
- **`Any`**: A dataframe containing the attestation data for the specified slot. If the `validators` column exists, it expands it into individual rows.

---

### `get_attestation_event`

```
def get_attestation_event(self, slot: Optional[int] = None, columns: Optional[str] = "*", 
                where: Optional[str] = None, time_interval: Optional[str] = None, network: str = "mainnet", 
                max_retries: int = 1, groupby: str = None, orderby: Optional[str] = None, 
                final_condition: Optional[str] = None, limit: int = None, 
                store_result_in_parquet: bool = None, custom_data_dir: str = None,
                add_final_keyword_to_query: bool = False) -> Any:
```

#### Description:
This function retrieves attestation event data for a specified slot from the `beacon_api_eth_v1_events_attestation` table.
#### Parameters:
- **slot**: `Optional[int]`  
  Slot number for which to retrieve attestation event data.

- **columns**: `Optional[str]`, default `"*"`  
  The columns to be fetched from the table.

- **where**: `Optional[str]`  
  Filtering condition for data retrieval.

- **time_interval**: `Optional[str]`  
  Specifies the time interval to filter the data.

- **network**: `str`, default `"mainnet"`  
  The Ethereum network from which the data is retrieved.

- **max_retries**: `int`, default `1`  
  Number of retries in case of query failure.

- **groupby**: `str`, default `None`  
  Specifies the column for grouping the data.

- **orderby**: `Optional[str]`  
  Specifies the column for ordering the results.

- **final_condition**: `Optional[str]`  
  A final SQL condition applied before the query execution.

- **limit**: `int`, default `None`  
  Limits the number of returned rows.

- **store_result_in_parquet**: `bool`, default `None`  
  If `True`, stores the output in a Parquet file.

- **custom_data_dir**: `Optional[str]`  
  Specifies the directory for storing the Parquet file, if applicable.

- **add_final_keyword_to_query**: `bool`, default `False`  
  If set to `True`, adds a final keyword to the SQL query before execution.

#### Returns:
- **`Any`**: A dataframe containing the attestation event data for the specified slot.

---

### `get_proposer`

```
def get_proposer(self, slot: Optional[int] = None, columns: Optional[str] = "*", where: Optional[str] = None, 
                      time_interval: Optional[str] = None, network: str = "mainnet", max_retries: int = 1, 
                      groupby: str = None, orderby: Optional[str] = None, final_condition: Optional[str] = None, limit: int = None,
                      store_result_in_parquet: bool = None, custom_data_dir: str = None) -> Any:
```

#### Description:
This method retrieves the proposer duty data for a specified slot from the `canonical_beacon_proposer_duty` table. It can be used to track the validators responsible for proposing blocks at a given slot.

#### Parameters:
- **slot**: `Optional[int]`  
  Slot number for which the proposer data is to be retrieved.

- **columns**: `Optional[str]`, default `"*"`  
  Specifies the columns to retrieve.

- **where**: `Optional[str]`  
  Apply a filter condition on the data.

- **time_interval**: `Optional[str]`  
  Restrict the data by a time range.

- **network**: `str`, default `"mainnet"`  
  The Ethereum network to use (default is `mainnet`).

- **max_retries**: `int`, default `1`  
  The number of retries for failed queries.

- **groupby**: `str`, default `None`  
  Group the results by a specific column.

- **orderby**: `Optional[str]`  
  Specifies the column to order the results by.

- **final_condition**: `Optional[str]`  
  A final condition applied before fetching the data.

- **limit**: `int`, default `None`  
  Limit the number of rows retrieved.

- **store_result_in_parquet**: `bool`, default `None`  
  If `True`, the result is stored in a Parquet file.

- **custom_data_dir**: `Optional[str]`  
  Directory to store the Parquet file if applicable.

#### Returns:
- **`Any`**: A dataframe containing proposer data for the given slot.


### `get_reorgs`

```
def get_reorgs(self, slots: List[int] = None, where: Optional[str] = None, 
                   time_interval: Optional[str] = None, network: str = "mainnet", max_retries: int = 1, 
                   groupby: str = None, orderby: Optional[str] = None, final_condition: Optional[str] = None, 
                   limit: int = None, store_result_in_parquet: bool = None, custom_data_dir: str = None) -> Any:
```

#### Description:
This function retrieves potential chain reorgs (reorganizations) based on slot depths from the `beacon_api_eth_v1_events_chain_reorg` table. It also compares the slots with a list of missed slots to identify potential reorgs.

#### Parameters:
- **slots**: `List[int]`, default `None`  
  A list of slots for which reorgs are checked.

- **where**: `Optional[str]`, default `None`  
  A filtering condition to narrow down the results.

- **time_interval**: `Optional[str]`, default `None`  
  Specifies a time interval for filtering data.

- **network**: `str`, default `"mainnet"`  
  The network to query, typically `mainnet`.

- **max_retries**: `int`, default `1`  
  Maximum number of retries in case of failed queries.

- **groupby**: `str`, default `None`  
  Group results by the specified column.

- **orderby**: `Optional[str]`, default `None`  
  Specify the column for ordering the data.

- **final_condition**: `Optional[str]`, default `None`  
  Final SQL condition applied to the query.

- **limit**: `int`, default `None`  
  Limit the number of rows retrieved.

- **store_result_in_parquet**: `bool`, default `None`  
  If `True`, stores the result in a Parquet file.

- **custom_data_dir**: `Optional[str]`, default `None`  
  Directory where the Parquet file will be stored.

#### Returns:
- **`Any`**: A DataFrame containing the reorg slots that are identified based on slot-depth and comparison with missed slots.

---

### `get_slots`

```
def get_slots(
    self, 
    slot: List[int] = None, 
    columns: Optional[str] = "*", 
    where: Optional[str] = None,            
    time_interval: Optional[str] = None, 
    network: str = "mainnet", 
    max_retries: int = 1,       
    groupby: str = None, 
    orderby: Optional[str] = None, 
    final_condition: Optional[str] = None, 
    limit: int = None,                  
    store_result_in_parquet: bool = None, 
    custom_data_dir: str = None, 
    add_missed: bool = True
) -> Any:
```

#### Description:
This method retrieves canonical Beacon chain block data for the specified slots from the `canonical_beacon_block` table. It can also retrieve missed slots and merge them with the existing data.

#### Parameters:
- **slot**: `List[int]`, default `None`  
  A list of slots for which the data will be retrieved.

- **columns**: `Optional[str]`, default `"*"`  
  Specifies the columns to retrieve from the table.

- **where**: `Optional[str]`, default `None`  
  A condition to filter the rows returned.

- **time_interval**: `Optional[str]`, default `None`  
  A time range filter to restrict the data retrieved.

- **network**: `str`, default `"mainnet"`  
  The network to query (default is `mainnet`).

- **max_retries**: `int`, default `1`  
  Maximum number of retries in case of failed data retrieval.

- **groupby**: `str`, default `None`  
  Group the result by the specified column.

- **orderby**: `Optional[str]`, default `None`  
  Specify the column for ordering the results.

- **final_condition**: `Optional[str]`, default `None`  
  Final SQL-like condition applied to the query.

- **limit**: `int`, default `None`  
  Limit the number of rows retrieved.

- **store_result_in_parquet**: `bool`, default `None`  
  If `True`, stores the result in a Parquet file.

- **custom_data_dir**: `Optional[str]`, default `None`  
  Directory where the Parquet file will be stored.

- **add_missed**: `bool`, default `True`  
  If set to `True`, attempts to find missed slots and include them in the result.

#### Returns:
- **`Any`**: A DataFrame containing data for the specified slots, potentially including missed slots.

---

### `get_missed_slots`

```
def get_missed_slots(
    self, 
    slots: List[int] = None, 
    columns: Optional[str] = "*",  
    where: Optional[str] = None, 
    time_interval: Optional[str] = None,  
    network: str = "mainnet", 
    max_retries: int = 1, 
    groupby: str = None, 
    orderby: Optional[str] = None,
    final_condition: Optional[str] = None, 
    limit: int = None, 
    store_result_in_parquet: bool = None, 
    custom_data_dir: str = None, 
    canonical: Optional = None
) -> Any:
```

#### Description:
This method retrieves the list of missed slots by comparing the available slots from a canonical dataset to the expected slot range.

#### Parameters:
- **slots**: `List[int]`, default `None`  
  List of slots for which the missed slots are checked.

- **columns**: `Optional[str]`, default `"*"`  
  Specifies the columns to retrieve (though typically only the slot column is needed).

- **where**: `Optional[str]`, default `None`  
  A condition to filter the rows.

- **time_interval**: `Optional[str]`, default `None`  
  Time range to restrict the data retrieved.

- **network**: `str`, default `"mainnet"`  
  Network to query (default is `mainnet`).

- **max_retries**: `int`, default `1`  
  Number of retry attempts for failed queries.

- **groupby**: `str`, default `None`  
  Group the result by the specified column.

- **orderby**: `Optional[str]`, default `None`  
  Specify the column to order the results.

- **final_condition**: `Optional[str]`, default `None`  
  Final condition applied before retrieving data.

- **limit**: `int`, default `None`  
  Limit the number of rows retrieved.

- **store_result_in_parquet**: `bool`, default `None`  
  If set to `True`, the result will be stored in a Parquet file.

- **custom_data_dir**: `Optional[str]`, default `None`  
  Directory where the Parquet file will be stored.

- **canonical**: `Optional`, default `None`  
  If provided, this is the canonical data that will be used to check for missed slots.

#### Returns:
- **`Any`**: A set of missed slots for the given range or query.

---

### `get_duties`

```
def get_duties(
    self, 
    slot: Optional[Union[int, List[int]]] = None, 
    columns: Optional[str] = "*",               
    where: Optional[str] = None, 
    time_interval: Optional[str] = None,                
    network: str = "mainnet", 
    max_retries: int = 1, 
    groupby: str = None, 
    orderby: Optional[str] = None,              
    final_condition: Optional[str] = None, 
    limit: int = None,               
    store_result_in_parquet: bool = None, 
    custom_data_dir: str = None
) -> Any:
```

#### Description:
This function retrieves beacon committee data, focusing on validator duties for specified slots. It expands validators into individual rows and provides slot-wise validator duties from the `beacon_api_eth_v1_beacon_committee` table.

#### Parameters:
- **slot**: `Optional[Union[int, List[int]]]`, default `None`  
  Slot or list of slots for which validator duties are retrieved.

- **columns**: `Optional[str]`, default `"*"`  
  Specifies the columns to retrieve from the table.

- **where**: `Optional[str]`, default `None`  
  A condition to filter the rows returned.

- **time_interval**: `Optional[str]`, default `None`  
  Specifies a time interval for filtering the data.

- **network**: `str`, default `"mainnet"`  
  Network to query (default is `mainnet`).

- **max_retries**: `int`, default `1`  
  Maximum number of retries in case of query failure.

- **groupby**: `str`, default `None`  
  Group the result by the specified column.

- **orderby**: `Optional[str]`, default `None`  
  Specify the column for ordering the data.

- **final_condition**: `Optional[str]`, default `None`  
  A final SQL condition applied to the query.

- **limit**: `int`, default `None`  
  Limit the number of rows retrieved.

- **store_result_in_parquet**: `bool`, default `None`  
  If `True`, the result is stored in a Parquet file.

- **custom_data_dir**: `Optional[str]`, default `None`  
  Directory where the Parquet file will be stored.

#### Returns:
- **`Any`**: A DataFrame containing expanded validator duties for the specified slots.


### `get_checkpoints`

```
def get_checkpoints(self, slot: int):
```

#### Description:
This method retrieves the checkpoints (head, target, and source block roots) for a given slot. The checkpoints are derived from previous and current epoch boundaries.

#### Parameters:
- **slot**: `int`  
  The slot for which the checkpoints are retrieved.

#### Returns:
- **`tuple`**: A tuple containing the block roots of the head, target, and source for the specified slot.

---

### `get_elaborated_attestations`

```
def get_elaborated_attestations(
    self, 
    slot: Optional[Union[int, List[int]]] = None, 
    what: str = "source,target,head", 
    columns: Optional[str] = "*", 
    where: Optional[str] = None,     
    time_interval: Optional[str] = None, 
    network: str = "mainnet", 
    max_retries: int = 1,    
    groupby: str = None, 
    orderby: Optional[str] = None, 
    final_condition: Optional[str] = None,   
    limit: int = None, 
    store_result_in_parquet: bool = None, 
    custom_data_dir: str = None, 
    only_status="correct,failed,offline"
) -> Any:
```

#### Description:
This method retrieves and processes attestation data for specified slots, including status information for validators who voted correctly, failed, or were offline. It checks the attestations against the source, target, and head block roots and determines the status of each validator.

#### Parameters:
- **slot**: `Optional[Union[int, List[int]]]`, default `None`  
  Slot or list of slots for which to retrieve attestation data.

- **what**: `str`, default `"source,target,head"`  
  Specifies which vote types to include: `source`, `target`, or `head`.

- **columns**: `Optional[str]`, default `"*"`  
  Specifies the columns to retrieve from the table.

- **where**: `Optional[str]`, default `None`  
  A filtering condition for the data.

- **time_interval**: `Optional[str]`, default `None`  
  Time range for filtering the data.

- **network**: `str`, default `"mainnet"`  
  The Ethereum network to query.

- **max_retries**: `int`, default `1`  
  Maximum number of retries in case of query failure.

- **groupby**: `str`, default `None`  
  Group the results by the specified column.

- **orderby**: `Optional[str]`, default `None`  
  Specify the column for ordering the results.

- **final_condition**: `Optional[str]`, default `None`  
  Final condition applied to the query before retrieving results.

- **limit**: `int`, default `None`  
  Limit the number of rows returned.

- **store_result_in_parquet**: `bool`, default `None`  
  If `True`, stores the result in a Parquet file.

- **custom_data_dir**: `Optional[str]`, default `None`  
  Directory where the Parquet file will be stored.

- **only_status**: `str`, default `"correct,failed,offline"`  
  Specifies the status types to include: `correct`, `failed`, or `offline`.

#### Returns:
- **`Any`**: A DataFrame containing the attestation status (correct, failed, offline) for each validator and vote type (source, target, head).

---

### `get_beacon_block_v2`

```
def get_beacon_block_v2(
    self, 
    slots: List[int] = None, 
    columns: Optional[str] = "*", 
    where: Optional[str] = None, 
    time_interval: Optional[str] = None, 
    network: str = "mainnet", 
    max_retries: int = 1, 
    groupby: str = None, 
    orderby: Optional[str] = None,
    final_condition: Optional[str] = None, 
    limit: int = None, 
    store_result_in_parquet: bool = None, 
    custom_data_dir: str = None
) -> Any:
```

#### Description:
This function retrieves beacon block data (version 2) for specified slots from the `beacon_api_eth_v2_beacon_block` table. It allows for flexible querying through various filters, groupings, and ordering parameters.

#### Parameters:
- **slots**: `List[int]`, default `None`  
  List of slots for which the beacon block data is retrieved.

- **columns**: `Optional[str]`, default `"*"`  
  Specifies the columns to retrieve from the table.

- **where**: `Optional[str]`, default `None`  
  A condition to filter the rows returned.

- **time_interval**: `Optional[str]`, default `None`  
  Specifies a time interval for filtering the data.

- **network**: `str`, default `"mainnet"`  
  The Ethereum network to query (default is `mainnet`).

- **max_retries**: `int`, default `1`  
  Maximum number of retries in case of query failure.

- **groupby**: `str`, default `None`  
  Group the result by the specified column.

- **orderby**: `Optional[str]`, default `None`  
  Specify the column for ordering the results.

- **final_condition**: `Optional[str]`, default `None`  
  A final SQL condition applied before retrieving data.

- **limit**: `int`, default `None`  
  Limit the number of rows retrieved.

- **store_result_in_parquet**: `bool`, default `None`  
  If `True`, stores the result in a Parquet file.

- **custom_data_dir**: `Optional[str]`, default `None`  
  Directory where the Parquet file will be stored.

#### Returns:
- **`Any`**: A DataFrame containing the beacon block data for the specified slots.

### `get_block_size`

```
def get_block_size(
    self, 
    slots: List[int], 
    columns: Optional[str] = "*", 
    where: Optional[str] = None, 
    time_interval: Optional[str] = None, 
    network: str = "mainnet", 
    max_retries: int = 1, 
    groupby: str = None, 
    orderby: Optional[str] = None, 
    final_condition: Optional[str] = None, 
    limit: int = None,      
    store_result_in_parquet: bool = None, 
    custom_data_dir: str = None, 
    add_missed: bool = True
) -> Any:
```

#### Description:
This method retrieves block size data for the specified slots, including compressed block size, total block size, and blob gas usage. It calculates the number of blobs used per block by dividing the `execution_payload_blob_gas_used` by a fixed constant (131072).

#### Parameters:
- **slots**: `List[int]`  
  A list of slots for which block size data is retrieved.

- **columns**: `Optional[str]`, default `"*"`  
  Specifies the columns to retrieve from the table.

- **where**: `Optional[str]`, default `None`  
  A condition to filter the rows returned.

- **time_interval**: `Optional[str]`, default `None`  
  Specifies a time interval for filtering the data.

- **network**: `str`, default `"mainnet"`  
  The Ethereum network to query (default is `mainnet`).

- **max_retries**: `int`, default `1`  
  Maximum number of retries in case of query failure.

- **groupby**: `str`, default `None`  
  Group the result by the specified column.

- **orderby**: `Optional[str]`, default `None`  
  Specify the column for ordering the results.

- **final_condition**: `Optional[str]`, default `None`  
  Final condition applied to the query.

- **limit**: `int`, default `None`  
  Limit the number of rows retrieved.

- **store_result_in_parquet**: `bool`, default `None`  
  If `True`, stores the result in a Parquet file.

- **custom_data_dir**: `Optional[str]`, default `None`  
  Directory where the Parquet file will be stored.

- **add_missed**: `bool`, default `True`  
  If `True`, includes missed slots in the result.

#### Returns:
- **`Any`**: A DataFrame containing block size data, including total block bytes and blob usage.

---

### `get_blob_events`

```
def get_blob_events(
    self, 
    slot: Optional[Union[int, List[int]]] = None, 
    columns: Optional[str] = "*", 
    where: Optional[str] = None, 
    time_interval: Optional[str] = None, 
    network: str = "mainnet", 
    max_retries: int = 1, 
    groupby: str = None, 
    orderby: Optional[str] = None,
    final_condition: Optional[str] = None, 
    limit: int = None, 
    store_result_in_parquet: bool = None, 
    custom_data_dir: str = None
) -> Any:
```

#### Description:
This method retrieves blob sidecar event data for the specified slots from the `beacon_api_eth_v1_events_blob_sidecar` table. It supports filtering by conditions, grouping, and ordering the results.

#### Parameters:
- **slot**: `Optional[Union[int, List[int]]]`, default `None`  
  Slot or list of slots for which blob events are retrieved.

- **columns**: `Optional[str]`, default `"*"`  
  Specifies the columns to retrieve from the table.

- **where**: `Optional[str]`, default `None`  
  A condition to filter the rows returned.

- **time_interval**: `Optional[str]`, default `None`  
  Specifies a time interval for filtering the data.

- **network**: `str`, default `"mainnet"`  
  The Ethereum network to query (default is `mainnet`).

- **max_retries**: `int`, default `1`  
  Maximum number of retries in case of query failure.

- **groupby**: `str`, default `None`  
  Group the result by the specified column.

- **orderby**: `Optional[str]`, default `None`  
  Specify the column for ordering the results.

- **final_condition**: `Optional[str]`, default `None`  
  Final condition applied before retrieving data.

- **limit**: `int`, default `None`  
  Limit the number of rows retrieved.

- **store_result_in_parquet**: `bool`, default `None`  
  If `True`, stores the result in a Parquet file.

- **custom_data_dir**: `Optional[str]`, default `None`  
  Directory where the Parquet file will be stored.

#### Returns:
- **`Any`**: A DataFrame containing blob event data for the specified slots.

---

### `get_blobs`

```
def get_blobs(
    self, 
    slot: Optional[Union[int, List[int]]] = None, 
    columns: Optional[str] = "*", 
    where: Optional[str] = None, 
    time_interval: Optional[str] = None, 
    network: str = "mainnet", 
    max_retries: int = 1, 
    groupby: str = None, 
    orderby: Optional[str] = None,
    final_condition: Optional[str] = None, 
    limit: int = None, 
    store_result_in_parquet: bool = None, 
    custom_data_dir: str = None
) -> Any:
```

#### Description:
This method retrieves canonical Beacon chain blob sidecar data for specified slots from the `canonical_beacon_blob_sidecar` table. It allows flexible filtering, grouping, and ordering options.

#### Parameters:
- **slot**: `Optional[Union[int, List[int]]]`, default `None`  
  Slot or list of slots for which blob sidecar data is retrieved.

- **columns**: `Optional[str]`, default `"*"`  
  Specifies the columns to retrieve from the table.

- **where**: `Optional[str]`, default `None`  
  A condition to filter the rows returned.

- **time_interval**: `Optional[str]`, default `None`  
  Specifies a time interval for filtering the data.

- **network**: `str`, default `"mainnet"`  
  The Ethereum network to query (default is `mainnet`).

- **max_retries**: `int`, default `1`  
  Maximum number of retries in case of query failure.

- **groupby**: `str`, default `None`  
  Group the result by the specified column.

- **orderby**: `Optional[str]`, default `None`  
  Specify the column for ordering the results.

- **final_condition**: `Optional[str]`, default `None`  
  Final SQL-like condition applied before retrieving data.

- **limit**: `int`, default `None`  
  Limit the number of rows returned.

- **store_result_in_parquet**: `bool`, default `None`  
  If `True`, stores the result in a Parquet file.

- **custom_data_dir**: `Optional[str]`, default `None`  
  Directory where the Parquet file will be stored.

#### Returns:
- **`Any`**: A DataFrame containing blob sidecar data for the specified slots.

---

### `get_withdrawals`

```
def get_withdrawals(
    self, 
    slot: Optional[Union[int, List[int]]] = None, 
    columns: Optional[str] = "*", 
    where: Optional[str] = None, 
    time_interval: Optional[str] = None, 
    network: str = "mainnet", 
    max_retries: int = 1, 
    groupby: str = None, 
    orderby: Optional[str] = None,
    final_condition: Optional[str] = None, 
    limit: int = None, 
    store_result_in_parquet: bool = None, 
    custom_data_dir: str = None
) -> Any:
```

#### Description:
This method retrieves withdrawal data for a given slot from the `canonical_beacon_block_withdrawal` table. It supports filtering and grouping of data, with the option to store results in a Parquet file.

#### Parameters:
- **slot**: `Optional[Union[int, List[int]]]`, default `None`  
  Slot or list of slots for which withdrawal data is retrieved.

- **columns**: `Optional[str]`, default `"*"`  
  Specifies the columns to retrieve from the table.

- **where**: `Optional[str]`, default `None`  
  A condition to filter the rows returned.

- **time_interval**: `Optional[str]`, default `None`  
  Specifies a time interval for filtering the data.

- **network**: `str`, default `"mainnet"`  
  The Ethereum network to query (default is `mainnet`).

- **max_retries**: `int`, default `1`  
  Maximum number of retries in case of query failure.

- **groupby**: `str`, default `None`  
  Group the result by the specified column.

- **orderby**: `Optional[str]`, default `None`  
  Specify the column for ordering the results.

- **final_condition**: `Optional[str]`, default `None`  
  Final condition applied to the query before retrieving results.

- **limit**: `int`, default `None`  
  Limit the number of rows retrieved.

- **store_result_in_parquet**: `bool`, default `None`  
  If `True`, stores the result in a Parquet file.

- **custom_data_dir**: `Optional[str]`, default `None`  
  Directory where the Parquet file will be stored.

#### Returns:
- **`Any`**: A DataFrame containing withdrawal data for the specified slots.


 
