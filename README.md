# PyXatu Configuration

<img src="https://github.com/nerolation/pyxatu/blob/main/logo/pyxatu.png?raw=true" width="400">



Pyxatu is a Python package for querying data from the [Xatu](https://ethpandaops.io/data/xatu/schema/) database and was developed to make access to Ethereum data as easy as possible without sacrificing on a rich set of features.

---

**Pyxatu comes out of the box with:**
* High level access to Xatu (Ethereum EL + CL)
* Mevboost Data API Interface
* Validator label mapping

  ## Install

  ```console
   pip install pyxatu
   pyxatu setup
   ```


`xatu setup` copies the default configuration file to your HOME directory as `.pyxatu_config.json`. Update this file with your actual Xatu credentials. Alternatively, you can use environment variables.
If you don't have credentials yet, please get in contact with someone from [EthPandaOps](https://ethpandaops.io/).

## Example Usage


As a first step, we always want to initialize PyXatu. 
**First, let's initialize Pyxatu**:
   ```python
   import pyxatu

   xatu = pyxatu.PyXatu()
   ```

We use `xatu` to execute our first query: We want to get the *block number* and the *gas used* of a block in a certain slot:

   ```python
   df = xatu.get_slots(
      slot = [9000000, 9000010],
      columns="slot, execution_payload_block_number, execution_payload_gas_used",
      orderby="slot"
   )

   print(df)
   ```

|    slot |   execution_payload_block_number |   execution_payload_gas_used |
|--------:|---------------------------------:|-----------------------------:|
| 9000000 |                         19796604 |                     18026681 |
| 9000001 |                         19796605 |                     13920219 |
| 9000002 |                         19796606 |                     12498513 |
| 9000003 |                         19796607 |                      2914192 |
| 9000004 |                         19796608 |                     29996743 |
| ...     |                         ...      |                     ...      |



Second, let's say we want to know which validator attested correctly in a certain epoch:

   ```python
   df = xatu.get_elaborated_attestations(slot = 9000000)

   print(df.head().to_markdown(index=False))
   ```


|    slot |   validator | status   | vote_type   |
|--------:|------------:|:---------|:------------|
| 9000000 |           7 | correct  | source      |
| 9000000 |     1179655 | correct  | source      |
| 9000000 |      524305 | correct  | source      |
| 9000000 |          17 | correct  | source      |
| 9000000 |     1179681 | correct  | source      |
| ...     |         ... | ...      | ...         |


Next, we want to get all bids accross all mevboost relays for a specific slot:


```python
df = xatu.mevboost.get_bids(slot = 9096969)

print(df.groupby("relay")["value"].median().reset_index().to_markdown(index=False))
```


| relay                  |       value |
|:-----------------------|------------:|
| aestus                 | 3.92872e+16 |
| bloxroute (max profit) | 3.89533e+16 |
| bloxroute (regulated)  | 3.89042e+16 |
| eden                   | 3.07634e+16 |
| flashbots              | 3.89779e+16 |


and the delivered mevboost payloads:

```python
df = xatu.mevboost.get_payloads(slot = 9814162)

print(df.groupby("relay")["value"].median().reset_index().to_markdown(index=False))
```

| relay                  |       value |
|:-----------------------|------------:|
| bloxroute (max profit) | 1.39261e+16 |
| ultra sound            | 1.39261e+16 |


What if we need a mappling from validator ids to labels:

```python
xatu.validators.mapping[["validator_id", "deposit_address", "label", "lido_node_operator"]]
```

|   validator_id | deposit_address                            | label   | lido_node_operator   |
|---------------:|:-------------------------------------------|:--------|:---------------------|
|        1545106 | 0xfddf38947afb03c621c71b06c9c70bce73f12999 | lido    | Develp GmbH          |
|        1545105 | 0xfddf38947afb03c621c71b06c9c70bce73f12999 | lido    | Develp GmbH          |
|        1546068 | 0xd523794c879d9ec028960a231f866758e405be34 | everstake |                      |
|        1546067 | 0xe3cbd06d7dadb3f4e6557bab7edd924cd1489e8f | mantle    |                      |
|        1546066 | 0xd4039ecc40aeda0582036437cf3ec02845da4c13 | kraken    |                      |
|        1546065 | 0xd4039ecc40aeda0582036437cf3ec02845da4c13 | kraken    |                      |
|        1545103 | 0xfddf38947afb03c621c71b06c9c70bce73f12999 | lido    | Launchnodes          |
|        1545102 | 0xfddf38947afb03c621c71b06c9c70bce73f12999 | lido    | Launchnodes          |


## Contribution Guidelines

Please follow these steps to contribute:

1. **Fork the Repository**: Start by forking the repository to your GitHub account.
2. **Create a New Branch**: Create a new branch for your feature or bugfix.
   ```
   git checkout -b feature/new-feature
   ```
3. **Write Tests**: Ensure your code is well-tested and follows the project's coding standards.
4. **Submit a Pull Request**: Once you're ready, submit a pull request for review.


New contributions that help improve PyXatu are more than welcome!

---

For any additional questions or support, feel free to open an issue on the [GitHub repository](https://github.com/nerolation/pyxatu).
