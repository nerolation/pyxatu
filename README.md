# PyXatu

Python client for querying Ethereum beacon chain data from Xatu.

## Installation

```bash
pip install pyxatu
```

## Configuration

### Environment Variables

```bash
export CLICKHOUSE_URL="https://your-clickhouse-server.com"
export CLICKHOUSE_USER="your_username"
export CLICKHOUSE_PASSWORD="your_password"
```

### Configuration File

Create `~/.pyxatu_config.json`:
```json
{
    "clickhouse": {
        "url": "https://your-clickhouse-server.com",
        "user": "your_username",
        "password": "your_password"
    }
}
```

## Usage

### Basic Queries

```python
import asyncio
from pyxatu import PyXatu, Network

async def main():
    async with PyXatu() as xatu:
        # Query slots
        slots = await xatu.get_slots(
            slot=[9000000, 9000010],
            network=Network.MAINNET
        )
        
        # Query attestations
        attestations = await xatu.get_attestations(
            slot=9000000,
            network=Network.MAINNET
        )

asyncio.run(main())
```

### Slot Analysis

```python
async with PyXatu() as xatu:
    # Include missed slots
    slots = await xatu.get_slots(
        slot=[9000000, 9000100],
        include_missed=True,
        network=Network.MAINNET
    )
    
    # Get missed slots only
    missed = await xatu.get_missed_slots(
        slot_range=[9000000, 9000100]
    )
```

### Attestation Performance

```python
async with PyXatu() as xatu:
    # Analyze attestation performance
    performance = await xatu.get_elaborated_attestations(
        slot=[9000000, 9000010],
        vote_types=['source', 'target', 'head'],
        status_filter=['correct', 'failed'],
        include_delay=True
    )
```

### Transaction Analysis

```python
async with PyXatu() as xatu:
    # Analyze transaction privacy
    transactions = await xatu.get_elaborated_transactions(
        slots=[9000000, 9000001, 9000002],
        include_external_mempool=True
    )
    
    # Get block metrics
    block_sizes = await xatu.get_block_sizes(
        slot=[9000000, 9001000],
        orderby="-blobs"
    )
```

## API Reference

### Main Methods

- `get_slots()` - Query beacon chain blocks
- `get_attestations()` - Query attestations
- `get_elaborated_attestations()` - Detailed attestation analysis
- `get_transactions()` - Query transactions
- `get_elaborated_transactions()` - Transaction privacy analysis
- `get_withdrawals()` - Query validator withdrawals
- `get_block_sizes()` - Block size metrics
- `get_proposer_duties()` - Proposer assignments

### Parameters

All query methods accept:
- `slot`: Single slot or range [start, end)
- `network`: Network enum (MAINNET, SEPOLIA, HOLESKY)
- `columns`: Columns to retrieve (default: "*")
- `limit`: Maximum rows to return
- `orderby`: Sort column (prefix with - for DESC)

## Architecture

```
pyxatu/
├── pyxatu.py              # Main interface
├── models.py              # Data models
├── config.py              # Configuration
├── clickhouse_client.py   # Database client
├── queries/               # Query modules
│   ├── slot_queries.py
│   ├── attestation_queries.py
│   ├── transaction_queries.py
│   └── validator_queries.py
├── mempool_connector.py   # Mempool integration
└── relay_connector.py     # MEV relay connector
```

## Technical Details

### Performance Optimizations

- **Partition Filtering**: Automatic `slot_start_date_time` filtering prevents full table scans
- **Connection Pooling**: Reuses database connections
- **Async Operations**: Non-blocking I/O for concurrent queries
- **Batch Processing**: Efficient handling of large datasets

### Security

- Parameterized queries prevent SQL injection
- Input validation on all parameters
- Secure credential storage with SecretStr
- Table whitelist enforcement

## Testing

```bash
# Run tests
pytest

# Run with coverage
pytest --cov=pyxatu --cov-report=html
```

## License

MIT