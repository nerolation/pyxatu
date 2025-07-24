# PyXatu 🚀

A secure, efficient, and modern Python client for querying Ethereum beacon chain data from Xatu.

[![Python Version](https://img.shields.io/badge/python-3.8%2B-blue)](https://www.python.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

## 🎯 Features

- **🔒 Security First**: No eval(), SQL injection prevention, secure credential handling
- **⚡ High Performance**: Async operations, connection pooling, ClickHouse partition optimization
- **🏗️ Modern Architecture**: Type hints, Pydantic models, clean modular design
- **📊 Comprehensive Data Access**: Slots, attestations, transactions, validators, MEV data
- **🔄 Automatic Retries**: Built-in retry logic with exponential backoff
- **🎨 Developer Friendly**: Intuitive API, great error messages, extensive documentation

## 📦 Installation

```bash
pip install pyxatu
```

For development:
```bash
pip install pyxatu[dev]
```

## 🚀 Quick Start

```python
import asyncio
from pyxatu import PyXatu, Network

async def main():
    # Initialize with environment variables or config file
    async with PyXatu() as xatu:
        # Get recent blocks
        blocks = await xatu.get_slots(
            slot=[9000000, 9000010],
            network=Network.MAINNET
        )
        print(f"Found {len(blocks)} blocks")
        
        # Get attestations for a specific slot
        attestations = await xatu.get_attestations(
            slot=9000000,
            network=Network.MAINNET
        )
        print(f"Found {len(attestations)} attestations")

asyncio.run(main())
```

## 🔧 Configuration

### Environment Variables (Recommended)

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

## 📚 Usage Examples

### Query Slots with Missed Blocks

```python
async with PyXatu() as xatu:
    # Get slots including missed blocks
    slots = await xatu.get_slots(
        slot=[9000000, 9000100],
        include_missed=True,
        network=Network.MAINNET
    )
    
    # Get only missed slots
    missed = await xatu.get_missed_slots(
        slot_range=[9000000, 9000100]
    )
    print(f"Missed slots: {missed}")
```

### Analyze Attestation Performance

```python
async with PyXatu() as xatu:
    # Get detailed attestation performance
    performance = await xatu.get_elaborated_attestations(
        slot=[9000000, 9000010],
        vote_types=['source', 'target', 'head'],
        status_filter=['correct', 'failed'],
        include_delay=True
    )
    
    # Analyze results
    correct = performance[performance['status'] == 'correct']
    print(f"Correct attestations: {len(correct)}/{len(performance)}")
```

### Analyze Transaction Privacy

```python
async with PyXatu() as xatu:
    # Get transactions with mempool analysis
    transactions = await xatu.get_elaborated_transactions(
        slots=[9000000, 9000001, 9000002],
        include_external_mempool=True
    )
    
    # Check private vs public transactions
    private_txs = transactions[transactions['private'] == True]
    print(f"Private transactions: {len(private_txs)}/{len(transactions)}")
```

### Query Block Metrics

```python
async with PyXatu() as xatu:
    # Get block size metrics including blob data
    block_sizes = await xatu.get_block_sizes(
        slot=[9000000, 9001000],
        orderby="-blobs"  # Order by blob count descending
    )
    
    print(f"Average block size: {block_sizes['block_total_bytes'].mean():,.0f} bytes")
    print(f"Max blobs in a block: {block_sizes['blobs'].max()}")
```

### MEV Data from Relays

```python
from pyxatu.relay_connector import RelayConnector

async with RelayConnector() as relay:
    # Get delivered payloads for a slot
    payloads = await relay.get_proposer_payload_delivered(
        slot=9000000,
        limit=100
    )
    
    # Get aggregated bid statistics
    stats = await relay.get_aggregate_bid_stats(slot=9000000)
    print(f"Winning bid: {stats['max_value_wei']} wei")
    print(f"Winning relay: {stats['winning_relay']}")
```

## 🏗️ Architecture

PyXatu features a clean, modular architecture:

```
pyxatu/
├── pyxatu.py          # Main interface
├── models.py          # Pydantic data models
├── config.py          # Configuration management
├── clickhouse_client.py # Async database client
├── queries/           # Domain-specific query modules
│   ├── slot_queries.py
│   ├── attestation_queries.py
│   ├── transaction_queries.py
│   └── validator_queries.py
├── mempool_connector.py # Mempool data integration
└── relay_connector.py   # MEV-Boost relay connector
```

## 🔒 Security Features

- **No eval()**: All JSON parsing uses safe methods
- **SQL Injection Prevention**: Parameterized queries throughout
- **Input Validation**: Comprehensive validation with Pydantic
- **Secure Credentials**: Passwords stored as SecretStr
- **Table Whitelist**: Only approved tables can be queried
- **Partition Optimization**: Automatic date filtering for performance

## ⚡ Performance Optimizations

### ClickHouse Partition Filtering
PyXatu automatically adds partition key filters to prevent full table scans:

```python
# This query automatically includes slot_start_date_time filtering
slots = await xatu.get_slots(slot=[9000000, 9001000])

# Generated SQL includes:
# WHERE slot BETWEEN 9000000 AND 9000999
# AND slot_start_date_time >= '2024-01-01 00:00:00' - INTERVAL 1 HOUR
# AND slot_start_date_time <= '2024-01-01 03:00:00' + INTERVAL 1 HOUR
```

### Connection Pooling
Automatic connection pooling for optimal resource usage:

```python
# Connections are automatically pooled and reused
async with PyXatu() as xatu:
    # Multiple queries reuse connections efficiently
    tasks = [
        xatu.get_slots(slot=i) 
        for i in range(9000000, 9000100)
    ]
    results = await asyncio.gather(*tasks)
```

## 🧪 Testing

Run the comprehensive test suite:

```bash
# Install test dependencies
pip install pyxatu[dev]

# Run all tests
pytest

# Run with coverage
pytest --cov=pyxatu --cov-report=html

# Run only security tests
pytest tests/test_security.py -v
```

## 🤝 Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request


## 📄 License

MIT License - see [LICENSE](LICENSE) for details.

## 🙏 Acknowledgments

- Built on top of [Xatu](https://github.com/ethpandaops/xatu) data
- Powered by [ClickHouse](https://clickhouse.com/)
- Inspired by the Ethereum community's need for accessible beacon chain data

## 📞 Support

- 📧 Open an issue on [GitHub](https://github.com/nerolation/pyxatu/issues)
- 💬 Join the discussion on [Discord](https://discord.gg/ethereum)
- 📚 Read the [documentation](https://github.com/nerolation/pyxatu/wiki)