# PyXatu

Python client for Ethereum beacon chain data from Xatu.

## Installation

```bash
pip install pyxatu
```

## Quick Start

```python
from pyxatu import PyXatu

# Simple query
with PyXatu() as xatu:
    slots = xatu.get_slots(limit=10)
    print(slots)
```

## Configuration

### Environment Variables
```bash
export CLICKHOUSE_URL="https://your-clickhouse-server.com"
export CLICKHOUSE_USER="your_username"
export CLICKHOUSE_PASSWORD="your_password"
```

### Config File
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

## Examples

### Slots & Blocks

```python
from pyxatu import PyXatu, Network

xatu = PyXatu()

# Get recent blocks
blocks = xatu.get_slots(limit=100)

# Get specific slot range
blocks = xatu.get_slots(
    slot=[9000000, 9000100],  # Start and end slot
    network=Network.MAINNET
)

# Include missed slots
all_slots = xatu.get_slots(
    slot=[9000000, 9000100],
    include_missed=True
)

# Get only missed slots
missed = xatu.get_missed_slots(slot_range=[9000000, 9001000])

# Get reorganizations
reorgs = xatu.get_reorgs(limit=10)
```

### Attestations

```python
# Basic attestation query
attestations = xatu.get_attestations(
    slot=9000000,
    limit=100
)

# Detailed attestation analysis
performance = xatu.get_elaborated_attestations(
    slot=[9000000, 9000100],
    vote_types=['source', 'target', 'head'],
    status_filter=['correct'],
    include_delay=True
)
```

### Validator Labels

```python
# Get label for single validator
label = xatu.get_validator_labels(indices=100)

# Bulk lookup
labels = xatu.get_validator_labels_bulk([100, 200, 300])
# Returns: {100: 'lido', 200: 'coinbase', 300: None}

# Get validators by entity
lido_validators = xatu.get_validators_by_entity('lido')

# Get entity statistics
stats = xatu.get_entity_statistics()
```

### Transactions

```python
# Get transactions
txs = xatu.get_transactions(
    slot=[9000000, 9000010],
    limit=1000
)

# Get withdrawals
withdrawals = xatu.get_withdrawals(
    slot=[9000000, 9001000],
    orderby="-amount"
)
```

### Raw SQL Queries

```python
# Execute custom queries
result = xatu.raw_query("""
    SELECT 
        slot DIV 32 as epoch,
        count() as blocks,
        avg(block_total_bytes) as avg_size
    FROM canonical_beacon_block
    WHERE slot BETWEEN %(start)s AND %(end)s
    GROUP BY epoch
    ORDER BY epoch
""", params={'start': 9000000, 'end': 9100000})
```

## CLI Usage

### Basic Queries
```bash
# Query slots
xatu slots query --slot 9000000:9000100

# Get attestations
xatu attestations query --slot 9000000 --limit 100

# Export to CSV
xatu slots query --slot 9000000:9000100 --format csv --output blocks.csv
```

### Advanced CLI
```bash
# Raw SQL query
xatu query "SELECT * FROM canonical_beacon_block WHERE slot = 9000000"

# Complex analysis
xatu query "
    SELECT 
        proposer_index,
        count() as blocks,
        avg(block_total_bytes) as avg_size
    FROM canonical_beacon_block
    WHERE slot BETWEEN 9000000 AND 9100000
    GROUP BY proposer_index
    ORDER BY blocks DESC
    LIMIT 10
" --format json

# Validator performance
xatu attestations elaborated --slot 9000000:9000100 --format csv
```

### Validator Labels CLI
```bash
# Get entity statistics
xatu labels stats

# Look up specific validators
xatu labels lookup 100 200 300

# Show validators for entity
xatu labels entity lido --limit 20

# Refresh labels
xatu labels refresh
```

## Advanced Usage

### Working with DataFrames
```python
import pandas as pd

with PyXatu() as xatu:
    # Get data as DataFrame
    slots = xatu.get_slots(slot=[9000000, 9000100])
    
    # Add validator labels
    if 'proposer_index' in slots.columns:
        proposer_labels = xatu.get_validator_labels_bulk(
            slots['proposer_index'].unique().tolist()
        )
        slots['proposer_entity'] = slots['proposer_index'].map(proposer_labels)
    
    # Analyze by entity
    by_entity = slots.groupby('proposer_entity').size()
    print(by_entity)
```

### Performance Analysis
```python
# Comprehensive validator performance
attestations = xatu.get_elaborated_attestations(
    slot=[9000000, 9010000],
    include_delay=True
)

# Get validator labels
validator_indices = attestations['attesting_validator_index'].unique()
labels = xatu.get_validator_labels_bulk(validator_indices.tolist())

# Add labels to DataFrame
attestations['entity'] = attestations['attesting_validator_index'].map(labels)

# Analyze by entity
entity_stats = attestations.groupby('entity').agg({
    'source_vote': 'mean',
    'target_vote': 'mean',
    'head_vote': 'mean',
    'inclusion_delay': 'mean'
}).round(4)

print(entity_stats)
```

### MEV Analysis
```python
# Query MEV relay data
mev_blocks = xatu.raw_query("""
    SELECT 
        slot,
        relay,
        builder_pubkey,
        value_wei / 1e18 as value_eth
    FROM mev_relay_bid_trace
    WHERE slot BETWEEN %(start)s AND %(end)s
    ORDER BY value_wei DESC
    LIMIT 100
""", params={'start': 9000000, 'end': 9001000})
```

### Large Dataset Handling
```python
# Process in chunks for memory efficiency
chunk_size = 10000
start_slot = 9000000
end_slot = 9100000

results = []
for chunk_start in range(start_slot, end_slot, chunk_size):
    chunk_end = min(chunk_start + chunk_size, end_slot)
    
    chunk_data = xatu.get_slots(
        slot=[chunk_start, chunk_end],
        columns="slot,proposer_index,block_total_bytes"
    )
    
    # Process chunk
    results.append(chunk_data.groupby('proposer_index').size())

# Combine results
final_result = pd.concat(results).groupby(level=0).sum()
```

## Available Tables

### Core Tables
- `canonical_beacon_block` - Beacon blocks
- `canonical_beacon_attestation` - Attestations  
- `canonical_beacon_block_execution_transaction` - Transactions
- `canonical_beacon_block_withdrawal` - Withdrawals
- `canonical_beacon_proposer_duty` - Proposer assignments
- `canonical_beacon_elaborated_attestation` - Attestation metrics

### MEV Tables
- `mev_relay_bid_trace` - Relay bids
- `mev_relay_proposer_payload_delivered` - Delivered payloads

### Event Tables
- `beacon_api_eth_v1_events_attestation` - Attestation events
- `beacon_api_eth_v1_events_block` - Block events
- `beacon_api_eth_v1_events_chain_reorg` - Reorg events

## Networks

- `Network.MAINNET` (default)
- `Network.SEPOLIA` 
- `Network.HOLESKY`

## Performance Tips

1. **Use specific columns**: Don't use `*` for large queries
2. **Filter by slot**: Always include slot ranges for partitioning
3. **Batch operations**: Process multiple items together
4. **Use context manager**: Ensures proper connection cleanup

## Error Handling

```python
from pyxatu import PyXatu

try:
    xatu = PyXatu()
    slots = xatu.get_slots(slot=9000000)
except ConnectionError:
    print("Failed to connect to ClickHouse")
except Exception as e:
    print(f"Query failed: {e}")
finally:
    xatu.close()
```

## License

MIT