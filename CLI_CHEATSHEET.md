# PyXatu CLI Cheat Sheet - LLM Quick Reference

## 🚀 Essential Commands (Copy-Paste Ready)

### Setup & Configuration
```bash
xatu setup                    # Initial setup
xatu setup --validate         # Test connection
xatu setup --show            # View config (password masked)
```

### Data Retrieval
```bash
# Single slot data
xatu slots --slot 10000000

# Range of slots  
xatu slots --start 10000000 --end 10000010

# Attestations for slot
xatu attestations --slot 10000000

# Transactions in slot
xatu transactions --slot 10000000

# Find missed slots
xatu missed --start 10000000 --end 10000100

# List all tables
xatu tables

# Show table columns
xatu tables --table canonical_beacon_block
```

### Custom Queries
```bash
# Basic query
xatu query "SELECT slot, proposer_index FROM canonical_beacon_block WHERE meta_network_name='mainnet' LIMIT 5"

# JSON output (best for programmatic use)
xatu query "SELECT slot FROM canonical_beacon_block WHERE meta_network_name='mainnet' LIMIT 10" --format json

# Save to file
xatu query "SELECT * FROM canonical_beacon_proposer_duty WHERE meta_network_name='mainnet' LIMIT 100" --save results.csv
```

## 📋 Key Tables & Columns

### canonical_beacon_block
- `slot, proposer_index, block_root, parent_root`
- `execution_payload_gas_used, execution_payload_transactions_count`

### canonical_beacon_elaborated_attestation  
- `slot, committee_index, validators, beacon_block_root`
- `source_root, target_root, block_slot`

### canonical_beacon_block_execution_transaction
- `hash, from_address, to_address, value, gas_used`
- `gas_price, transaction_type, slot`

### mempool_transaction
- `hash, gas_price, gas_limit, value, from_address, to_address`

## ⚡ Performance Tips

1. **Always filter by network**: `WHERE meta_network_name='mainnet'`
2. **Use LIMIT**: Add `LIMIT N` to control result size
3. **Use FINAL for latest data**: `FROM table FINAL`
4. **Filter by slot range**: `WHERE slot >= X AND slot < Y`

## 🛠️ Common Use Cases

### Block Analysis
```bash
# Recent blocks with gas usage
xatu slots --start 10000000 --end 10000010 --columns "slot,proposer_index,execution_payload_gas_used"

# Missed slots in range
xatu missed --start 10000000 --end 10000100
```

### Transaction Analysis
```bash
# High-value transactions
xatu query "SELECT hash, value, gas_used FROM canonical_beacon_block_execution_transaction WHERE meta_network_name='mainnet' AND value > 1000000000000000000 LIMIT 10"

# Gas usage by slot
xatu query "SELECT slot, AVG(gas_used) as avg_gas FROM canonical_beacon_block_execution_transaction WHERE meta_network_name='mainnet' AND slot >= 10000000 GROUP BY slot LIMIT 10"
```

### Validator Analysis
```bash
# Top block proposers
xatu query "SELECT proposer_index, COUNT(*) as blocks FROM canonical_beacon_block WHERE meta_network_name='mainnet' AND slot >= 10000000 GROUP BY proposer_index ORDER BY blocks DESC LIMIT 10"

# Attestation participation for slot
xatu attestations --slot 10000000 --columns "slot,validators"
```

## 📤 Output Formats

| Format | Usage | Best For |
|--------|-------|----------|
| `table` | Default human-readable | Terminal viewing |
| `json` | `--format json` | APIs, jq processing |
| `csv` | `--format csv` | Spreadsheets, analysis |

## 🔧 Quick Troubleshooting

| Issue | Solution |
|-------|----------|
| No config | `xatu setup` |
| Connection failed | `xatu setup --validate` |
| Too much data | Add `--limit N` |
| No results | Check slot range exists |
| Slow query | Add network filter |

## 💡 LLM Best Practices

1. **Use JSON format** for structured output: `--format json`
2. **Save large results** to files: `--save filename.json`
3. **Always include network filter** in custom queries
4. **Start with small limits** when exploring: `LIMIT 10`
5. **Use table discovery**: `xatu tables` then `xatu tables --table NAME`

## 🎯 Most Common LLM Commands

```bash
# Explore available data
xatu tables

# Get sample data  
xatu slots --slot 10000000 --format json

# Custom analysis query
xatu query "SELECT slot, proposer_index FROM canonical_beacon_block WHERE meta_network_name='mainnet' LIMIT 5" --format json

# Export for processing
xatu transactions --start 10000000 --end 10000005 --format json --save tx_data.json
```

**Copy these commands and modify slot numbers/parameters as needed.**