# 🚀 PyXatu CLI Reference - LLM Optimized

**Quick Start**: `xatu setup` → `xatu slots --slot 10000000` → `xatu query "SELECT slot LIMIT 5"`

## Core Commands

### `xatu setup`
🔧 **Configure credentials and test connection**

**Options:**
- `--show` - Show current config (password masked)
- `--validate` - Test database connection

**Examples:**
```bash
xatu setup                 # Create new config
xatu setup --show          # View current settings  
xatu setup --validate      # Test connection
```

**Config Format** (`~/.pyxatu_config.json`):
```json
{
  "CLICKHOUSE_USER": "your-username",
  "CLICKHOUSE_PASSWORD": "your-password",
  "CLICKHOUSE_URL": "https://clickhouse.xatu.ethpandaops.io"
}
```

---

### `xatu query`
🔍 **Execute custom SQL queries**

**Options:**
- `--format [table|json|csv]` - Output format (default: table)
- `--limit INT` - Limit results
- `--save FILE` - Save to file
- `--config FILE` - Custom config path

**Examples:**
```bash
# Basic query
xatu query "SELECT slot, proposer_index FROM canonical_beacon_block LIMIT 5"

# With network filter (recommended for performance)
xatu query "SELECT slot FROM canonical_beacon_block WHERE meta_network_name='mainnet' LIMIT 10"

# JSON output
xatu query "SELECT slot, block_root FROM canonical_beacon_block LIMIT 5" --format json

# Save results
xatu query "SELECT * FROM canonical_beacon_proposer_duty LIMIT 100" --save results.csv
```

**Performance Tips:**
- Always include `meta_network_name='mainnet'` filter
- Use `LIMIT` to control result size
- Use `FINAL` for latest data: `FROM table FINAL`

---

### `xatu tables`
📋 **List tables and columns**

**Options:**
- `--table NAME` - Show columns for specific table
- `--format [table|json|csv]` - Output format
- `--save FILE` - Save to file

**Examples:**
```bash
xatu tables                                    # List all tables
xatu tables --table canonical_beacon_block     # Show table columns
xatu tables --format json                      # JSON output
```

**Key Tables:**
- `canonical_beacon_block` - Beacon chain blocks
- `canonical_beacon_proposer_duty` - Block proposer duties
- `canonical_beacon_elaborated_attestation` - Attestation data
- `mempool_transaction` - Mempool transactions
- `canonical_beacon_block_execution_transaction` - Execution transactions

---

### `xatu slots`
🧱 **Retrieve beacon chain slot data**

**Options:**
- `--slot INT` - Single slot
- `--start INT --end INT` - Slot range  
- `--columns "col1,col2"` - Specific columns
- `--add-missed/--no-add-missed` - Include missed slots (default: yes)
- `--limit INT` - Limit results
- `--format [table|json|csv]` - Output format
- `--save FILE` - Save to file

**Examples:**
```bash
xatu slots --slot 10000000                           # Single slot
xatu slots --start 10000000 --end 10000010           # Range
xatu slots --slot 10000000 --columns "slot,proposer_index"  # Specific columns
xatu slots --start 10000000 --end 10000005 --format json    # JSON output
```

**Common Columns:**
- `slot, proposer_index, block_root, parent_root`
- `execution_payload_gas_used, execution_payload_transactions_count`
- `execution_payload_block_hash, execution_payload_fee_recipient`

---

### `xatu attestations`
✅ **Retrieve attestation data**

**Options:**
- `--slot INT` - Single slot
- `--start INT --end INT` - Slot range
- `--columns "col1,col2"` - Specific columns
- `--limit INT` - Limit results
- `--format [table|json|csv]` - Output format
- `--save FILE` - Save to file

**Examples:**
```bash
xatu attestations --slot 10000000                    # Attestations for slot
xatu attestations --start 10000000 --end 10000002    # Range
xatu attestations --slot 10000000 --columns "slot,validators"  # Specific columns
```

**Common Columns:**
- `slot, committee_index, validators, beacon_block_root`
- `source_root, target_root, block_slot`
- `aggregation_bits, signature`

---

### `xatu transactions`
💸 **Retrieve execution layer transactions**

**Options:**
- `--slot INT` - Single slot
- `--start INT --end INT` - Slot range
- `--columns "col1,col2"` - Specific columns
- `--limit INT` - Limit results
- `--format [table|json|csv]` - Output format
- `--save FILE` - Save to file

**Examples:**
```bash
xatu transactions --slot 10000000                    # Transactions in slot
xatu transactions --start 10000000 --end 10000002    # Range
xatu transactions --slot 10000000 --columns "hash,gas_used"  # Specific columns
```

**Common Columns:**
- `hash, from_address, to_address, value, gas_used`
- `gas_price, transaction_type, slot, block_number`
- `max_fee_per_gas, max_priority_fee_per_gas`

---

### `xatu missed`
❌ **Find missed slots**

**Options:**
- `--slot INT` - Check specific slot
- `--start INT --end INT` - Range to check
- `--format [table|json|csv]` - Output format
- `--save FILE` - Save to file

**Examples:**
```bash
xatu missed --start 10000000 --end 10000100  # Find missed in range
xatu missed --slot 10000000                  # Check if slot was missed
```

---

## Common Usage Patterns

### 🔍 Data Exploration
```bash
# List available tables
xatu tables

# Explore table structure
xatu tables --table canonical_beacon_block

# Get recent slots
xatu slots --start 10000000 --end 10000010

# Check for missed slots
xatu missed --start 10000000 --end 10000100
```

### 📊 Analysis Workflows
```bash
# Get block data with proposer info
xatu slots --start 10000000 --end 10000010 --columns "slot,proposer_index,execution_payload_gas_used"

# Export to CSV for analysis
xatu transactions --start 10000000 --end 10000005 --format csv --save tx_data.csv

# Get attestation participation
xatu attestations --slot 10000000 --format json --save attestations.json
```

### 🛠️ Custom Queries
```bash
# Gas usage analysis
xatu query "SELECT slot, AVG(gas_used) as avg_gas FROM canonical_beacon_block_execution_transaction WHERE meta_network_name='mainnet' AND slot >= 10000000 AND slot < 10000100 GROUP BY slot ORDER BY slot"

# Validator performance
xatu query "SELECT proposer_index, COUNT(*) as blocks_proposed FROM canonical_beacon_block WHERE meta_network_name='mainnet' AND slot >= 10000000 GROUP BY proposer_index ORDER BY blocks_proposed DESC LIMIT 10"

# MEV analysis  
xatu query "SELECT slot, execution_payload_fee_recipient, execution_payload_gas_used FROM canonical_beacon_block WHERE meta_network_name='mainnet' AND slot >= 10000000 LIMIT 100"
```

---

## Output Formats

### Table (Default)
Human-readable tabular format with column alignment.

### JSON
Structured data format, ideal for programmatic processing:
```bash
xatu slots --slot 10000000 --format json | jq '.[] | .slot'
```

### CSV  
Comma-separated values for spreadsheet analysis:
```bash
xatu slots --start 10000000 --end 10000010 --format csv > slots.csv
```

---

## Error Handling

### Common Issues
1. **No config**: Run `xatu setup` first
2. **Network required**: Add `WHERE meta_network_name='mainnet'` to queries
3. **Large results**: Use `--limit` to control output size
4. **Connection failed**: Check config with `xatu setup --validate`

### Exit Codes
- `0` - Success
- `1` - Error (configuration, query, or network issue)

---

## Global Options

All data commands support:
- `--config FILE` - Custom configuration file
- `--format [table|json|csv]` - Output format
- `--save FILE` - Save results to file  
- `--limit INT` - Limit number of results

---

## Quick Reference Card

| Task | Command |
|------|---------|
| Setup | `xatu setup` |
| Test connection | `xatu setup --validate` |
| List tables | `xatu tables` |
| Single slot | `xatu slots --slot N` |
| Slot range | `xatu slots --start N --end M` |
| Custom query | `xatu query "SQL"` |
| Save results | `... --save file.csv` |
| JSON output | `... --format json` |
| Find missed | `xatu missed --start N --end M` |

**Most useful for LLMs**: Combine `--format json` with `--save` for structured data output.