import click
import importlib.resources as resources
from pathlib import Path
import shutil
import json
import sys
from typing import Optional, List
from ..core import PyXatu

@click.group()
@click.version_option(version='1.9.1', prog_name='xatu')
def cli():
    """
    🚀 PyXatu CLI - Ethereum Beacon Chain & Execution Layer Data Query Tool
    
    Query Ethereum data from the Xatu ClickHouse database with ease.
    Use 'xatu COMMAND --help' for detailed command information.
    
    Quick Start:
      1. xatu setup                    # Configure credentials
      2. xatu slots --slot 10000000    # Get slot data
      3. xatu query "SELECT slot LIMIT 5" # Custom query
    """
    pass

@click.command()
@click.option('--show', is_flag=True, help='Show current configuration')
@click.option('--validate', is_flag=True, help='Validate configuration and test connection')
def setup(show: bool, validate: bool):
    """
    🔧 Configure PyXatu with your Xatu ClickHouse credentials.
    
    Creates ~/.pyxatu_config.json with connection settings.
    
    EXAMPLES:
      xatu setup                 # Setup new configuration
      xatu setup --show          # Show current config
      xatu setup --validate      # Test connection
    
    CONFIG FORMAT:
      {
        "CLICKHOUSE_USER": "your-username",
        "CLICKHOUSE_PASSWORD": "your-password", 
        "CLICKHOUSE_URL": "https://clickhouse.xatu.ethpandaops.io"
      }
    """
    home = Path.home()
    user_config_path = home / '.pyxatu_config.json'
    
    if show:
        if user_config_path.exists():
            with open(user_config_path, 'r') as f:
                config = json.load(f)
                config['CLICKHOUSE_PASSWORD'] = '***masked***'
                print(json.dumps(config, indent=2))
        else:
            print("❌ No configuration found. Run 'xatu setup' first.")
        return
    
    if validate:
        if not user_config_path.exists():
            print("❌ No configuration found. Run 'xatu setup' first.")
            return
        try:
            client = PyXatu(config_path=str(user_config_path))
            result = client.execute_query("SELECT 1 as test")
            print("✅ Configuration valid and connection successful!")
            print(f"🔗 Connected to: {client.clickhouse_url}")
            print(f"👤 User: {client.clickhouse_user}")
        except Exception as e:
            print(f"❌ Configuration test failed: {e}")
        return

    try:
        print("🔍 Looking for default config template...")
        default_config_file = resources.files('pyxatu') / 'config.json'
        
        if not user_config_path.exists():
            shutil.copy(default_config_file, user_config_path)
            print(f"✅ Configuration template created: {user_config_path}")
            print("📝 Please edit the file with your credentials:")
            print(f"   📂 {user_config_path}")
            print("\n💡 Then run: xatu setup --validate")
        else:
            print(f"⚠️  Configuration already exists: {user_config_path}")
            print("💡 Use 'xatu setup --show' to view or 'xatu setup --validate' to test")
    except Exception as e:
        print(f"❌ Setup failed: {e}")

@click.command()
@click.argument('sql_query')
@click.option('--config', type=click.Path(exists=True), help='Custom config file path', default=None)
@click.option('--format', 'output_format', type=click.Choice(['table', 'json', 'csv']), default='table', help='Output format')
@click.option('--limit', type=int, help='Limit number of results', default=None)
@click.option('--save', type=click.Path(), help='Save results to file')
def query(sql_query: str, config: Optional[str], output_format: str, limit: Optional[int], save: Optional[str]):
    """
    🔍 Execute custom SQL queries against the Xatu ClickHouse database.
    
    EXAMPLES:
      # Basic query
      xatu query "SELECT slot, proposer_index FROM canonical_beacon_block LIMIT 5"
      
      # With network filter (recommended)
      xatu query "SELECT slot FROM canonical_beacon_block WHERE meta_network_name='mainnet' LIMIT 10"
      
      # Save results
      xatu query "SELECT * FROM canonical_beacon_proposer_duty LIMIT 100" --save results.csv
      
      # JSON output
      xatu query "SELECT slot, block_root FROM canonical_beacon_block LIMIT 5" --format json
    
    TIPS:
    - Always include 'meta_network_name' filter for better performance
    - Use LIMIT to control result size
    - Available tables: run 'xatu tables' to see all options
    """
    try:
        client = PyXatu(config_path=config, no_validator_gadget=True)
        
        # Add LIMIT if specified and not already in query
        if limit and 'LIMIT' not in sql_query.upper():
            sql_query += f" LIMIT {limit}"
        
        result = client.execute_query(sql_query)
        
        if result is None or result.empty:
            print("📭 No results returned")
            return
        
        # Format output
        if output_format == 'json':
            output = result.to_json(orient='records', indent=2)
        elif output_format == 'csv':
            output = result.to_csv(index=False)
        else:  # table
            output = result.to_string(index=False)
        
        # Save or print
        if save:
            Path(save).write_text(output)
            print(f"💾 Results saved to: {save}")
            print(f"📊 Records: {len(result)}")
        else:
            print(output)
            print(f"\n📊 Total records: {len(result)}")
            
    except Exception as e:
        print(f"❌ Query failed: {e}")
        sys.exit(1)

# Common options for data commands
def common_options(f):
    """Common CLI options for data retrieval commands."""
    f = click.option('--config', type=click.Path(exists=True), help='Custom config file path', default=None)(f)
    f = click.option('--format', 'output_format', type=click.Choice(['table', 'json', 'csv']), default='table', help='Output format')(f)
    f = click.option('--save', type=click.Path(), help='Save results to file')(f)
    f = click.option('--limit', type=int, help='Limit number of results')(f)
    return f

@click.command()
@click.option('--table', help='Show columns for specific table')
@common_options
def tables(table: Optional[str], config: Optional[str], output_format: str, save: Optional[str], limit: Optional[int]):
    """
    📋 List available tables and their columns.
    
    EXAMPLES:
      xatu tables                           # List all tables
      xatu tables --table canonical_beacon_block  # Show columns for specific table
      xatu tables --format json            # JSON output
    
    COMMON TABLES:
    - canonical_beacon_block: Beacon chain blocks
    - canonical_beacon_proposer_duty: Block proposer duties  
    - canonical_beacon_elaborated_attestation: Attestation data
    - mempool_transaction: Mempool transactions
    """
    try:
        client = PyXatu(config_path=config, no_validator_gadget=True)
        
        if table:
            # Show columns for specific table
            columns = client.get_columns(table)
            if columns is not None and not columns.empty:
                print(f"📋 Columns in '{table}':")
                for col in columns.iloc[:, 0]:
                    print(f"  • {col}")
            else:
                print(f"❌ Table '{table}' not found or no columns available")
        else:
            # List all tables
            from ..utils import CONSTANTS
            tables_data = CONSTANTS['TABLES']
            
            if output_format == 'json':
                output = json.dumps(tables_data, indent=2)
            else:
                output = "\n📋 Available Tables:\n"
                for table_name, full_name in tables_data.items():
                    output += f"  • {table_name}\n"
            
            if save:
                Path(save).write_text(output)
                print(f"💾 Tables list saved to: {save}")
            else:
                print(output)
                
    except Exception as e:
        print(f"❌ Failed to retrieve tables: {e}")
        sys.exit(1)

@click.command()
@click.option('--slot', type=int, help='Specific slot number')
@click.option('--start', type=int, help='Start slot for range')
@click.option('--end', type=int, help='End slot for range') 
@click.option('--add-missed', is_flag=True, default=True, help='Include missed slots')
@click.option('--columns', help='Comma-separated column names')
@common_options
def slots(slot: Optional[int], start: Optional[int], end: Optional[int], add_missed: bool, 
         columns: Optional[str], config: Optional[str], output_format: str, save: Optional[str], limit: Optional[int]):
    """
    🧱 Retrieve beacon chain slot data.
    
    EXAMPLES:
      xatu slots --slot 10000000              # Single slot
      xatu slots --start 10000000 --end 10000010  # Range of slots
      xatu slots --slot 10000000 --columns "slot,proposer_index"  # Specific columns
      xatu slots --start 10000000 --end 10000005 --format json     # JSON output
    
    COMMON COLUMNS:
    - slot, proposer_index, block_root, parent_root
    - execution_payload_gas_used, execution_payload_transactions_count
    """
    try:
        client = PyXatu(config_path=config, no_validator_gadget=True)
        
        # Determine slot range
        if slot:
            slot_range = slot
        elif start and end:
            slot_range = [start, end]
        else:
            print("❌ Specify either --slot or both --start and --end")
            sys.exit(1)
            
        kwargs = {
            'slot': slot_range,
            'add_missed': add_missed
        }
        
        if columns:
            kwargs['columns'] = columns
        if limit:
            kwargs['limit'] = limit
            
        result = client.get_slots(**kwargs)
        
        if result is None or result.empty:
            print("📭 No slot data found")
            return
            
        # Format and output
        if output_format == 'json':
            output = result.to_json(orient='records', indent=2)
        elif output_format == 'csv':
            output = result.to_csv(index=False)
        else:
            output = result.to_string(index=False)
            
        if save:
            Path(save).write_text(output)
            print(f"💾 Slot data saved to: {save}")
            print(f"📊 Records: {len(result)}")
        else:
            print(output)
            print(f"\n📊 Total slots: {len(result)}")
            
    except Exception as e:
        print(f"❌ Failed to retrieve slots: {e}")
        sys.exit(1)

@click.command()
@click.option('--slot', type=int, help='Specific slot number')
@click.option('--start', type=int, help='Start slot for range')
@click.option('--end', type=int, help='End slot for range')
@click.option('--columns', help='Comma-separated column names')
@common_options  
def attestations(slot: Optional[int], start: Optional[int], end: Optional[int], columns: Optional[str],
                config: Optional[str], output_format: str, save: Optional[str], limit: Optional[int]):
    """
    ✅ Retrieve attestation data from beacon chain.
    
    EXAMPLES:
      xatu attestations --slot 10000000       # Attestations for slot
      xatu attestations --start 10000000 --end 10000002  # Range
      xatu attestations --slot 10000000 --columns "slot,validators"  # Specific columns
    
    COMMON COLUMNS:
    - slot, committee_index, validators, beacon_block_root
    - source_root, target_root, block_slot
    """
    try:
        client = PyXatu(config_path=config, no_validator_gadget=True)
        
        # Determine slot range
        if slot:
            slot_range = slot
        elif start and end:
            slot_range = [start, end]
        else:
            print("❌ Specify either --slot or both --start and --end")
            sys.exit(1)
            
        kwargs = {'slot': slot_range}
        if columns:
            kwargs['columns'] = columns
        if limit:
            kwargs['limit'] = limit
            
        result = client.get_attestation(**kwargs)
        
        if result is None or result.empty:
            print("📭 No attestation data found")
            return
            
        # Format and output
        if output_format == 'json':
            output = result.to_json(orient='records', indent=2)
        elif output_format == 'csv':
            output = result.to_csv(index=False)
        else:
            output = result.to_string(index=False)
            
        if save:
            Path(save).write_text(output)
            print(f"💾 Attestation data saved to: {save}")
            print(f"📊 Records: {len(result)}")
        else:
            print(output)
            print(f"\n📊 Total attestations: {len(result)}")
            
    except Exception as e:
        print(f"❌ Failed to retrieve attestations: {e}")
        sys.exit(1)

@click.command()
@click.option('--slot', type=int, help='Specific slot number')
@click.option('--start', type=int, help='Start slot for range')
@click.option('--end', type=int, help='End slot for range')
@click.option('--columns', help='Comma-separated column names')
@common_options
def transactions(slot: Optional[int], start: Optional[int], end: Optional[int], columns: Optional[str],
                config: Optional[str], output_format: str, save: Optional[str], limit: Optional[int]):
    """
    💸 Retrieve execution layer transaction data.
    
    EXAMPLES:
      xatu transactions --slot 10000000       # Transactions in slot
      xatu transactions --start 10000000 --end 10000002  # Range
      xatu transactions --slot 10000000 --columns "hash,gas_used"  # Specific columns
    
    COMMON COLUMNS:
    - hash, from_address, to_address, value, gas_used
    - gas_price, transaction_type, slot
    """
    try:
        client = PyXatu(config_path=config, no_validator_gadget=True)
        
        # Determine slot range
        if slot:
            slot_range = slot
        elif start and end:
            slot_range = [start, end]
        else:
            print("❌ Specify either --slot or both --start and --end")
            sys.exit(1)
            
        kwargs = {'slot': slot_range}
        if columns:
            kwargs['columns'] = columns
        if limit:
            kwargs['limit'] = limit
            
        result = client.get_transactions(**kwargs)
        
        if result is None or result.empty:
            print("📭 No transaction data found")
            return
            
        # Format and output
        if output_format == 'json':
            output = result.to_json(orient='records', indent=2)
        elif output_format == 'csv':
            output = result.to_csv(index=False)
        else:
            output = result.to_string(index=False)
            
        if save:
            Path(save).write_text(output)
            print(f"💾 Transaction data saved to: {save}")
            print(f"📊 Records: {len(result)}")
        else:
            print(output)
            print(f"\n📊 Total transactions: {len(result)}")
            
    except Exception as e:
        print(f"❌ Failed to retrieve transactions: {e}")
        sys.exit(1)

@click.command()
@click.option('--slot', type=int, help='Specific slot number')
@click.option('--start', type=int, help='Start slot for range')
@click.option('--end', type=int, help='End slot for range')
@common_options
def missed(slot: Optional[int], start: Optional[int], end: Optional[int],
          config: Optional[str], output_format: str, save: Optional[str], limit: Optional[int]):
    """
    ❌ Find missed slots in a range.
    
    EXAMPLES:
      xatu missed --start 10000000 --end 10000100  # Find missed slots in range
      xatu missed --slot 10000000  # Check if specific slot was missed
    """
    try:
        client = PyXatu(config_path=config, no_validator_gadget=True)
        
        # Determine slot range
        if slot:
            slot_range = [slot, slot + 1]
        elif start and end:
            slot_range = [start, end]
        else:
            print("❌ Specify either --slot or both --start and --end")
            sys.exit(1)
            
        missed_slots = client.get_missed_slots(slot=slot_range)
        
        if not missed_slots:
            print("✅ No missed slots found in range")
            return
            
        # Convert to list and format
        missed_list = sorted(list(missed_slots))
        
        if output_format == 'json':
            output = json.dumps(missed_list, indent=2)
        elif output_format == 'csv':
            output = "slot\n" + "\n".join(map(str, missed_list))
        else:
            output = f"❌ Missed slots ({len(missed_list)} total):\n" + "\n".join(f"  • {s}" for s in missed_list[:50])
            if len(missed_list) > 50:
                output += f"\n  ... and {len(missed_list) - 50} more"
            
        if save:
            Path(save).write_text(output)
            print(f"💾 Missed slots saved to: {save}")
            print(f"📊 Total missed: {len(missed_list)}")
        else:
            print(output)
            
    except Exception as e:
        print(f"❌ Failed to retrieve missed slots: {e}")
        sys.exit(1)

# Register all commands
cli.add_command(setup)
cli.add_command(query)
cli.add_command(tables)
cli.add_command(slots)
cli.add_command(attestations)
cli.add_command(transactions)
cli.add_command(missed)

if __name__ == "__main__":
    cli()