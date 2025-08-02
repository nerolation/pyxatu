#!/usr/bin/env python3
"""
PyXatu CLI - Command Line Interface for Ethereum Beacon Chain Data Queries

This module provides a comprehensive CLI for accessing Ethereum beacon chain data
through the PyXatu library with smart defaults and flexible output options.
"""

import click
import json
import sys
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, List, Tuple, Any
import pandas as pd
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich import box
import importlib.resources as resources
import shutil

from . import PyXatu
from .utils import slot_to_timestamp, timestamp_to_slot
from .schema import get_schema_manager, TableInfo

console = Console()

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])

DEFAULT_LIMIT = 100
DEFAULT_NETWORK = 'mainnet'
OUTPUT_FORMATS = ['table', 'json', 'csv', 'parquet', 'raw']

def parse_slot_range(slot: str) -> Optional[List[int]]:
    """Parse slot parameter which can be a single slot or range (start:end)."""
    if not slot:
        return None
    
    if ':' in slot:
        try:
            start, end = slot.split(':')
            return [int(start), int(end)]
        except ValueError:
            raise click.BadParameter(f"Invalid slot range format: {slot}. Use 'start:end'")
    else:
        try:
            return int(slot)
        except ValueError:
            raise click.BadParameter(f"Invalid slot number: {slot}")

def parse_time_range(time_range: str) -> Optional[List[str]]:
    """Parse time range in ISO format (start:end)."""
    if not time_range:
        return None
    
    if ':' not in time_range:
        raise click.BadParameter("Time range must be in format 'start:end' (ISO 8601)")
    
    try:
        start, end = time_range.split(':')
        datetime.fromisoformat(start.replace('Z', '+00:00'))
        datetime.fromisoformat(end.replace('Z', '+00:00'))
        return [start, end]
    except ValueError as e:
        raise click.BadParameter(f"Invalid time format: {e}")

def parse_columns(columns: str) -> Optional[List[str]]:
    """Parse comma-separated column names."""
    if not columns or columns == '*':
        return None
    return [col.strip() for col in columns.split(',')]

def format_output(df: pd.DataFrame, format: str, output_file: Optional[str] = None):
    """Format and output the DataFrame according to specified format."""
    if df is None or df.empty:
        console.print("[yellow]No data found[/yellow]")
        return
    
    if format == 'table':
        table = Table(box=box.ROUNDED)
        
        for col in df.columns:
            table.add_column(str(col), overflow="fold")
        
        for _, row in df.head(100).iterrows():
            table.add_row(*[str(val) for val in row])
        
        console.print(table)
        if len(df) > 100:
            console.print(f"\n[dim]Showing first 100 of {len(df)} rows[/dim]")
    
    elif format == 'json':
        output = df.to_json(orient='records', indent=2)
        if output_file:
            Path(output_file).write_text(output)
            console.print(f"[green]Data saved to {output_file}[/green]")
        else:
            console.print(output)
    
    elif format == 'csv':
        if output_file:
            df.to_csv(output_file, index=False)
            console.print(f"[green]Data saved to {output_file}[/green]")
        else:
            console.print(df.to_csv(index=False))
    
    elif format == 'parquet':
        if not output_file:
            output_file = f"pyxatu_output_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        df.to_parquet(output_file, index=False)
        console.print(f"[green]Data saved to {output_file}[/green]")
    
    elif format == 'raw':
        console.print(df)

def common_options(f):
    """Decorator for common query options."""
    f = click.option('--slot', '-s', help='Slot number or range (e.g., 1000 or 1000:2000)')(f)
    f = click.option('--time-range', '-t', help='Time range in ISO format (start:end)')(f)
    f = click.option('--columns', '-c', default='*', help='Comma-separated columns or * for all')(f)
    f = click.option('--where', '-w', help='Custom SQL WHERE clause')(f)
    f = click.option('--network', '-n', default=DEFAULT_NETWORK, help=f'Network name (default: {DEFAULT_NETWORK})')(f)
    f = click.option('--limit', '-l', type=int, default=DEFAULT_LIMIT, help=f'Max rows to return (default: {DEFAULT_LIMIT})')(f)
    f = click.option('--orderby', '-o', help='Order by column (prefix with - for DESC)')(f)
    f = click.option('--groupby', '-g', help='Group by column')(f)
    f = click.option('--format', '-f', type=click.Choice(OUTPUT_FORMATS), default='table', help='Output format')(f)
    f = click.option('--output', '-O', help='Output file path (required for parquet format)')(f)
    f = click.option('--config', type=click.Path(exists=True), help='Config file path')(f)
    return f

def execute_query(xatu: PyXatu, method_name: str, **kwargs):
    """Execute a query method with progress indicator and schema validation."""
    format_type = kwargs.pop('format', 'table')
    output_file = kwargs.pop('output', None)
    config = kwargs.pop('config', None)
    
    # Schema validation
    schema_mgr = get_schema_manager()
    validation = schema_mgr.validate_method_params(
        method_name,
        columns=kwargs.get('columns'),
        network=kwargs.get('network')
    )
    
    if not validation['valid']:
        console.print("[red]Validation errors:[/red]")
        for error in validation['errors']:
            console.print(f"  • {error}")
        if validation['suggestions']:
            console.print("\n[yellow]Suggestions:[/yellow]")
            for key, value in validation['suggestions'].items():
                if isinstance(value, list):
                    console.print(f"  {key}: {', '.join(value)}")
                else:
                    console.print(f"  {key}: {value}")
        sys.exit(1)
    
    if validation['warnings']:
        for warning in validation['warnings']:
            console.print(f"[yellow]Warning: {warning}[/yellow]")
    
    kwargs = {k: v for k, v in kwargs.items() if v is not None}
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        transient=True,
    ) as progress:
        progress.add_task(description="Executing query...", total=None)
        
        try:
            method = getattr(xatu, method_name)
            result = method(**kwargs)
            
            if result is not None:
                format_output(result, format_type, output_file)
                console.print(f"\n[dim]Retrieved {len(result)} rows[/dim]")
            else:
                console.print("[red]Query failed[/red]")
        except Exception as e:
            console.print(f"[red]Error: {e}[/red]")
            sys.exit(1)

@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option()
def cli():
    """
    PyXatu CLI - Query Ethereum Beacon Chain Data
    
    A comprehensive tool for accessing and analyzing Ethereum beacon chain data
    with support for slots, attestations, transactions, validators, and more.
    
    Examples:
    
      # Setup configuration
      xatu setup
      
      # Query recent slots
      xatu slots --slot 8000000:8000100 --format json
      
      # Get missed slots with custom output
      xatu missed-slots --limit 50 --output missed.csv --format csv
      
      # Query attestations for specific slot range
      xatu attestations --slot 8000000:8000010 --columns "slot,committee_index,attesting_validator_index"
    """
    pass

@cli.command()
def setup():
    """Initialize PyXatu configuration in home directory."""
    home = Path.home()
    user_config_path = home / '.pyxatu_config.json'
    
    try:
        default_config_file = resources.files('pyxatu.data') / 'config.json'
        
        if not user_config_path.exists():
            shutil.copy(default_config_file, user_config_path)
            console.print(f"[green]✓[/green] Configuration created at {user_config_path}")
            console.print("\n[yellow]Please edit the configuration file with your credentials:[/yellow]")
            console.print(f"  {user_config_path}")
        else:
            console.print(f"[yellow]Configuration already exists at {user_config_path}[/yellow]")
            if click.confirm("Do you want to overwrite it?"):
                shutil.copy(default_config_file, user_config_path)
                console.print("[green]✓[/green] Configuration reset to defaults")
    except Exception as e:
        console.print(f"[red]Error during setup: {e}[/red]")
        sys.exit(1)

@cli.group()
def slots():
    """Query beacon chain slot/block data."""
    pass

@slots.command('query')
@common_options
@click.option('--include-missed', is_flag=True, help='Include missed slots')
def slots_query(slot, time_range, columns, where, network, limit, orderby, groupby, format, output, config, include_missed):
    """Query beacon chain slots.
    
    Common columns: slot, slot_start_date_time, block_root, proposer_index,
    block_total_bytes, execution_payload_blob_gas_used
    """
    xatu = PyXatu(config_path=config)
    execute_query(
        xatu, 'get_slots',
        slot=parse_slot_range(slot),
        time_interval=parse_time_range(time_range),
        columns=parse_columns(columns),
        where=where,
        network=network,
        limit=limit,
        orderby=orderby,
        groupby=groupby,
        include_missed_slots=include_missed,
        format=format,
        output=output,
        config=config
    )

@slots.command('missed')
@common_options
def slots_missed(slot, time_range, columns, where, network, limit, orderby, groupby, format, output, config):
    """Query missed slots only."""
    xatu = PyXatu(config_path=config)
    execute_query(
        xatu, 'get_missed_slots',
        slot=parse_slot_range(slot),
        time_interval=parse_time_range(time_range),
        columns=parse_columns(columns),
        where=where,
        network=network,
        limit=limit,
        orderby=orderby,
        groupby=groupby,
        format=format,
        output=output,
        config=config
    )

@slots.command('reorgs')
@common_options
def slots_reorgs(slot, time_range, columns, where, network, limit, orderby, groupby, format, output, config):
    """Query chain reorganizations."""
    xatu = PyXatu(config_path=config)
    execute_query(
        xatu, 'get_reorgs',
        slot=parse_slot_range(slot),
        time_interval=parse_time_range(time_range),
        columns=parse_columns(columns),
        where=where,
        network=network,
        limit=limit,
        orderby=orderby,
        groupby=groupby,
        format=format,
        output=output,
        config=config
    )

@slots.command('checkpoints')
@common_options
def slots_checkpoints(slot, time_range, columns, where, network, limit, orderby, groupby, format, output, config):
    """Query beacon chain checkpoints."""
    xatu = PyXatu(config_path=config)
    execute_query(
        xatu, 'get_checkpoints',
        slot=parse_slot_range(slot),
        time_interval=parse_time_range(time_range),
        columns=parse_columns(columns),
        where=where,
        network=network,
        limit=limit,
        orderby=orderby,
        groupby=groupby,
        format=format,
        output=output,
        config=config
    )

@slots.command('block-v2')
@common_options
def slots_block_v2(slot, time_range, columns, where, network, limit, orderby, groupby, format, output, config):
    """Query beacon block v2 data."""
    xatu = PyXatu(config_path=config)
    execute_query(
        xatu, 'get_beacon_block_v2',
        slot=parse_slot_range(slot),
        time_interval=parse_time_range(time_range),
        columns=parse_columns(columns),
        where=where,
        network=network,
        limit=limit,
        orderby=orderby,
        groupby=groupby,
        format=format,
        output=output,
        config=config
    )

@slots.command('size')
@common_options
def slots_size(slot, time_range, columns, where, network, limit, orderby, groupby, format, output, config):
    """Query block size metrics including blob counts."""
    xatu = PyXatu(config_path=config)
    execute_query(
        xatu, 'get_block_size',
        slot=parse_slot_range(slot),
        time_interval=parse_time_range(time_range),
        columns=parse_columns(columns),
        where=where,
        network=network,
        limit=limit,
        orderby=orderby,
        groupby=groupby,
        format=format,
        output=output,
        config=config
    )

@cli.group()
def attestations():
    """Query attestation data."""
    pass

@attestations.command('query')
@common_options
def attestations_query(slot, time_range, columns, where, network, limit, orderby, groupby, format, output, config):
    """Query attestation data.
    
    Common columns: slot, validators (array), block_slot, source_root,
    target_root, beacon_block_root
    """
    xatu = PyXatu(config_path=config)
    execute_query(
        xatu, 'get_attestation',
        slot=parse_slot_range(slot),
        time_interval=parse_time_range(time_range),
        columns=parse_columns(columns),
        where=where,
        network=network,
        limit=limit,
        orderby=orderby,
        groupby=groupby,
        format=format,
        output=output,
        config=config
    )

@attestations.command('events')
@common_options
def attestations_events(slot, time_range, columns, where, network, limit, orderby, groupby, format, output, config):
    """Query attestation events."""
    xatu = PyXatu(config_path=config)
    execute_query(
        xatu, 'get_attestation_event',
        slot=parse_slot_range(slot),
        time_interval=parse_time_range(time_range),
        columns=parse_columns(columns),
        where=where,
        network=network,
        limit=limit,
        orderby=orderby,
        groupby=groupby,
        format=format,
        output=output,
        config=config
    )

@attestations.command('elaborated')
@common_options
def attestations_elaborated(slot, time_range, columns, where, network, limit, orderby, groupby, format, output, config):
    """Query detailed attestations with voting status."""
    xatu = PyXatu(config_path=config)
    execute_query(
        xatu, 'get_elaborated_attestations',
        slot=parse_slot_range(slot),
        time_interval=parse_time_range(time_range),
        columns=parse_columns(columns),
        where=where,
        network=network,
        limit=limit,
        orderby=orderby,
        groupby=groupby,
        format=format,
        output=output,
        config=config
    )

@attestations.command('duties')
@common_options
def attestations_duties(slot, time_range, columns, where, network, limit, orderby, groupby, format, output, config):
    """Query validator committee duties."""
    xatu = PyXatu(config_path=config)
    execute_query(
        xatu, 'get_duties',
        slot=parse_slot_range(slot),
        time_interval=parse_time_range(time_range),
        columns=parse_columns(columns),
        where=where,
        network=network,
        limit=limit,
        orderby=orderby,
        groupby=groupby,
        format=format,
        output=output,
        config=config
    )

@cli.group()
def transactions():
    """Query transaction data."""
    pass

@transactions.command('beacon')
@common_options
def transactions_beacon(slot, time_range, columns, where, network, limit, orderby, groupby, format, output, config):
    """Query beacon chain transactions."""
    xatu = PyXatu(config_path=config)
    execute_query(
        xatu, 'get_transactions',
        slot=parse_slot_range(slot),
        time_interval=parse_time_range(time_range),
        columns=parse_columns(columns),
        where=where,
        network=network,
        limit=limit,
        orderby=orderby,
        groupby=groupby,
        format=format,
        output=output,
        config=config
    )

@transactions.command('execution')
@common_options
def transactions_execution(slot, time_range, columns, where, network, limit, orderby, groupby, format, output, config):
    """Query execution layer transactions.
    
    Common columns: slot, hash, block_number, from, to, value, gas_price
    """
    xatu = PyXatu(config_path=config)
    execute_query(
        xatu, 'get_el_transactions',
        slot=parse_slot_range(slot),
        time_interval=parse_time_range(time_range),
        columns=parse_columns(columns),
        where=where,
        network=network,
        limit=limit,
        orderby=orderby,
        groupby=groupby,
        format=format,
        output=output,
        config=config
    )

@transactions.command('mempool')
@common_options
def transactions_mempool(slot, time_range, columns, where, network, limit, orderby, groupby, format, output, config):
    """Query mempool transactions."""
    xatu = PyXatu(config_path=config)
    execute_query(
        xatu, 'get_mempool',
        slot=parse_slot_range(slot),
        time_interval=parse_time_range(time_range),
        columns=parse_columns(columns),
        where=where,
        network=network,
        limit=limit,
        orderby=orderby,
        groupby=groupby,
        format=format,
        output=output,
        config=config
    )

@transactions.command('elaborated')
@common_options
def transactions_elaborated(slot, time_range, columns, where, network, limit, orderby, groupby, format, output, config):
    """Query transactions with privacy analysis."""
    xatu = PyXatu(config_path=config)
    execute_query(
        xatu, 'get_elaborated_transactions',
        slot=parse_slot_range(slot),
        time_interval=parse_time_range(time_range),
        columns=parse_columns(columns),
        where=where,
        network=network,
        limit=limit,
        orderby=orderby,
        groupby=groupby,
        format=format,
        output=output,
        config=config
    )

@transactions.command('withdrawals')
@common_options
def transactions_withdrawals(slot, time_range, columns, where, network, limit, orderby, groupby, format, output, config):
    """Query withdrawal data."""
    xatu = PyXatu(config_path=config)
    execute_query(
        xatu, 'get_withdrawals',
        slot=parse_slot_range(slot),
        time_interval=parse_time_range(time_range),
        columns=parse_columns(columns),
        where=where,
        network=network,
        limit=limit,
        orderby=orderby,
        groupby=groupby,
        format=format,
        output=output,
        config=config
    )

@transactions.command('blobs')
@common_options
def transactions_blobs(slot, time_range, columns, where, network, limit, orderby, groupby, format, output, config):
    """Query blob sidecar data (EIP-4844)."""
    xatu = PyXatu(config_path=config)
    execute_query(
        xatu, 'get_blobs',
        slot=parse_slot_range(slot),
        time_interval=parse_time_range(time_range),
        columns=parse_columns(columns),
        where=where,
        network=network,
        limit=limit,
        orderby=orderby,
        groupby=groupby,
        format=format,
        output=output,
        config=config
    )

@transactions.command('blob-events')
@common_options
def transactions_blob_events(slot, time_range, columns, where, network, limit, orderby, groupby, format, output, config):
    """Query blob sidecar events."""
    xatu = PyXatu(config_path=config)
    execute_query(
        xatu, 'get_blob_events',
        slot=parse_slot_range(slot),
        time_interval=parse_time_range(time_range),
        columns=parse_columns(columns),
        where=where,
        network=network,
        limit=limit,
        orderby=orderby,
        groupby=groupby,
        format=format,
        output=output,
        config=config
    )

@cli.group()
def validators():
    """Query validator data."""
    pass

@validators.command('proposer')
@common_options
def validators_proposer(slot, time_range, columns, where, network, limit, orderby, groupby, format, output, config):
    """Query proposer duty assignments."""
    xatu = PyXatu(config_path=config)
    execute_query(
        xatu, 'get_proposer',
        slot=parse_slot_range(slot),
        time_interval=parse_time_range(time_range),
        columns=parse_columns(columns),
        where=where,
        network=network,
        limit=limit,
        orderby=orderby,
        groupby=groupby,
        format=format,
        output=output,
        config=config
    )

@validators.command('block-events')
@common_options
def validators_block_events(slot, time_range, columns, where, network, limit, orderby, groupby, format, output, config):
    """Query block events."""
    xatu = PyXatu(config_path=config)
    execute_query(
        xatu, 'get_blockevent',
        slot=parse_slot_range(slot),
        time_interval=parse_time_range(time_range),
        columns=parse_columns(columns),
        where=where,
        network=network,
        limit=limit,
        orderby=orderby,
        groupby=groupby,
        format=format,
        output=output,
        config=config
    )

@cli.command()
@click.argument('query_name')
@common_options
def query(query_name, slot, time_range, columns, where, network, limit, orderby, groupby, format, output, config):
    """
    Execute a query by method name (for backwards compatibility).
    
    Examples:
      xatu query get_slots --slot 8000000:8000100
      xatu query get_attestation --limit 50
    """
    xatu = PyXatu(config_path=config)
    
    if not hasattr(xatu, query_name):
        console.print(f"[red]Unknown query method: {query_name}[/red]")
        console.print("\nAvailable methods:")
        methods = [m for m in dir(xatu) if m.startswith('get_') and callable(getattr(xatu, m))]
        for method in sorted(methods):
            console.print(f"  - {method}")
        sys.exit(1)
    
    execute_query(
        xatu, query_name,
        slot=parse_slot_range(slot),
        time_interval=parse_time_range(time_range),
        columns=parse_columns(columns),
        where=where,
        network=network,
        limit=limit,
        orderby=orderby,
        groupby=groupby,
        format=format,
        output=output,
        config=config
    )

@cli.command()
def list_methods():
    """List all available query methods."""
    xatu = PyXatu()
    console.print("[bold]Available PyXatu Query Methods:[/bold]\n")
    
    methods = [m for m in dir(xatu) if m.startswith('get_') and callable(getattr(xatu, m))]
    
    method_groups = {
        'Slots/Blocks': ['get_slots', 'get_missed_slots', 'get_reorgs', 'get_checkpoints', 'get_beacon_block_v2', 'get_block_size'],
        'Attestations': ['get_attestation', 'get_attestation_event', 'get_elaborated_attestations', 'get_duties'],
        'Transactions': ['get_transactions', 'get_el_transactions', 'get_mempool', 'get_elaborated_transactions', 'get_withdrawals', 'get_blobs', 'get_blob_events'],
        'Validators': ['get_proposer', 'get_blockevent']
    }
    
    for group, group_methods in method_groups.items():
        console.print(f"[yellow]{group}:[/yellow]")
        for method in group_methods:
            if method in methods:
                console.print(f"  • {method}")
        console.print()

@cli.command()
@click.argument('slot', type=int)
def slot_info(slot):
    """Get information about a specific slot (timestamp, epoch, etc.)."""
    try:
        timestamp = slot_to_timestamp(slot)
        epoch = slot // 32
        
        console.print(f"\n[bold]Slot {slot} Information:[/bold]")
        console.print(f"  Epoch: {epoch}")
        console.print(f"  Timestamp: {timestamp}")
        console.print(f"  UTC Time: {datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()}")
        console.print(f"  Position in epoch: {slot % 32}/31")
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")

@cli.command()
@click.argument('timestamp', type=str)
def timestamp_to_slot_cmd(timestamp):
    """Convert timestamp to slot number."""
    try:
        if timestamp.isdigit():
            ts = int(timestamp)
        else:
            dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            ts = int(dt.timestamp())
        
        slot = timestamp_to_slot(ts)
        console.print(f"Slot: {slot}")
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")

@cli.command()
@click.option('--network', '-n', help='Filter by network')
@click.option('--method', '-m', help='Filter by PyXatu method name')
def list_tables(network, method):
    """List available tables with their metadata."""
    schema_mgr = get_schema_manager()
    
    if method:
        # Show specific table for method
        table_info = schema_mgr.get_table_by_method(method)
        if table_info:
            console.print(schema_mgr.format_table_info(table_info))
        else:
            console.print(f"[red]No table found for method: {method}[/red]")
            console.print("\nAvailable methods:")
            for m, t in schema_mgr._method_to_table.items():
                console.print(f"  • {m}")
    elif network:
        # Filter by network
        tables = schema_mgr.get_tables_for_network(network)
        if tables:
            console.print(f"[bold]Tables available for {network}:[/bold]\n")
            for table in tables:
                console.print(f"[yellow]{table.table_name}[/yellow]")
                console.print(f"  Method: {table.pyxatu_method}")
                console.print(f"  Description: {table.description}")
                console.print()
        else:
            console.print(f"[red]No tables found for network: {network}[/red]")
    else:
        # List all tables
        all_tables = schema_mgr.get_all_tables()
        console.print(f"[bold]Available Tables ({len(all_tables)} total):[/bold]\n")
        
        # Group by data source
        canonical = []
        beacon_api = []
        mempool = []
        
        for table_name, table_info in all_tables.items():
            if table_name.startswith('canonical_'):
                canonical.append(table_info)
            elif table_name.startswith('beacon_api_'):
                beacon_api.append(table_info)
            elif table_name.startswith('mempool_'):
                mempool.append(table_info)
        
        if canonical:
            console.print("[green]Canonical Tables (Most Reliable):[/green]")
            for table in canonical:
                console.print(f"  • {table.table_name} ({table.pyxatu_method})")
            console.print()
        
        if beacon_api:
            console.print("[yellow]Beacon API Tables (Real-time):[/yellow]")
            for table in beacon_api:
                console.print(f"  • {table.table_name} ({table.pyxatu_method})")
            console.print()
        
        if mempool:
            console.print("[cyan]Mempool Tables:[/cyan]")
            for table in mempool:
                console.print(f"  • {table.table_name} ({table.pyxatu_method})")

@cli.command()
@click.argument('table_name')
def table_info(table_name):
    """Show detailed information about a specific table."""
    schema_mgr = get_schema_manager()
    table_info = schema_mgr.get_table_info(table_name)
    
    if table_info:
        console.print(schema_mgr.format_table_info(table_info))
    else:
        console.print(f"[red]Table not found: {table_name}[/red]")
        
        # Suggest similar tables
        all_tables = schema_mgr.get_all_tables()
        suggestions = [t for t in all_tables.keys() if table_name.lower() in t.lower()]
        if suggestions:
            console.print("\nDid you mean:")
            for suggestion in suggestions[:5]:
                console.print(f"  • {suggestion}")

@cli.command()
@click.argument('query_type')
def suggest_query(query_type):
    """Suggest PyXatu methods for a query type."""
    schema_mgr = get_schema_manager()
    suggestions = schema_mgr.suggest_method_for_query(query_type)
    
    if suggestions:
        console.print(f"[bold]Suggested methods for '{query_type}':[/bold]\n")
        for method in suggestions:
            table_info = schema_mgr.get_table_by_method(method)
            if table_info:
                console.print(f"[yellow]{method}[/yellow]")
                console.print(f"  Table: {table_info.table_name}")
                console.print(f"  Description: {table_info.description}")
                console.print()
    else:
        console.print(f"[yellow]No specific suggestions for '{query_type}'[/yellow]")
        console.print("\nTry one of these query types:")
        console.print("  • slot, block, attestation, validator")
        console.print("  • transaction, withdrawal, blob, reorg")
        console.print("  • duty, committee, proposer")

@cli.group()
def labels():
    """Manage validator entity labels."""
    pass

@labels.command('stats')
def label_stats():
    """Show statistics about validator entities."""
    with PyXatu() as xatu:
        stats = xatu.get_entity_statistics()
        
        if stats.empty:
            console.print("[yellow]No entity statistics available. Run 'xatu labels refresh' first.[/yellow]")
            return
        
        console.print("[bold]Validator Entity Statistics:[/bold]\n")
        
        # Create a table
        table = Table(box=box.ROUNDED)
        table.add_column("Entity", style="cyan")
        table.add_column("Validators", justify="right", style="green")
        table.add_column("Percentage", justify="right")
        
        for _, row in stats.iterrows():
            table.add_row(
                row['entity'],
                f"{row['validator_count']:,}",
                f"{row['percentage']:.2f}%"
            )
        
        console.print(table)
        
        total = stats['validator_count'].sum()
        console.print(f"\n[dim]Total labeled validators: {total:,}[/dim]")

@labels.command('lookup')
@click.argument('validator_indices', nargs=-1, type=int, required=True)
def label_lookup(validator_indices):
    """Look up entity labels for validator indices."""
    with PyXatu() as xatu:
        labels = xatu.get_validator_labels_bulk(list(validator_indices))
        
        console.print("[bold]Validator Labels:[/bold]\n")
        
        for idx in validator_indices:
            label = labels.get(idx)
            if label:
                console.print(f"  {idx}: [green]{label}[/green]")
            else:
                console.print(f"  {idx}: [dim]Unknown[/dim]")

@labels.command('entity')
@click.argument('entity_name')
@click.option('--limit', '-l', type=int, default=10, help='Number of validators to show')
def label_entity(entity_name, limit):
    """Show validators for a specific entity."""
    with PyXatu() as xatu:
        validators = xatu.get_validators_by_entity(entity_name)
        
        if not validators:
            console.print(f"[yellow]No validators found for entity '{entity_name}'[/yellow]")
            return
        
        console.print(f"[bold]Validators for {entity_name}:[/bold]")
        console.print(f"Total: {len(validators):,} validators\n")
        
        # Show sample
        sample = validators[:limit]
        for idx in sample:
            console.print(f"  • {idx}")
        
        if len(validators) > limit:
            console.print(f"\n[dim]... and {len(validators) - limit:,} more[/dim]")

@labels.command('refresh')
def label_refresh():
    """Refresh validator label data from sources."""
    with PyXatu() as xatu:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            transient=True,
        ) as progress:
            progress.add_task(description="Refreshing validator labels...", total=None)
            xatu.refresh_validator_labels()
        
        console.print("[green]✓[/green] Validator labels refreshed successfully")
        
        # Show updated stats
        stats = xatu.get_entity_statistics()
        if not stats.empty:
            total = stats['validator_count'].sum()
            console.print(f"\nTotal labeled validators: {total:,}")
            console.print(f"Total entities: {len(stats)}")

@labels.command('lido-operators')
def label_lido_operators():
    """Show Lido node operators and their validator counts."""
    with PyXatu() as xatu:
        manager = xatu.get_label_manager()
        operators = manager.get_lido_operators()
        
        if not operators:
            console.print("[yellow]No Lido operators found. Run 'xatu labels refresh' first.[/yellow]")
            return
        
        console.print("[bold]Lido Node Operators:[/bold]\n")
        
        # Create a table
        table = Table(box=box.ROUNDED)
        table.add_column("ID", style="dim")
        table.add_column("Operator Name", style="cyan")
        table.add_column("Validators", justify="right", style="green")
        table.add_column("Percentage", justify="right")
        
        total_validators = sum(op['validator_count'] for op in operators)
        
        for op in operators:
            percentage = (op['validator_count'] / total_validators * 100) if total_validators > 0 else 0
            table.add_row(
                str(op['id']),
                op['name'],
                f"{op['validator_count']:,}",
                f"{percentage:.2f}%"
            )
        
        console.print(table)
        console.print(f"\n[dim]Total Lido validators: {total_validators:,}[/dim]")

@cli.command()
@click.argument('method_name')
def show_columns(method_name):
    """Show available columns for a PyXatu method."""
    schema_mgr = get_schema_manager()
    table_info = schema_mgr.get_table_by_method(method_name)
    
    if table_info:
        console.print(f"[bold]Columns for {method_name}:[/bold]\n")
        console.print(f"Table: {table_info.table_name}")
        console.print(f"Partitioned by: {table_info.partitioning_column}\n")
        
        # Create a table for column display
        table = Table(box=box.ROUNDED)
        table.add_column("Column Name", style="cyan")
        table.add_column("Type", style="green")
        table.add_column("Description")
        
        for col in table_info.common_columns:
            table.add_row(col.name, col.type, col.description)
        
        console.print(table)
        
        console.print(f"\n[dim]Use these column names with --columns parameter[/dim]")
        console.print(f"[dim]Example: xatu query {method_name} --columns \"{','.join([c.name for c in table_info.common_columns[:3]])}\"[/dim]")
    else:
        console.print(f"[red]Method not found: {method_name}[/red]")
        console.print("\nUse 'xatu list-methods' to see available methods")

if __name__ == '__main__':
    cli()