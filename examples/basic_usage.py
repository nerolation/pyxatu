"""Basic usage examples for the PyXatu library."""

import asyncio
from pyxatu import PyXatu
from pyxatu.models import Network


async def basic_slot_queries():
    """Demonstrate basic slot/block queries."""
    # Initialize PyXatu with environment variables
    async with PyXatu(use_env_vars=True) as xatu:
        print("=== Basic Slot Queries ===\n")
        
        # Get recent slots
        recent_slots = await xatu.get_slots(
            slot=[9000000, 9000010],
            columns="slot,proposer_index,block_root",
            network=Network.MAINNET,
            include_missed=True,
            orderby="slot"
        )
        print(f"Found {len(recent_slots)} slots (including missed)")
        print(recent_slots.head())
        
        # Get missed slots in a range
        missed = await xatu.get_missed_slots(
            slot_range=[9000000, 9000100],
            network=Network.MAINNET
        )
        print(f"\nMissed slots: {missed[:5]}...")
        
        # Check for reorgs
        reorgs = await xatu.get_reorgs(
            slot=[9000000, 9001000],
            network=Network.MAINNET
        )
        print(f"\nFound {len(reorgs)} reorgs")


async def attestation_analysis():
    """Demonstrate attestation performance analysis."""
    async with PyXatu(use_env_vars=True) as xatu:
        print("\n=== Attestation Analysis ===\n")
        
        # Get basic attestation data
        attestations = await xatu.get_attestations(
            slot=9000000,
            columns="slot,committee_index,validators",
            limit=10
        )
        print(f"Found {len(attestations)} attestation entries")
        
        # Get elaborated attestation performance
        performance = await xatu.get_elaborated_attestations(
            slot=[9000000, 9000005],
            vote_types=['source', 'target', 'head'],
            status_filter=['correct', 'failed'],
            include_delay=True
        )
        print(f"\nAttestation performance data: {len(performance)} entries")
        
        # Analyze by status
        if not performance.empty:
            status_counts = performance['status'].value_counts()
            print("\nAttestation status distribution:")
            print(status_counts)


async def transaction_privacy_analysis():
    """Demonstrate transaction privacy analysis."""
    async with PyXatu(use_env_vars=True) as xatu:
        print("\n=== Transaction Privacy Analysis ===\n")
        
        # Get transactions for specific slots
        transactions = await xatu.get_transactions(
            slot=[9000000, 9000002],
            columns="slot,hash,from_address,value",
            limit=20
        )
        print(f"Found {len(transactions)} transactions")
        
        # Analyze transaction privacy (private vs public mempool)
        # Note: This requires mempool data sources to be configured
        elaborated_txs = await xatu.get_elaborated_transactions(
            slots=[9000000, 9000001],
            include_external_mempool=False  # Only use Xatu mempool data
        )
        
        if not elaborated_txs.empty:
            private_count = elaborated_txs['private'].sum()
            total_count = len(elaborated_txs)
            print(f"\nPrivate transactions: {private_count}/{total_count} "
                  f"({private_count/total_count*100:.1f}%)")


async def block_metrics():
    """Demonstrate block size and metrics queries."""
    async with PyXatu(use_env_vars=True) as xatu:
        print("\n=== Block Metrics ===\n")
        
        # Get block sizes including blob data
        block_sizes = await xatu.get_block_sizes(
            slot=[9000000, 9000010],
            orderby="-blobs"  # Order by blob count descending
        )
        
        if not block_sizes.empty:
            print("Block size metrics:")
            print(f"Average compressed size: {block_sizes['block_total_bytes_compressed'].mean():.0f} bytes")
            print(f"Max blob count: {block_sizes['blobs'].max()}")
            
        # Get withdrawal data
        withdrawals = await xatu.get_withdrawals(
            slot=[9000000, 9000100],
            columns="slot,validator_index,amount",
            limit=10
        )
        print(f"\nFound {len(withdrawals)} withdrawals")


async def custom_queries():
    """Demonstrate custom query execution (use with caution)."""
    async with PyXatu(use_env_vars=True) as xatu:
        print("\n=== Custom Queries ===\n")
        
        # Get available columns for a table
        columns = await xatu.get_table_columns('canonical_beacon_block')
        print(f"Available columns in canonical_beacon_block: {len(columns)}")
        print(f"First 5 columns: {columns[:5]}")
        
        # Execute a custom query with parameters (safe from SQL injection)
        custom_result = await xatu.execute_query(
            """
            SELECT 
                slot,
                proposer_index,
                COUNT(*) as count
            FROM canonical_beacon_block
            WHERE slot BETWEEN %(start_slot)s AND %(end_slot)s
              AND meta_network_name = %(network)s
            GROUP BY slot, proposer_index
            ORDER BY count DESC
            LIMIT 5
            """,
            params={
                'start_slot': 9000000,
                'end_slot': 9001000,
                'network': 'mainnet'
            }
        )
        print(f"\nTop proposers by block count:")
        print(custom_result)


async def main():
    """Run all examples."""
    examples = [
        basic_slot_queries,
        attestation_analysis,
        transaction_privacy_analysis,
        block_metrics,
        custom_queries
    ]
    
    for example in examples:
        try:
            await example()
        except Exception as e:
            print(f"\nError in {example.__name__}: {e}")
            print("Make sure you have configured your ClickHouse credentials!")
    
    print("\n=== Examples Complete ===")


if __name__ == "__main__":
    # Run the examples
    asyncio.run(main())