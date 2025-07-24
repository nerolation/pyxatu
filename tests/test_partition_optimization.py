"""Test partition optimization for ClickHouse queries."""

import pytest
from datetime import datetime, timezone

from pyxatu.clickhouse_client import ClickHouseQueryBuilder
from pyxatu.utils import slot_to_timestamp


class TestPartitionOptimization:
    """Test that queries include partition key filters for performance."""
    
    def test_single_slot_partition_filter(self):
        """Test partition filter for single slot query."""
        builder = ClickHouseQueryBuilder()
        
        slot = 9000000
        query, params = (
            builder
            .select('*')
            .from_table('canonical_beacon_block')
            .where_slot_with_partition(slot)
            .build()
        )
        
        # Check slot filter is present
        assert 'slot = %(param_0)s' in query
        assert params['param_0'] == slot
        
        # Check partition filter is present
        assert 'slot_start_date_time >=' in query
        assert 'slot_start_date_time <=' in query
        assert 'INTERVAL 1 MINUTE' in query
        
        # Verify the timestamp is correct
        expected_time = slot_to_timestamp(slot)
        expected_str = expected_time.strftime('%Y-%m-%d %H:%M:%S')
        assert expected_str in query
        
    def test_slot_range_partition_filter(self):
        """Test partition filter for slot range query."""
        builder = ClickHouseQueryBuilder()
        
        start_slot = 9000000
        end_slot = 9001000
        
        query, params = (
            builder
            .select('*')
            .from_table('canonical_beacon_block')
            .where_slot_with_partition(start_slot, end_slot)
            .build()
        )
        
        # Check slot range filter
        assert 'slot BETWEEN %(param_0)s AND %(param_1)s' in query
        assert params['param_0'] == start_slot
        assert params['param_1'] == end_slot - 1
        
        # Check partition filter is present
        assert 'slot_start_date_time >=' in query
        assert 'slot_start_date_time <=' in query
        assert 'INTERVAL 1 HOUR' in query  # Larger buffer for ranges
        
        # Verify timestamps
        start_time = slot_to_timestamp(start_slot)
        end_time = slot_to_timestamp(end_slot)
        assert start_time.strftime('%Y-%m-%d %H:%M:%S') in query
        assert end_time.strftime('%Y-%m-%d %H:%M:%S') in query
        
    def test_partition_filter_prevents_full_scan(self):
        """Test that partition filter is always added with slot filter."""
        builder = ClickHouseQueryBuilder()
        
        # Large slot range that would scan tons of data without partition filter
        start_slot = 1000000
        end_slot = 9000000  # 8 million slots!
        
        query, _ = (
            builder
            .select('COUNT(*)')
            .from_table('canonical_beacon_block')
            .where_slot_with_partition(start_slot, end_slot)
            .where('meta_network_name', '=', 'mainnet')
            .build()
        )
        
        # Ensure partition filter is present to avoid full scan
        assert 'slot_start_date_time >=' in query
        assert 'slot_start_date_time <=' in query
        
        # Both filters should be present
        assert 'slot BETWEEN' in query
        assert 'meta_network_name' in query
        
    def test_partition_optimization_with_complex_query(self):
        """Test partition optimization works with complex queries."""
        builder = ClickHouseQueryBuilder()
        
        query, params = (
            builder
            .select(['slot', 'proposer_index', 'COUNT(*) as count'])
            .from_table('canonical_beacon_block')
            .where_slot_with_partition(9000000, 9001000)
            .where('meta_network_name', '=', 'mainnet')
            .where('proposer_index', '>', 0)
            .group_by(['slot', 'proposer_index'])
            .order_by('count', desc=True)
            .limit(100)
            .build()
        )
        
        # All components should be present
        assert 'slot BETWEEN' in query
        assert 'slot_start_date_time' in query
        assert 'meta_network_name' in query
        assert 'proposer_index >' in query
        assert 'GROUP BY' in query
        assert 'ORDER BY' in query
        assert 'LIMIT' in query
        
        # Partition filter should come early in WHERE clause
        where_clause = query.split('WHERE')[1].split('GROUP BY')[0]
        # Partition filter should be among the first conditions
        assert 'slot_start_date_time' in where_clause
        
    def test_no_partition_filter_without_slot(self):
        """Test that queries without slot filter don't add partition filter."""
        builder = ClickHouseQueryBuilder()
        
        query, params = (
            builder
            .select('*')
            .from_table('canonical_beacon_block')
            .where('meta_network_name', '=', 'mainnet')
            .limit(10)
            .build()
        )
        
        # No partition filter should be added
        assert 'slot_start_date_time' not in query
        # But network filter should be present
        assert 'meta_network_name' in query
        
    def test_partition_filter_edge_cases(self):
        """Test partition filter handles edge cases correctly."""
        builder = ClickHouseQueryBuilder()
        
        # Test with slot 0 (genesis)
        query, _ = (
            builder
            .select('*')
            .from_table('canonical_beacon_block')
            .where_slot_with_partition(0)
            .build()
        )
        
        assert 'slot = %(param_0)s' in query
        assert 'slot_start_date_time' in query
        assert '2020-12-01' in query  # Genesis date
        
        # Test with very large slot
        builder.reset()
        large_slot = 10_000_000
        query, _ = (
            builder
            .select('*')
            .from_table('canonical_beacon_block')
            .where_slot_with_partition(large_slot)
            .build()
        )
        
        assert 'slot = %(param_0)s' in query
        assert 'slot_start_date_time' in query
        
    def test_multiple_tables_with_partition(self):
        """Test partition optimization works across different tables."""
        tables = [
            'canonical_beacon_block',
            'canonical_beacon_elaborated_attestation',
            'beacon_api_eth_v1_events_attestation',
            'canonical_beacon_block_execution_transaction',
            'canonical_beacon_proposer_duty'
        ]
        
        for table in tables:
            builder = ClickHouseQueryBuilder()
            query, _ = (
                builder
                .select('*')
                .from_table(table)
                .where_slot_with_partition(9000000)
                .build()
            )
            
            assert f'FROM {table}' in query
            assert 'slot = %(param_0)s' in query
            assert 'slot_start_date_time' in query