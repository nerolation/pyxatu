"""Tests for utility functions."""

import pytest
from datetime import datetime, timezone, timedelta

from pyxatu.utils import (
    SECONDS_PER_SLOT, SLOTS_PER_EPOCH, GENESIS_TIME,
    GAS_PER_BLOB, MAX_BLOBS_PER_BLOCK, NETWORK_GENESIS_TIMES,
    slot_to_timestamp, timestamp_to_slot,
    slot_to_epoch, epoch_to_slot, get_epoch_boundary_slots,
    calculate_inclusion_delay, format_slot_time,
    get_finalized_epoch, is_epoch_boundary,
    get_committee_index_count, estimate_validators_per_slot
)


class TestConstants:
    """Test that constants have expected values."""
    
    def test_consensus_constants(self):
        """Test Ethereum consensus layer constants."""
        assert SECONDS_PER_SLOT == 12
        assert SLOTS_PER_EPOCH == 32
        assert isinstance(GENESIS_TIME, datetime)
        assert GENESIS_TIME.tzinfo == timezone.utc
        
    def test_blob_constants(self):
        """Test EIP-4844 blob constants."""
        assert GAS_PER_BLOB == 131072
        assert MAX_BLOBS_PER_BLOCK == 6
        
    def test_network_genesis_times(self):
        """Test network genesis times are defined."""
        assert 'mainnet' in NETWORK_GENESIS_TIMES
        assert 'sepolia' in NETWORK_GENESIS_TIMES
        assert 'holesky' in NETWORK_GENESIS_TIMES
        
        # All should be timezone-aware
        for network, genesis in NETWORK_GENESIS_TIMES.items():
            assert isinstance(genesis, datetime)
            assert genesis.tzinfo == timezone.utc


class TestSlotTimestampConversion:
    """Test slot/timestamp conversion functions."""
    
    def test_slot_to_timestamp_mainnet(self):
        """Test converting slots to timestamps on mainnet."""
        # Slot 0 should be genesis time
        assert slot_to_timestamp(0) == GENESIS_TIME
        
        # Slot 1 should be 12 seconds after genesis
        assert slot_to_timestamp(1) == GENESIS_TIME + timedelta(seconds=12)
        
        # Slot 32 (epoch 1) should be 384 seconds after genesis
        assert slot_to_timestamp(32) == GENESIS_TIME + timedelta(seconds=384)
        
        # Large slot number
        slot_1m = slot_to_timestamp(1000000)
        expected = GENESIS_TIME + timedelta(seconds=1000000 * 12)
        assert slot_1m == expected
        
    def test_slot_to_timestamp_other_networks(self):
        """Test slot to timestamp conversion on other networks."""
        # Sepolia
        sepolia_slot_0 = slot_to_timestamp(0, 'sepolia')
        assert sepolia_slot_0 == NETWORK_GENESIS_TIMES['sepolia']
        
        sepolia_slot_100 = slot_to_timestamp(100, 'sepolia')
        expected = NETWORK_GENESIS_TIMES['sepolia'] + timedelta(seconds=100 * 12)
        assert sepolia_slot_100 == expected
        
        # Holesky
        holesky_slot_0 = slot_to_timestamp(0, 'holesky')
        assert holesky_slot_0 == NETWORK_GENESIS_TIMES['holesky']
        
    def test_slot_to_timestamp_invalid_network(self):
        """Test error handling for invalid network."""
        with pytest.raises(ValueError, match="Unknown network"):
            slot_to_timestamp(0, 'invalid_network')
            
    def test_timestamp_to_slot_mainnet(self):
        """Test converting timestamps to slots on mainnet."""
        # Genesis time should be slot 0
        assert timestamp_to_slot(GENESIS_TIME) == 0
        
        # 12 seconds after genesis should be slot 1
        assert timestamp_to_slot(GENESIS_TIME + timedelta(seconds=12)) == 1
        
        # 1 hour after genesis
        seconds_in_hour = 3600
        expected_slot = seconds_in_hour // 12
        assert timestamp_to_slot(GENESIS_TIME + timedelta(hours=1)) == expected_slot
        
    def test_timestamp_to_slot_unix_timestamp(self):
        """Test converting unix timestamps to slots."""
        # Convert genesis to unix timestamp
        genesis_unix = int(GENESIS_TIME.timestamp())
        assert timestamp_to_slot(genesis_unix) == 0
        
        # 1000 seconds after genesis
        assert timestamp_to_slot(genesis_unix + 1000) == 1000 // 12
        
    def test_timestamp_to_slot_naive_datetime(self):
        """Test converting naive datetime to slot."""
        # Create naive datetime (no timezone)
        naive_dt = GENESIS_TIME.replace(tzinfo=None)
        
        # Should work but assume UTC
        assert timestamp_to_slot(naive_dt) == 0
        
    def test_timestamp_to_slot_before_genesis(self):
        """Test error handling for timestamps before genesis."""
        before_genesis = GENESIS_TIME - timedelta(seconds=1)
        
        with pytest.raises(ValueError, match="before genesis"):
            timestamp_to_slot(before_genesis)
            
    def test_round_trip_conversion(self):
        """Test that slot->timestamp->slot is consistent."""
        test_slots = [0, 1, 32, 100, 1000, 1000000]
        
        for slot in test_slots:
            timestamp = slot_to_timestamp(slot)
            converted_back = timestamp_to_slot(timestamp)
            assert converted_back == slot


class TestEpochConversion:
    """Test epoch-related conversion functions."""
    
    def test_slot_to_epoch(self):
        """Test converting slots to epochs."""
        # First epoch (0)
        assert slot_to_epoch(0) == 0
        assert slot_to_epoch(31) == 0
        
        # Second epoch (1)
        assert slot_to_epoch(32) == 1
        assert slot_to_epoch(63) == 1
        
        # Large slot numbers
        assert slot_to_epoch(1000) == 31
        assert slot_to_epoch(32000) == 1000
        
    def test_epoch_to_slot(self):
        """Test converting epochs to starting slots."""
        assert epoch_to_slot(0) == 0
        assert epoch_to_slot(1) == 32
        assert epoch_to_slot(2) == 64
        assert epoch_to_slot(100) == 3200
        assert epoch_to_slot(1000) == 32000
        
    def test_get_epoch_boundary_slots(self):
        """Test getting epoch boundary slots."""
        # Epoch 0
        start, end = get_epoch_boundary_slots(0)
        assert start == 0
        assert end == 31
        
        start, end = get_epoch_boundary_slots(15)  # Middle of epoch 0
        assert start == 0
        assert end == 31
        
        # Epoch 1
        start, end = get_epoch_boundary_slots(32)
        assert start == 32
        assert end == 63
        
        start, end = get_epoch_boundary_slots(50)  # Middle of epoch 1
        assert start == 32
        assert end == 63
        
        # Large epoch
        start, end = get_epoch_boundary_slots(10000)
        expected_epoch = 10000 // 32
        assert start == expected_epoch * 32
        assert end == start + 31
        
    def test_is_epoch_boundary(self):
        """Test epoch boundary detection."""
        # Epoch boundaries
        assert is_epoch_boundary(0) is True
        assert is_epoch_boundary(32) is True
        assert is_epoch_boundary(64) is True
        assert is_epoch_boundary(320) is True
        
        # Not epoch boundaries
        assert is_epoch_boundary(1) is False
        assert is_epoch_boundary(31) is False
        assert is_epoch_boundary(33) is False
        assert is_epoch_boundary(100) is False


class TestAttestationHelpers:
    """Test attestation-related helper functions."""
    
    def test_calculate_inclusion_delay(self):
        """Test inclusion delay calculation."""
        # Immediate inclusion (same slot)
        assert calculate_inclusion_delay(1000, 1000) == 0
        
        # Normal delays
        assert calculate_inclusion_delay(1000, 1001) == 1
        assert calculate_inclusion_delay(1000, 1005) == 5
        assert calculate_inclusion_delay(1000, 1032) == 32
        
    def test_calculate_inclusion_delay_invalid(self):
        """Test error handling for invalid inclusion delays."""
        with pytest.raises(ValueError, match="cannot be before"):
            calculate_inclusion_delay(1000, 999)
            
    def test_get_finalized_epoch(self):
        """Test finalized epoch calculation."""
        # Epoch 0-1 (no finalization yet)
        assert get_finalized_epoch(0) == 0
        assert get_finalized_epoch(31) == 0
        assert get_finalized_epoch(32) == 0
        assert get_finalized_epoch(63) == 0
        
        # Epoch 2 (epoch 0 can be finalized)
        assert get_finalized_epoch(64) == 0
        assert get_finalized_epoch(95) == 0
        
        # Epoch 3 (epoch 1 can be finalized)
        assert get_finalized_epoch(96) == 1
        assert get_finalized_epoch(127) == 1
        
        # Large slot numbers
        assert get_finalized_epoch(10000) == 10000 // 32 - 2
        
        # Finalization is always 2 epochs behind
        for slot in [100, 200, 500, 1000, 5000]:
            current_epoch = slot_to_epoch(slot)
            finalized = get_finalized_epoch(slot)
            assert current_epoch - finalized >= 2


class TestFormattingFunctions:
    """Test formatting helper functions."""
    
    def test_format_slot_time(self):
        """Test slot time formatting."""
        # Slot 0 (genesis)
        formatted = format_slot_time(0)
        assert "2020-12-01 12:00:23 UTC" in formatted
        
        # Some slot in the future
        formatted = format_slot_time(1000000)
        assert "UTC" in formatted
        assert formatted.count(':') == 2  # HH:MM:SS format
        assert formatted.count('-') == 2  # YYYY-MM-DD format
        
    def test_format_slot_time_other_networks(self):
        """Test slot time formatting for other networks."""
        # Sepolia slot 0
        formatted = format_slot_time(0, 'sepolia')
        assert "2022-06-20" in formatted
        
        # Holesky slot 0
        formatted = format_slot_time(0, 'holesky')
        assert "2023-09-28" in formatted


class TestValidatorHelpers:
    """Test validator-related helper functions."""
    
    def test_get_committee_index_count(self):
        """Test committee index count."""
        assert get_committee_index_count() == 64
        
    def test_estimate_validators_per_slot(self):
        """Test validator per slot estimation."""
        # With 32,000 validators (minimum for mainnet)
        assert estimate_validators_per_slot(32000) == 1000
        
        # With 500,000 validators (approximate mainnet size)
        assert estimate_validators_per_slot(500000) == 15625
        
        # With 1,000,000 validators
        assert estimate_validators_per_slot(1000000) == 31250
        
        # Edge case: fewer validators than slots per epoch
        assert estimate_validators_per_slot(10) == 0


class TestEdgeCases:
    """Test edge cases and boundary conditions."""
    
    def test_zero_slots_and_epochs(self):
        """Test handling of zero values."""
        assert slot_to_epoch(0) == 0
        assert epoch_to_slot(0) == 0
        assert slot_to_timestamp(0) == GENESIS_TIME
        assert is_epoch_boundary(0) is True
        
    def test_large_values(self):
        """Test handling of large slot/epoch numbers."""
        large_slot = 10**9  # 1 billion
        
        # Should not raise errors
        timestamp = slot_to_timestamp(large_slot)
        epoch = slot_to_epoch(large_slot)
        
        assert isinstance(timestamp, datetime)
        assert isinstance(epoch, int)
        assert epoch == large_slot // 32
        
    def test_slot_timestamp_precision(self):
        """Test that slot timestamps are precise to the second."""
        slot = 12345
        timestamp = slot_to_timestamp(slot)
        
        # Should have no microseconds
        assert timestamp.microsecond == 0
        
        # Should be exactly slot * 12 seconds after genesis
        expected = GENESIS_TIME + timedelta(seconds=slot * 12)
        assert timestamp == expected