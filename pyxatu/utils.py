"""Utility functions and constants for PyXatu."""

from datetime import datetime, timezone, timedelta
from typing import Union, Optional

# Ethereum consensus layer constants
SECONDS_PER_SLOT = 12
SLOTS_PER_EPOCH = 32
GENESIS_TIME = datetime(2020, 12, 1, 12, 0, 23, tzinfo=timezone.utc)

# Blob and gas constants (EIP-4844)
GAS_PER_BLOB = 131072
MAX_BLOBS_PER_BLOCK = 6

# Network genesis times
NETWORK_GENESIS_TIMES = {
    'mainnet': GENESIS_TIME,
    'sepolia': datetime(2022, 6, 20, 22, 0, 0, tzinfo=timezone.utc),
    'holesky': datetime(2023, 9, 28, 12, 0, 0, tzinfo=timezone.utc),
}


def slot_to_timestamp(slot: int, network: str = 'mainnet') -> datetime:
    """Convert a slot number to its corresponding timestamp.
    
    Args:
        slot: The slot number
        network: The network name (mainnet, sepolia, holesky)
        
    Returns:
        The timestamp for the start of the slot
        
    Raises:
        ValueError: If network is not recognized
    """
    genesis = NETWORK_GENESIS_TIMES.get(network)
    if genesis is None:
        raise ValueError(f"Unknown network: {network}")
        
    return genesis + timedelta(seconds=slot * SECONDS_PER_SLOT)


def timestamp_to_slot(timestamp: Union[datetime, int], network: str = 'mainnet') -> int:
    """Convert a timestamp to its corresponding slot number.
    
    Args:
        timestamp: The timestamp (datetime or unix timestamp)
        network: The network name
        
    Returns:
        The slot number
        
    Raises:
        ValueError: If network is not recognized or timestamp is before genesis
    """
    genesis = NETWORK_GENESIS_TIMES.get(network)
    if genesis is None:
        raise ValueError(f"Unknown network: {network}")
        
    if isinstance(timestamp, int):
        timestamp = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    elif timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
        
    if timestamp < genesis:
        raise ValueError(f"Timestamp {timestamp} is before genesis time {genesis}")
        
    delta = timestamp - genesis
    return int(delta.total_seconds() // SECONDS_PER_SLOT)


def slot_to_epoch(slot: int) -> int:
    """Convert a slot number to its epoch number.
    
    Args:
        slot: The slot number
        
    Returns:
        The epoch number
    """
    return slot // SLOTS_PER_EPOCH


def epoch_to_slot(epoch: int) -> int:
    """Convert an epoch number to its starting slot.
    
    Args:
        epoch: The epoch number
        
    Returns:
        The first slot of the epoch
    """
    return epoch * SLOTS_PER_EPOCH


def get_epoch_boundary_slots(slot: int) -> tuple[int, int]:
    """Get the start and end slots for the epoch containing the given slot.
    
    Args:
        slot: Any slot in the epoch
        
    Returns:
        Tuple of (epoch_start_slot, epoch_end_slot)
    """
    epoch = slot_to_epoch(slot)
    start = epoch_to_slot(epoch)
    end = start + SLOTS_PER_EPOCH - 1
    return start, end


def calculate_inclusion_delay(attestation_slot: int, inclusion_slot: int) -> int:
    """Calculate the inclusion delay for an attestation.
    
    Args:
        attestation_slot: The slot where the attestation was created
        inclusion_slot: The slot where the attestation was included
        
    Returns:
        The inclusion delay in slots
        
    Raises:
        ValueError: If inclusion slot is before attestation slot
    """
    if inclusion_slot < attestation_slot:
        raise ValueError(
            f"Inclusion slot {inclusion_slot} cannot be before "
            f"attestation slot {attestation_slot}"
        )
    return inclusion_slot - attestation_slot


def format_slot_time(slot: int, network: str = 'mainnet') -> str:
    """Format a slot number as a human-readable timestamp.
    
    Args:
        slot: The slot number
        network: The network name
        
    Returns:
        Formatted timestamp string
    """
    timestamp = slot_to_timestamp(slot, network)
    return timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')


def get_finalized_epoch(current_slot: int) -> int:
    """Get the last finalized epoch given the current slot.
    
    Finalization happens 2 epochs behind the current epoch.
    
    Args:
        current_slot: The current slot number
        
    Returns:
        The last finalized epoch number
    """
    current_epoch = slot_to_epoch(current_slot)
    return max(0, current_epoch - 2)


def is_epoch_boundary(slot: int) -> bool:
    """Check if a slot is at an epoch boundary.
    
    Args:
        slot: The slot number
        
    Returns:
        True if the slot is the first slot of an epoch
    """
    return slot % SLOTS_PER_EPOCH == 0


def get_committee_index_count() -> int:
    """Get the maximum number of committees per slot.
    
    Returns:
        Maximum committee count (64 for mainnet)
    """
    return 64


def estimate_validators_per_slot(total_validators: int) -> int:
    """Estimate the number of validators assigned per slot.
    
    Args:
        total_validators: Total number of active validators
        
    Returns:
        Estimated validators per slot
    """
    return total_validators // SLOTS_PER_EPOCH