"""PyXatu - Python client for querying Ethereum beacon chain data."""

# Import the synchronous version by default for simplicity
from pyxatu.pyxatu_sync import PyXatu
# Keep async version available for those who need it
from pyxatu.pyxatu import PyXatu as PyXatuAsync
from pyxatu.models import (
    Network, VoteType, AttestationStatus,
    Block, Attestation, Transaction, Withdrawal
)
from pyxatu.config import PyXatuConfig, ConfigManager

__version__ = "1.9.1"
__author__ = "PyXatu Contributors"

__all__ = [
    # Main classes
    "PyXatu",  # Synchronous version (default)
    "PyXatuAsync",  # Async version for advanced use
    
    # Enums
    "Network",
    "VoteType", 
    "AttestationStatus",
    
    # Data models
    "Block",
    "Attestation",
    "Transaction",
    "Withdrawal",
    
    # Configuration
    "PyXatuConfig",
    "ConfigManager",
]

# Module-level docstring for help()
__doc__ = """
PyXatu - Query Ethereum blockchain data from Xatu

Quick Start:
    from pyxatu import PyXatu
    
    with PyXatu() as xatu:
        # Get recent blocks
        blocks = xatu.get_slots(slot=[9000000, 9000010])
        print(blocks)

Environment Variables:
    - CLICKHOUSE_URL or PYXATU_CLICKHOUSE_URL
    - CLICKHOUSE_USER or PYXATU_CLICKHOUSE_USER  
    - CLICKHOUSE_PASSWORD or PYXATU_CLICKHOUSE_PASSWORD
    - PYXATU_LOG_LEVEL (DEBUG, INFO, WARNING, ERROR)

For more information, see the documentation at:
https://github.com/nerolation/pyxatu
"""