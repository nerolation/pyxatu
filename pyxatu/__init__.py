"""PyXatu - Secure and efficient Ethereum blockchain data queries.

Key features:
- Security: No eval(), parameterized queries, input validation
- Performance: Async operations, connection pooling, caching
- Simplicity: Clean API, modular design, type safety
- Reliability: Comprehensive error handling, retry logic
"""

from pyxatu.pyxatu import PyXatu
from pyxatu.models import (
    Network, VoteType, AttestationStatus,
    Block, Attestation, Transaction, Withdrawal
)
from pyxatu.config import PyXatuConfig, ConfigManager

__version__ = "1.9.1"
__author__ = "PyXatu Contributors"

__all__ = [
    # Main class
    "PyXatu",
    
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
    import asyncio
    from pyxatu import PyXatu
    
    async def main():
        async with PyXatu() as xatu:
            # Get recent blocks
            blocks = await xatu.get_slots(slot=[9000000, 9000010])
            print(blocks)
    
    asyncio.run(main())

Environment Variables:
    - CLICKHOUSE_URL or PYXATU_CLICKHOUSE_URL
    - CLICKHOUSE_USER or PYXATU_CLICKHOUSE_USER  
    - CLICKHOUSE_PASSWORD or PYXATU_CLICKHOUSE_PASSWORD
    - PYXATU_LOG_LEVEL (DEBUG, INFO, WARNING, ERROR)

For more information, see the documentation at:
https://github.com/nerolation/pyxatu
"""