"""
PyXatu - Python interface for the Xatu ClickHouse database.

This package provides a comprehensive interface for querying Ethereum beacon chain
and execution layer data from the Xatu database.
"""

# Main API - maintain backward compatibility
from .core import PyXatu

# Core components
from .core import ClickhouseClient, DataRetriever

# External integrations  
from .integrations import ValidatorGadget, MempoolConnector, MevBoostCaller, DocsScraper

# Utilities
from .utils import CONSTANTS, PyXatuHelpers, retry_on_failure

# CLI
from .cli import cli

__version__ = "1.9.1"

__all__ = [
    # Main API
    'PyXatu',
    
    # Core components
    'ClickhouseClient', 
    'DataRetriever',
    
    # Integrations
    'ValidatorGadget', 
    'MempoolConnector', 
    'MevBoostCaller', 
    'DocsScraper',
    
    # Utilities
    'CONSTANTS', 
    'PyXatuHelpers', 
    'retry_on_failure',
    
    # CLI
    'cli',
    
    # Version
    '__version__'
]