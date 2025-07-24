"""Base classes and interfaces for PyXatu components."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, TypeVar, Generic
import logging
from datetime import datetime

from pyxatu.models import QueryParams, Network


T = TypeVar('T')


class BaseClient(ABC):
    """Abstract base class for database clients."""
    
    @abstractmethod
    async def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute a query and return results."""
        pass
    
    @abstractmethod
    async def execute_query_df(self, query: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Execute a query and return results as DataFrame."""
        pass
    
    @abstractmethod
    async def close(self) -> None:
        """Close the client connection."""
        pass


class BaseDataFetcher(ABC, Generic[T]):
    """Abstract base class for data fetchers."""
    
    def __init__(self, client: BaseClient):
        self.client = client
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @abstractmethod
    async def fetch(self, params: QueryParams) -> List[T]:
        """Fetch data based on query parameters."""
        pass
    
    @abstractmethod
    def get_table_name(self) -> str:
        """Return the table name this fetcher queries."""
        pass


class BaseConnector(ABC):
    """Abstract base class for external data connectors."""
    
    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to external service."""
        pass
    
    @abstractmethod
    async def disconnect(self) -> None:
        """Close connection to external service."""
        pass
    
    @abstractmethod
    async def fetch_data(self, **kwargs) -> Any:
        """Fetch data from external service."""
        pass


class CacheManager(ABC):
    """Abstract base class for cache management."""
    
    @abstractmethod
    async def get(self, key: str) -> Optional[Any]:
        """Retrieve value from cache."""
        pass
    
    @abstractmethod
    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Store value in cache with optional TTL."""
        pass
    
    @abstractmethod
    async def delete(self, key: str) -> None:
        """Remove value from cache."""
        pass
    
    @abstractmethod
    async def clear(self) -> None:
        """Clear all cached values."""
        pass


class ConfigProvider(ABC):
    """Abstract base class for configuration management."""
    
    @abstractmethod
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value."""
        pass
    
    @abstractmethod
    def set(self, key: str, value: Any) -> None:
        """Set configuration value."""
        pass
    
    @abstractmethod
    def validate(self) -> bool:
        """Validate configuration."""
        pass


class BaseValidator(ABC):
    """Abstract base class for data validators."""
    
    @abstractmethod
    def validate(self, data: Any) -> bool:
        """Validate data according to rules."""
        pass
    
    @abstractmethod
    def sanitize(self, data: Any) -> Any:
        """Sanitize data for safe usage."""
        pass


class QueryBuilder(ABC):
    """Abstract base class for SQL query builders."""
    
    @abstractmethod
    def select(self, columns: List[str]) -> 'QueryBuilder':
        """Add SELECT clause."""
        pass
    
    @abstractmethod
    def from_table(self, table: str) -> 'QueryBuilder':
        """Add FROM clause."""
        pass
    
    @abstractmethod
    def where(self, condition: str, params: Optional[Dict[str, Any]] = None) -> 'QueryBuilder':
        """Add WHERE condition."""
        pass
    
    @abstractmethod
    def order_by(self, column: str, desc: bool = False) -> 'QueryBuilder':
        """Add ORDER BY clause."""
        pass
    
    @abstractmethod
    def limit(self, count: int) -> 'QueryBuilder':
        """Add LIMIT clause."""
        pass
    
    @abstractmethod
    def build(self) -> tuple[str, Dict[str, Any]]:
        """Build the query and return query string with parameters."""
        pass


class DataProcessor(ABC, Generic[T]):
    """Abstract base class for data processing pipelines."""
    
    @abstractmethod
    async def process(self, data: List[T]) -> List[T]:
        """Process data through the pipeline."""
        pass
    
    @abstractmethod
    def add_step(self, step: 'ProcessingStep[T]') -> 'DataProcessor[T]':
        """Add a processing step to the pipeline."""
        pass


class ProcessingStep(ABC, Generic[T]):
    """Abstract base class for individual processing steps."""
    
    @abstractmethod
    async def execute(self, data: T) -> T:
        """Execute the processing step on data."""
        pass


class ResultFormatter(ABC):
    """Abstract base class for formatting query results."""
    
    @abstractmethod
    def format(self, data: Any, format_type: str = "json") -> Any:
        """Format data according to specified type."""
        pass


class ErrorHandler(ABC):
    """Abstract base class for error handling strategies."""
    
    @abstractmethod
    def handle(self, error: Exception, context: Dict[str, Any]) -> Any:
        """Handle an error with context information."""
        pass
    
    @abstractmethod
    def should_retry(self, error: Exception) -> bool:
        """Determine if operation should be retried."""
        pass