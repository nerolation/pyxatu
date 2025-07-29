"""Schema management for PyXatu."""

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Set, Any
from dataclasses import dataclass
from datetime import datetime, timezone
import importlib.resources as resources

from .models import Network


@dataclass
class ColumnInfo:
    """Information about a table column."""
    name: str
    type: str
    description: str


@dataclass
class TableInfo:
    """Complete information about a ClickHouse table."""
    description: str
    table_name: str
    partitioning_column: str
    networks_available: List[str]
    pyxatu_method: str
    data_characteristics: Dict[str, str]
    common_columns: List[ColumnInfo]
    
    def get_column_names(self) -> List[str]:
        """Get list of column names."""
        return [col.name for col in self.common_columns]
    
    def validate_columns(self, columns: List[str]) -> List[str]:
        """Validate that requested columns exist in table."""
        if not columns or columns == ['*']:
            return []
        
        available = set(self.get_column_names())
        requested = set(columns)
        invalid = requested - available
        
        return list(invalid)
    
    def validate_network(self, network: str) -> bool:
        """Check if network is available for this table."""
        return network.lower() in [n.lower() for n in self.networks_available]


class SchemaManager:
    """Manages table schemas and provides validation utilities."""
    
    def __init__(self, schema_path: Optional[Path] = None):
        """Initialize schema manager.
        
        Args:
            schema_path: Path to schema JSON file. If None, uses bundled schema.
        """
        self.logger = logging.getLogger(__name__)
        self._tables: Dict[str, TableInfo] = {}
        self._method_to_table: Dict[str, str] = {}
        self._general_notes: Dict[str, Any] = {}
        
        self._load_schema(schema_path)
    
    def _load_schema(self, schema_path: Optional[Path] = None):
        """Load schema from JSON file."""
        try:
            if schema_path and schema_path.exists():
                with open(schema_path) as f:
                    data = json.load(f)
            else:
                # Load bundled schema
                schema_file = resources.files('pyxatu') / 'xatu_tables_schema.json'
                data = json.loads(schema_file.read_text())
            
            # Parse tables
            for table_name, table_data in data.get('tables', {}).items():
                columns = [
                    ColumnInfo(
                        name=col['name'],
                        type=col['type'],
                        description=col['description']
                    )
                    for col in table_data.get('common_columns', [])
                ]
                
                table_info = TableInfo(
                    description=table_data['description'],
                    table_name=table_name,
                    partitioning_column=table_data['partitioning_column'],
                    networks_available=table_data['networks_available'],
                    pyxatu_method=table_data['pyxatu_method'],
                    data_characteristics=table_data['data_characteristics'],
                    common_columns=columns
                )
                
                self._tables[table_name] = table_info
                self._method_to_table[table_data['pyxatu_method']] = table_name
            
            self._general_notes = data.get('general_notes', {})
            
            self.logger.info(f"Loaded schema for {len(self._tables)} tables")
            
        except Exception as e:
            self.logger.warning(f"Failed to load schema: {e}. Continuing without schema validation.")
    
    def get_table_info(self, table_name: str) -> Optional[TableInfo]:
        """Get information about a specific table."""
        return self._tables.get(table_name)
    
    def get_table_by_method(self, method_name: str) -> Optional[TableInfo]:
        """Get table info by PyXatu method name."""
        table_name = self._method_to_table.get(method_name)
        if table_name:
            return self._tables.get(table_name)
        return None
    
    def get_all_tables(self) -> Dict[str, TableInfo]:
        """Get all table information."""
        return self._tables.copy()
    
    def get_tables_for_network(self, network: str) -> List[TableInfo]:
        """Get all tables available for a specific network."""
        return [
            table for table in self._tables.values()
            if network.lower() in [n.lower() for n in table.networks_available]
        ]
    
    def validate_method_params(self, method_name: str, columns: Optional[List[str]] = None, 
                             network: Optional[str] = None) -> Dict[str, Any]:
        """Validate parameters for a PyXatu method.
        
        Returns dict with validation results:
        - valid: bool
        - errors: List[str]
        - warnings: List[str]
        - suggestions: Dict[str, Any]
        """
        result = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'suggestions': {}
        }
        
        table_info = self.get_table_by_method(method_name)
        if not table_info:
            result['warnings'].append(f"No schema information for method {method_name}")
            return result
        
        # Validate columns
        if columns and columns != ['*']:
            invalid_cols = table_info.validate_columns(columns)
            if invalid_cols:
                result['valid'] = False
                result['errors'].append(f"Invalid columns: {', '.join(invalid_cols)}")
                result['suggestions']['available_columns'] = table_info.get_column_names()
        
        # Validate network
        if network:
            if not table_info.validate_network(network):
                result['valid'] = False
                result['errors'].append(f"Network '{network}' not available for this table")
                result['suggestions']['available_networks'] = table_info.networks_available
        
        return result
    
    def get_partitioning_hints(self, table_name: str, slot_range: Optional[List[int]] = None,
                             time_range: Optional[List[str]] = None) -> Dict[str, Any]:
        """Get partitioning hints for efficient queries."""
        hints = {
            'use_time_filter': False,
            'partition_column': None,
            'estimated_partitions': None
        }
        
        table_info = self._tables.get(table_name)
        if not table_info:
            return hints
        
        hints['partition_column'] = table_info.partitioning_column
        
        # Determine if time filtering would be beneficial
        if table_info.partitioning_column in ['slot_start_date_time', 'event_date_time']:
            if slot_range or time_range:
                hints['use_time_filter'] = True
                
                # Estimate partition count for slot ranges
                if slot_range and isinstance(slot_range, list) and len(slot_range) == 2:
                    # Assuming daily partitions, 7200 slots per day (12 second slots)
                    days = (slot_range[1] - slot_range[0]) / 7200
                    hints['estimated_partitions'] = max(1, int(days))
        
        return hints
    
    def get_genesis_time(self, network: str) -> Optional[datetime]:
        """Get genesis time for a network."""
        genesis_times = {
            'mainnet': '2020-12-01 12:00:23 UTC',
            'sepolia': '2022-06-20 22:00:00 UTC',
            'holesky': '2023-09-28 12:00:00 UTC'
        }
        
        genesis_str = genesis_times.get(network.lower())
        if genesis_str:
            return datetime.strptime(genesis_str, '%Y-%m-%d %H:%M:%S %Z').replace(tzinfo=timezone.utc)
        return None
    
    def get_data_availability_info(self, table_name: str) -> Optional[str]:
        """Get information about when data became available for a table."""
        table_info = self._tables.get(table_name)
        if table_info:
            return table_info.data_characteristics.get('data_available_from')
        return None
    
    def suggest_method_for_query(self, query_type: str) -> List[str]:
        """Suggest PyXatu methods based on query type."""
        suggestions = []
        
        query_keywords = {
            'slot': ['get_slots', 'get_missed_slots'],
            'block': ['get_slots', 'get_beacon_block_v2', 'get_blockevent'],
            'attestation': ['get_attestation', 'get_attestation_event', 'get_elaborated_attestations'],
            'validator': ['get_proposer', 'get_duties', 'get_elaborated_attestations'],
            'transaction': ['get_transactions', 'get_el_transactions', 'get_mempool', 'get_elaborated_transactions'],
            'withdrawal': ['get_withdrawals'],
            'blob': ['get_blobs', 'get_blob_events'],
            'reorg': ['get_reorgs'],
            'duty': ['get_duties', 'get_proposer'],
            'committee': ['get_duties'],
            'proposer': ['get_proposer']
        }
        
        query_lower = query_type.lower()
        for keyword, methods in query_keywords.items():
            if keyword in query_lower:
                suggestions.extend(methods)
        
        return list(set(suggestions))  # Remove duplicates
    
    def format_table_info(self, table_info: TableInfo) -> str:
        """Format table information for display."""
        lines = [
            f"Table: {table_info.table_name}",
            f"Description: {table_info.description}",
            f"PyXatu Method: {table_info.pyxatu_method}",
            f"Networks: {', '.join(table_info.networks_available)}",
            f"Partitioned by: {table_info.partitioning_column}",
            "\nData Characteristics:"
        ]
        
        for key, value in table_info.data_characteristics.items():
            lines.append(f"  - {key.replace('_', ' ').title()}: {value}")
        
        lines.append("\nCommon Columns:")
        for col in table_info.common_columns:
            lines.append(f"  - {col.name} ({col.type}): {col.description}")
        
        return '\n'.join(lines)


# Global schema manager instance
_schema_manager: Optional[SchemaManager] = None


def get_schema_manager() -> SchemaManager:
    """Get or create the global schema manager instance."""
    global _schema_manager
    if _schema_manager is None:
        _schema_manager = SchemaManager()
    return _schema_manager