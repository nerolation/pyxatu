# Core Components

## Files

### `base.py`
Abstract base classes for the library:
- `BaseClient` - Base for all client implementations
- `BaseDataFetcher` - Base for data fetchers
- `BaseConnector` - Base for external connectors
- `ConfigProvider` - Base for configuration providers

### `clickhouse_client.py`
ClickHouse database client:
- `ClickHouseClient` - Async HTTP client for ClickHouse
- `ClickHouseQueryBuilder` - SQL query builder with safety features