# PyXatu Test Suite

Comprehensive test coverage for all PyXatu components.

## Test Files

### Core Tests
- `test_pyxatu_main.py` - Main PyXatu class functionality
  - Initialization and configuration
  - Connection management
  - All query methods (slots, attestations, transactions, etc.)
  - Validator label integration
  - Error handling
  - Backward compatibility

### Component Tests
- `test_clickhouse_client.py` - ClickHouse client
  - Query builder functionality
  - Connection pooling
  - Query execution and retries
  - Error handling

- `test_query_modules.py` - Query module implementations
  - SlotDataFetcher
  - AttestationDataFetcher
  - TransactionDataFetcher
  - ValidatorDataFetcher
  - Parameter validation

- `test_validator_labels.py` - Validator labeling system
  - Entity mapping and caching
  - Dune Spellbook parsing
  - Batch contract logic
  - Exit information tracking
  - Lido operator mapping

- `test_schema_management.py` - Schema validation
  - Table information management
  - Column validation
  - Network availability checking
  - Query method suggestions

### Integration Tests
- `test_mempool_connector.py` - Mempool data integration
  - Flashbots API integration
  - Blocknative API integration
  - Transaction deduplication
  - Caching functionality

- `test_relay_connector.py` - MEV relay integration
  - Relay block fetching
  - Bid analysis
  - Builder dominance metrics
  - Multi-relay aggregation

- `test_integration.py` - End-to-end workflows
  - Complete analysis workflows
  - Validator performance analysis
  - Transaction privacy analysis
  - MEV analysis
  - Error recovery
  - Concurrent queries

### Existing Tests
- `test_config.py` - Configuration management
- `test_models.py` - Data models and validation
- `test_utils.py` - Utility functions
- `test_security.py` - Security features
- `test_imports.py` - Import verification
- `test_partition_optimization.py` - Query optimization

## Running Tests

### Run all tests with coverage:
```bash
pytest -v --cov=pyxatu --cov-report=html tests/
```

### Run specific test file:
```bash
pytest -v tests/test_pyxatu_main.py
```

### Run tests by category:
```bash
# Unit tests only
pytest -v -m "not integration" tests/

# Integration tests only
pytest -v -m integration tests/
```

### Run with the test runner:
```bash
./run_all_tests.py
```

## Test Coverage Goals

- Core functionality: 100%
- Query methods: 95%+
- Error handling: 90%+
- Integration workflows: 85%+

## Writing New Tests

1. Follow the existing test structure
2. Use fixtures for common setup
3. Mock external dependencies
4. Test both success and failure cases
5. Include edge cases
6. Add integration tests for new workflows