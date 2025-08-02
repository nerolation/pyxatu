# PyXatu Structure

## Core (`core/`)
Base classes and ClickHouse client implementation.

## Connectors (`connectors/`)
External data source connectors (MEV relays, mempool).

## Data (`data/`)
Static configuration and schema files.

## Queries (`queries/`)
Query builders for different data types (slots, attestations, transactions, validators).

## Main Files
- `pyxatu.py` - Main client class
- `cli.py` - Command-line interface
- `models.py` - Data models
- `config.py` - Configuration management
- `validator_labels.py` - Validator entity mapping
- `schema.py` - Schema validation
- `utils.py` - Utility functions