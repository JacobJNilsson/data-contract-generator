# Data Ingestion Contract Generator

[![CI](https://github.com/JacobJNilsson/data-ingestion-contract-generator/actions/workflows/ci.yml/badge.svg)](https://github.com/JacobJNilsson/data-ingestion-contract-generator/actions/workflows/ci.yml)
[![Python 3.13+](https://img.shields.io/badge/python-3.13+-blue.svg)](https://www.python.org/downloads/)

A CLI tool and framework that automatically generates and validates contracts for data ingestion pipelines. Designed to help developers and AI agents build reliable, type-safe data workflows with automated schema detection and quality assessment.

## Overview

Building data ingestion pipelines is complex and error-prone, especially when dealing with:

- Diverse file formats and encodings (CSV, JSON, etc.)
- International number formats (European vs US)
- Data quality issues (UTF-8 BOMs, sparse data, missing values)
- Schema mismatches between source and destination
- Transformation logic that's hard to track and maintain

This tool solves these problems with a **three-contract architecture** that separates concerns and makes data pipelines explicit, validated, and maintainable.

## Three-Contract Architecture

### 1. Source Contracts

Describe where data comes from. Automatically analyzes and documents:

- **File sources**: CSV, JSON, and other file formats with encoding detection
- **Database sources**: PostgreSQL, MySQL, SQLite tables and queries
- Schema inference with data types
- Quality metrics and data profiling
- Format-specific handling (UTF-8 BOM, European numbers, etc.)

### 2. Destination Contracts

Define where data goes. Specifies:

- Target schema and data types
- Validation rules and constraints
- Required fields and uniqueness constraints
- Data quality requirements

### 3. Transformation Contracts

Map source to destination. Defines:

- Field mappings between source and destination
- Transformation logic (type conversions, formatting)
- Enrichment rules (derived fields, lookups)
- Execution configuration (batch size, error handling)

See the [API documentation](core/README.md) for detailed usage and examples.

## Quick Start

### Prerequisites

- Python 3.13+
- [uv](https://github.com/astral-sh/uv) package manager

### Installation

```bash
# Install with uv tool
uv tool install https://github.com/JacobJNilsson/data-ingestion-contract-generator/releases/download/v0.1.0/ingestion_contract_mcp-0.1.0-py3-none-any.whl

# Verify
contract-gen --version
```

Download the latest release from [GitHub Releases](https://github.com/JacobJNilsson/data-ingestion-contract-generator/releases).

> **Note:** Requires Python 3.13+ and [uv](https://github.com/astral-sh/uv) (`curl -LsSf https://astral.sh/uv/install.sh | sh`)

#### For Developers

```bash
# Clone the repository
git clone https://github.com/JacobJNilsson/data-ingestion-contract-generator.git
cd data-ingestion-contract-generator

# Install dependencies
uv sync

# Run tests to verify setup
make test
```

## Command Line Interface (CLI)

The `contract-gen` CLI provides direct access to contract generation and validation.

> **For local development:** Prefix commands with `uv run` (e.g., `uv run contract-gen --help`)

### Key Commands

```bash
# Generate source contract from CSV
contract-gen source csv data/transactions.csv --id transactions --output contracts/source.json

# Generate source contract from Database
contract-gen source database postgresql --conn "postgresql://user:pass@localhost:5432/db" --table users --output contracts/users.json

# List available Supabase tables
contract-gen source supabase list --url https://xxxxx.supabase.co --api-key eyJhbGc...

# Generate source contract from Supabase (simple, but no primary keys)
contract-gen source supabase analyze --url https://xxxxx.supabase.co --api-key eyJhbGc... --table users --output contracts/users.json

# For full schema introspection with primary keys, use database source instead:
contract-gen source database list --conn "postgresql://postgres:[PASSWORD]@db.[PROJECT-REF].supabase.co:5432/postgres" --type postgresql

# Generate destination contract (CSV)
contract-gen destination csv --id output_data --output contracts/destination.json

# Generate destination contract (Supabase - service_role key recommended)
contract-gen destination supabase --url https://xxxxx.supabase.co --api-key eyJhbGc... --table users --id users_dest

# Generate destination contract (Database)
contract-gen destination database --conn postgresql://user:pass@localhost/db --table my_table --id my_dest --type postgresql

# Validate contracts
contract-gen validate contracts/source.json

# Get help
contract-gen --help
```

**Key features:**

- Auto-detects CSV encoding and delimiters
- Support for PostgreSQL, MySQL, SQLite, and Supabase
- Native Supabase integration with API key authentication
- Multiple output formats (JSON, YAML)
- Pretty-printed output with syntax highlighting
- Comprehensive validation with detailed error messages
- Batch processing support

## AI Integration (MCP Server)

This tool also functions as an MCP (Model Context Protocol) server, making its capabilities available to AI assistants like Cursor.

Add to your `.cursor/mcp.json`:

```json
{
  "mcpServers": {
    "contract-generator": {
      "command": "uv",
      "args": [
        "--directory",
        "/absolute/path/to/data-ingestion-contract-generator",
        "run",
        "mcp_server/server.py"
      ]
    }
  }
}
```

Then restart Cursor, and the contract generation tools will be available to the AI.

## Development

### Setup Development Environment

```bash
# Install dev dependencies
uv sync --all-extras

# Install pre-commit hooks (optional)
pre-commit install
```

### Available Commands

```bash
make check         # Run all checks (lint + format-check + mypy)
make test          # Run pytest test suite
make format        # Format code with Ruff
```

All code is fully typed with Python 3.13+ type hints. CI runs automatically on pull requests.

## Documentation

- **[Core Library](core/README.md)** - Detailed logic and examples
- **[Project Architecture](.cursor/rules/project.mdc)** - Architecture decisions and conventions
- **[Git Workflow](.cursor/rules/git.mdc)** - Commit guidelines and workflow
- **[Python Guidelines](.cursor/rules/python.mdc)** - Coding standards

## Use Cases

**Command Line / CI/CD:**

- Generate contracts from scripts and pipelines
- Validate contracts before deployment
- Integrate with GitHub Actions or GitLab CI

**Interactive Development:**

- Use with Cursor for AI-assisted contract generation
- Real-time validation and schema analysis

**Data Engineering:**

- Document data sources automatically
- Track schema changes over time
- Ensure data quality across pipelines

## Requirements

- **Python**: 3.13+
- **Pydantic**: 2.0.0+
- **Typer**: 0.12.0+ (for CLI)
- **Rich**: 13.0.0+ (for CLI pretty output)
- **MCP**: 1.0.0+ (optional, for AI integration)

See `pyproject.toml` for complete dependency list.

## License

This project is licensed under **AGPL-3.0** for open source use. See [LICENSE](LICENSE) for details.

**Commercial licenses** are available if you want to use this software in a closed-source product. Contact [jacobjnilsson@gmail.com](mailto:jacobjnilsson@gmail.com) for inquiries.
