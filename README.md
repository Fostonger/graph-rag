# GraphRAG

A code navigation service that provides IDE-like features through an MCP server.
Reads from SQLite databases produced by external indexers (e.g., swift-scip-indexer).

## Architecture

GraphRAG acts as a **query layer** on top of pre-indexed code databases:

1. **External Indexer** (e.g., swift-scip-indexer) parses source code and produces a SQLite database
2. **GraphRAG** reads this database and exposes navigation tools via MCP

This separation allows:
- Language-specific indexers optimized for each ecosystem
- GraphRAG to focus on querying and serving results
- Multiple indexers to produce compatible databases

## Navigation Tools

The MCP server provides IDE-like navigation:

- `go_to_definition` - Find symbol definitions with inheritance, conformances, and members
- `find_references` - Find all usages of a symbol with context snippets
- `find_implementations` - Find all implementations of a protocol/interface
- `search_symbols` - Search symbols by name with wildcard support and filters

## Quick Start

```bash
# Install
pip install -e .

# Run external indexer to create database (e.g., swift-scip-indexer)
swift-scip-indexer /path/to/project -o index.db

# Start MCP server
graphrag-mcp --db index.db

# Or use CLI for quick queries
graphrag status --db index.db
graphrag search "FeaturePresenter" --db index.db
graphrag definition "FeaturePresenter" --db index.db
graphrag references "FeaturePresenter" --db index.db
```

## CLI Commands

- `graphrag status` - Show database info (indexed files, symbols, etc.)
- `graphrag search <query>` - Search for symbols
- `graphrag definition <symbol>` - Find symbol definition
- `graphrag references <symbol>` - Find symbol references

## Database Schema

External indexers must produce SQLite databases with this schema:

```sql
-- Metadata storage
CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);

-- Indexing state
CREATE TABLE index_state (
    last_commit_hash TEXT NOT NULL,
    last_indexed_at INTEGER NOT NULL,
    indexed_files TEXT
);

-- Document/file information
CREATE TABLE documents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    relative_path TEXT NOT NULL UNIQUE,
    language TEXT NOT NULL DEFAULT 'swift',
    indexed_at INTEGER NOT NULL
);

-- Symbol definitions
CREATE TABLE symbols (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol_id TEXT NOT NULL,
    kind TEXT,
    documentation TEXT,
    file_id INTEGER REFERENCES documents(id)
);

-- Symbol relationships (conforms, inherits, overrides)
CREATE TABLE relationships (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol_id TEXT NOT NULL,
    target_symbol_id TEXT NOT NULL,
    kind TEXT NOT NULL
);

-- Symbol occurrences (definitions and references)
CREATE TABLE occurrences (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol_id TEXT NOT NULL,
    file_id INTEGER NOT NULL REFERENCES documents(id),
    start_line INTEGER NOT NULL,
    start_column INTEGER NOT NULL,
    end_line INTEGER NOT NULL,
    end_column INTEGER NOT NULL,
    roles INTEGER NOT NULL,  -- Bitmask: definition=1, reference=8
    enclosing_symbol TEXT,
    snippet TEXT
);
```

## Configuration

Create a `config.yaml`:

```yaml
# Path to external indexer SQLite database
db_path: index.db

# Path to repository (for reference)
repo_path: .
```

Or pass options directly:

```bash
graphrag-mcp --db /path/to/index.db --repo /path/to/repo
```

## Development

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest tests/ -v

# Type checking
mypy src/
```

See `docs/ARCHITECTURE.md` for design notes.
