# GraphRAG MVP

An experimental repository indexer that parses Swift sources with Tree-sitter,
persists entity metadata to SQLite, and exposes discovery tools through an
Anthropic MCP server for Cursor-like LLM agents.

## Components

- **Indexer CLI**: `graphrag init|update|status` performs initial scans and
  incremental updates tied to Git commit hashes.
- **SQLite store**: Normalized schema maintains entities, members, extensions,
  and per-commit snapshots.
- **MCP server**: `graphrag-mcp` exposes `find_entities` and `get_members`
  tools backed by the indexed data.

## Quick start

```bash
pip install -e .
graphrag init --repo /path/to/repo --db /tmp/graphrag.db
graphrag-mcp --db /tmp/graphrag.db
```

See `docs/ARCHITECTURE.md` for design notes and `config.yaml` for defaults.

