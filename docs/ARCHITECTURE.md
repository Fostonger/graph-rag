# GraphRAG Architecture (MVP)

## Goals

1. Parse Swift sources using Tree-sitter, capturing entities (types, protocols,
   extensions) and their members.
2. Persist metadata in SQLite so that commits can be replayed, diffed, and
   queried efficiently.
3. Provide a thin MCP server that surfaces entity search tools to a Cursor-like
   LLM agent.

## High-level layout

```
src/graphrag/
├── cli/         # Typer-based CLI entrypoints
├── db/          # SQLite schema + connection helpers
├── indexer/     # Tree-sitter adapters + ingest orchestrators
├── mcp/         # Anthropic MCP server implementation
└── models/      # Pydantic schemas shared across layers
```

### Future GraphRAG expansion

The SQLite schema intentionally leaves room for:

- `relationships` table storing edges (e.g., inheritance, member references).
- Embedding metadata tables for semantic search.
- Pre-computed path traversals for call graphs.

These can attach to the existing `entity_id` / `member_id` foreign keys without
breaking backward compatibility.

