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

### Feature branch indexer (client-side)

- `FeatureBranchIndexer` keeps a separate SQLite file (default `graphrag-feature.db`) so the master DB can be shipped to remote services while feature work and unstaged files stay local.
- When `graphrag update` runs on a non-master branch it:
  1. Updates the master DB as before.
  2. Resets the feature DB if the branch changed since the last run.
  3. Replays commits that exist only on the feature branch (starting at the merge-base) and adds a synthetic `worktree:<branch>` commit for unstaged/untracked `.swift` files.
- Only one feature branch is tracked at a time—switching branches drops the previous DB, keeping the client cache reversible.

### Build-system aware module metadata

- A `DependenciesWorker` scans every `Project.swift` manifest (currently Tuist/Geko
  compatible) to discover targets, their product types, and source roots.
- Each parsed entity is annotated with both the resolved module name and a
  `target_type` (`app` vs `test`) derived from the target's product.
- This metadata is persisted in `entity_versions.properties` so downstream
  queries can filter out unit-test-only code paths.

### SCIP-based navigation

The MCP server provides IDE-like code navigation tools:
- `go_to_definition` - Find symbol definitions with inheritance info
- `find_references` - Find all usages of a symbol
- `find_implementations` - Find protocol/interface implementations
- `search_symbols` - Search symbols by name with wildcards

These tools are backed by SCIP (Source Code Index Protocol) data stored in
the `symbols`, `occurrences`, and `symbol_relationships` tables.

### Future GraphRAG expansion

The SQLite schema intentionally leaves room for:

- `relationships` table storing edges (e.g., inheritance, member references).
- Embedding metadata tables for semantic search.
- Pre-computed path traversals for call graphs.

These can attach to the existing `entity_id` / `member_id` foreign keys without
breaking backward compatibility.

