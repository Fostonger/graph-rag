# GraphRAG MVP

An experimental repository indexer that parses Swift sources with Tree-sitter,
persists entity metadata to SQLite, and exposes discovery tools through an
Anthropic MCP server for Cursor-like LLM agents.

## Components

- **Indexer CLI**: `graphrag init|update|status` performs initial scans and
  incremental updates tied to Git commit hashes.
- **SQLite store**: Normalized schema maintains entities, members, extensions,
  and per-commit snapshots.
- **MCP server**: `graphrag-mcp` exposes `find_entities`, `get_members`, and
  `get_graph` tools backed by the indexed data.

## Graph tooling

- `find_entities` accepts case-insensitive glob-style filters (e.g.
  `*BuilderTests` or `Widget*`), making it easier to target large batches of
  symbols.
- `get_graph` accepts two optional parameters:
  - `max_hops` limits downstream traversal and defaults to the `graph.max_hops`
    value from `config.yaml`.
  - `targetType` filters nodes by build target (`app`, `test`, or `all`) so
    graphs can exclude unit-test-only code.
- Graph traversal defaults are controlled via the `graph` section of
  `config.yaml`, which also selects the build system integration (`tuist` or
  `geko`) used to discover module/target metadata. The dependencies worker scans
  all `Project.swift` manifests so indexed entities are annotated with their
  target type for downstream filtering.

## Quick start

```bash
pip install -e .
graphrag init --repo /path/to/repo --db /tmp/graphrag.db
graphrag-mcp --db /tmp/graphrag.db
```

See `docs/ARCHITECTURE.md` for design notes and `config.yaml` for defaults.

## Feature branch awareness

- A second SQLite file (default `graphrag-feature.db`, override with `--feature-db` or `feature_db_path`) stores commits that exist only on the currently checked-out non-master branch plus a synthetic snapshot of unstaged/untracked `.swift` files.
- `graphrag update` always refreshes the master DB first; if HEAD is on a feature branch it then refreshes the feature DB and indexes dirty files so Cursor-like agents see in-flight edits.
- The feature DB tracks only one branch at a time—switching to another branch wipes the previous data so stale records do not leak between branches.

