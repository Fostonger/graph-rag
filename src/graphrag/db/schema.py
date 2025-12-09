"""Schema definitions for external indexer database.

The database is created by external indexers (e.g., swift-scip-indexer).
This module provides:
- Schema validation to ensure the database has expected tables
- SymbolRole constants for occurrence role bitmasks
"""
from __future__ import annotations

import sqlite3


# Expected tables in the external indexer database
EXPECTED_TABLES = frozenset({
    "metadata",
    "index_state", 
    "documents",
    "symbols",
    "relationships",
    "occurrences",
})


class SchemaError(Exception):
    """Raised when database schema validation fails."""
    pass


def validate_schema(conn: sqlite3.Connection) -> None:
    """Validate that the database has the expected external indexer schema.
    
    Args:
        conn: SQLite connection to validate
        
    Raises:
        SchemaError: If required tables are missing
    """
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    )
    existing_tables = {row[0] for row in cursor.fetchall()}
    
    missing = EXPECTED_TABLES - existing_tables
    if missing:
        raise SchemaError(
            f"Database is missing required tables: {', '.join(sorted(missing))}. "
            "Ensure the database was created by an external indexer."
        )


def get_metadata(conn: sqlite3.Connection, key: str) -> str | None:
    """Get a metadata value from the database.
    
    Args:
        conn: SQLite connection
        key: Metadata key to retrieve
        
    Returns:
        The metadata value, or None if not found
    """
    row = conn.execute(
        "SELECT value FROM metadata WHERE key = ?",
        (key,)
    ).fetchone()
    return row[0] if row else None


def get_index_state(conn: sqlite3.Connection) -> dict | None:
    """Get the current index state.
    
    Returns:
        Dict with last_commit_hash, last_indexed_at, indexed_files
        or None if no state exists
    """
    row = conn.execute(
        "SELECT last_commit_hash, last_indexed_at, indexed_files FROM index_state"
    ).fetchone()
    if not row:
        return None
    return {
        "last_commit_hash": row[0],
        "last_indexed_at": row[1],
        "indexed_files": row[2],
    }


# SCIP Role constants (matches external indexer)
class SymbolRole:
    """Bitmask values for occurrence roles (matches SCIP protocol)."""
    DEFINITION = 1
    IMPORT = 2
    WRITE_ACCESS = 4
    READ_ACCESS = 8
    REFERENCE = 8  # Alias for READ_ACCESS (most common usage)
    GENERATED = 16
    TEST = 32
