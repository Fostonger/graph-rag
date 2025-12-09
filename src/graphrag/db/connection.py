"""Database connection utilities for external indexer databases.

The database is created and managed by external indexers.
This module provides read-only connections optimized for queries.
"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from .schema import SchemaError, validate_schema


def _configure_connection(conn: sqlite3.Connection) -> None:
    """Configure connection for optimal read performance.
    
    The external indexer already sets WAL mode and other write-related PRAGMAs.
    We just need to configure for reading.
    """
    # Set row factory for dict-like access
    conn.row_factory = sqlite3.Row
    
    # Read-only performance optimizations
    conn.execute("PRAGMA query_only = ON;")


def connect(db_path: Path, validate: bool = True) -> sqlite3.Connection:
    """Create a read-only connection to an external indexer database.
    
    Args:
        db_path: Path to the SQLite database created by external indexer
        validate: If True, validate that expected tables exist
        
    Returns:
        Configured SQLite connection
        
    Raises:
        FileNotFoundError: If database file doesn't exist
        SchemaError: If validation fails (missing tables)
    """
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")
    
    # Use URI mode for read-only access
    uri = f"file:{db_path}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    _configure_connection(conn)
    
    if validate:
        validate_schema(conn)
    
    return conn


@contextmanager
def get_connection(db_path: Path, validate: bool = True) -> Iterator[sqlite3.Connection]:
    """Context manager for database connection.
    
    Args:
        db_path: Path to the SQLite database
        validate: If True, validate schema on connection
        
    Yields:
        Configured SQLite connection
    """
    conn = connect(db_path, validate=validate)
    try:
        yield conn
    finally:
        conn.close()
