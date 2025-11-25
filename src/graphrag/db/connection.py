from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


# Performance tuning constants
CACHE_SIZE_KB = 100_000  # 100MB page cache
MMAP_SIZE_BYTES = 2_147_483_648  # 2GB memory-mapped I/O


def _configure_connection(conn: sqlite3.Connection, readonly: bool = False) -> None:
    """Configure connection with performance-optimized PRAGMAs.
    
    Args:
        conn: SQLite connection to configure
        readonly: If True, skip write-related PRAGMAs and optimize for reads
    """
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute(f"PRAGMA cache_size=-{CACHE_SIZE_KB};")  # negative = KB
    conn.execute(f"PRAGMA mmap_size={MMAP_SIZE_BYTES};")
    conn.execute("PRAGMA temp_store=MEMORY;")
    
    if not readonly:
        conn.execute("PRAGMA foreign_keys=ON;")
    
    conn.row_factory = sqlite3.Row


def connect(db_path: Path) -> sqlite3.Connection:
    """Create a read-write connection with optimized settings."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    _configure_connection(conn, readonly=False)
    return conn


def connect_readonly(db_path: Path) -> sqlite3.Connection:
    """Create a read-only connection optimized for query performance.
    
    Uses URI mode to open the database in read-only mode, which allows
    multiple concurrent readers and prevents accidental writes.
    """
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")
    
    # Use URI mode for true read-only access
    uri = f"file:{db_path}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    _configure_connection(conn, readonly=True)
    return conn


@contextmanager
def get_connection(db_path: Path) -> Iterator[sqlite3.Connection]:
    """Context manager for read-write connection with auto-commit."""
    conn = connect(db_path)
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


@contextmanager
def get_readonly_connection(db_path: Path) -> Iterator[sqlite3.Connection]:
    """Context manager for read-only connection optimized for queries."""
    conn = connect_readonly(db_path)
    try:
        yield conn
    finally:
        conn.close()

