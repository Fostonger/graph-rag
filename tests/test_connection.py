"""Tests for database connection configuration and optimizations."""

import sqlite3
from pathlib import Path

import pytest

from graphrag.db.connection import (
    CACHE_SIZE_KB,
    MMAP_SIZE_BYTES,
    connect,
    connect_readonly,
    get_connection,
    get_readonly_connection,
)
from graphrag.db.schema import apply_schema


def test_connect_applies_performance_pragmas(tmp_path: Path):
    """Verify that connect() applies all performance-related PRAGMAs."""
    db_path = tmp_path / "test.db"
    conn = connect(db_path)
    
    try:
        # Check journal mode
        result = conn.execute("PRAGMA journal_mode;").fetchone()
        assert result[0].lower() == "wal"
        
        # Check synchronous mode
        result = conn.execute("PRAGMA synchronous;").fetchone()
        assert result[0] == 1  # NORMAL = 1
        
        # Check cache size (negative value means KB)
        result = conn.execute("PRAGMA cache_size;").fetchone()
        assert result[0] == -CACHE_SIZE_KB
        
        # Check mmap size (SQLite may round down for page alignment)
        result = conn.execute("PRAGMA mmap_size;").fetchone()
        assert result[0] >= MMAP_SIZE_BYTES * 0.99  # Allow small rounding
        
        # Check temp store
        result = conn.execute("PRAGMA temp_store;").fetchone()
        assert result[0] == 2  # MEMORY = 2
        
        # Check foreign keys enabled for read-write connections
        result = conn.execute("PRAGMA foreign_keys;").fetchone()
        assert result[0] == 1
        
        # Check row factory is set
        assert conn.row_factory == sqlite3.Row
    finally:
        conn.close()


def test_connect_creates_parent_directories(tmp_path: Path):
    """Verify that connect() creates parent directories if needed."""
    db_path = tmp_path / "nested" / "dirs" / "test.db"
    assert not db_path.parent.exists()
    
    conn = connect(db_path)
    conn.close()
    
    assert db_path.exists()
    assert db_path.parent.exists()


def test_connect_readonly_applies_performance_pragmas(tmp_path: Path):
    """Verify that connect_readonly() applies read-optimized PRAGMAs."""
    db_path = tmp_path / "test.db"
    
    # First create the database
    write_conn = connect(db_path)
    write_conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY);")
    write_conn.commit()
    write_conn.close()
    
    # Now open read-only
    conn = connect_readonly(db_path)
    
    try:
        # Check cache size
        result = conn.execute("PRAGMA cache_size;").fetchone()
        assert result[0] == -CACHE_SIZE_KB
        
        # Check mmap size (SQLite may round down for page alignment)
        result = conn.execute("PRAGMA mmap_size;").fetchone()
        assert result[0] >= MMAP_SIZE_BYTES * 0.99  # Allow small rounding
        
        # Check temp store
        result = conn.execute("PRAGMA temp_store;").fetchone()
        assert result[0] == 2  # MEMORY = 2
        
        # Check row factory is set
        assert conn.row_factory == sqlite3.Row
    finally:
        conn.close()


def test_connect_readonly_prevents_writes(tmp_path: Path):
    """Verify that connect_readonly() opens database in read-only mode."""
    db_path = tmp_path / "test.db"
    
    # Create the database with a table
    write_conn = connect(db_path)
    write_conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY);")
    write_conn.commit()
    write_conn.close()
    
    # Open read-only and try to write
    conn = connect_readonly(db_path)
    
    try:
        with pytest.raises(sqlite3.OperationalError) as exc_info:
            conn.execute("INSERT INTO test (id) VALUES (1);")
        assert "readonly" in str(exc_info.value).lower()
    finally:
        conn.close()


def test_connect_readonly_raises_for_missing_file(tmp_path: Path):
    """Verify that connect_readonly() raises FileNotFoundError for missing DB."""
    db_path = tmp_path / "nonexistent.db"
    
    with pytest.raises(FileNotFoundError) as exc_info:
        connect_readonly(db_path)
    
    assert "nonexistent.db" in str(exc_info.value)


def test_get_connection_context_manager(tmp_path: Path):
    """Verify that get_connection() context manager commits on success."""
    db_path = tmp_path / "test.db"
    
    with get_connection(db_path) as conn:
        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY);")
        conn.execute("INSERT INTO test (id) VALUES (1);")
    
    # Verify data was committed
    with get_connection(db_path) as conn:
        result = conn.execute("SELECT id FROM test;").fetchone()
        assert result["id"] == 1


def test_get_readonly_connection_context_manager(tmp_path: Path):
    """Verify that get_readonly_connection() context manager works correctly."""
    db_path = tmp_path / "test.db"
    
    # Create database with data
    with get_connection(db_path) as conn:
        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT);")
        conn.execute("INSERT INTO test (id, value) VALUES (1, 'hello');")
    
    # Read using readonly connection
    with get_readonly_connection(db_path) as conn:
        result = conn.execute("SELECT value FROM test WHERE id = 1;").fetchone()
        assert result["value"] == "hello"


def test_connection_row_factory_returns_dict_like_rows(tmp_path: Path):
    """Verify that row factory allows dict-like access to columns."""
    db_path = tmp_path / "test.db"
    
    with get_connection(db_path) as conn:
        conn.execute("CREATE TABLE test (id INTEGER, name TEXT, value REAL);")
        conn.execute("INSERT INTO test VALUES (1, 'foo', 3.14);")
        
        row = conn.execute("SELECT * FROM test;").fetchone()
        
        # Should support both index and key access
        assert row[0] == 1
        assert row["id"] == 1
        assert row["name"] == "foo"
        assert row["value"] == 3.14

