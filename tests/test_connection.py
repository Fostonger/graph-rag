"""Tests for database connection to external indexer databases."""

import sqlite3
from pathlib import Path

import pytest

from graphrag.db.connection import connect, get_connection
from graphrag.db.schema import SchemaError

from conftest import create_external_indexer_db


def test_connect_opens_readonly(tmp_path: Path):
    """Verify that connect() opens database in read-only mode."""
    db_path = tmp_path / "test.db"
    create_external_indexer_db(db_path).close()
    
    conn = connect(db_path)
    
    try:
        # Should fail to write
        with pytest.raises(sqlite3.OperationalError) as exc_info:
            conn.execute("INSERT INTO metadata (key, value) VALUES ('test', 'value')")
        assert "readonly" in str(exc_info.value).lower()
    finally:
        conn.close()


def test_connect_sets_row_factory(tmp_path: Path):
    """Verify that connect() sets row factory for dict-like access."""
    db_path = tmp_path / "test.db"
    create_external_indexer_db(db_path).close()
    
    conn = connect(db_path)
    
    try:
        assert conn.row_factory == sqlite3.Row
        
        # Test dict-like access
        row = conn.execute("SELECT key, value FROM metadata LIMIT 1").fetchone()
        assert row["key"] is not None
        assert row["value"] is not None
    finally:
        conn.close()


def test_connect_raises_for_missing_file(tmp_path: Path):
    """Verify that connect() raises FileNotFoundError for missing database."""
    db_path = tmp_path / "nonexistent.db"
    
    with pytest.raises(FileNotFoundError) as exc_info:
        connect(db_path)
    
    assert "nonexistent.db" in str(exc_info.value)


def test_connect_validates_schema_by_default(tmp_path: Path):
    """Verify that connect() validates schema by default."""
    db_path = tmp_path / "invalid.db"
    
    # Create an incomplete database
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT)")
    conn.close()
    
    with pytest.raises(SchemaError):
        connect(db_path)


def test_connect_can_skip_validation(tmp_path: Path):
    """Verify that connect() can skip schema validation."""
    db_path = tmp_path / "invalid.db"
    
    # Create an incomplete database
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT)")
    conn.close()
    
    # Should not raise with validate=False
    conn = connect(db_path, validate=False)
    conn.close()


def test_get_connection_context_manager(tmp_path: Path):
    """Verify that get_connection() context manager works correctly."""
    db_path = tmp_path / "test.db"
    create_external_indexer_db(db_path).close()
    
    with get_connection(db_path) as conn:
        row = conn.execute("SELECT value FROM metadata WHERE key = 'version'").fetchone()
        assert row["value"] == "1"


def test_get_connection_closes_on_exit(tmp_path: Path):
    """Verify that get_connection() closes connection on context exit."""
    db_path = tmp_path / "test.db"
    create_external_indexer_db(db_path).close()
    
    with get_connection(db_path) as conn:
        pass
    
    # Connection should be closed - trying to use it should fail
    with pytest.raises(sqlite3.ProgrammingError):
        conn.execute("SELECT 1")


def test_connection_row_factory_dict_access(tmp_path: Path):
    """Verify that rows support both index and key access."""
    db_path = tmp_path / "test.db"
    create_external_indexer_db(db_path).close()
    
    with get_connection(db_path) as conn:
        row = conn.execute("SELECT key, value FROM metadata LIMIT 1").fetchone()
        
        # Index access
        assert row[0] is not None
        assert row[1] is not None
        
        # Key access
        assert row["key"] is not None
        assert row["value"] is not None
