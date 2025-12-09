"""Tests for database schema validation."""

import sqlite3
from pathlib import Path

import pytest

from graphrag.db.schema import (
    EXPECTED_TABLES,
    SchemaError,
    SymbolRole,
    get_index_state,
    get_metadata,
    validate_schema,
)

from conftest import create_external_indexer_db


def test_expected_tables_defined():
    """Verify expected tables are defined."""
    assert "metadata" in EXPECTED_TABLES
    assert "index_state" in EXPECTED_TABLES
    assert "documents" in EXPECTED_TABLES
    assert "symbols" in EXPECTED_TABLES
    assert "relationships" in EXPECTED_TABLES
    assert "occurrences" in EXPECTED_TABLES


def test_validate_schema_passes_for_valid_db(tmp_path: Path):
    """Verify validate_schema passes for a valid external indexer database."""
    db_path = tmp_path / "test.db"
    conn = create_external_indexer_db(db_path)
    
    # Should not raise
    validate_schema(conn)
    conn.close()


def test_validate_schema_raises_for_missing_tables(tmp_path: Path):
    """Verify validate_schema raises SchemaError when tables are missing."""
    db_path = tmp_path / "empty.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    
    # Create only some tables
    conn.execute("CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT)")
    conn.execute("CREATE TABLE documents (id INTEGER PRIMARY KEY)")
    
    with pytest.raises(SchemaError) as exc_info:
        validate_schema(conn)
    
    error_msg = str(exc_info.value)
    assert "missing required tables" in error_msg.lower()
    assert "symbols" in error_msg or "occurrences" in error_msg
    conn.close()


def test_symbol_role_constants():
    """Verify SymbolRole constants match SCIP protocol."""
    assert SymbolRole.DEFINITION == 1
    assert SymbolRole.IMPORT == 2
    assert SymbolRole.WRITE_ACCESS == 4
    assert SymbolRole.READ_ACCESS == 8
    assert SymbolRole.REFERENCE == 8  # Alias
    assert SymbolRole.GENERATED == 16
    assert SymbolRole.TEST == 32


def test_get_metadata(tmp_path: Path):
    """Verify get_metadata retrieves values correctly."""
    db_path = tmp_path / "test.db"
    conn = create_external_indexer_db(db_path)
    
    assert get_metadata(conn, "version") == "1"
    assert get_metadata(conn, "tool") == "test-indexer"
    assert get_metadata(conn, "project_root") == "/test/project"
    assert get_metadata(conn, "nonexistent") is None
    conn.close()


def test_get_index_state(tmp_path: Path):
    """Verify get_index_state retrieves state correctly."""
    db_path = tmp_path / "test.db"
    conn = create_external_indexer_db(db_path)
    
    state = get_index_state(conn)
    
    assert state is not None
    assert state["last_commit_hash"] == "abc123"
    assert state["last_indexed_at"] == 1700000000
    conn.close()


def test_get_index_state_empty_db(tmp_path: Path):
    """Verify get_index_state returns None for empty index_state."""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE index_state (last_commit_hash TEXT, last_indexed_at INTEGER, indexed_files TEXT)")
    
    state = get_index_state(conn)
    
    assert state is None
    conn.close()


def test_documents_table_structure(tmp_path: Path):
    """Verify documents table has correct structure."""
    db_path = tmp_path / "test.db"
    conn = create_external_indexer_db(db_path)
    
    # Insert a document
    conn.execute("""
        INSERT INTO documents (relative_path, language, indexed_at)
        VALUES ('Sources/Test.swift', 'swift', 1700000000)
    """)
    
    row = conn.execute("SELECT * FROM documents WHERE relative_path = 'Sources/Test.swift'").fetchone()
    
    assert row["relative_path"] == "Sources/Test.swift"
    assert row["language"] == "swift"
    assert row["indexed_at"] == 1700000000
    conn.close()


def test_symbols_table_structure(tmp_path: Path):
    """Verify symbols table has correct structure."""
    db_path = tmp_path / "test.db"
    conn = create_external_indexer_db(db_path)
    
    # Insert document first (for FK)
    conn.execute("""
        INSERT INTO documents (relative_path, language, indexed_at)
        VALUES ('Sources/Test.swift', 'swift', 1700000000)
    """)
    
    # Insert symbol
    conn.execute("""
        INSERT INTO symbols (symbol_id, kind, documentation, file_id)
        VALUES ('swift Test MyClass#', 'class', 'A test class', 1)
    """)
    
    row = conn.execute("SELECT * FROM symbols WHERE symbol_id = 'swift Test MyClass#'").fetchone()
    
    assert row["symbol_id"] == "swift Test MyClass#"
    assert row["kind"] == "class"
    assert row["documentation"] == "A test class"
    assert row["file_id"] == 1
    conn.close()


def test_occurrences_table_structure(tmp_path: Path):
    """Verify occurrences table has correct structure."""
    db_path = tmp_path / "test.db"
    conn = create_external_indexer_db(db_path)
    
    # Insert document
    conn.execute("""
        INSERT INTO documents (relative_path, language, indexed_at)
        VALUES ('Sources/Test.swift', 'swift', 1700000000)
    """)
    
    # Insert occurrence
    conn.execute("""
        INSERT INTO occurrences (symbol_id, file_id, start_line, start_column, end_line, end_column, roles, snippet)
        VALUES ('swift Test MyClass#', 1, 10, 7, 10, 14, 1, 'class MyClass {')
    """)
    
    row = conn.execute("SELECT * FROM occurrences").fetchone()
    
    assert row["symbol_id"] == "swift Test MyClass#"
    assert row["file_id"] == 1
    assert row["start_line"] == 10
    assert row["start_column"] == 7
    assert row["roles"] == SymbolRole.DEFINITION
    assert row["snippet"] == "class MyClass {"
    conn.close()


def test_relationships_table_structure(tmp_path: Path):
    """Verify relationships table has correct structure."""
    db_path = tmp_path / "test.db"
    conn = create_external_indexer_db(db_path)
    
    # Insert relationship
    conn.execute("""
        INSERT INTO relationships (symbol_id, target_symbol_id, kind)
        VALUES ('swift Test MyClass#', 'swift Test IProtocol#', 'conforms')
    """)
    
    row = conn.execute("SELECT * FROM relationships").fetchone()
    
    assert row["symbol_id"] == "swift Test MyClass#"
    assert row["target_symbol_id"] == "swift Test IProtocol#"
    assert row["kind"] == "conforms"
    conn.close()
