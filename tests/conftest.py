"""Shared test fixtures for GraphRAG tests.

These fixtures create databases with the external indexer schema format.
"""
import sqlite3
from pathlib import Path
from typing import Callable

import pytest


def create_external_indexer_db(db_path: Path) -> sqlite3.Connection:
    """Create a test database with external indexer schema.
    
    This mimics what an external indexer would produce.
    """
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    
    # Apply PRAGMAs like external indexer would
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA cache_size = -20000;")  # 80MB
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA foreign_keys = ON;")
    
    # Create metadata table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)
    
    # Create index_state table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS index_state (
            last_commit_hash TEXT NOT NULL,
            last_indexed_at INTEGER NOT NULL,
            indexed_files TEXT
        )
    """)
    
    # Create documents table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS documents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            relative_path TEXT NOT NULL UNIQUE,
            language TEXT NOT NULL DEFAULT 'swift',
            indexed_at INTEGER NOT NULL
        )
    """)
    
    # Create symbols table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS symbols (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol_id TEXT NOT NULL,
            kind TEXT,
            documentation TEXT,
            file_id INTEGER,
            FOREIGN KEY(file_id) REFERENCES documents(id) ON DELETE CASCADE
        )
    """)
    
    # Create relationships table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS relationships (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol_id TEXT NOT NULL,
            target_symbol_id TEXT NOT NULL,
            kind TEXT NOT NULL
        )
    """)
    
    # Create occurrences table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS occurrences (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol_id TEXT NOT NULL,
            file_id INTEGER NOT NULL,
            start_line INTEGER NOT NULL,
            start_column INTEGER NOT NULL,
            end_line INTEGER NOT NULL,
            end_column INTEGER NOT NULL,
            roles INTEGER NOT NULL,
            enclosing_symbol TEXT,
            snippet TEXT,
            FOREIGN KEY(file_id) REFERENCES documents(id) ON DELETE CASCADE
        )
    """)
    
    # Create indexes
    conn.execute("CREATE INDEX IF NOT EXISTS idx_documents_path ON documents(relative_path)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_symbols_id ON symbols(symbol_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_symbols_file ON symbols(file_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_occurrences_symbol ON occurrences(symbol_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_occurrences_file ON occurrences(file_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_relationships_symbol ON relationships(symbol_id)")
    
    # Insert metadata
    conn.execute("INSERT INTO metadata (key, value) VALUES ('version', '1')")
    conn.execute("INSERT INTO metadata (key, value) VALUES ('tool', 'test-indexer')")
    conn.execute("INSERT INTO metadata (key, value) VALUES ('project_root', '/test/project')")
    
    # Insert initial index state
    conn.execute("""
        INSERT INTO index_state (last_commit_hash, last_indexed_at, indexed_files)
        VALUES ('abc123', 1700000000, '[]')
    """)
    
    conn.commit()
    return conn


def seed_test_data(conn: sqlite3.Connection) -> None:
    """Seed the database with sample SCIP-like data for testing.
    
    Creates:
    - MyClass (class, conforms to IMyProtocol)
    - IMyProtocol (protocol)
    - doSomething (function, member of MyClass)
    - MockMyClass (class in TestModule, conforms to IMyProtocol)
    """
    # Insert documents
    conn.execute("""
        INSERT INTO documents (relative_path, language, indexed_at) VALUES
        ('Sources/MyClass.swift', 'swift', 1700000000),
        ('Sources/IMyProtocol.swift', 'swift', 1700000000),
        ('Sources/Assembly.swift', 'swift', 1700000000),
        ('Tests/MyClassTests.swift', 'swift', 1700000000),
        ('Tests/Mocks/MockMyClass.swift', 'swift', 1700000000)
    """)
    
    # Get document IDs
    docs = {row[0]: row[1] for row in conn.execute(
        "SELECT relative_path, id FROM documents"
    ).fetchall()}
    
    # Insert symbols
    conn.execute("""
        INSERT INTO symbols (symbol_id, kind, documentation, file_id) VALUES
        ('swift MyModule MyClass#', 'class', 'A sample class for testing.', ?),
        ('swift MyModule IMyProtocol#', 'protocol', NULL, ?),
        ('swift MyModule MyClass#doSomething().', 'function', NULL, ?),
        ('swift TestModule MockMyClass#', 'class', NULL, ?)
    """, (
        docs['Sources/MyClass.swift'],
        docs['Sources/IMyProtocol.swift'],
        docs['Sources/MyClass.swift'],
        docs['Tests/Mocks/MockMyClass.swift'],
    ))
    
    # Insert relationships
    conn.execute("""
        INSERT INTO relationships (symbol_id, target_symbol_id, kind) VALUES
        ('swift MyModule MyClass#', 'swift MyModule IMyProtocol#', 'conforms'),
        ('swift TestModule MockMyClass#', 'swift MyModule IMyProtocol#', 'conforms')
    """)
    
    # Insert occurrences
    conn.execute("""
        INSERT INTO occurrences (symbol_id, file_id, start_line, start_column, end_line, end_column, roles, snippet, enclosing_symbol) VALUES
        ('swift MyModule MyClass#', ?, 10, 7, 10, 14, 1, 'class MyClass: IMyProtocol {', NULL),
        ('swift MyModule MyClass#', ?, 15, 20, 15, 27, 8, 'let instance = MyClass()', 'swift MyModule Assembly#register().'),
        ('swift MyModule MyClass#', ?, 5, 10, 5, 17, 8, 'var sut: MyClass!', 'swift TestModule MyClassTests#'),
        ('swift MyModule IMyProtocol#', ?, 5, 10, 5, 21, 1, 'protocol IMyProtocol {', NULL),
        ('swift MyModule MyClass#doSomething().', ?, 15, 10, 15, 21, 1, 'func doSomething() {', 'swift MyModule MyClass#'),
        ('swift TestModule MockMyClass#', ?, 3, 7, 3, 18, 1, 'class MockMyClass: IMyProtocol {', NULL)
    """, (
        docs['Sources/MyClass.swift'],
        docs['Sources/Assembly.swift'],
        docs['Tests/MyClassTests.swift'],
        docs['Sources/IMyProtocol.swift'],
        docs['Sources/MyClass.swift'],
        docs['Tests/Mocks/MockMyClass.swift'],
    ))
    
    conn.commit()


@pytest.fixture
def external_db(tmp_path: Path) -> sqlite3.Connection:
    """Create a test database with external indexer schema and test data."""
    db_path = tmp_path / "test.db"
    conn = create_external_indexer_db(db_path)
    seed_test_data(conn)
    return conn


@pytest.fixture
def db_factory(tmp_path: Path) -> Callable[[str], sqlite3.Connection]:
    """Factory fixture to create multiple test databases."""
    def _create(name: str) -> sqlite3.Connection:
        db_path = tmp_path / name
        return create_external_indexer_db(db_path)
    return _create
