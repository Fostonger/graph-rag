from __future__ import annotations

import sqlite3


SCHEMA_VERSION = 2


def apply_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        """
    )
    conn.execute(
        """
        INSERT OR IGNORE INTO schema_meta (key, value) VALUES ('version', ?);
        """,
        (str(SCHEMA_VERSION),),
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS commits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hash TEXT UNIQUE NOT NULL,
            parent_hash TEXT,
            branch TEXT NOT NULL,
            is_master INTEGER DEFAULT 0,
            indexed_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            path TEXT UNIQUE NOT NULL,
            language TEXT NOT NULL
        );
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS entities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stable_id TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            kind TEXT NOT NULL,
            module TEXT,
            language TEXT NOT NULL,
            primary_file_id INTEGER REFERENCES files(id) ON DELETE SET NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS entity_files (
            entity_id INTEGER NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
            file_id INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
            is_primary INTEGER DEFAULT 0,
            UNIQUE(entity_id, file_id)
        );
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS entity_versions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_id INTEGER NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
            commit_id INTEGER NOT NULL REFERENCES commits(id) ON DELETE CASCADE,
            file_id INTEGER REFERENCES files(id) ON DELETE SET NULL,
            start_line INTEGER,
            end_line INTEGER,
            signature TEXT,
            docstring TEXT,
            code TEXT,
            properties TEXT,
            is_deleted INTEGER DEFAULT 0
        );
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS members (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_id INTEGER NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
            stable_id TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            kind TEXT NOT NULL
        );
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS member_versions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            member_id INTEGER NOT NULL REFERENCES members(id) ON DELETE CASCADE,
            commit_id INTEGER NOT NULL REFERENCES commits(id) ON DELETE CASCADE,
            file_id INTEGER REFERENCES files(id) ON DELETE SET NULL,
            start_line INTEGER,
            end_line INTEGER,
            signature TEXT,
            code TEXT,
            is_deleted INTEGER DEFAULT 0
        );
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS entity_relationships (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_entity_id INTEGER NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
            target_entity_id INTEGER REFERENCES entities(id) ON DELETE SET NULL,
            target_name TEXT NOT NULL,
            target_module TEXT,
            edge_type TEXT NOT NULL,
            metadata TEXT,
            commit_id INTEGER NOT NULL REFERENCES commits(id) ON DELETE CASCADE,
            is_deleted INTEGER DEFAULT 0
        );
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS extensions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_id INTEGER NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
            commit_id INTEGER NOT NULL REFERENCES commits(id) ON DELETE CASCADE,
            file_id INTEGER REFERENCES files(id) ON DELETE SET NULL,
            extended_type TEXT NOT NULL,
            constraints TEXT,
            start_line INTEGER,
            end_line INTEGER,
            code TEXT
        );
        """
    )

    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_entities_name ON entities(name);
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_entities_module ON entities(module);
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_entity_files_file ON entity_files(file_id);
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_entity_versions_entity_commit
            ON entity_versions(entity_id, commit_id);
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_members_name ON members(name);
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_member_versions_member_commit
            ON member_versions(member_id, commit_id);
        """
    )

    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_relationships_source
            ON entity_relationships(source_entity_id, edge_type);
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_relationships_target
            ON entity_relationships(target_entity_id, edge_type);
        """
    )

    # Performance indexes for graph queries (added in schema v2)
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_entity_versions_lookup
            ON entity_versions(entity_id, commit_id DESC);
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_entity_versions_not_deleted
            ON entity_versions(is_deleted, entity_id, commit_id DESC)
            WHERE is_deleted = 0;
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_relationships_composite
            ON entity_relationships(
                source_entity_id,
                target_entity_id,
                target_name,
                edge_type,
                commit_id DESC
            );
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_members_entity
            ON members(entity_id);
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_relationships_commit
            ON entity_relationships(commit_id, is_deleted);
        """
    )

