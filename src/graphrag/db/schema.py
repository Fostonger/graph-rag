from __future__ import annotations

import sqlite3


SCHEMA_VERSION = 4


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
            stable_id TEXT UNIQUE NOT NULL,
            entity_id INTEGER NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
            extended_type TEXT NOT NULL,
            module TEXT,
            language TEXT NOT NULL DEFAULT 'swift'
        );
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS extension_versions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            extension_id INTEGER NOT NULL REFERENCES extensions(id) ON DELETE CASCADE,
            commit_id INTEGER NOT NULL REFERENCES commits(id) ON DELETE CASCADE,
            file_id INTEGER REFERENCES files(id) ON DELETE SET NULL,
            start_line INTEGER,
            end_line INTEGER,
            signature TEXT,
            code TEXT,
            visibility TEXT,
            constraints TEXT,
            conformances TEXT,
            properties TEXT,
            is_deleted INTEGER DEFAULT 0
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

    # Materialized views for fast graph queries (added in schema v3)
    # These tables store pre-computed latest state and are rebuilt during indexing
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS entity_latest (
            stable_id TEXT PRIMARY KEY,
            entity_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            kind TEXT NOT NULL,
            module TEXT,
            file_path TEXT,
            signature TEXT,
            properties TEXT,
            member_names TEXT,
            target_type TEXT,
            visibility TEXT,
            commit_hash TEXT
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS relationship_latest (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_stable_id TEXT NOT NULL,
            source_name TEXT NOT NULL,
            target_stable_id TEXT,
            target_name TEXT NOT NULL,
            target_module TEXT,
            edge_type TEXT NOT NULL,
            metadata TEXT,
            UNIQUE(source_stable_id, target_stable_id, target_name, target_module, edge_type)
        );
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_entity_latest_name
            ON entity_latest(name);
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_relationship_latest_source
            ON relationship_latest(source_stable_id);
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_relationship_latest_target
            ON relationship_latest(target_stable_id);
        """
    )

    # Extension materialized view (added in schema v4)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS extension_latest (
            stable_id TEXT PRIMARY KEY,
            extension_id INTEGER NOT NULL,
            entity_id INTEGER NOT NULL,
            entity_stable_id TEXT NOT NULL,
            extended_type TEXT NOT NULL,
            module TEXT,
            file_path TEXT,
            signature TEXT,
            visibility TEXT,
            constraints TEXT,
            conformances TEXT,
            member_names TEXT,
            target_type TEXT,
            commit_hash TEXT
        );
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_extensions_entity
            ON extensions(entity_id);
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_extension_versions_extension_commit
            ON extension_versions(extension_id, commit_id);
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_extension_latest_entity
            ON extension_latest(entity_stable_id);
        """
    )

