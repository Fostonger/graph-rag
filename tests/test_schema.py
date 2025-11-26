"""Tests for database schema and index creation."""

import sqlite3
from pathlib import Path

import pytest

from graphrag.db.schema import SCHEMA_VERSION, apply_schema


def test_schema_version_is_current():
    """Verify schema version is at expected value."""
    assert SCHEMA_VERSION == 4


def test_apply_schema_creates_all_tables(tmp_path: Path):
    """Verify that apply_schema creates all required tables."""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    
    apply_schema(conn)
    
    # Query all tables
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"
    ).fetchall()
    table_names = {row["name"] for row in tables}
    
    expected_tables = {
        "schema_meta",
        "commits",
        "files",
        "entities",
        "entity_files",
        "entity_versions",
        "members",
        "member_versions",
        "entity_relationships",
        "extensions",
        "extension_versions",
        # Materialized views (schema v3+)
        "entity_latest",
        "relationship_latest",
        "extension_latest",
    }
    
    assert expected_tables.issubset(table_names)
    conn.close()


def test_apply_schema_creates_all_indexes(tmp_path: Path):
    """Verify that apply_schema creates all required indexes."""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    
    apply_schema(conn)
    
    # Query all indexes
    indexes = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%' ORDER BY name;"
    ).fetchall()
    index_names = {row["name"] for row in indexes}
    
    expected_indexes = {
        # Original indexes
        "idx_entities_name",
        "idx_entities_module",
        "idx_entity_files_file",
        "idx_entity_versions_entity_commit",
        "idx_members_name",
        "idx_member_versions_member_commit",
        "idx_relationships_source",
        "idx_relationships_target",
        # Performance indexes (schema v2)
        "idx_entity_versions_lookup",
        "idx_entity_versions_not_deleted",
        "idx_relationships_composite",
        "idx_members_entity",
        "idx_relationships_commit",
        # Materialized view indexes (schema v3)
        "idx_entity_latest_name",
        "idx_relationship_latest_source",
        "idx_relationship_latest_target",
        # Extension indexes (schema v4)
        "idx_extensions_entity",
        "idx_extension_versions_extension_commit",
        "idx_extension_latest_entity",
    }
    
    assert expected_indexes.issubset(index_names), (
        f"Missing indexes: {expected_indexes - index_names}"
    )
    conn.close()


def test_apply_schema_is_idempotent(tmp_path: Path):
    """Verify that apply_schema can be called multiple times safely."""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    
    # Apply schema twice
    apply_schema(conn)
    apply_schema(conn)
    
    # Should not raise and tables should exist
    tables = conn.execute(
        "SELECT COUNT(*) as cnt FROM sqlite_master WHERE type='table';"
    ).fetchone()
    assert tables["cnt"] > 0
    conn.close()


def _get_plan_text(rows) -> str:
    """Extract text from EXPLAIN QUERY PLAN rows."""
    parts = []
    for row in rows:
        # EXPLAIN QUERY PLAN returns (id, parent, notused, detail)
        if len(row) >= 4:
            parts.append(str(row[3]))  # detail column
        else:
            parts.append(" ".join(str(col) for col in row))
    return " ".join(parts)


def test_entity_versions_lookup_index_used_in_max_query(tmp_path: Path):
    """Verify that the lookup index is used for MAX(commit_id) queries."""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    
    apply_schema(conn)
    
    # Insert some test data
    conn.execute("INSERT INTO commits (hash, branch, is_master) VALUES ('abc', 'main', 1);")
    conn.execute("INSERT INTO files (path, language) VALUES ('test.swift', 'swift');")
    conn.execute(
        "INSERT INTO entities (stable_id, name, kind, language) VALUES ('e1', 'Entity1', 'class', 'swift');"
    )
    conn.execute(
        "INSERT INTO entity_versions (entity_id, commit_id, is_deleted) VALUES (1, 1, 0);"
    )
    conn.commit()
    
    # Check query plan for the typical MAX(commit_id) pattern
    plan = conn.execute(
        """
        EXPLAIN QUERY PLAN
        SELECT entity_id, MAX(commit_id) AS commit_id
        FROM entity_versions
        GROUP BY entity_id
        """
    ).fetchall()
    
    plan_text = _get_plan_text(plan)
    
    # The plan should use an index (either the new lookup index or entity_commit)
    assert "SCAN" not in plan_text or "INDEX" in plan_text or "COVERING" in plan_text
    conn.close()


def test_relationships_composite_index_used_for_partition_query(tmp_path: Path):
    """Verify that composite index is used for relationship partition queries."""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    
    apply_schema(conn)
    
    # Insert test data
    conn.execute("INSERT INTO commits (hash, branch, is_master) VALUES ('abc', 'main', 1);")
    conn.execute("INSERT INTO files (path, language) VALUES ('test.swift', 'swift');")
    conn.execute(
        "INSERT INTO entities (stable_id, name, kind, language) VALUES ('e1', 'Src', 'class', 'swift');"
    )
    conn.execute(
        "INSERT INTO entities (stable_id, name, kind, language) VALUES ('e2', 'Tgt', 'class', 'swift');"
    )
    conn.execute(
        """
        INSERT INTO entity_relationships 
        (source_entity_id, target_entity_id, target_name, edge_type, commit_id, is_deleted)
        VALUES (1, 2, 'Tgt', 'strongReference', 1, 0);
        """
    )
    conn.commit()
    
    # Check query plan for source entity lookup
    plan = conn.execute(
        """
        EXPLAIN QUERY PLAN
        SELECT * FROM entity_relationships
        WHERE source_entity_id = 1
        ORDER BY commit_id DESC
        """
    ).fetchall()
    
    plan_text = _get_plan_text(plan)
    
    # Should use an index for the source_entity_id lookup
    assert "INDEX" in plan_text or "USING" in plan_text
    conn.close()


def test_members_entity_index_used_for_entity_lookup(tmp_path: Path):
    """Verify that members_entity index is used for member lookups by entity."""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    
    apply_schema(conn)
    
    # Insert test data
    conn.execute(
        "INSERT INTO entities (stable_id, name, kind, language) VALUES ('e1', 'MyClass', 'class', 'swift');"
    )
    conn.execute(
        "INSERT INTO members (entity_id, stable_id, name, kind) VALUES (1, 'm1', 'method1', 'method');"
    )
    conn.commit()
    
    # Check query plan for member lookup by entity
    plan = conn.execute(
        """
        EXPLAIN QUERY PLAN
        SELECT name FROM members WHERE entity_id = 1
        """
    ).fetchall()
    
    plan_text = _get_plan_text(plan)
    
    # Should use the members_entity index
    assert "idx_members_entity" in plan_text or "INDEX" in plan_text
    conn.close()


def test_partial_index_filters_deleted_entities(tmp_path: Path):
    """Verify the partial index on is_deleted works for non-deleted entity queries."""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    
    apply_schema(conn)
    
    # Insert test data with both deleted and non-deleted versions
    conn.execute("INSERT INTO commits (hash, branch, is_master) VALUES ('c1', 'main', 1);")
    conn.execute("INSERT INTO commits (hash, branch, is_master) VALUES ('c2', 'main', 1);")
    conn.execute(
        "INSERT INTO entities (stable_id, name, kind, language) VALUES ('e1', 'Entity1', 'class', 'swift');"
    )
    conn.execute(
        "INSERT INTO entity_versions (entity_id, commit_id, is_deleted) VALUES (1, 1, 0);"
    )
    conn.execute(
        "INSERT INTO entity_versions (entity_id, commit_id, is_deleted) VALUES (1, 2, 1);"
    )
    conn.commit()
    
    # Check query plan for non-deleted entity versions
    plan = conn.execute(
        """
        EXPLAIN QUERY PLAN
        SELECT entity_id, MAX(commit_id) AS commit_id
        FROM entity_versions
        WHERE is_deleted = 0
        GROUP BY entity_id
        """
    ).fetchall()
    
    plan_text = _get_plan_text(plan)
    
    # Should use an index
    assert "INDEX" in plan_text or "USING" in plan_text
    conn.close()

