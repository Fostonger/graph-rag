"""Tests for in-memory graph cache."""

import sqlite3
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from graphrag.db import schema
from graphrag.db.cache import (
    GraphCache,
    get_global_cache,
    reset_global_cache,
)
from graphrag.db.repository import MetadataRepository
from graphrag.models.records import EntityRecord, RelationshipRecord


def _entity(name: str, stable_id: str, path: str) -> EntityRecord:
    return EntityRecord(
        name=name,
        kind="class",
        module="TestModule",
        language="swift",
        file_path=Path(path),
        start_line=1,
        end_line=10,
        signature=f"class {name}",
        code=f"class {name} {{}}",
        stable_id=stable_id,
        target_type="app",
        members=[],
    )


@pytest.fixture
def populated_db(tmp_path: Path):
    """Create a database with entities and relationships, with materialized views populated."""
    db_path = tmp_path / "cache_test.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    schema.apply_schema(conn)
    
    entity_a = _entity("EntityA", "a", "Sources/A.swift")
    entity_b = _entity("EntityB", "b", "Sources/B.swift")
    
    repo = MetadataRepository(conn)
    commit = repo.record_commit("commit-abc123", None, "master", True)
    entity_map = repo.persist_entities(commit, [entity_a, entity_b])
    repo.persist_relationships(
        commit,
        entity_map,
        [
            RelationshipRecord(
                source_stable_id=entity_a.stable_id,
                target_name=entity_b.name,
                edge_type="strongReference",
                target_module="TestModule",
            ),
        ],
    )
    repo.rebuild_latest_tables()
    conn.commit()
    
    yield conn, "commit-abc123"
    
    conn.close()


def test_cache_loads_entities_on_miss(populated_db):
    """Verify cache loads entities on first access."""
    conn, commit_hash = populated_db
    cache = GraphCache()
    
    entities = cache.get_entities(conn, commit_hash)
    
    assert len(entities) == 2
    assert "a" in entities
    assert "b" in entities
    assert entities["a"]["name"] == "EntityA"


def test_cache_loads_relationships_on_miss(populated_db):
    """Verify cache loads relationships on first access."""
    conn, commit_hash = populated_db
    cache = GraphCache()
    
    relationships = cache.get_relationships(conn, commit_hash)
    
    assert len(relationships) == 1
    assert relationships[0]["source_stable_id"] == "a"
    assert relationships[0]["edge_type"] == "strongReference"


def test_cache_returns_cached_data_on_hit(populated_db):
    """Verify cache returns cached data without reloading on subsequent access."""
    conn, commit_hash = populated_db
    cache = GraphCache()
    
    # First access - miss
    entities1 = cache.get_entities(conn, commit_hash)
    
    # Second access - should be a hit
    entities2 = cache.get_entities(conn, commit_hash)
    
    assert entities1 is entities2  # Same object reference
    
    stats = cache.get_stats()
    assert stats["hit_count"] == 1
    assert stats["miss_count"] == 1


def test_cache_invalidates_on_commit_change(populated_db):
    """Verify cache invalidates when commit hash changes."""
    conn, commit_hash = populated_db
    cache = GraphCache()
    
    # Load with original commit
    entities1 = cache.get_entities(conn, commit_hash)
    
    # Access with different commit hash
    entities2 = cache.get_entities(conn, "different-commit-hash")
    
    # Should be a fresh load (different object)
    assert entities1 is not entities2
    
    stats = cache.get_stats()
    assert stats["miss_count"] == 2  # Both were misses


def test_cache_invalidate_clears_data(populated_db):
    """Verify invalidate() clears cached data."""
    conn, commit_hash = populated_db
    cache = GraphCache()
    
    # Load data
    cache.get_entities(conn, commit_hash)
    cache.get_relationships(conn, commit_hash)
    
    assert cache.get_stats()["is_populated"] is True
    
    # Invalidate
    cache.invalidate()
    
    assert cache.get_stats()["is_populated"] is False
    assert cache.get_stats()["entity_count"] == 0


def test_cache_ttl_expiration(populated_db):
    """Verify cache expires after TTL."""
    conn, commit_hash = populated_db
    cache = GraphCache(ttl_seconds=1)  # 1 second TTL
    
    # Load data
    entities1 = cache.get_entities(conn, commit_hash)
    
    # Should be cached
    entities2 = cache.get_entities(conn, commit_hash)
    assert entities1 is entities2
    
    # Wait for TTL to expire
    time.sleep(1.1)
    
    # Should reload due to TTL expiration
    entities3 = cache.get_entities(conn, commit_hash)
    assert entities1 is not entities3
    
    stats = cache.get_stats()
    assert stats["miss_count"] == 2  # First load + TTL reload


def test_cache_stats_tracking(populated_db):
    """Verify cache stats are tracked correctly."""
    conn, commit_hash = populated_db
    cache = GraphCache()
    
    # Initial stats
    stats = cache.get_stats()
    assert stats["hit_count"] == 0
    assert stats["miss_count"] == 0
    assert stats["hit_rate"] == 0.0
    
    # First access - miss
    cache.get_entities(conn, commit_hash)
    
    # Second access - hit
    cache.get_entities(conn, commit_hash)
    
    # Third access - hit
    cache.get_entities(conn, commit_hash)
    
    stats = cache.get_stats()
    assert stats["hit_count"] == 2
    assert stats["miss_count"] == 1
    assert stats["hit_rate"] == pytest.approx(2/3)


def test_global_cache_singleton():
    """Verify global cache is a singleton."""
    reset_global_cache()
    
    cache1 = get_global_cache()
    cache2 = get_global_cache()
    
    assert cache1 is cache2
    
    reset_global_cache()


def test_global_cache_reset():
    """Verify global cache can be reset."""
    reset_global_cache()
    
    cache1 = get_global_cache()
    cache1._hits = 100  # Modify internal state
    
    reset_global_cache()
    
    cache2 = get_global_cache()
    assert cache2.get_stats()["hit_count"] == 0
    
    reset_global_cache()


def test_cache_handles_empty_database(tmp_path: Path):
    """Verify cache handles empty database gracefully."""
    db_path = tmp_path / "empty.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    schema.apply_schema(conn)
    conn.commit()
    
    cache = GraphCache()
    
    entities = cache.get_entities(conn, "empty-commit")
    relationships = cache.get_relationships(conn, "empty-commit")
    
    assert entities == {}
    assert relationships == []
    
    conn.close()


def test_cache_consistency_entities_and_relationships(populated_db):
    """Verify entities and relationships stay in sync."""
    conn, commit_hash = populated_db
    cache = GraphCache()
    
    # Load entities first
    entities = cache.get_entities(conn, commit_hash)
    
    # Change commit hash (simulate data change)
    relationships = cache.get_relationships(conn, "new-commit")
    
    # Entities should be reloaded too (cache invalidated)
    entities_after = cache.get_entities(conn, "new-commit")
    
    # The entities object should be refreshed (cache was invalidated by new commit)
    assert cache.get_stats()["last_commit_hash"] == "new-commit"

