"""Tests for materialized view tables and fast loading paths."""

import sqlite3
from pathlib import Path

import pytest

from graphrag.db import schema
from graphrag.db.queries import (
    _load_entities,
    _load_entities_fast,
    _load_relationships,
    _load_relationships_fast,
    get_entity_graph,
)
from graphrag.db.repository import MetadataRepository
from graphrag.models.records import EntityRecord, MemberRecord, RelationshipRecord


def _entity(
    name: str, stable_id: str, path: str, target_type: str = "app", members: list = None
) -> EntityRecord:
    return EntityRecord(
        name=name,
        kind="class",
        module="MyModule",
        language="swift",
        file_path=Path(path),
        start_line=1,
        end_line=50,
        signature=f"class {name}",
        code=f"class {name} {{ ... }}",
        stable_id=stable_id,
        target_type=target_type,
        members=members or [],
    )


def test_rebuild_latest_tables_creates_entity_data(tmp_path: Path):
    """Verify rebuild_latest_tables populates entity_latest with correct data."""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    schema.apply_schema(conn)
    
    members = [
        MemberRecord(
            name="method1",
            kind="method",
            signature="func method1()",
            code="func method1() {}",
            start_line=10,
            end_line=12,
        ),
    ]
    entity = _entity("MyClass", "myclass-stable", "Sources/MyClass.swift", members=members)
    
    repo = MetadataRepository(conn)
    commit = repo.record_commit("commit1", None, "master", True)
    repo.persist_entities(commit, [entity])
    conn.commit()
    
    # Rebuild materialized views
    repo.rebuild_latest_tables()
    conn.commit()
    
    # Verify entity_latest has the data
    row = conn.execute(
        "SELECT * FROM entity_latest WHERE stable_id = ?",
        ("myclass-stable",),
    ).fetchone()
    
    assert row is not None
    assert row["name"] == "MyClass"
    assert row["kind"] == "class"
    assert row["module"] == "MyModule"
    assert "method1" in row["member_names"]
    conn.close()


def test_rebuild_latest_tables_creates_relationship_data(tmp_path: Path):
    """Verify rebuild_latest_tables populates relationship_latest with correct data."""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    schema.apply_schema(conn)
    
    entity_a = _entity("EntityA", "a", "Sources/A.swift")
    entity_b = _entity("EntityB", "b", "Sources/B.swift")
    
    repo = MetadataRepository(conn)
    commit = repo.record_commit("commit1", None, "master", True)
    entity_map = repo.persist_entities(commit, [entity_a, entity_b])
    repo.persist_relationships(
        commit,
        entity_map,
        [
            RelationshipRecord(
                source_stable_id=entity_a.stable_id,
                target_name=entity_b.name,
                edge_type="strongReference",
                target_module="MyModule",
                metadata={"test": True},
            ),
        ],
    )
    conn.commit()
    
    # Rebuild materialized views
    repo.rebuild_latest_tables()
    conn.commit()
    
    # Verify relationship_latest has the data
    row = conn.execute(
        "SELECT * FROM relationship_latest WHERE source_stable_id = ?",
        ("a",),
    ).fetchone()
    
    assert row is not None
    assert row["source_name"] == "EntityA"
    assert row["target_stable_id"] == "b"
    assert row["edge_type"] == "strongReference"
    conn.close()


def test_rebuild_latest_tables_excludes_deleted_entities(tmp_path: Path):
    """Verify rebuild_latest_tables excludes deleted entities."""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    schema.apply_schema(conn)
    
    entity = _entity("DeletedClass", "deleted", "Sources/Deleted.swift")
    
    repo = MetadataRepository(conn)
    commit1 = repo.record_commit("commit1", None, "master", True)
    repo.persist_entities(commit1, [entity])
    
    commit2 = repo.record_commit("commit2", "commit1", "master", True)
    repo.mark_entities_deleted_for_file(Path("Sources/Deleted.swift"), commit2)
    conn.commit()
    
    # Rebuild materialized views
    repo.rebuild_latest_tables()
    conn.commit()
    
    # Verify deleted entity is not in entity_latest
    row = conn.execute(
        "SELECT * FROM entity_latest WHERE stable_id = ?",
        ("deleted",),
    ).fetchone()
    
    assert row is None
    conn.close()


def test_load_entities_fast_matches_standard_loading(tmp_path: Path):
    """Verify _load_entities_fast returns same data as _load_entities."""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    schema.apply_schema(conn)
    
    entities = [
        _entity("ClassA", "a", "Sources/A.swift"),
        _entity("ClassB", "b", "Sources/B.swift"),
        _entity("ClassC", "c", "Sources/C.swift"),
    ]
    
    repo = MetadataRepository(conn)
    commit = repo.record_commit("commit1", None, "master", True)
    repo.persist_entities(commit, entities)
    conn.commit()
    
    # Rebuild materialized views
    repo.rebuild_latest_tables()
    conn.commit()
    
    # Load using both methods
    standard_entities, _ = _load_entities(conn, "master")
    fast_entities = _load_entities_fast(conn, "master")
    
    # Compare keys
    assert set(standard_entities.keys()) == set(fast_entities.keys())
    
    # Compare entity data
    for stable_id in standard_entities:
        standard = standard_entities[stable_id]
        fast = fast_entities[stable_id]
        
        assert standard["name"] == fast["name"]
        assert standard["kind"] == fast["kind"]
        assert standard["module"] == fast["module"]
        assert standard["stable_id"] == fast["stable_id"]
    
    conn.close()


def test_load_relationships_fast_matches_standard_loading(tmp_path: Path):
    """Verify _load_relationships_fast returns same data as _load_relationships."""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    schema.apply_schema(conn)
    
    entity_a = _entity("EntityA", "a", "Sources/A.swift")
    entity_b = _entity("EntityB", "b", "Sources/B.swift")
    
    repo = MetadataRepository(conn)
    commit = repo.record_commit("commit1", None, "master", True)
    entity_map = repo.persist_entities(commit, [entity_a, entity_b])
    repo.persist_relationships(
        commit,
        entity_map,
        [
            RelationshipRecord(
                source_stable_id=entity_a.stable_id,
                target_name=entity_b.name,
                edge_type="strongReference",
                target_module="MyModule",
            ),
            RelationshipRecord(
                source_stable_id=entity_b.stable_id,
                target_name=entity_a.name,
                edge_type="weakReference",
                target_module="MyModule",
            ),
        ],
    )
    conn.commit()
    
    # Rebuild materialized views
    repo.rebuild_latest_tables()
    conn.commit()
    
    # Load using both methods
    standard_rels, _ = _load_relationships(conn, "master")
    fast_rels = _load_relationships_fast(conn, "master")
    
    assert len(standard_rels) == len(fast_rels)
    
    # Build comparable sets
    standard_set = {
        (r["source_stable_id"], r["target_stable_id"], r["edge_type"])
        for r in standard_rels
    }
    fast_set = {
        (r["source_stable_id"], r["target_stable_id"], r["edge_type"])
        for r in fast_rels
    }
    
    assert standard_set == fast_set
    conn.close()


def test_get_entity_graph_fast_path_produces_same_result(tmp_path: Path):
    """Verify get_entity_graph with use_fast_path produces equivalent graph."""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    schema.apply_schema(conn)
    
    assembly = _entity("MyAssembly", "assembly", "Sources/Assembly.swift")
    presenter = _entity("MyPresenter", "presenter", "Sources/Presenter.swift")
    view = _entity("MyView", "view", "Sources/View.swift")
    
    repo = MetadataRepository(conn)
    commit = repo.record_commit("commit1", None, "master", True)
    entity_map = repo.persist_entities(commit, [assembly, presenter, view])
    repo.persist_relationships(
        commit,
        entity_map,
        [
            RelationshipRecord(
                source_stable_id=assembly.stable_id,
                target_name=presenter.name,
                edge_type="creates",
                target_module="MyModule",
            ),
            RelationshipRecord(
                source_stable_id=assembly.stable_id,
                target_name=view.name,
                edge_type="creates",
                target_module="MyModule",
            ),
            RelationshipRecord(
                source_stable_id=view.stable_id,
                target_name=presenter.name,
                edge_type="strongReference",
                target_module="MyModule",
            ),
        ],
    )
    conn.commit()
    
    # Rebuild materialized views
    repo.rebuild_latest_tables()
    conn.commit()
    
    # Get graph using both methods
    standard_graph = get_entity_graph(
        conn, None, entity_name="MyView", stop_name="MyAssembly", use_fast_path=False
    )
    fast_graph = get_entity_graph(
        conn, None, entity_name="MyView", stop_name="MyAssembly", use_fast_path=True
    )
    
    # Compare node sets
    standard_nodes = {n["name"] for n in standard_graph["nodes"]}
    fast_nodes = {n["name"] for n in fast_graph["nodes"]}
    assert standard_nodes == fast_nodes
    
    # Compare edge sets
    standard_edges = {
        (e["source"], e["target"], e["type"]) for e in standard_graph["edges"]
    }
    fast_edges = {
        (e["source"], e["target"], e["type"]) for e in fast_graph["edges"]
    }
    assert standard_edges == fast_edges
    
    conn.close()


def test_rebuild_latest_tables_handles_null_target(tmp_path: Path):
    """Verify rebuild handles relationships to external entities (null target_entity_id)."""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    schema.apply_schema(conn)
    
    entity = _entity("MyClass", "myclass", "Sources/MyClass.swift")
    
    repo = MetadataRepository(conn)
    commit = repo.record_commit("commit1", None, "master", True)
    entity_map = repo.persist_entities(commit, [entity])
    repo.persist_relationships(
        commit,
        entity_map,
        [
            RelationshipRecord(
                source_stable_id=entity.stable_id,
                target_name="ExternalBase",
                edge_type="superclass",
                target_module="ExternalModule",
            ),
        ],
    )
    conn.commit()
    
    # Rebuild materialized views
    repo.rebuild_latest_tables()
    conn.commit()
    
    # Verify relationship with null target is included
    row = conn.execute(
        "SELECT * FROM relationship_latest WHERE target_name = 'ExternalBase'"
    ).fetchone()
    
    assert row is not None
    assert row["target_stable_id"] is None
    conn.close()


def test_rebuild_latest_tables_is_idempotent(tmp_path: Path):
    """Verify rebuild_latest_tables can be called multiple times safely."""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    schema.apply_schema(conn)
    
    entity = _entity("MyClass", "myclass", "Sources/MyClass.swift")
    
    repo = MetadataRepository(conn)
    commit = repo.record_commit("commit1", None, "master", True)
    repo.persist_entities(commit, [entity])
    conn.commit()
    
    # Call rebuild multiple times
    repo.rebuild_latest_tables()
    repo.rebuild_latest_tables()
    repo.rebuild_latest_tables()
    conn.commit()
    
    # Should still have exactly one entity
    count = conn.execute("SELECT COUNT(*) as cnt FROM entity_latest").fetchone()["cnt"]
    assert count == 1
    
    conn.close()

