import sqlite3
from pathlib import Path

import pytest

from graphrag.db import schema
from graphrag.db.queries import get_entity_graph, _load_entities
from graphrag.db.repository import MetadataRepository
from graphrag.models.records import EntityRecord, MemberRecord, RelationshipRecord


def _entity(name: str, stable_id: str, path: str, target_type: str = "app") -> EntityRecord:
    return EntityRecord(
        name=name,
        kind="class",
        module="MyModule",
        language="swift",
        file_path=Path(path),
        start_line=1,
        end_line=5,
        signature=f"class {name}",
        code=f"class {name} {{}}",
        stable_id=stable_id,
        target_type=target_type,
        members=[],
    )


def test_get_entity_graph_merges_master_and_feature(tmp_path):
    master_conn = sqlite3.connect(tmp_path / "master.db")
    master_conn.row_factory = sqlite3.Row
    feature_conn = sqlite3.connect(tmp_path / "feature.db")
    feature_conn.row_factory = sqlite3.Row
    schema.apply_schema(master_conn)
    schema.apply_schema(feature_conn)

    assembly = _entity("MyModuleAssembly", "assembly", "Sources/Assembly.swift")
    presenter = _entity("MyModulePresenter", "presenter", "Sources/Presenter.swift")
    view = _entity(
        "MyModuleViewController", "view", "Sources/ViewController.swift"
    )
    worker = _entity("NetworkWorker", "worker", "Sources/Worker.swift")

    master_repo = MetadataRepository(master_conn)
    master_commit = master_repo.record_commit("master1", None, "master", True)
    master_map = master_repo.persist_entities(
        master_commit, [assembly, presenter, view]
    )
    master_repo.persist_relationships(
        master_commit,
        master_map,
        [
            RelationshipRecord(
                source_stable_id=assembly.stable_id,
                target_name=presenter.name,
                edge_type="creates",
                target_module="MyModule",
                metadata={"member": "makePresenter"},
            ),
            RelationshipRecord(
                source_stable_id=assembly.stable_id,
                target_name=view.name,
                edge_type="creates",
                target_module="MyModule",
                metadata={"member": "makeViewController"},
            ),
            RelationshipRecord(
                source_stable_id=view.stable_id,
                target_name=presenter.name,
                edge_type="strongReference",
                target_module="MyModule",
            ),
            RelationshipRecord(
                source_stable_id=presenter.stable_id,
                target_name=view.name,
                edge_type="weakReference",
                target_module="MyModule",
            ),
            RelationshipRecord(
                source_stable_id=presenter.stable_id,
                target_name="BasePresenter",
                edge_type="superclass",
                target_module="MyModule",
            ),
            RelationshipRecord(
                source_stable_id=view.stable_id,
                target_name="ViewInput",
                edge_type="conforms",
                target_module="MyModule",
            ),
        ],
    )
    master_conn.commit()

    feature_repo = MetadataRepository(feature_conn)
    feature_commit = feature_repo.record_commit("feature1", None, "feature/foo", False)
    feature_repo.persist_entities(feature_commit, [view])
    assembly_map = feature_repo.persist_entities(feature_commit, [assembly])
    presenter_map = feature_repo.persist_entities(feature_commit, [presenter])
    feature_repo.persist_entities(feature_commit, [worker])

    feature_repo.persist_relationships(
        feature_commit,
        assembly_map,
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
                source_stable_id=assembly.stable_id,
                target_name=worker.name,
                edge_type="creates",
                target_module="MyModule",
            ),
        ],
    )
    feature_repo.persist_relationships(
        feature_commit,
        presenter_map,
        [
            RelationshipRecord(
                source_stable_id=presenter.stable_id,
                target_name=view.name,
                edge_type="weakReference",
                target_module="MyModule",
                metadata={"branch": "feature"},
            ),
            RelationshipRecord(
                source_stable_id=presenter.stable_id,
                target_name=worker.name,
                edge_type="strongReference",
                target_module="MyModule",
            ),
        ],
    )
    feature_conn.commit()

    graph = get_entity_graph(
        master_conn,
        feature_conn,
        entity_name="MyModuleViewController",
        stop_name="MyModuleAssembly",
    )

    edge_set = {(edge["source"], edge["target"], edge["type"]) for edge in graph["edges"]}
    assert ("MyModuleViewController", "MyModuleAssembly", "createdBy") in edge_set
    assert ("MyModulePresenter", "MyModuleAssembly", "createdBy") in edge_set
    assert ("MyModuleViewController", "MyModulePresenter", "strongReference") in edge_set
    assert ("MyModulePresenter", "NetworkWorker", "strongReference") not in edge_set
    assert ("MyModulePresenter", "BasePresenter", "superclass") in edge_set
    assert ("MyModuleViewController", "ViewInput", "conforms") in edge_set

    weak_edge = next(
        edge for edge in graph["edges"] if edge["type"] == "weakReference"
    )
    assert weak_edge["metadata"]["origin"] == "feature"

    expanded_graph = get_entity_graph(
        master_conn,
        feature_conn,
        entity_name="MyModuleViewController",
        stop_name="MyModuleAssembly",
        include_sibling_subgraphs=True,
    )
    expanded_edges = {
        (edge["source"], edge["target"], edge["type"])
        for edge in expanded_graph["edges"]
    }
    assert ("MyModulePresenter", "NetworkWorker", "strongReference") in expanded_edges

    node_names = {node["name"] for node in graph["nodes"]}
    assert "MyModuleAssembly" not in node_names
    assert "MyModuleViewController" in node_names


def test_get_entity_graph_applies_feature_deletions(tmp_path):
    master_conn = sqlite3.connect(tmp_path / "master_del.db")
    master_conn.row_factory = sqlite3.Row
    feature_conn = sqlite3.connect(tmp_path / "feature_del.db")
    feature_conn.row_factory = sqlite3.Row
    schema.apply_schema(master_conn)
    schema.apply_schema(feature_conn)

    assembly = _entity("MyModuleAssembly", "assembly", "Sources/Assembly.swift")
    presenter = _entity("MyModulePresenter", "presenter", "Sources/Presenter.swift")
    view = _entity("MyModuleViewController", "view", "Sources/ViewController.swift")
    obsolete = _entity("ObsoleteView", "obsolete", "Sources/Obsolete.swift")

    master_repo = MetadataRepository(master_conn)
    master_commit = master_repo.record_commit("master-del", None, "master", True)
    master_map = master_repo.persist_entities(
        master_commit, [assembly, presenter, view, obsolete]
    )
    master_repo.persist_relationships(
        master_commit,
        master_map,
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
                source_stable_id=presenter.stable_id,
                target_name=view.name,
                edge_type="weakReference",
                target_module="MyModule",
            ),
            RelationshipRecord(
                source_stable_id=presenter.stable_id,
                target_name=obsolete.name,
                edge_type="strongReference",
                target_module="MyModule",
            ),
        ],
    )
    master_conn.commit()

    feature_repo = MetadataRepository(feature_conn)
    base_commit = feature_repo.record_commit("feature-base", None, "feature/foo", False)
    base_map = feature_repo.persist_entities(
        base_commit, [assembly, presenter, view, obsolete]
    )
    feature_repo.persist_relationships(
        base_commit,
        base_map,
        [
            RelationshipRecord(
                source_stable_id=presenter.stable_id,
                target_name=view.name,
                edge_type="weakReference",
                target_module="MyModule",
            ),
            RelationshipRecord(
                source_stable_id=presenter.stable_id,
                target_name=obsolete.name,
                edge_type="strongReference",
                target_module="MyModule",
            ),
        ],
    )
    delete_commit = feature_repo.record_commit(
        "feature-del", base_commit, "feature/foo", False
    )
    feature_repo.mark_entities_deleted_for_file(Path("Sources/Obsolete.swift"), delete_commit)
    feature_conn.commit()

    graph = get_entity_graph(
        master_conn,
        feature_conn,
        entity_name="MyModulePresenter",
        stop_name="MyModuleAssembly",
    )

    node_names = {node["name"] for node in graph["nodes"]}
    assert "ObsoleteView" not in node_names
    edge_set = {(edge["source"], edge["target"], edge["type"]) for edge in graph["edges"]}
    assert ("MyModulePresenter", "ObsoleteView", "strongReference") not in edge_set
    assert ("MyModulePresenter", "MyModuleViewController", "weakReference") in edge_set


def test_get_entity_graph_limits_hops(tmp_path):
    conn = sqlite3.connect(tmp_path / "hops.db")
    conn.row_factory = sqlite3.Row
    schema.apply_schema(conn)
    a = _entity("EntityA", "a", "Sources/A.swift")
    b = _entity("EntityB", "b", "Sources/B.swift")
    c = _entity("EntityC", "c", "Sources/C.swift")
    repo = MetadataRepository(conn)
    commit = repo.record_commit("hops", None, "master", True)
    entity_map = repo.persist_entities(commit, [a, b, c])
    repo.persist_relationships(
        commit,
        entity_map,
        [
            RelationshipRecord(
                source_stable_id=a.stable_id,
                target_name=b.name,
                edge_type="strongReference",
                target_module="MyModule",
            ),
            RelationshipRecord(
                source_stable_id=b.stable_id,
                target_name=c.name,
                edge_type="strongReference",
                target_module="MyModule",
            ),
        ],
    )
    conn.commit()

    limited_graph = get_entity_graph(
        conn,
        None,
        entity_name="EntityA",
        include_sibling_subgraphs=True,
        max_hops=1,
    )
    limited_edges = {
        (edge["source"], edge["target"], edge["type"])
        for edge in limited_graph["edges"]
    }
    assert ("EntityB", "EntityC", "strongReference") not in limited_edges
    assert ("EntityA", "EntityB", "strongReference") in limited_edges


def test_get_entity_graph_zero_hops_removes_references(tmp_path):
    conn = sqlite3.connect(tmp_path / "zero-hops.db")
    conn.row_factory = sqlite3.Row
    schema.apply_schema(conn)
    source = _entity("Source", "source", "Sources/Source.swift")
    target = _entity("Target", "target", "Sources/Target.swift")
    repo = MetadataRepository(conn)
    commit = repo.record_commit("zero", None, "master", True)
    entity_map = repo.persist_entities(commit, [source, target])
    repo.persist_relationships(
        commit,
        entity_map,
        [
            RelationshipRecord(
                source_stable_id=source.stable_id,
                target_name=target.name,
                edge_type="strongReference",
                target_module="MyModule",
            )
        ],
    )
    conn.commit()

    limited_graph = get_entity_graph(
        conn,
        None,
        entity_name="Source",
        include_sibling_subgraphs=False,
        max_hops=0,
    )
    assert limited_graph["edges"] == []
    node_names = {node["name"] for node in limited_graph["nodes"]}
    assert node_names == {"Source"}


def test_get_entity_graph_respects_target_type_filter(tmp_path):
    conn = sqlite3.connect(tmp_path / "target-filter.db")
    conn.row_factory = sqlite3.Row
    schema.apply_schema(conn)
    presenter = _entity("MyModulePresenter", "presenter", "Sources/Presenter.swift", target_type="app")
    presenter_tests = _entity(
        "MyModulePresenterTests",
        "presenter_tests",
        "Tests/PresenterTests.swift",
        target_type="test",
    )
    repo = MetadataRepository(conn)
    commit = repo.record_commit("main", None, "master", True)
    entity_map = repo.persist_entities(commit, [presenter, presenter_tests])
    repo.persist_relationships(
        commit,
        entity_map,
        [
            RelationshipRecord(
                source_stable_id=presenter.stable_id,
                target_name="MyModuleAssembly",
                edge_type="creates",
                target_module="MyModule",
            ),
            RelationshipRecord(
                source_stable_id=presenter_tests.stable_id,
                target_name=presenter.name,
                edge_type="strongReference",
                target_module="MyModule",
            ),
        ],
    )
    conn.commit()

    graph_app = get_entity_graph(
        conn,
        None,
        entity_name="MyModulePresenter",
        target_type="app",
    )
    app_nodes = {node["name"] for node in graph_app["nodes"]}
    assert "MyModulePresenterTests" not in app_nodes

    with pytest.raises(ValueError):
        get_entity_graph(
            conn,
            None,
            entity_name="MyModulePresenterTests",
            target_type="app",
        )

    graph_tests = get_entity_graph(
        conn,
        None,
        entity_name="MyModulePresenterTests",
        target_type="test",
    )
    test_nodes = {node["name"] for node in graph_tests["nodes"]}
    assert test_nodes == {"MyModulePresenterTests"}


def _entity_with_members(
    name: str, stable_id: str, path: str, members: list
) -> EntityRecord:
    """Create an entity with members for testing."""
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
        target_type="app",
        members=members,
    )


def test_load_entities_includes_member_names(tmp_path):
    """Verify that _load_entities correctly loads member names from batch query."""
    conn = sqlite3.connect(tmp_path / "members.db")
    conn.row_factory = sqlite3.Row
    schema.apply_schema(conn)
    
    # Create entity with members
    members = [
        MemberRecord(
            name="doSomething",
            kind="method",
            signature="func doSomething()",
            code="func doSomething() {}",
            start_line=10,
            end_line=12,
        ),
        MemberRecord(
            name="myProperty",
            kind="property",
            signature="var myProperty: String",
            code="var myProperty: String = \"\"",
            start_line=5,
            end_line=5,
        ),
    ]
    entity = _entity_with_members(
        "MyClass", "myclass-stable", "Sources/MyClass.swift", members
    )
    
    repo = MetadataRepository(conn)
    commit = repo.record_commit("test1", None, "master", True)
    repo.persist_entities(commit, [entity])
    conn.commit()
    
    # Load entities and check member names
    entities, tombstones = _load_entities(conn, "master")
    
    assert len(entities) == 1
    assert len(tombstones) == 0
    
    loaded = entities["myclass-stable"]
    assert set(loaded["member_names"]) == {"doSomething", "myProperty"}
    conn.close()


def test_load_entities_handles_no_members(tmp_path):
    """Verify that _load_entities works correctly for entities without members."""
    conn = sqlite3.connect(tmp_path / "no-members.db")
    conn.row_factory = sqlite3.Row
    schema.apply_schema(conn)
    
    entity = _entity("EmptyClass", "empty-stable", "Sources/Empty.swift")
    
    repo = MetadataRepository(conn)
    commit = repo.record_commit("test1", None, "master", True)
    repo.persist_entities(commit, [entity])
    conn.commit()
    
    entities, tombstones = _load_entities(conn, "master")
    
    assert len(entities) == 1
    loaded = entities["empty-stable"]
    assert loaded["member_names"] == []
    conn.close()


def test_load_entities_handles_tombstones(tmp_path):
    """Verify that _load_entities correctly identifies deleted entities."""
    conn = sqlite3.connect(tmp_path / "tombstones.db")
    conn.row_factory = sqlite3.Row
    schema.apply_schema(conn)
    
    entity = _entity("DeletedClass", "deleted-stable", "Sources/Deleted.swift")
    
    repo = MetadataRepository(conn)
    commit1 = repo.record_commit("commit1", None, "master", True)
    repo.persist_entities(commit1, [entity])
    
    # Mark as deleted
    commit2 = repo.record_commit("commit2", "commit1", "master", True)
    repo.mark_entities_deleted_for_file(Path("Sources/Deleted.swift"), commit2)
    conn.commit()
    
    entities, tombstones = _load_entities(conn, "master")
    
    assert "deleted-stable" not in entities
    assert "deleted-stable" in tombstones
    conn.close()


def test_load_entities_batch_performance(tmp_path):
    """Verify that _load_entities uses batch loading (2 queries, not N+1)."""
    conn = sqlite3.connect(tmp_path / "perf.db")
    conn.row_factory = sqlite3.Row
    schema.apply_schema(conn)
    
    # Create many entities with members
    entities_to_create = []
    for i in range(100):
        members = [
            MemberRecord(
                name=f"method{j}",
                kind="method",
                signature=f"func method{j}()",
                code=f"func method{j}() {{}}",
                start_line=j * 5,
                end_line=j * 5 + 3,
            )
            for j in range(5)
        ]
        entity = _entity_with_members(
            f"Class{i}", f"class-{i}", f"Sources/Class{i}.swift", members
        )
        entities_to_create.append(entity)
    
    repo = MetadataRepository(conn)
    commit = repo.record_commit("test", None, "master", True)
    repo.persist_entities(commit, entities_to_create)
    conn.commit()
    
    # Load entities - this should use 2 queries, not 101
    entities, _ = _load_entities(conn, "master")
    
    assert len(entities) == 100
    
    # Verify member names are loaded correctly for all
    for i in range(100):
        entity = entities[f"class-{i}"]
        assert len(entity["member_names"]) == 5
        assert f"method0" in entity["member_names"]
    
    conn.close()

