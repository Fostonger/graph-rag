import sqlite3
from pathlib import Path

from graphrag.db import schema
from graphrag.db.queries import get_entity_graph
from graphrag.db.repository import MetadataRepository
from graphrag.models.records import EntityRecord, RelationshipRecord


def _entity(name: str, stable_id: str, path: str) -> EntityRecord:
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

