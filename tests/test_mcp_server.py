import asyncio
import json
from pathlib import Path

import pytest

from graphrag.config import Settings
from graphrag.db import schema
from graphrag.db.connection import connect
from graphrag.db.repository import MetadataRepository
from graphrag.mcp import server as mcp_server
from graphrag.models.records import EntityRecord, MemberRecord, RelationshipRecord


def build_entity(file_path: Path) -> EntityRecord:
    member = MemberRecord(
        name="greet",
        kind="function",
        signature="func greet() -> String",
        code="func greet() -> String { return \"hi\" }",
        start_line=5,
        end_line=7,
    )
    return EntityRecord(
        name="Greeter",
        kind="struct",
        module="Sources",
        language="swift",
        file_path=file_path,
        start_line=1,
        end_line=10,
        signature="struct Greeter",
        code="struct Greeter {}",
        stable_id="stable-greeter",
        members=[member],
    )


def seed_db(db_path: Path) -> None:
    conn = connect(db_path)
    schema.apply_schema(conn)
    repo = MetadataRepository(conn)
    commit_id = repo.record_commit("abc123", None, "master", True)
    repo.persist_entities(commit_id, [build_entity(Path("Sources/Greeter.swift"))])
    conn.commit()
    conn.close()


def seed_graph_db(db_path: Path) -> None:
    conn = connect(db_path)
    schema.apply_schema(conn)
    repo = MetadataRepository(conn)
    commit_id = repo.record_commit("graph", None, "master", True)
    assembly = EntityRecord(
        name="MyModuleAssembly",
        kind="class",
        module="MyModule",
        language="swift",
        file_path=Path("Sources/Assembly.swift"),
        start_line=1,
        end_line=5,
        signature="class MyModuleAssembly",
        code="class MyModuleAssembly {}",
        stable_id="assembly",
        members=[],
    )
    presenter = EntityRecord(
        name="MyModulePresenter",
        kind="class",
        module="MyModule",
        language="swift",
        file_path=Path("Sources/Presenter.swift"),
        start_line=1,
        end_line=5,
        signature="class MyModulePresenter",
        code="class MyModulePresenter {}",
        stable_id="presenter",
        members=[],
    )
    view = EntityRecord(
        name="MyModuleViewController",
        kind="class",
        module="MyModule",
        language="swift",
        file_path=Path("Sources/ViewController.swift"),
        start_line=1,
        end_line=5,
        signature="class MyModuleViewController",
        code="class MyModuleViewController {}",
        stable_id="view",
        members=[],
    )
    id_map = repo.persist_entities(commit_id, [assembly, presenter, view])
    repo.persist_relationships(
        commit_id,
        id_map,
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
            RelationshipRecord(
                source_stable_id=presenter.stable_id,
                target_name=view.name,
                edge_type="weakReference",
                target_module="MyModule",
            ),
        ],
    )
    conn.commit()
    conn.close()


def test_mcp_tools_resolve_entities_and_members(tmp_path):
    db_path = tmp_path / "mcp.db"
    seed_db(db_path)
    settings = Settings(repo_path=tmp_path, db_path=db_path)
    original = mcp_server.runtime_settings
    mcp_server.runtime_settings = settings
    try:
        entities = asyncio.run(
            mcp_server.handle_call_tool(
            "find_entities", {"query": "Greeter", "include_code": False}
        )
        )
        payload = json.loads(entities[0].text)
        assert payload["count"] == 1
        assert payload["entities"][0]["name"] == "Greeter"

        members = asyncio.run(
            mcp_server.handle_call_tool(
                "get_members",
                {"entities": ["Greeter"], "members": ["greet"], "include_code": True},
            )
        )
        member_payload = json.loads(members[0].text)
        assert member_payload["count"] == 1
        assert member_payload["members"][0]["member_name"] == "greet"
    finally:
        mcp_server.runtime_settings = original


def test_mcp_get_graph_tool(tmp_path):
    db_path = tmp_path / "graph.db"
    seed_graph_db(db_path)
    settings = Settings(repo_path=tmp_path, db_path=db_path)
    original = mcp_server.runtime_settings
    mcp_server.runtime_settings = settings
    try:
        response = asyncio.run(
            mcp_server.handle_call_tool(
                "get_graph",
                {
                    "entity": "MyModuleViewController",
                    "stop_at": "MyModuleAssembly",
                    "direction": "both",
                },
            )
        )
        payload = json.loads(response[0].text)
        graph = payload["graph"]
        edge_types = {(edge["source"], edge["target"], edge["type"]) for edge in graph["edges"]}
        assert ("MyModuleViewController", "MyModuleAssembly", "createdBy") in edge_types
        assert ("MyModulePresenter", "MyModuleViewController", "weakReference") in edge_types
        node_names = {node["name"] for node in graph["nodes"]}
        assert "MyModuleAssembly" not in node_names
        assert "MyModuleViewController" in node_names
    finally:
        mcp_server.runtime_settings = original

