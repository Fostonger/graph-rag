import asyncio
import json
from pathlib import Path

import pytest
from git import Repo

from graphrag.config import Settings
from graphrag.db import schema
from graphrag.db.connection import connect
from graphrag.db.repository import MetadataRepository
from graphrag.mcp import server as mcp_server
from graphrag.models.records import EntityRecord, MemberRecord, RelationshipRecord


def build_entity(
    file_path: Path,
    name: str = "Greeter",
    stable_id: str = "stable-greeter",
    target_type: str = "app",
) -> EntityRecord:
    member = MemberRecord(
        name="greet",
        kind="function",
        signature="func greet() -> String",
        code="func greet() -> String { return \"hi\" }",
        start_line=5,
        end_line=7,
    )
    return EntityRecord(
        name=name,
        kind="struct",
        module="Sources",
        language="swift",
        file_path=file_path,
        start_line=1,
        end_line=10,
        signature=f"struct {name}",
        code=f"struct {name} {{}}",
        stable_id=stable_id,
        target_type=target_type,
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
        target_type="app",
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
        target_type="app",
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
        target_type="app",
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
    conn = connect(db_path)
    schema.apply_schema(conn)
    try:
        repo = MetadataRepository(conn)
        commit_id = repo.record_commit("pattern", None, "master", True)
        repo.persist_entities(
            commit_id,
            [
                build_entity(
                    Path("Tests/UserBuilderTests.swift"),
                    name="UserBuilderTests",
                    stable_id="stable-builder-tests",
                    target_type="test",
                )
            ],
        )
        conn.commit()
    finally:
        conn.close()
    settings = Settings(repo_path=tmp_path, db_path=db_path)
    original = mcp_server.runtime_settings
    mcp_server.runtime_settings = settings
    try:
        entities = asyncio.run(
            mcp_server.handle_call_tool(
                "find_entities", {"query": "*buildertests, Greeter", "include_code": False}
            )
        )
        payload = json.loads(entities[0].text)
        assert payload["count"] == 2
        returned = {row["name"] for row in payload["entities"]}
        assert returned == {"Greeter", "UserBuilderTests"}
        greeter_entry = next(row for row in payload["entities"] if row["name"] == "Greeter")
        assert greeter_entry["target_type"] == "app"
        builder_entry = next(row for row in payload["entities"] if row["name"] == "UserBuilderTests")
        assert builder_entry["target_type"] == "test"

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


# --- Branch switching tests ---


def _init_git_repo(path: Path) -> Repo:
    """Initialize a git repository with master branch."""
    repo = Repo.init(path, initial_branch="master")
    repo.git.config("user.email", "test@example.com")
    repo.git.config("user.name", "GraphRag Tests")
    sources = path / "Sources"
    sources.mkdir(exist_ok=True)
    file_path = sources / "Dummy.swift"
    file_path.write_text("struct Dummy {}\n")
    repo.index.add([str(file_path.relative_to(path))])
    repo.index.commit("init master")
    return repo


def _seed_master_db_with_rebuild(db_path: Path, entities: list[EntityRecord]) -> None:
    """Seed master DB and rebuild materialized views."""
    conn = connect(db_path)
    schema.apply_schema(conn)
    repo = MetadataRepository(conn)
    commit_id = repo.record_commit("master-hash", None, "master", True)
    repo.persist_entities(commit_id, entities)
    repo.rebuild_latest_tables()
    conn.commit()
    conn.close()


def _seed_feature_db_with_branch(
    db_path: Path, branch: str, entities: list[EntityRecord]
) -> None:
    """Seed feature DB for a specific branch and rebuild views."""
    conn = connect(db_path)
    schema.apply_schema(conn)
    repo = MetadataRepository(conn)
    repo.set_schema_value("feature_branch", branch)
    commit_id = repo.record_commit("feature-hash", None, branch, False)
    repo.persist_entities(commit_id, entities)
    repo.rebuild_latest_tables()
    conn.commit()
    conn.close()


def test_mcp_get_graph_ignores_feature_db_on_default_branch(tmp_path):
    """
    MCP get_graph should use only master DB when on default branch,
    even if a feature DB exists with different data.
    """
    repo = _init_git_repo(tmp_path)
    assert repo.active_branch.name == "master"

    db_path = tmp_path / "master.db"
    feature_db_path = tmp_path / "feature.db"

    # Seed master DB with MasterEntity
    master_entity = EntityRecord(
        name="MasterEntity",
        kind="class",
        module="App",
        language="swift",
        file_path=Path("Sources/Master.swift"),
        start_line=1,
        end_line=5,
        signature="class MasterEntity",
        code="class MasterEntity {}",
        stable_id="master-entity",
        target_type="app",
        members=[],
    )
    _seed_master_db_with_rebuild(db_path, [master_entity])

    # Seed feature DB (for a different branch) with FeatureEntity
    feature_entity = EntityRecord(
        name="FeatureEntity",
        kind="class",
        module="Feature",
        language="swift",
        file_path=Path("Sources/Feature.swift"),
        start_line=1,
        end_line=5,
        signature="class FeatureEntity",
        code="class FeatureEntity {}",
        stable_id="feature-entity",
        target_type="app",
        members=[],
    )
    _seed_feature_db_with_branch(feature_db_path, "feature/foo", [feature_entity])

    settings = Settings(
        repo_path=tmp_path,
        db_path=db_path,
        feature_db_path=feature_db_path,
        default_branch="master",
    )
    original = mcp_server.runtime_settings
    mcp_server.runtime_settings = settings
    try:
        # Should find MasterEntity from master DB
        response = asyncio.run(
            mcp_server.handle_call_tool("get_graph", {"entity": "MasterEntity"})
        )
        payload = json.loads(response[0].text)
        assert payload["graph"]["entity"]["name"] == "MasterEntity"

        # Should NOT find FeatureEntity (feature DB ignored on default branch)
        with pytest.raises(ValueError, match="was not found"):
            asyncio.run(
                mcp_server.handle_call_tool("get_graph", {"entity": "FeatureEntity"})
            )
    finally:
        mcp_server.runtime_settings = original


def test_mcp_get_graph_ignores_stale_feature_db_after_branch_switch(tmp_path):
    """
    Reproduce the original bug: after switching from feature branch to main,
    get_graph should work correctly using only master DB.
    """
    repo = _init_git_repo(tmp_path)

    db_path = tmp_path / "master.db"
    feature_db_path = tmp_path / "feature.db"

    # Seed master DB
    master_entity = EntityRecord(
        name="AppController",
        kind="class",
        module="App",
        language="swift",
        file_path=Path("Sources/AppController.swift"),
        start_line=1,
        end_line=5,
        signature="class AppController",
        code="class AppController {}",
        stable_id="app-controller",
        target_type="app",
        members=[],
    )
    _seed_master_db_with_rebuild(db_path, [master_entity])

    # Simulate: user was on feature branch and indexed feature DB
    repo.git.checkout("-b", "feature/new-feature")
    feature_entity = EntityRecord(
        name="NewFeatureController",
        kind="class",
        module="Feature",
        language="swift",
        file_path=Path("Sources/NewFeature.swift"),
        start_line=1,
        end_line=5,
        signature="class NewFeatureController",
        code="class NewFeatureController {}",
        stable_id="new-feature",
        target_type="app",
        members=[],
    )
    _seed_feature_db_with_branch(feature_db_path, "feature/new-feature", [feature_entity])

    # Switch back to master (simulating user switching branches)
    repo.git.checkout("master")
    assert repo.active_branch.name == "master"

    settings = Settings(
        repo_path=tmp_path,
        db_path=db_path,
        feature_db_path=feature_db_path,
        default_branch="master",
    )
    original = mcp_server.runtime_settings
    mcp_server.runtime_settings = settings
    try:
        # This was failing before the fix - feature DB was corrupting queries
        response = asyncio.run(
            mcp_server.handle_call_tool("get_graph", {"entity": "AppController"})
        )
        payload = json.loads(response[0].text)
        assert payload["graph"]["entity"]["name"] == "AppController"

        # Feature entity should NOT be accessible from master
        with pytest.raises(ValueError, match="was not found"):
            asyncio.run(
                mcp_server.handle_call_tool(
                    "get_graph", {"entity": "NewFeatureController"}
                )
            )
    finally:
        mcp_server.runtime_settings = original


def test_mcp_get_graph_uses_feature_db_when_branch_matches(tmp_path):
    """
    When on the correct feature branch, get_graph should use feature DB overlay.
    """
    repo = _init_git_repo(tmp_path)
    repo.git.checkout("-b", "feature/active")
    assert repo.active_branch.name == "feature/active"

    db_path = tmp_path / "master.db"
    feature_db_path = tmp_path / "feature.db"

    # Seed master DB
    master_entity = EntityRecord(
        name="BaseEntity",
        kind="class",
        module="App",
        language="swift",
        file_path=Path("Sources/Base.swift"),
        start_line=1,
        end_line=5,
        signature="class BaseEntity",
        code="class BaseEntity {}",
        stable_id="base-entity",
        target_type="app",
        members=[],
    )
    _seed_master_db_with_rebuild(db_path, [master_entity])

    # Seed feature DB for the current branch
    feature_entity = EntityRecord(
        name="ActiveFeature",
        kind="class",
        module="Feature",
        language="swift",
        file_path=Path("Sources/ActiveFeature.swift"),
        start_line=1,
        end_line=5,
        signature="class ActiveFeature",
        code="class ActiveFeature {}",
        stable_id="active-feature",
        target_type="app",
        members=[],
    )
    _seed_feature_db_with_branch(feature_db_path, "feature/active", [feature_entity])

    settings = Settings(
        repo_path=tmp_path,
        db_path=db_path,
        feature_db_path=feature_db_path,
        default_branch="master",
    )
    original = mcp_server.runtime_settings
    mcp_server.runtime_settings = settings
    try:
        # Should find both master and feature entities
        base_response = asyncio.run(
            mcp_server.handle_call_tool("get_graph", {"entity": "BaseEntity"})
        )
        base_payload = json.loads(base_response[0].text)
        assert base_payload["graph"]["entity"]["name"] == "BaseEntity"

        feature_response = asyncio.run(
            mcp_server.handle_call_tool("get_graph", {"entity": "ActiveFeature"})
        )
        feature_payload = json.loads(feature_response[0].text)
        assert feature_payload["graph"]["entity"]["name"] == "ActiveFeature"
    finally:
        mcp_server.runtime_settings = original

