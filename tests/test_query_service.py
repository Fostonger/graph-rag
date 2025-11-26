"""Tests for QueryService branch-aware database connection logic."""

from pathlib import Path

import pytest
from git import Repo

from graphrag.config import Settings
from graphrag.db import schema
from graphrag.db.connection import connect
from graphrag.db.query_service import QueryService
from graphrag.db.repository import MetadataRepository
from graphrag.models.records import EntityRecord, MemberRecord, RelationshipRecord


def _init_repo(path: Path) -> Repo:
    """Initialize a git repository with master branch and initial commit."""
    repo = Repo.init(path, initial_branch="master")
    repo.git.config("user.email", "test@example.com")
    repo.git.config("user.name", "GraphRag Tests")
    sources = path / "Sources"
    sources.mkdir()
    file_path = sources / "Greeter.swift"
    file_path.write_text("struct Greeter {}\n")
    repo.index.add([str(file_path.relative_to(path))])
    repo.index.commit("init master")
    return repo


def _settings(repo_path: Path, db_name: str = "master.db", feature_db_name: str = "feature.db") -> Settings:
    return Settings(
        repo_path=repo_path,
        db_path=repo_path / db_name,
        feature_db_path=repo_path / feature_db_name,
        default_branch="master",
    )


def _create_entity(
    name: str,
    stable_id: str,
    file_path: Path,
    target_type: str = "app",
) -> EntityRecord:
    """Create a test entity record."""
    return EntityRecord(
        name=name,
        kind="class",
        module="TestModule",
        language="swift",
        file_path=file_path,
        start_line=1,
        end_line=10,
        signature=f"class {name}",
        code=f"class {name} {{}}",
        stable_id=stable_id,
        target_type=target_type,
        members=[],
    )


def _seed_master_db(db_path: Path, entities: list[EntityRecord]) -> None:
    """Seed the master database with entities."""
    conn = connect(db_path)
    schema.apply_schema(conn)
    repo = MetadataRepository(conn)
    commit_id = repo.record_commit("master-abc123", None, "master", True)
    repo.persist_entities(commit_id, entities)
    repo.rebuild_latest_tables()
    conn.commit()
    conn.close()


def _seed_feature_db(
    db_path: Path,
    branch: str,
    entities: list[EntityRecord],
    relationships: list[RelationshipRecord] | None = None,
) -> None:
    """Seed the feature database with entities for a specific branch."""
    conn = connect(db_path)
    schema.apply_schema(conn)
    repo = MetadataRepository(conn)
    repo.set_schema_value("feature_branch", branch)
    commit_id = repo.record_commit("feature-xyz789", None, branch, False)
    entity_ids = repo.persist_entities(commit_id, entities)
    if relationships:
        repo.persist_relationships(commit_id, entity_ids, relationships)
    repo.rebuild_latest_tables()
    conn.commit()
    conn.close()


class TestQueryServiceBranchValidation:
    """Tests for branch validation logic in QueryService."""

    def test_should_use_feature_db_returns_false_on_default_branch(self, tmp_path):
        """When on the default branch (master), feature DB should not be used."""
        repo = _init_repo(tmp_path)
        # Stay on master branch
        assert repo.active_branch.name == "master"

        settings = _settings(tmp_path)
        # Create feature DB with some data
        feature_entity = _create_entity(
            "FeatureOnly", "feature-only", Path("Sources/FeatureOnly.swift")
        )
        _seed_feature_db(settings.feature_db_path, "feature/foo", [feature_entity])

        service = QueryService(settings)
        assert service._should_use_feature_db() is False

    def test_should_use_feature_db_returns_false_when_branch_mismatch(self, tmp_path):
        """When feature DB was indexed for a different branch, it should not be used."""
        repo = _init_repo(tmp_path)
        # Create and switch to feature/bar
        repo.git.checkout("-b", "feature/bar")
        assert repo.active_branch.name == "feature/bar"

        settings = _settings(tmp_path)
        # Create feature DB indexed for feature/foo (different branch)
        feature_entity = _create_entity(
            "FeatureOnly", "feature-only", Path("Sources/FeatureOnly.swift")
        )
        _seed_feature_db(settings.feature_db_path, "feature/foo", [feature_entity])

        service = QueryService(settings)
        assert service._should_use_feature_db() is False

    def test_should_use_feature_db_returns_true_when_branch_matches(self, tmp_path):
        """When feature DB branch matches current branch, it should be used."""
        repo = _init_repo(tmp_path)
        # Create and switch to feature/foo
        repo.git.checkout("-b", "feature/foo")
        assert repo.active_branch.name == "feature/foo"

        settings = _settings(tmp_path)
        # Create feature DB indexed for the same branch
        feature_entity = _create_entity(
            "FeatureOnly", "feature-only", Path("Sources/FeatureOnly.swift")
        )
        _seed_feature_db(settings.feature_db_path, "feature/foo", [feature_entity])

        service = QueryService(settings)
        assert service._should_use_feature_db() is True

    def test_should_use_feature_db_returns_false_when_feature_db_missing(self, tmp_path):
        """When feature DB file doesn't exist, it should not be used."""
        repo = _init_repo(tmp_path)
        repo.git.checkout("-b", "feature/foo")

        settings = _settings(tmp_path)
        # Don't create the feature DB file

        service = QueryService(settings)
        assert service._should_use_feature_db() is False

    def test_should_use_feature_db_returns_false_on_detached_head(self, tmp_path):
        """When on detached HEAD, feature DB should not be used."""
        repo = _init_repo(tmp_path)
        # Detach HEAD
        repo.git.checkout(repo.head.commit.hexsha)

        settings = _settings(tmp_path)
        feature_entity = _create_entity(
            "FeatureOnly", "feature-only", Path("Sources/FeatureOnly.swift")
        )
        _seed_feature_db(settings.feature_db_path, "feature/foo", [feature_entity])

        service = QueryService(settings)
        assert service._should_use_feature_db() is False


class TestQueryServiceGraphQueries:
    """Tests for graph queries with branch-aware connections."""

    def test_get_graph_uses_master_only_on_default_branch(self, tmp_path):
        """On default branch, get_graph should only use master DB data."""
        repo = _init_repo(tmp_path)
        # Stay on master
        assert repo.active_branch.name == "master"

        settings = _settings(tmp_path)

        # Seed master DB with an entity
        master_entity = _create_entity(
            "MasterEntity", "master-entity", Path("Sources/Master.swift")
        )
        _seed_master_db(settings.db_path, [master_entity])

        # Seed feature DB (for a different branch) with an entity that would
        # mark MasterEntity as deleted or modified
        feature_entity = _create_entity(
            "FeatureEntity", "feature-entity", Path("Sources/Feature.swift")
        )
        _seed_feature_db(settings.feature_db_path, "feature/foo", [feature_entity])

        service = QueryService(settings)
        # Should find MasterEntity (feature DB not used)
        graph = service.get_graph("MasterEntity")
        assert graph["entity"]["name"] == "MasterEntity"

    def test_get_graph_ignores_stale_feature_db(self, tmp_path):
        """When on a different branch than feature DB, feature DB is ignored."""
        repo = _init_repo(tmp_path)
        repo.git.checkout("-b", "feature/bar")

        settings = _settings(tmp_path)

        # Seed master DB
        master_entity = _create_entity(
            "SharedEntity", "shared-entity", Path("Sources/Shared.swift")
        )
        _seed_master_db(settings.db_path, [master_entity])

        # Seed feature DB for feature/foo (not the current branch)
        feature_entity = _create_entity(
            "FeatureOnlyEntity", "feature-only", Path("Sources/FeatureOnly.swift")
        )
        _seed_feature_db(settings.feature_db_path, "feature/foo", [feature_entity])

        service = QueryService(settings)
        # SharedEntity should be found from master (feature DB ignored)
        graph = service.get_graph("SharedEntity")
        assert graph["entity"]["name"] == "SharedEntity"

        # FeatureOnlyEntity should NOT be found (it's only in stale feature DB)
        with pytest.raises(ValueError, match="was not found"):
            service.get_graph("FeatureOnlyEntity")

    def test_get_graph_uses_feature_db_when_branch_matches(self, tmp_path):
        """When current branch matches feature DB, feature overlay is applied."""
        repo = _init_repo(tmp_path)
        repo.git.checkout("-b", "feature/foo")

        settings = _settings(tmp_path)

        # Seed master DB
        master_entity = _create_entity(
            "MasterEntity", "master-entity", Path("Sources/Master.swift")
        )
        _seed_master_db(settings.db_path, [master_entity])

        # Seed feature DB for feature/foo (current branch)
        feature_entity = _create_entity(
            "FeatureEntity", "feature-entity", Path("Sources/Feature.swift")
        )
        _seed_feature_db(settings.feature_db_path, "feature/foo", [feature_entity])

        service = QueryService(settings)
        # Both entities should be findable
        master_graph = service.get_graph("MasterEntity")
        assert master_graph["entity"]["name"] == "MasterEntity"

        feature_graph = service.get_graph("FeatureEntity")
        assert feature_graph["entity"]["name"] == "FeatureEntity"


class TestQueryServiceBranchSwitching:
    """Tests for the branch switching scenario that caused the original bug."""

    def test_get_graph_works_after_switching_from_feature_to_main(self, tmp_path):
        """
        Reproduce the original bug scenario:
        1. Index on feature branch
        2. Switch to main
        3. get_graph should still work using master DB only
        """
        repo = _init_repo(tmp_path)
        settings = _settings(tmp_path)

        # Seed master DB with an entity
        master_entity = _create_entity(
            "AppViewModel", "app-viewmodel", Path("Sources/AppViewModel.swift")
        )
        _seed_master_db(settings.db_path, [master_entity])

        # Switch to feature branch and create feature DB
        repo.git.checkout("-b", "feature/new-feature")
        feature_entity = _create_entity(
            "FeatureViewModel", "feature-viewmodel", Path("Sources/FeatureViewModel.swift")
        )
        _seed_feature_db(settings.feature_db_path, "feature/new-feature", [feature_entity])

        # Verify feature entity is accessible on feature branch
        service = QueryService(settings)
        feature_graph = service.get_graph("FeatureViewModel")
        assert feature_graph["entity"]["name"] == "FeatureViewModel"

        # Switch back to master
        repo.git.checkout("master")
        assert repo.active_branch.name == "master"

        # Create new service instance (simulates MCP server handling new request)
        service = QueryService(settings)

        # Should be able to query master entity
        # (This was failing before the fix - feature DB was corrupting queries)
        master_graph = service.get_graph("AppViewModel")
        assert master_graph["entity"]["name"] == "AppViewModel"

        # Feature entity should NOT be accessible from master
        # (feature DB is ignored when on default branch)
        with pytest.raises(ValueError, match="was not found"):
            service.get_graph("FeatureViewModel")

    def test_get_graph_works_after_switching_between_feature_branches(self, tmp_path):
        """
        Test switching between different feature branches:
        1. Index on feature/foo
        2. Switch to feature/bar
        3. get_graph should not use stale feature/foo data
        """
        repo = _init_repo(tmp_path)
        settings = _settings(tmp_path)

        # Seed master DB
        master_entity = _create_entity(
            "SharedEntity", "shared-entity", Path("Sources/Shared.swift")
        )
        _seed_master_db(settings.db_path, [master_entity])

        # Create feature/foo and seed feature DB
        repo.git.checkout("-b", "feature/foo")
        foo_entity = _create_entity(
            "FooEntity", "foo-entity", Path("Sources/Foo.swift")
        )
        _seed_feature_db(settings.feature_db_path, "feature/foo", [foo_entity])

        # Verify FooEntity is accessible on feature/foo
        service = QueryService(settings)
        foo_graph = service.get_graph("FooEntity")
        assert foo_graph["entity"]["name"] == "FooEntity"

        # Switch to feature/bar (feature DB still has feature/foo data)
        repo.git.checkout("master")
        repo.git.checkout("-b", "feature/bar")
        assert repo.active_branch.name == "feature/bar"

        # Create new service instance
        service = QueryService(settings)

        # SharedEntity should be accessible (from master)
        shared_graph = service.get_graph("SharedEntity")
        assert shared_graph["entity"]["name"] == "SharedEntity"

        # FooEntity should NOT be accessible (stale feature DB ignored)
        with pytest.raises(ValueError, match="was not found"):
            service.get_graph("FooEntity")

