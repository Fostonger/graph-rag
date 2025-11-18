from pathlib import Path

from git import Repo

from graphrag.config import Settings
from graphrag.db.connection import connect
from graphrag.db import schema
from graphrag.indexer.feature_service import FeatureBranchIndexer
from graphrag.indexer.service import build_registry


def _init_repo(path: Path) -> Repo:
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


def _settings(repo_path: Path, feature_db_name: str = "feature.db") -> Settings:
    return Settings(
        repo_path=repo_path,
        db_path=repo_path / "master.db",
        feature_db_path=repo_path / feature_db_name,
        default_branch="master",
    )


def test_feature_indexer_indexes_branch_commits_and_worktree(tmp_path):
    repo = _init_repo(tmp_path)
    repo.git.checkout("-b", "feature/foo")

    greeter = tmp_path / "Sources" / "Greeter.swift"
    greeter.write_text("struct Greeter { func greet() {} }\n")
    repo.index.add(["Sources/Greeter.swift"])
    repo.index.commit("feature update")

    # unstaged modifications
    greeter.write_text("struct Greeter { func greet() {} func bye() {} }\n")
    new_file = tmp_path / "Sources" / "NewType.swift"
    new_file.write_text("struct NewType {}\n")

    settings = _settings(tmp_path)
    registry = build_registry(settings)
    indexer = FeatureBranchIndexer(settings, registry)
    result = indexer.update()

    assert not result.skipped
    assert result.branch == "feature/foo"
    assert result.commits and len(result.commits) == 1
    assert set(result.worktree_files) == {
        "Sources/Greeter.swift",
        "Sources/NewType.swift",
    }

    conn = connect(settings.feature_db_path)
    schema.apply_schema(conn)
    try:
        rows = conn.execute(
            "SELECT hash FROM commits WHERE LENGTH(hash) = 40"
        ).fetchall()
        assert len(rows) == 1
        worktree = conn.execute(
            "SELECT hash FROM commits WHERE hash LIKE 'worktree:%'"
        ).fetchone()
        assert worktree is not None
        tracked_branch = conn.execute(
            "SELECT value FROM schema_meta WHERE key = 'feature_branch'"
        ).fetchone()
        assert tracked_branch["value"] == "feature/foo"
    finally:
        conn.close()


def test_feature_indexer_resets_when_branch_changes(tmp_path):
    repo = _init_repo(tmp_path)
    repo.git.checkout("-b", "feature/foo")
    path = tmp_path / "Sources" / "Greeter.swift"
    path.write_text("struct Greeter { func greet() {} }\n")
    repo.index.add(["Sources/Greeter.swift"])
    repo.index.commit("feature foo commit")

    settings = _settings(tmp_path, "feature-reset.db")
    registry = build_registry(settings)
    indexer = FeatureBranchIndexer(settings, registry)
    result_foo = indexer.update()
    assert result_foo.branch == "feature/foo"

    repo.git.checkout("master")
    repo.git.checkout("-b", "feature/bar")
    path.write_text("struct Greeter { func bar() {} }\n")
    repo.index.add(["Sources/Greeter.swift"])
    repo.index.commit("feature bar commit")

    result_bar = indexer.update()
    assert result_bar.branch == "feature/bar"
    assert not result_bar.skipped

    conn = connect(settings.feature_db_path)
    schema.apply_schema(conn)
    try:
        branches = conn.execute("SELECT DISTINCT branch FROM commits").fetchall()
        assert {row["branch"] for row in branches} == {"feature/bar"}
        tracked_branch = conn.execute(
            "SELECT value FROM schema_meta WHERE key = 'feature_branch'"
        ).fetchone()
        assert tracked_branch["value"] == "feature/bar"
    finally:
        conn.close()


def test_feature_indexer_skips_default_branch(tmp_path):
    repo = _init_repo(tmp_path)
    settings = _settings(tmp_path, "feature-skip.db")
    registry = build_registry(settings)
    indexer = FeatureBranchIndexer(settings, registry)
    result = indexer.update()
    assert result.skipped
    assert result.skipped_reason == "on default branch"


def test_feature_indexer_persists_relationships(tmp_path):
    repo = _init_repo(tmp_path)
    repo.git.checkout("-b", "feature/graph")
    graph_file = tmp_path / "Sources" / "Graph.swift"
    graph_file.write_text(
        """
        class Presenter {
            weak var view: View?
        }

        class View {
            var presenter: Presenter
            init(presenter: Presenter) {
                self.presenter = presenter
            }
        }
        """
    )
    repo.index.add(["Sources/Graph.swift"])
    repo.index.commit("add graph types")

    settings = _settings(tmp_path, "feature-graph.db")
    registry = build_registry(settings)
    indexer = FeatureBranchIndexer(settings, registry)
    result = indexer.update()
    assert not result.skipped

    conn = connect(settings.feature_db_path)
    schema.apply_schema(conn)
    try:
        rows = conn.execute(
            """
            SELECT edge_type, target_name FROM entity_relationships WHERE is_deleted = 0
            """
        ).fetchall()
        edge_types = {(row["edge_type"], row["target_name"]) for row in rows}
        assert ("strongReference", "Presenter") in edge_types
        assert ("weakReference", "View") in edge_types
    finally:
        conn.close()

