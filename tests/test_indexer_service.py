from pathlib import Path

from git import Repo

from graphrag.config import Settings
from graphrag.db import schema
from graphrag.db.connection import connect
from graphrag.indexer.service import IndexerService, build_registry


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


def test_indexer_service_handles_missing_anchor_commit(tmp_path):
    repo = _init_repo(tmp_path)
    settings = Settings(
        repo_path=tmp_path,
        db_path=tmp_path / "master.db",
        feature_db_path=tmp_path / "feature.db",
        default_branch="master",
    )
    registry = build_registry(settings)
    service = IndexerService(settings, registry=registry)
    head_hash = service.initialize()

    greeter = tmp_path / "Sources" / "Greeter.swift"
    greeter.write_text("struct Greeter { func hi() {} }\n")
    repo.index.add(["Sources/Greeter.swift"])
    new_commit = repo.index.commit("add greet function")

    # Corrupt the stored master hash so it references a non-existent commit
    conn = connect(settings.db_path)
    schema.apply_schema(conn)
    try:
        conn.execute(
            "UPDATE commits SET hash = ? WHERE hash = ?",
            ("deadbeefdeadbeefdeadbeefdeadbeefdeadbeef", head_hash),
        )
        conn.commit()
    finally:
        conn.close()

    processed = service.update()
    assert processed
    assert processed[-1] == new_commit.hexsha

    conn = connect(settings.db_path)
    schema.apply_schema(conn)
    try:
        row = conn.execute(
            "SELECT hash FROM commits WHERE is_master = 1 ORDER BY id DESC LIMIT 1"
        ).fetchone()
        assert row["hash"] == new_commit.hexsha
    finally:
        conn.close()

