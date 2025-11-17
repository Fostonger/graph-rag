import sqlite3
from pathlib import Path

from graphrag.db import schema
from graphrag.db.repository import MetadataRepository
from graphrag.models.records import EntityRecord, MemberRecord


def make_entity(path: str = "Sources/Greeter.swift") -> EntityRecord:
    file_path = Path(path)
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


def test_repository_persist_entities(tmp_path):
    db_path = tmp_path / "graphrag.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    schema.apply_schema(conn)
    repo = MetadataRepository(conn)

    commit_id = repo.record_commit("abc123", None, "master", True)
    repo.persist_entities(commit_id, [make_entity()])

    entity = conn.execute("SELECT * FROM entities").fetchone()
    assert entity["name"] == "Greeter"

    entity_version = conn.execute("SELECT * FROM entity_versions").fetchone()
    assert entity_version["entity_id"] == entity["id"]

    member = conn.execute("SELECT * FROM members").fetchone()
    assert member["name"] == "greet"

    member_version = conn.execute("SELECT * FROM member_versions").fetchone()
    assert member_version["member_id"] == member["id"]

