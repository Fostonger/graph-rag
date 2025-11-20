import sqlite3
from pathlib import Path

from graphrag.db import schema
from graphrag.db.queries import find_entities
from graphrag.db.repository import MetadataRepository
from graphrag.db.queries import find_entities
from graphrag.models.records import EntityRecord, MemberRecord, RelationshipRecord


def make_entity(
    name: str = "Greeter",
    stable_id: str = "stable-greeter",
    path: str = "Sources/Greeter.swift",
    module: str = "Sources",
) -> EntityRecord:
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
        name=name,
        kind="struct",
        module=module,
        language="swift",
        file_path=file_path,
        start_line=1,
        end_line=10,
        signature="struct Greeter",
        code="struct Greeter {}",
        stable_id=stable_id,
        members=[member],
    )


def test_repository_persist_entities(tmp_path):
    db_path = tmp_path / "graphrag.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    schema.apply_schema(conn)
    repo = MetadataRepository(conn)

    commit_id = repo.record_commit("abc123", None, "master", True)
    id_map = repo.persist_entities(commit_id, [make_entity()])

    assert "stable-greeter" in id_map

    entity = conn.execute("SELECT * FROM entities").fetchone()
    assert entity["name"] == "Greeter"

    entity_version = conn.execute("SELECT * FROM entity_versions").fetchone()
    assert entity_version["entity_id"] == entity["id"]

    member = conn.execute("SELECT * FROM members").fetchone()
    assert member["name"] == "greet"

    member_version = conn.execute("SELECT * FROM member_versions").fetchone()
    assert member_version["member_id"] == member["id"]


def test_repository_persist_relationships(tmp_path):
    db_path = tmp_path / "graphrag.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    schema.apply_schema(conn)
    repo = MetadataRepository(conn)

    commit_id = repo.record_commit("abc123", None, "master", True)
    source_entity = make_entity()
    target_entity = make_entity(
        name="Helper",
        stable_id="stable-helper",
        path="Sources/Helper.swift",
    )
    entity_map = repo.persist_entities(commit_id, [source_entity, target_entity])
    relationships = [
        RelationshipRecord(
            source_stable_id=source_entity.stable_id,
            target_name="Helper",
            target_module=source_entity.module,
            edge_type="strongReference",
            metadata={"member": "helper"},
        )
    ]
    source_map = {source_entity.stable_id: entity_map[source_entity.stable_id]}
    repo.persist_relationships(commit_id, source_map, relationships)

    rows = conn.execute(
        "SELECT target_name, edge_type FROM entity_relationships WHERE is_deleted = 0"
    ).fetchall()
    assert len(rows) == 1
    assert rows[0]["target_name"] == "Helper"
    assert rows[0]["edge_type"] == "strongReference"

    # replace relationships to ensure old edges are tombstoned
    second_commit = repo.record_commit("def456", "abc123", "master", True)
    repo.persist_relationships(
        second_commit,
        source_map,
        [
            RelationshipRecord(
                source_stable_id=source_entity.stable_id,
                target_name="Helper",
                target_module=source_entity.module,
                edge_type="weakReference",
            )
        ],
    )
    active = conn.execute(
        """
        WITH ranked AS (
            SELECT
                er.*,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        er.source_entity_id,
                        COALESCE(er.target_entity_id, -1),
                        er.target_name,
                        COALESCE(er.target_module, ''),
                        er.edge_type
                    ORDER BY er.commit_id DESC, er.id DESC
                ) AS rn
            FROM entity_relationships er
        )
        SELECT edge_type FROM ranked
        WHERE rn = 1 AND is_deleted = 0
        """
    ).fetchall()
    assert len(active) == 1
    assert active[0]["edge_type"] == "weakReference"


def test_find_entities_includes_target_type(tmp_path):
    db_path = tmp_path / "find.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    schema.apply_schema(conn)
    repo = MetadataRepository(conn)
    commit_id = repo.record_commit("find", None, "master", True)
    entity = make_entity(name="Helper", stable_id="helper", module="Helpers")
    entity.target_type = "test"
    repo.persist_entities(commit_id, [entity])
    conn.commit()

    rows = find_entities(conn, "Helper")
    assert rows
    assert rows[0]["target_type"] == "test"


def test_find_entities_supports_wildcards(tmp_path):
    db_path = tmp_path / "graphrag.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    schema.apply_schema(conn)
    repo = MetadataRepository(conn)

    commit_id = repo.record_commit("abc123", None, "master", True)
    repo.persist_entities(
        commit_id,
        [
            make_entity(
                name="UserBuilderTests",
                stable_id="stable-builder",
                path="Tests/UserBuilderTests.swift",
                module="Tests/User",
            ),
            make_entity(
                name="SomeClassHelper",
                stable_id="stable-helper",
                path="Sources/SomeClassHelper.swift",
                module="Sources",
            ),
            make_entity(
                name="MyClass",
                stable_id="stable-exact",
                path="Sources/MyClass.swift",
                module="Sources",
            ),
            make_entity(
                name="MyClassCopy",
                stable_id="stable-copy",
                path="Sources/MyClassCopy.swift",
                module="Sources",
            ),
        ],
    )

    rows = find_entities(conn, "*BuilderTests, SomeClass*, MyClass", limit=10)
    names = {row["name"] for row in rows}
    assert names == {"UserBuilderTests", "SomeClassHelper", "MyClass"}

    lower_case_rows = find_entities(conn, "*buildertests", limit=10)
    assert {row["name"] for row in lower_case_rows} == {"UserBuilderTests"}

