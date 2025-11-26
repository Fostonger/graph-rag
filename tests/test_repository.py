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


def test_persist_relationships_resolves_cross_module_targets(tmp_path):
    """Test that relationships correctly resolve targets in different modules.
    
    When entity A in ModuleX references entity B in ModuleY, the relationship
    has target_module=ModuleX (source's module, not target's). The lookup
    should fall back to searching by name only when module-specific lookup fails.
    """
    db_path = tmp_path / "cross-module.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    schema.apply_schema(conn)
    repo = MetadataRepository(conn)

    commit_id = repo.record_commit("abc123", None, "master", True)
    
    # Create source entity in FeatureModule
    source_entity = make_entity(
        name="FeaturePresenter",
        stable_id="stable-presenter",
        path="FeatureModule/Presenter.swift",
        module="FeatureModule",
    )
    
    # Create target entity in a DIFFERENT module (SimpleCoreIO)
    target_entity = make_entity(
        name="ISimpleService",
        stable_id="stable-service",
        path="SimpleCoreIO/ISimpleService.swift",
        module="SimpleCoreIO",
    )
    
    # Persist both entities
    entity_map = repo.persist_entities(commit_id, [source_entity, target_entity])
    
    # Create relationship with target_module = source's module (not target's)
    # This simulates how SwiftParser creates relationships
    relationships = [
        RelationshipRecord(
            source_stable_id=source_entity.stable_id,
            target_name="ISimpleService",
            target_module="FeatureModule",  # Wrong module! But this is how parser works
            edge_type="strongReference",
            metadata={"member": "service"},
        )
    ]
    
    source_map = {source_entity.stable_id: entity_map[source_entity.stable_id]}
    repo.persist_relationships(commit_id, source_map, relationships)
    
    # Verify the relationship was created with target_entity_id resolved
    row = conn.execute(
        """
        SELECT target_entity_id, target_name FROM entity_relationships
        WHERE source_entity_id = ? AND is_deleted = 0
        """,
        (entity_map[source_entity.stable_id],),
    ).fetchone()
    
    assert row is not None
    assert row["target_name"] == "ISimpleService"
    # The target_entity_id should be resolved despite module mismatch
    assert row["target_entity_id"] == entity_map[target_entity.stable_id], (
        "Cross-module relationship should resolve target_entity_id via fallback lookup"
    )


def test_persist_relationships_same_module_lookup_takes_precedence(tmp_path):
    """Test that when target exists in same module, it's preferred over cross-module match."""
    db_path = tmp_path / "same-module.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    schema.apply_schema(conn)
    repo = MetadataRepository(conn)

    commit_id = repo.record_commit("abc123", None, "master", True)
    
    # Create source entity in FeatureModule
    source_entity = make_entity(
        name="FeaturePresenter",
        stable_id="stable-presenter",
        path="FeatureModule/Presenter.swift",
        module="FeatureModule",
    )
    
    # Create target entity in SAME module
    target_same_module = make_entity(
        name="Helper",
        stable_id="stable-helper-feature",
        path="FeatureModule/Helper.swift",
        module="FeatureModule",
    )
    
    # Create another entity with same name in DIFFERENT module
    target_other_module = make_entity(
        name="Helper",
        stable_id="stable-helper-other",
        path="OtherModule/Helper.swift",
        module="OtherModule",
    )
    
    # Persist all entities
    entity_map = repo.persist_entities(
        commit_id, [source_entity, target_same_module, target_other_module]
    )
    
    # Create relationship - target_module matches one of the targets
    relationships = [
        RelationshipRecord(
            source_stable_id=source_entity.stable_id,
            target_name="Helper",
            target_module="FeatureModule",  # Matches target_same_module
            edge_type="strongReference",
        )
    ]
    
    source_map = {source_entity.stable_id: entity_map[source_entity.stable_id]}
    repo.persist_relationships(commit_id, source_map, relationships)
    
    # Verify the relationship resolved to the same-module target
    row = conn.execute(
        """
        SELECT target_entity_id FROM entity_relationships
        WHERE source_entity_id = ? AND is_deleted = 0
        """,
        (entity_map[source_entity.stable_id],),
    ).fetchone()
    
    assert row is not None
    # Should prefer the same-module match
    assert row["target_entity_id"] == entity_map[target_same_module.stable_id]


def test_resolve_pending_relationships_fixes_ordering_issue(tmp_path):
    """Test that relationships created before target entities are resolved later.
    
    This simulates the file ordering issue where FeatureAssembly.swift is indexed
    before FeaturePresenter.swift, causing the relationship to have NULL target_entity_id
    initially. After all files are indexed, resolve_pending_relationships should fix it.
    """
    db_path = tmp_path / "ordering.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    schema.apply_schema(conn)
    repo = MetadataRepository(conn)

    commit_id = repo.record_commit("abc123", None, "master", True)
    
    # Step 1: Index Assembly first (like alphabetical file ordering)
    assembly = make_entity(
        name="FeatureAssembly",
        stable_id="stable-assembly",
        path="FeatureModule/Assembly/FeatureAssembly.swift",
        module="FeatureModule",
    )
    assembly_map = repo.persist_entities(commit_id, [assembly])
    
    # Create relationships from Assembly to Presenter and ViewModelBuilder
    # These target entities DON'T EXIST YET (simulating file ordering)
    relationships = [
        RelationshipRecord(
            source_stable_id=assembly.stable_id,
            target_name="FeaturePresenter",
            target_module="FeatureModule",
            edge_type="creates",
        ),
        RelationshipRecord(
            source_stable_id=assembly.stable_id,
            target_name="FeatureViewModelBuilder",
            target_module="FeatureModule",
            edge_type="creates",
        ),
    ]
    repo.persist_relationships(commit_id, assembly_map, relationships)
    
    # At this point, relationships should have NULL target_entity_id
    null_targets = conn.execute(
        """
        SELECT COUNT(*) as cnt FROM entity_relationships
        WHERE source_entity_id = ? AND target_entity_id IS NULL AND is_deleted = 0
        """,
        (assembly_map[assembly.stable_id],),
    ).fetchone()
    assert null_targets["cnt"] == 2, "Both relationships should have NULL target initially"
    
    # Step 2: Index target entities (simulating later files in alphabetical order)
    presenter = make_entity(
        name="FeaturePresenter",
        stable_id="stable-presenter",
        path="FeatureModule/Presenter/FeaturePresenter.swift",
        module="FeatureModule",
    )
    viewmodel_builder = make_entity(
        name="FeatureViewModelBuilder",
        stable_id="stable-vmbuilder",
        path="FeatureModule/ViewModel/FeatureViewModelBuilder.swift",
        module="FeatureModule",
    )
    target_map = repo.persist_entities(commit_id, [presenter, viewmodel_builder])
    
    # Step 3: Call resolve_pending_relationships (done by rebuild_latest_tables)
    updated = repo.resolve_pending_relationships()
    
    # Both relationships should now be resolved
    assert updated == 2, f"Expected 2 relationships to be updated, got {updated}"
    
    # Verify target_entity_id is now filled in
    resolved = conn.execute(
        """
        SELECT target_name, target_entity_id FROM entity_relationships
        WHERE source_entity_id = ? AND is_deleted = 0
        ORDER BY target_name
        """,
        (assembly_map[assembly.stable_id],),
    ).fetchall()
    
    assert len(resolved) == 2
    
    # FeaturePresenter should be resolved
    presenter_rel = next(r for r in resolved if r["target_name"] == "FeaturePresenter")
    assert presenter_rel["target_entity_id"] == target_map[presenter.stable_id], (
        "FeaturePresenter relationship should be resolved"
    )
    
    # FeatureViewModelBuilder should be resolved
    vmbuilder_rel = next(r for r in resolved if r["target_name"] == "FeatureViewModelBuilder")
    assert vmbuilder_rel["target_entity_id"] == target_map[viewmodel_builder.stable_id], (
        "FeatureViewModelBuilder relationship should be resolved"
    )


def test_resolve_pending_relationships_handles_cross_module(tmp_path):
    """Test that cross-module relationships are resolved even with module mismatch.
    
    When FeaturePresenter has target_module=FeatureModule but references ISimpleService
    which is actually in SimpleCoreIO, the fallback lookup should still resolve it.
    """
    db_path = tmp_path / "cross-module-ordering.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    schema.apply_schema(conn)
    repo = MetadataRepository(conn)

    commit_id = repo.record_commit("abc123", None, "master", True)
    
    # Step 1: Index source entity first
    presenter = make_entity(
        name="FeaturePresenter",
        stable_id="stable-presenter",
        path="FeatureModule/Presenter.swift",
        module="FeatureModule",
    )
    presenter_map = repo.persist_entities(commit_id, [presenter])
    
    # Create relationship with wrong target_module (source's module, not target's)
    relationships = [
        RelationshipRecord(
            source_stable_id=presenter.stable_id,
            target_name="ISimpleService",
            target_module="FeatureModule",  # Wrong! Target is in SimpleCoreIO
            edge_type="strongReference",
        ),
    ]
    repo.persist_relationships(commit_id, presenter_map, relationships)
    
    # Relationship should have NULL target (entity doesn't exist yet)
    null_check = conn.execute(
        """
        SELECT target_entity_id FROM entity_relationships
        WHERE source_entity_id = ? AND is_deleted = 0
        """,
        (presenter_map[presenter.stable_id],),
    ).fetchone()
    assert null_check["target_entity_id"] is None
    
    # Step 2: Index target entity in DIFFERENT module
    service = make_entity(
        name="ISimpleService",
        stable_id="stable-service",
        path="SimpleCoreIO/ISimpleService.swift",
        module="SimpleCoreIO",  # Different from target_module in relationship!
    )
    service_map = repo.persist_entities(commit_id, [service])
    
    # Step 3: Resolve pending relationships
    updated = repo.resolve_pending_relationships()
    
    # Should still resolve via fallback (lookup without module constraint)
    assert updated == 1, f"Expected 1 relationship to be updated, got {updated}"
    
    resolved = conn.execute(
        """
        SELECT target_entity_id FROM entity_relationships
        WHERE source_entity_id = ? AND is_deleted = 0
        """,
        (presenter_map[presenter.stable_id],),
    ).fetchone()
    
    assert resolved["target_entity_id"] == service_map[service.stable_id], (
        "Cross-module relationship should be resolved via fallback lookup"
    )

