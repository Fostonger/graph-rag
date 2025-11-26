from __future__ import annotations

import json
from dataclasses import dataclass
import sqlite3
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

from ..models.records import EntityRecord, ExtensionRecord, MemberRecord, RelationshipRecord


@dataclass
class MetadataRepository:
    conn: sqlite3.Connection

    # --- commit helpers ---
    def record_commit(
        self,
        commit_hash: str,
        parent_hash: Optional[str],
        branch: str,
        is_master: bool,
    ) -> int:
        self.conn.execute(
            """
            INSERT OR IGNORE INTO commits (hash, parent_hash, branch, is_master)
            VALUES (?, ?, ?, ?)
            """,
            (commit_hash, parent_hash, branch, int(is_master)),
        )
        row = self.conn.execute(
            "SELECT id FROM commits WHERE hash = ?",
            (commit_hash,),
        ).fetchone()
        assert row, "commit insert failed"
        return int(row["id"])

    def latest_master_commit(self) -> Optional[str]:
        row = self.conn.execute(
            """
            SELECT hash FROM commits
            WHERE is_master = 1
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
        return row["hash"] if row else None

    def latest_commit_for_branch(self, branch: str) -> Optional[str]:
        row = self.conn.execute(
            """
            SELECT hash FROM commits
            WHERE branch = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (branch,),
        ).fetchone()
        return row["hash"] if row else None

    def latest_real_commit_for_branch(self, branch: str) -> Optional[str]:
        row = self.conn.execute(
            """
            SELECT hash FROM commits
            WHERE branch = ? AND LENGTH(hash) = 40
            ORDER BY id DESC
            LIMIT 1
            """,
            (branch,),
        ).fetchone()
        return row["hash"] if row else None

    def delete_commit(self, commit_hash: str) -> None:
        self.conn.execute(
            "DELETE FROM commits WHERE hash = ?",
            (commit_hash,),
        )

    # --- file helpers ---
    def ensure_file(self, path: Path, language: str) -> int:
        path_str = str(path)
        self.conn.execute(
            """
            INSERT OR IGNORE INTO files (path, language)
            VALUES (?, ?)
            """,
            (path_str, language),
        )
        row = self.conn.execute(
            "SELECT id FROM files WHERE path = ?",
            (path_str,),
        ).fetchone()
        assert row, "file insert failed"
        return int(row["id"])

    # --- entities ---
    def upsert_entity(self, record: EntityRecord, file_id: int) -> int:
        row = self.conn.execute(
            "SELECT id FROM entities WHERE stable_id = ?",
            (record.stable_id,),
        ).fetchone()
        if row:
            entity_id = int(row["id"])
            self.conn.execute(
                """
                UPDATE entities
                SET name = ?, kind = ?, module = ?, language = ?, primary_file_id = ?
                WHERE id = ?
                """,
                (
                    record.name,
                    record.kind,
                    record.module,
                    record.language,
                    file_id,
                    entity_id,
                ),
            )
        else:
            cur = self.conn.execute(
                """
                INSERT INTO entities (stable_id, name, kind, module, language, primary_file_id)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    record.stable_id,
                    record.name,
                    record.kind,
                    record.module,
                    record.language,
                    file_id,
                ),
            )
            entity_id = int(cur.lastrowid)

        self.conn.execute(
            """
            INSERT INTO entity_files (entity_id, file_id, is_primary)
            VALUES (?, ?, ?)
            ON CONFLICT(entity_id, file_id) DO UPDATE
                SET is_primary = excluded.is_primary
            """,
            (entity_id, file_id, 1),
        )
        return entity_id

    def record_entity_version(
        self,
        entity_id: int,
        commit_id: int,
        file_id: int,
        record: EntityRecord,
        is_deleted: bool = False,
    ) -> None:
        props_dict = {
            "extended_type": record.extended_type,
            "member_count": len(record.members),
        }
        if record.target_type:
            props_dict["target_type"] = record.target_type
        if record.visibility:
            props_dict["visibility"] = record.visibility
        props = json.dumps(props_dict)
        self.conn.execute(
            """
            INSERT INTO entity_versions (
                entity_id, commit_id, file_id, start_line, end_line,
                signature, docstring, code, properties, is_deleted
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                entity_id,
                commit_id,
                file_id,
                record.start_line,
                record.end_line,
                record.signature,
                record.docstring,
                record.code,
                props,
                int(is_deleted),
            ),
        )

    def mark_entities_deleted_for_file(self, file_path: Path, commit_id: int) -> None:
        row = self.conn.execute(
            "SELECT id FROM files WHERE path = ?",
            (str(file_path),),
        ).fetchone()
        if not row:
            return
        file_id = int(row["id"])
        entity_rows = self.conn.execute(
            """
            SELECT entity_id FROM entity_files
            WHERE file_id = ?
            """,
            (file_id,),
        ).fetchall()
        for entity_row in entity_rows:
            entity_id = int(entity_row["entity_id"])
            self.conn.execute(
                """
                INSERT INTO entity_versions (
                    entity_id, commit_id, file_id, is_deleted
                ) VALUES (?, ?, ?, 1)
                """,
                (entity_id, commit_id, file_id),
            )
            member_rows = self.conn.execute(
                """
                SELECT id FROM members WHERE entity_id = ?
                """,
                (entity_id,),
            ).fetchall()
            for member_row in member_rows:
                member_id = int(member_row["id"])
                self.conn.execute(
                    """
                    INSERT INTO member_versions (member_id, commit_id, file_id, is_deleted)
                    VALUES (?, ?, ?, 1)
                    """,
                    (member_id, commit_id, file_id),
                )
            self._tombstone_relationships([entity_id], commit_id)

        self.conn.execute(
            "DELETE FROM entity_files WHERE file_id = ?",
            (file_id,),
        )
        
        # Also mark extensions deleted for this file
        self.mark_extensions_deleted_for_file(file_path, commit_id)

    # --- members ---
    def upsert_member(self, entity_id: int, member: MemberRecord) -> int:
        row = self.conn.execute(
            "SELECT id FROM members WHERE stable_id = ?",
            (self._member_stable_id(entity_id, member),),
        ).fetchone()
        if row:
            member_id = int(row["id"])
            self.conn.execute(
                """
                UPDATE members SET name = ?, kind = ?
                WHERE id = ?
                """,
                (member.name, member.kind, member_id),
            )
        else:
            cur = self.conn.execute(
                """
                INSERT INTO members (entity_id, stable_id, name, kind)
                VALUES (?, ?, ?, ?)
                """,
                (
                    entity_id,
                    self._member_stable_id(entity_id, member),
                    member.name,
                    member.kind,
                ),
            )
            member_id = int(cur.lastrowid)
        return member_id

    def record_member_version(
        self,
        member_id: int,
        commit_id: int,
        file_id: int,
        member: MemberRecord,
        is_deleted: bool = False,
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO member_versions (
                member_id, commit_id, file_id, start_line, end_line, signature, code, is_deleted
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                member_id,
                commit_id,
                file_id,
                member.start_line,
                member.end_line,
                member.signature,
                member.code,
                int(is_deleted),
            ),
        )

    # --- extensions ---
    def upsert_extension(
        self, record: ExtensionRecord, entity_id: int
    ) -> int:
        """Insert or update an extension record, linked to its parent entity."""
        row = self.conn.execute(
            "SELECT id FROM extensions WHERE stable_id = ?",
            (record.stable_id,),
        ).fetchone()
        if row:
            extension_id = int(row["id"])
            self.conn.execute(
                """
                UPDATE extensions
                SET entity_id = ?, extended_type = ?, module = ?, language = ?
                WHERE id = ?
                """,
                (
                    entity_id,
                    record.extended_type,
                    record.module,
                    record.language,
                    extension_id,
                ),
            )
        else:
            cur = self.conn.execute(
                """
                INSERT INTO extensions (stable_id, entity_id, extended_type, module, language)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    record.stable_id,
                    entity_id,
                    record.extended_type,
                    record.module,
                    record.language,
                ),
            )
            extension_id = int(cur.lastrowid)
        return extension_id

    def record_extension_version(
        self,
        extension_id: int,
        commit_id: int,
        file_id: int,
        record: ExtensionRecord,
        is_deleted: bool = False,
    ) -> None:
        """Record a version of an extension at a specific commit."""
        props_dict = {}
        if record.target_type:
            props_dict["target_type"] = record.target_type
        props = json.dumps(props_dict) if props_dict else None
        conformances = json.dumps(record.conformances) if record.conformances else None
        
        self.conn.execute(
            """
            INSERT INTO extension_versions (
                extension_id, commit_id, file_id, start_line, end_line,
                signature, code, visibility, constraints, conformances, properties, is_deleted
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                extension_id,
                commit_id,
                file_id,
                record.start_line,
                record.end_line,
                record.signature,
                record.code,
                record.visibility,
                record.constraints,
                conformances,
                props,
                int(is_deleted),
            ),
        )

    def persist_extensions(
        self,
        commit_id: int,
        extensions: Iterable[ExtensionRecord],
        entity_id_map: Dict[str, int],
    ) -> Dict[str, int]:
        """Persist extension records, linking them to their parent entities.
        
        Args:
            commit_id: The commit ID for versioning
            extensions: Extension records to persist
            entity_id_map: Map of entity stable_id -> entity_id for linking
            
        Returns:
            Map of extension stable_id -> extension_id
        """
        ext_id_map: Dict[str, int] = {}
        for record in extensions:
            # Find parent entity by name lookup if not in the current batch
            parent_entity_id = self._lookup_entity_id(record.extended_type, record.module)
            if parent_entity_id is None:
                # Try without module restriction
                parent_entity_id = self._lookup_entity_id(record.extended_type, None)
            if parent_entity_id is None:
                # Skip extensions for types we don't have indexed
                continue
                
            file_id = self.ensure_file(record.file_path, record.language)
            extension_id = self.upsert_extension(record, parent_entity_id)
            ext_id_map[record.stable_id] = extension_id
            self.record_extension_version(extension_id, commit_id, file_id, record)
            
            # Also persist extension members linked to the parent entity
            for member in record.members:
                member_id = self.upsert_member(parent_entity_id, member)
                self.record_member_version(member_id, commit_id, file_id, member)
        
        return ext_id_map

    def mark_extensions_deleted_for_file(self, file_path: Path, commit_id: int) -> None:
        """Mark all extensions in a file as deleted."""
        row = self.conn.execute(
            "SELECT id FROM files WHERE path = ?",
            (str(file_path),),
        ).fetchone()
        if not row:
            return
        file_id = int(row["id"])
        
        # Find extensions that have versions in this file
        extension_rows = self.conn.execute(
            """
            SELECT DISTINCT ev.extension_id
            FROM extension_versions ev
            WHERE ev.file_id = ?
            """,
            (file_id,),
        ).fetchall()
        
        for ext_row in extension_rows:
            extension_id = int(ext_row["extension_id"])
            self.conn.execute(
                """
                INSERT INTO extension_versions (
                    extension_id, commit_id, file_id, is_deleted
                ) VALUES (?, ?, ?, 1)
                """,
                (extension_id, commit_id, file_id),
            )

    # --- query helpers ---
    def latest_commit(self) -> Optional[str]:
        row = self.conn.execute(
            "SELECT hash FROM commits ORDER BY id DESC LIMIT 1"
        ).fetchone()
        return row["hash"] if row else None

    # --- schema meta helpers ---
    def get_schema_value(self, key: str) -> Optional[str]:
        row = self.conn.execute(
            "SELECT value FROM schema_meta WHERE key = ?",
            (key,),
        ).fetchone()
        return row["value"] if row else None

    def set_schema_value(self, key: str, value: str) -> None:
        self.conn.execute(
            """
            INSERT INTO schema_meta (key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (key, value),
        )

    # --- aggregations ---
    def persist_entities(
        self,
        commit_id: int,
        records: Iterable[EntityRecord],
    ) -> Dict[str, int]:
        id_map: Dict[str, int] = {}
        for record in records:
            file_id = self.ensure_file(record.file_path, record.language)
            entity_id = self.upsert_entity(record, file_id)
            id_map[record.stable_id] = entity_id
            self.record_entity_version(entity_id, commit_id, file_id, record)
            for member in record.members:
                member_id = self.upsert_member(entity_id, member)
                self.record_member_version(member_id, commit_id, file_id, member)
        return id_map

    def persist_relationships(
        self,
        commit_id: int,
        entity_id_map: Dict[str, int],
        relationships: Iterable[RelationshipRecord],
    ) -> None:
        rel_list = list(relationships)
        if not entity_id_map and not rel_list:
            return
        source_ids: Set[int] = set(entity_id_map.values())
        source_cache: Dict[str, Optional[int]] = {
            stable_id: entity_id for stable_id, entity_id in entity_id_map.items()
        }
        target_cache: Dict[Tuple[str, Optional[str]], Optional[int]] = {}
        resolved: List[Tuple[int, RelationshipRecord, Optional[int]]] = []
        for rel in rel_list:
            source_id = source_cache.get(rel.source_stable_id)
            if source_id is None:
                source_id = self._lookup_entity_id_by_stable_id(rel.source_stable_id)
                source_cache[rel.source_stable_id] = source_id
            if source_id is None:
                continue
            source_ids.add(source_id)
            target_key = (rel.target_name, rel.target_module)
            if target_key not in target_cache:
                target_cache[target_key] = self._lookup_entity_id(
                    rel.target_name, rel.target_module
                )
            target_id = target_cache[target_key]
            resolved.append((source_id, rel, target_id))

        if not source_ids:
            return

        self._tombstone_relationships(list(source_ids), commit_id)
        for source_id, rel, target_id in resolved:
            self._insert_relationship(
                source_entity_id=source_id,
                target_entity_id=target_id,
                target_name=rel.target_name,
                target_module=rel.target_module,
                edge_type=rel.edge_type,
                metadata=rel.metadata,
                commit_id=commit_id,
                is_deleted=False,
            )

    # --- internal ---
    def _member_stable_id(self, entity_id: int, member: MemberRecord) -> str:
        return f"{entity_id}:{member.kind}:{member.name}"

    def _lookup_entity_id(self, name: str, module: Optional[str]) -> Optional[int]:
        if module:
            row = self.conn.execute(
                """
                SELECT id FROM entities
                WHERE name = ? AND module = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (name, module),
            ).fetchone()
        else:
            row = self.conn.execute(
                """
                SELECT id FROM entities
                WHERE name = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (name,),
            ).fetchone()
        return int(row["id"]) if row else None

    def _lookup_entity_id_by_stable_id(self, stable_id: str) -> Optional[int]:
        row = self.conn.execute(
            "SELECT id FROM entities WHERE stable_id = ?",
            (stable_id,),
        ).fetchone()
        return int(row["id"]) if row else None

    def _tombstone_relationships(
        self, source_entity_ids: Sequence[int], commit_id: int
    ) -> None:
        if not source_entity_ids:
            return
        placeholders = ",".join("?" for _ in source_entity_ids)
        rows = self.conn.execute(
            f"""
            SELECT
                source_entity_id,
                target_entity_id,
                target_name,
                target_module,
                edge_type,
                metadata
            FROM entity_relationships
            WHERE source_entity_id IN ({placeholders})
              AND is_deleted = 0
            """,
            tuple(source_entity_ids),
        ).fetchall()
        for row in rows:
            metadata = json.loads(row["metadata"]) if row["metadata"] else {}
            self._insert_relationship(
                source_entity_id=int(row["source_entity_id"]),
                target_entity_id=int(row["target_entity_id"])
                if row["target_entity_id"] is not None
                else None,
                target_name=row["target_name"],
                target_module=row["target_module"],
                edge_type=row["edge_type"],
                metadata=metadata,
                commit_id=commit_id,
                is_deleted=True,
            )

    def _insert_relationship(
        self,
        source_entity_id: int,
        target_entity_id: Optional[int],
        target_name: str,
        target_module: Optional[str],
        edge_type: str,
        metadata: dict,
        commit_id: int,
        is_deleted: bool,
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO entity_relationships (
                source_entity_id,
                target_entity_id,
                target_name,
                target_module,
                edge_type,
                metadata,
                commit_id,
                is_deleted
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                source_entity_id,
                target_entity_id,
                target_name,
                target_module,
                edge_type,
                json.dumps(metadata, sort_keys=True) if metadata else None,
                commit_id,
                int(is_deleted),
            ),
        )

    def rebuild_latest_tables(self) -> None:
        """Rebuild the materialized entity_latest, relationship_latest, and extension_latest tables.
        
        This should be called after indexing to update the denormalized views
        that enable fast graph queries without complex joins.
        """
        # Clear existing data
        self.conn.execute("DELETE FROM entity_latest;")
        self.conn.execute("DELETE FROM relationship_latest;")
        self.conn.execute("DELETE FROM extension_latest;")
        
        # Rebuild entity_latest from versioned data
        # Note: We first find the latest commit per entity, then pick a single version_id
        # for that commit to avoid duplicates when same entity appears in multiple files
        self.conn.execute(
            """
            INSERT INTO entity_latest (
                stable_id, entity_id, name, kind, module, file_path,
                signature, properties, member_names, target_type, visibility, commit_hash
            )
            WITH latest_commit AS (
                SELECT entity_id, MAX(commit_id) AS commit_id
                FROM entity_versions
                GROUP BY entity_id
            ),
            latest_version AS (
                SELECT lc.entity_id, lc.commit_id, MAX(ev.id) AS version_id
                FROM latest_commit lc
                JOIN entity_versions ev
                    ON ev.entity_id = lc.entity_id
                   AND ev.commit_id = lc.commit_id
                GROUP BY lc.entity_id, lc.commit_id
            ),
            member_agg AS (
                SELECT entity_id, GROUP_CONCAT(name, '|') AS names
                FROM members
                GROUP BY entity_id
            )
            SELECT
                e.stable_id,
                e.id,
                e.name,
                e.kind,
                e.module,
                f.path,
                ev.signature,
                ev.properties,
                COALESCE(ma.names, ''),
                json_extract(ev.properties, '$.target_type'),
                json_extract(ev.properties, '$.visibility'),
                c.hash
            FROM latest_version lv
            JOIN entity_versions ev ON ev.id = lv.version_id
            JOIN entities e ON e.id = lv.entity_id
            LEFT JOIN files f ON f.id = ev.file_id
            LEFT JOIN member_agg ma ON ma.entity_id = e.id
            JOIN commits c ON c.id = ev.commit_id
            WHERE ev.is_deleted = 0
            """
        )
        
        # Rebuild relationship_latest from versioned data
        self.conn.execute(
            """
            INSERT OR REPLACE INTO relationship_latest (
                source_stable_id, source_name, target_stable_id,
                target_name, target_module, edge_type, metadata
            )
            WITH latest AS (
                SELECT
                    source_entity_id,
                    COALESCE(target_entity_id, -1) AS target_entity_id_key,
                    target_name,
                    COALESCE(target_module, '') AS target_module_key,
                    edge_type,
                    MAX(commit_id) AS max_commit_id
                FROM entity_relationships
                GROUP BY
                    source_entity_id,
                    COALESCE(target_entity_id, -1),
                    target_name,
                    COALESCE(target_module, ''),
                    edge_type
            ),
            latest_rel AS (
                SELECT
                    er.source_entity_id,
                    er.target_entity_id,
                    er.target_name,
                    er.target_module,
                    er.edge_type,
                    er.metadata,
                    er.is_deleted,
                    MAX(er.id) AS id
                FROM latest
                JOIN entity_relationships er ON
                    er.source_entity_id = latest.source_entity_id
                    AND COALESCE(er.target_entity_id, -1) = latest.target_entity_id_key
                    AND er.target_name = latest.target_name
                    AND COALESCE(er.target_module, '') = latest.target_module_key
                    AND er.edge_type = latest.edge_type
                    AND er.commit_id = latest.max_commit_id
                GROUP BY
                    er.source_entity_id,
                    er.target_entity_id,
                    er.target_name,
                    er.target_module,
                    er.edge_type
            )
            SELECT
                src.stable_id,
                src.name,
                tgt.stable_id,
                lr.target_name,
                lr.target_module,
                lr.edge_type,
                lr.metadata
            FROM latest_rel lr
            JOIN entities src ON src.id = lr.source_entity_id
            LEFT JOIN entities tgt ON tgt.id = lr.target_entity_id
            WHERE lr.is_deleted = 0
            """
        )
        
        # Rebuild extension_latest from versioned data
        self.conn.execute(
            """
            INSERT INTO extension_latest (
                stable_id, extension_id, entity_id, entity_stable_id,
                extended_type, module, file_path, signature, visibility,
                constraints, conformances, member_names, target_type, commit_hash
            )
            WITH latest AS (
                SELECT extension_id, MAX(commit_id) AS commit_id
                FROM extension_versions
                GROUP BY extension_id
            )
            SELECT
                ext.stable_id,
                ext.id,
                ext.entity_id,
                e.stable_id,
                ext.extended_type,
                ext.module,
                f.path,
                ev.signature,
                ev.visibility,
                ev.constraints,
                ev.conformances,
                '',  -- member_names not tracked separately for extensions
                json_extract(ev.properties, '$.target_type'),
                c.hash
            FROM latest
            JOIN extension_versions ev
                ON ev.extension_id = latest.extension_id
               AND ev.commit_id = latest.commit_id
            JOIN extensions ext ON ext.id = latest.extension_id
            JOIN entities e ON e.id = ext.entity_id
            LEFT JOIN files f ON f.id = ev.file_id
            JOIN commits c ON c.id = ev.commit_id
            WHERE ev.is_deleted = 0
            """
        )

