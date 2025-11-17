from __future__ import annotations

import json
from dataclasses import dataclass
import sqlite3
from pathlib import Path
from typing import Iterable, Optional

from ..models.records import EntityRecord, MemberRecord


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
        props = json.dumps(
            {
                "extended_type": record.extended_type,
                "member_count": len(record.members),
            }
        )
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

        self.conn.execute(
            "DELETE FROM entity_files WHERE file_id = ?",
            (file_id,),
        )

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

    # --- query helpers ---
    def latest_commit(self) -> Optional[str]:
        row = self.conn.execute(
            "SELECT hash FROM commits ORDER BY id DESC LIMIT 1"
        ).fetchone()
        return row["hash"] if row else None

    # --- aggregations ---
    def persist_entities(
        self,
        commit_id: int,
        records: Iterable[EntityRecord],
    ) -> None:
        for record in records:
            file_id = self.ensure_file(record.file_path, record.language)
            entity_id = self.upsert_entity(record, file_id)
            self.record_entity_version(entity_id, commit_id, file_id, record)
            for member in record.members:
                member_id = self.upsert_member(entity_id, member)
                self.record_member_version(member_id, commit_id, file_id, member)

    # --- internal ---
    def _member_stable_id(self, entity_id: int, member: MemberRecord) -> str:
        return f"{entity_id}:{member.kind}:{member.name}"

