from __future__ import annotations

from dataclasses import dataclass
import sqlite3
from pathlib import Path
from typing import Iterable, List, Optional

from git import GitCommandError, Repo

from ..config import Settings
from ..db import schema
from ..db.connection import connect, get_connection
from ..db.repository import MetadataRepository
from ..models.records import EntityRecord
from .base import ParserRegistry
from .git_utils import (
    changed_swift_files,
    commits_since,
    file_content_at_commit,
    open_repo,
)


@dataclass
class FeatureUpdateResult:
    branch: Optional[str]
    commits: List[str]
    worktree_files: List[str]
    skipped_reason: Optional[str] = None

    @property
    def skipped(self) -> bool:
        return self.skipped_reason is not None


class FeatureBranchIndexer:
    WORKTREE_HASH_PREFIX = "worktree"

    def __init__(self, settings: Settings, registry: ParserRegistry) -> None:
        self.settings = settings
        self.repo: Repo = open_repo(settings.repo_path)
        self.registry = registry

    def update(self) -> FeatureUpdateResult:
        branch = self._current_branch()
        if not branch:
            return FeatureUpdateResult(
                branch=None,
                commits=[],
                worktree_files=[],
                skipped_reason="detached HEAD",
            )
        if branch == self.settings.default_branch:
            return FeatureUpdateResult(
                branch=branch,
                commits=[],
                worktree_files=[],
                skipped_reason="on default branch",
            )

        if self._tracked_branch_conflicts(branch):
            self._reset_feature_db()

        commits_processed: List[str] = []
        worktree_files: List[str] = []
        with get_connection(self.settings.feature_db_path) as conn:
            schema.apply_schema(conn)
            store = MetadataRepository(conn)
            store.set_schema_value("feature_branch", branch)
            commits_processed = self._index_branch_commits(store, branch)
            worktree_files = self._index_worktree(store, branch)

        return FeatureUpdateResult(
            branch=branch,
            commits=commits_processed,
            worktree_files=worktree_files,
            skipped_reason=None,
        )

    # --- internal helpers ---
    def _index_branch_commits(self, store: MetadataRepository, branch: str) -> List[str]:
        last_real_hash = store.latest_real_commit_for_branch(branch)
        anchor_hash = last_real_hash or self._merge_base_hash(branch)
        commits = commits_since(self.repo, anchor_hash, branch)
        processed: List[str] = []
        for commit in commits:
            commit_id = store.record_commit(
                commit_hash=commit.hexsha,
                parent_hash=commit.parents[0].hexsha if commit.parents else None,
                branch=branch,
                is_master=False,
            )
            for rel_path in changed_swift_files(commit):
                content = file_content_at_commit(self.repo, commit, rel_path)
                path_obj = Path(rel_path)
                if content is None:
                    store.mark_entities_deleted_for_file(path_obj, commit_id)
                    continue
                records = self._parse_file(content, path_obj)
                store.persist_entities(commit_id, records)
            processed.append(commit.hexsha)
        return processed

    def _index_worktree(self, store: MetadataRepository, branch: str) -> List[str]:
        changes = self._collect_worktree_changes()
        worktree_hash = self._worktree_hash(branch)
        store.delete_commit(worktree_hash)
        if not changes:
            return []
        head_hash = getattr(self.repo.head, "commit", None)
        parent_hash = head_hash.hexsha if head_hash else None
        commit_id = store.record_commit(
            commit_hash=worktree_hash,
            parent_hash=parent_hash,
            branch=branch,
            is_master=False,
        )

        for rel_path, status in changes.items():
            path_obj = Path(rel_path)
            if status == "deleted":
                store.mark_entities_deleted_for_file(path_obj, commit_id)
                continue
            content = self._read_worktree_content(path_obj)
            if content is None:
                continue
            records = self._parse_file(content, path_obj)
            store.persist_entities(commit_id, records)

        return sorted(changes.keys())

    def _collect_worktree_changes(self) -> dict[str, str]:
        changes: dict[str, str] = {}
        diff_index = self.repo.index.diff(None)
        for diff in diff_index:
            rel_path = diff.b_path or diff.a_path or ""
            if not rel_path.endswith(".swift"):
                continue
            change_type = "deleted" if diff.change_type == "D" else "modified"
            changes[rel_path] = change_type
        for rel_path in self.repo.untracked_files:
            if rel_path.endswith(".swift"):
                changes[rel_path] = "untracked"
        return changes

    def _tracked_branch_conflicts(self, branch: str) -> bool:
        db_path = self.settings.feature_db_path
        if not db_path.exists():
            return False
        conn = connect(db_path)
        try:
            row = conn.execute(
                "SELECT value FROM schema_meta WHERE key = ?",
                ("feature_branch",),
            ).fetchone()
            tracked = row["value"] if row else None
            return bool(tracked and tracked != branch)
        except sqlite3.OperationalError:
            # schema hasn't been created yet, treat as no conflict
            return False
        finally:
            conn.close()

    def _reset_feature_db(self) -> None:
        db_path = self.settings.feature_db_path
        if db_path.exists():
            db_path.unlink()
        wal_path = db_path.with_name(db_path.name + "-wal")
        shm_path = db_path.with_name(db_path.name + "-shm")
        if wal_path.exists():
            wal_path.unlink()
        if shm_path.exists():
            shm_path.unlink()

    def _worktree_hash(self, branch: str) -> str:
        return f"{self.WORKTREE_HASH_PREFIX}:{branch}"

    def _read_worktree_content(self, relative_path: Path) -> Optional[str]:
        path_on_disk = self.settings.repo_path / relative_path
        if not path_on_disk.exists():
            return None
        return path_on_disk.read_text(encoding="utf-8")

    def _parse_file(self, content: str, relative_path: Path) -> Iterable[EntityRecord]:
        adapter = self.registry.get("swift")
        return adapter.parse(content, relative_path)

    def _current_branch(self) -> Optional[str]:
        try:
            return self.repo.active_branch.name
        except (TypeError, GitCommandError):
            return None

    def _merge_base_hash(self, branch: str) -> Optional[str]:
        default_ref = getattr(self.repo.heads, self.settings.default_branch, None)
        if default_ref is None:
            return None
        try:
            bases = self.repo.merge_base(branch, self.settings.default_branch)
        except GitCommandError:
            return None
        if not bases:
            return None
        return bases[0].hexsha

