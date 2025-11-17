from __future__ import annotations

from pathlib import Path
from typing import Iterable, List

from git import Repo

from ..config import Settings
from ..db import schema
from ..db.connection import get_connection
from ..db.repository import MetadataRepository
from ..models.records import EntityRecord
from .base import ParserRegistry
from .git_utils import (
    changed_swift_files,
    commits_since,
    file_content_at_commit,
    get_branch_head,
    open_repo,
)
from .swift_parser import SwiftParser


class IndexerService:
    def __init__(self, settings: Settings, registry: ParserRegistry | None = None) -> None:
        self.settings = settings
        self.repo: Repo = open_repo(settings.repo_path)
        self.registry = registry or build_registry(settings.languages)

    # --- public API ---
    def initialize(self) -> str:
        head = get_branch_head(self.repo, self.settings.default_branch)
        tracked_files = self._tracked_swift_files()
        with get_connection(self.settings.db_path) as conn:
            schema.apply_schema(conn)
            store = MetadataRepository(conn)
            commit_id = store.record_commit(
                commit_hash=head.hexsha,
                parent_hash=head.parents[0].hexsha if head.parents else None,
                branch=self.settings.default_branch,
                is_master=True,
            )
            for rel_path in tracked_files:
                content = file_content_at_commit(self.repo, head, rel_path)
                if not content:
                    continue
                records = self._parse_file(content, Path(rel_path))
                store.persist_entities(commit_id, records)
        return head.hexsha

    def update(self) -> List[str]:
        with get_connection(self.settings.db_path) as conn:
            schema.apply_schema(conn)
            store = MetadataRepository(conn)
            last_hash = store.latest_master_commit()
        commits = commits_since(self.repo, last_hash, self.settings.default_branch)
        processed: List[str] = []
        if not commits:
            return processed
        with get_connection(self.settings.db_path) as conn:
            schema.apply_schema(conn)
            store = MetadataRepository(conn)
            for commit in commits:
                commit_id = store.record_commit(
                    commit_hash=commit.hexsha,
                    parent_hash=commit.parents[0].hexsha if commit.parents else None,
                    branch=self.settings.default_branch,
                    is_master=True,
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

    # --- helpers ---
    def _parse_file(self, content: str, relative_path: Path) -> Iterable[EntityRecord]:
        adapter = self.registry.get("swift")
        return adapter.parse(content, relative_path)

    def _tracked_swift_files(self) -> List[str]:
        files = self.repo.git.ls_files("*.swift")
        return [line.strip() for line in files.splitlines() if line.strip()]


def build_registry(languages: Iterable[str]) -> ParserRegistry:
    registry = ParserRegistry()
    langs = set(languages)
    if "swift" in langs:
        registry.register(SwiftParser())
    return registry

