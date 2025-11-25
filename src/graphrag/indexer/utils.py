from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from .dependencies import DependenciesWorker
from .project_parsers import ProjectMetadata, SwiftGekoProjectParser, TargetMetadata


@dataclass(frozen=True)
class ModuleMetadata:
    module: str
    target_type: Optional[str] = None

class ModuleResolver:
    def __init__(
        self,
        project_root: Optional[Path],
        dependencies: Optional[DependenciesWorker] = None,
    ) -> None:
        self.project_root = project_root
        self._dir_cache: dict[Path, ModuleMetadata] = {}
        self._project_cache: dict[Path, Optional[ProjectMetadata]] = {}
        self._dependencies = dependencies
        self._project_parser: Optional[SwiftGekoProjectParser] = None

    def resolve(self, relative_path: Path) -> str:
        return self.resolve_metadata(relative_path).module

    def resolve_metadata(self, relative_path: Path) -> ModuleMetadata:
        if self._dependencies:
            info = self._dependencies.target_for_file(relative_path)
            if info:
                return ModuleMetadata(module=info.name, target_type=info.target_type)
        if not self.project_root:
            return ModuleMetadata(self._fallback(relative_path))
        abs_path = (self.project_root / relative_path).resolve()
        current = abs_path.parent
        while current and current != current.parent:
            if current in self._dir_cache:
                return self._dir_cache[current]
            project_file = current / "Project.swift"
            if project_file.exists():
                metadata = self._project_metadata(project_file)
                if metadata:
                    target = self._match_target(metadata, project_file.parent, abs_path)
                    if target:
                        module_meta = ModuleMetadata(
                            module=target.name,
                            target_type=target.target_type,
                        )
                    elif metadata.targets:
                        first = metadata.targets[0]
                        module_meta = ModuleMetadata(
                            module=first.name, target_type=first.target_type
                        )
                    else:
                        module_meta = ModuleMetadata(module=metadata.name)
                    self._dir_cache[current] = module_meta
                    return module_meta
            current = current.parent
        return ModuleMetadata(self._fallback(relative_path))

    def _project_metadata(self, path: Path) -> Optional[ProjectMetadata]:
        if path in self._project_cache:
            return self._project_cache[path]
        parser = self._ensure_project_parser()
        try:
            metadata = parser.parse(path)
        except ValueError:
            metadata = None
        self._project_cache[path] = metadata
        return metadata

    def _ensure_project_parser(self) -> SwiftGekoProjectParser:
        if self._project_parser is None:
            self._project_parser = SwiftGekoProjectParser()
        return self._project_parser

    def _match_target(
        self, metadata: ProjectMetadata, project_dir: Path, file_path: Path
    ) -> Optional[TargetMetadata]:
        for target in metadata.targets:
            for pattern in target.sources:
                root = self._source_root_from_pattern(project_dir, pattern)
                if root and self._path_is_within(file_path, root):
                    return target
        return None

    def _source_root_from_pattern(self, project_dir: Path, pattern: str) -> Optional[Path]:
        cleaned = pattern.strip().replace("\\", "/")
        if not cleaned:
            return project_dir.resolve()
        cut = len(cleaned)
        for token in ("{", "*"):
            idx = cleaned.find(token)
            if idx != -1 and idx < cut:
                cut = idx
        cleaned = cleaned[:cut].rstrip("/")
        candidate = Path(cleaned)
        if candidate.is_absolute():
            base = candidate
        else:
            base = (project_dir / cleaned) if cleaned else project_dir
        try:
            return base.resolve()
        except OSError:
            return base

    def _path_is_within(self, candidate: Path, root: Path) -> bool:
        try:
            candidate.relative_to(root)
            return True
        except ValueError:
            return False

    def _fallback(self, path: Path) -> str:
        parts = list(path.parts)
        if len(parts) >= 2:
            return "/".join(parts[:-1])
        return path.parent.name or "root"


def compute_stable_id(language: str, module: str, name: str) -> str:
    raw = f"{language}:{module}:{name}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()

