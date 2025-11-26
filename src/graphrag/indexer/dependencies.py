from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, List, Optional

from ..config import Settings
from .project_parsers import ProjectMetadata, SwiftGekoProjectParser, TestTargetMetadata


@dataclass(frozen=True)
class TargetInfo:
    name: str
    target_type: str
    source_roots: List[Path]
    sources: List[str] = field(default_factory=list)
    tests: List[TestTargetMetadata] = field(default_factory=list)


class DependenciesWorker:
    def __init__(self, project_root: Path) -> None:
        self.project_root = project_root

    def target_for_file(self, relative_path: Path) -> Optional[TargetInfo]:
        raise NotImplementedError


class TuistDependenciesWorker(DependenciesWorker):

    def __init__(self, project_root: Path) -> None:
        super().__init__(project_root)
        self._parser = SwiftGekoProjectParser()
        self._project_cache: dict[Path, Optional[ProjectMetadata]] = {}
        self._targets: List[TargetInfo] = self._load_targets()

    def target_for_file(self, relative_path: Path) -> Optional[TargetInfo]:
        best: Optional[TargetInfo] = None
        best_depth = -1
        abs_path: Optional[Path] = None
        if self.project_root:
            abs_path = (self.project_root / relative_path).resolve()
        for target in self._targets:
            for root in target.source_roots:
                if self._path_is_within(relative_path, root):
                    depth = len(root.parts)
                elif abs_path is not None and root.is_absolute():
                    if not self._path_is_within(abs_path, root):
                        continue
                    depth = len(root.parts)
                else:
                    continue
                if depth > best_depth:
                    best = target
                    best_depth = depth
        return best

    def _path_is_within(self, candidate: Path, root: Path) -> bool:
        candidate_parts = candidate.parts
        root_parts = root.parts
        if not root_parts:
            return True
        if len(candidate_parts) < len(root_parts):
            return False
        return candidate_parts[: len(root_parts)] == root_parts

    def _load_targets(self) -> List[TargetInfo]:
        targets: List[TargetInfo] = []
        for project_file in self._project_files():
            metadata = self._project_metadata(project_file)
            if metadata is None:
                continue
            for target in metadata.targets:
                # First, add entries for test sources (higher priority)
                for test in target.tests:
                    if not test.sources:
                        continue
                    test_source_roots = [
                        self._normalize_source(project_file.parent, src)
                        for src in test.sources
                    ]
                    test_name = self._test_target_name(target.name, test.tests_type)
                    targets.append(
                        TargetInfo(
                            name=test_name,
                            target_type="test",
                            source_roots=test_source_roots,
                            sources=test.sources,
                            tests=[],
                        )
                    )
                # Then add the main target sources
                sources = target.sources or self._default_sources(project_file.parent, target.name)
                source_roots = [
                    self._normalize_source(project_file.parent, src) for src in sources
                ]
                targets.append(
                    TargetInfo(
                        name=target.name,
                        target_type=target.target_type,
                        source_roots=source_roots,
                        sources=sources,
                        tests=target.tests,
                    )
                )
        return targets

    def _test_target_name(self, base_name: str, tests_type: str) -> str:
        """Generate a test target name based on the base target and test type."""
        type_suffix = tests_type.capitalize() if tests_type else ""
        return f"{base_name}{type_suffix}Tests"

    def _project_metadata(self, project_file: Path) -> Optional[ProjectMetadata]:
        if project_file in self._project_cache:
            return self._project_cache[project_file]
        try:
            metadata = self._parser.parse(project_file)
        except ValueError:
            metadata = None
        self._project_cache[project_file] = metadata
        return metadata

    def _project_files(self) -> Iterable[Path]:
        if not self.project_root.exists():
            return []
        return self.project_root.rglob("Project.swift")

    def _default_sources(self, project_dir: Path, target_name: str) -> List[str]:
        default = project_dir / "Targets" / target_name / "Sources"
        rel = default.relative_to(self.project_root)
        return [rel.as_posix()]

    def _normalize_source(self, project_dir: Path, source: str) -> Path:
        cleaned = source.strip().replace("\\", "/")
        if not cleaned:
            return project_dir.resolve()
        # Cut at first wildcard or brace pattern
        cut = len(cleaned)
        for token in ("{", "*"):
            idx = cleaned.find(token)
            if idx != -1 and idx < cut:
                cut = idx
        cleaned = cleaned[:cut].rstrip("/")
        if not cleaned:
            return project_dir.resolve()
        absolute = (project_dir / cleaned).resolve()
        try:
            relative = absolute.relative_to(self.project_root)
        except ValueError:
            relative = absolute
        return relative


class GekoDependenciesWorker(TuistDependenciesWorker):
    """Geko projects follow the same structure as Tuist ones."""

    pass


def build_dependencies_worker(settings: Settings) -> Optional[DependenciesWorker]:
    build_system = (settings.graph.build_system or "").lower()
    project_root = settings.repo_path
    if build_system == "tuist":
        return TuistDependenciesWorker(project_root)
    if build_system == "geko":
        return GekoDependenciesWorker(project_root)
    return None

