from __future__ import annotations

from dataclasses import dataclass
import re
from pathlib import Path
from typing import Iterable, List, Optional

from ..config import Settings


@dataclass(frozen=True)
class TargetInfo:
    name: str
    target_type: str
    source_roots: List[Path]


class DependenciesWorker:
    def __init__(self, project_root: Path) -> None:
        self.project_root = project_root

    def target_for_file(self, relative_path: Path) -> Optional[TargetInfo]:
        raise NotImplementedError


class TuistDependenciesWorker(DependenciesWorker):
    TARGET_RE = re.compile(
        r"Target\s*\(\s*name\s*:\s*\"(?P<name>[^\"]+)\"(?P<body>.*?)\)",
        re.DOTALL,
    )
    PRODUCT_RE = re.compile(r"product\s*:\s*\.(?P<product>[A-Za-z0-9_]+)")
    SOURCES_RE = re.compile(r"sources\s*:\s*\[(?P<sources>.*?)\]", re.DOTALL)
    STRING_RE = re.compile(r"\"([^\"]+)\"")

    def __init__(self, project_root: Path) -> None:
        super().__init__(project_root)
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
            try:
                text = project_file.read_text(encoding="utf-8")
            except OSError:
                continue
            for match in self.TARGET_RE.finditer(text):
                name = match.group("name").strip()
                body = match.group("body")
                product = self._extract_product(body)
                target_type = self._classify_target(product)
                sources = self._extract_sources(body)
                if not sources:
                    sources = self._default_sources(project_file.parent, name)
                targets.append(
                    TargetInfo(
                        name=name,
                        target_type=target_type,
                        source_roots=[self._normalize_source(project_file.parent, src) for src in sources],
                    )
                )
        return targets

    def _project_files(self) -> Iterable[Path]:
        if not self.project_root.exists():
            return []
        return self.project_root.rglob("Project.swift")

    def _extract_product(self, body: str) -> str:
        match = self.PRODUCT_RE.search(body)
        return match.group("product") if match else "app"

    def _extract_sources(self, body: str) -> List[str]:
        match = self.SOURCES_RE.search(body)
        if not match:
            return []
        payload = match.group("sources")
        return [entry for entry in self.STRING_RE.findall(payload)]

    def _default_sources(self, project_dir: Path, target_name: str) -> List[str]:
        default = project_dir / "Targets" / target_name / "Sources"
        rel = default.relative_to(self.project_root)
        return [rel.as_posix()]

    def _normalize_source(self, project_dir: Path, source: str) -> Path:
        cleaned = source.rstrip("/")
        if cleaned.endswith("**"):
            cleaned = cleaned[:-2].rstrip("/").rstrip("*")
        absolute = (project_dir / cleaned).resolve()
        try:
            relative = absolute.relative_to(self.project_root)
        except ValueError:
            relative = absolute
        return relative

    def _classify_target(self, product: str) -> str:
        lowered = product.lower()
        if "test" in lowered:
            return "test"
        return "app"


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

