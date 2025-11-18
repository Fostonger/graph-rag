from __future__ import annotations

import hashlib
from pathlib import Path
import re
from typing import Optional


class ModuleResolver:
    def __init__(self, project_root: Optional[Path]) -> None:
        self.project_root = project_root
        self._dir_cache: dict[Path, str] = {}
        self._project_cache: dict[Path, Optional[str]] = {}

    def resolve(self, relative_path: Path) -> str:
        if not self.project_root:
            return self._fallback(relative_path)
        abs_path = (self.project_root / relative_path).resolve()
        current = abs_path.parent
        while current and current != current.parent:
            if current in self._dir_cache:
                return self._dir_cache[current]
            project_file = current / "Project.swift"
            if project_file.exists():
                module_name = self._module_name_from_project(project_file)
                if module_name:
                    self._dir_cache[current] = module_name
                    return module_name
            current = current.parent
        return self._fallback(relative_path)

    def _module_name_from_project(self, path: Path) -> Optional[str]:
        if path in self._project_cache:
            cached = self._project_cache[path]
            if cached:
                return cached
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            self._project_cache[path] = None
            return None
        match = re.search(r'Project\s*\(.*?name\s*:\s*"([^"]+)"', text, re.DOTALL)
        module_name: Optional[str] = None
        if match:
            module_name = match.group(1)
        if not module_name:
            fallback = re.search(r'name\s*:\s*"([^"]+)"', text)
            module_name = fallback.group(1) if fallback else None
        self._project_cache[path] = module_name
        return module_name

    def _fallback(self, path: Path) -> str:
        parts = list(path.parts)
        if len(parts) >= 2:
            return "/".join(parts[:-1])
        return path.parent.name or "root"


def compute_stable_id(language: str, module: str, name: str) -> str:
    raw = f"{language}:{module}:{name}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()

