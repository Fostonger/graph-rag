from __future__ import annotations

import hashlib
from pathlib import Path


def derive_module(path: Path) -> str:
    parts = list(path.parts)
    if len(parts) >= 2:
        return "/".join(parts[:-1])
    return path.parent.name or "root"


def compute_stable_id(language: str, module: str, name: str) -> str:
    raw = f"{language}:{module}:{name}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()

