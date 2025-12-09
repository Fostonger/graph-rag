"""Configuration for GraphRAG.

GraphRAG reads from external indexer databases, so configuration is minimal:
- db_path: Path to the SQLite database produced by external indexer
- repo_path: Path to the repository (for context/reference only)
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import yaml
from pydantic import BaseModel, Field, field_validator


class Settings(BaseModel):
    """GraphRAG settings."""
    
    db_path: Path = Field(
        default_factory=lambda: Path("index.db").resolve(),
        description="Path to external indexer SQLite database"
    )
    repo_path: Path = Field(
        default_factory=lambda: Path(".").resolve(),
        description="Path to repository (for reference)"
    )

    @field_validator("repo_path", "db_path", mode="before")
    def _coerce_path(cls, value: str | Path) -> Path:
        return Path(value).expanduser().resolve()


def load_settings(config_path: Optional[Path] = None) -> Settings:
    """Load configuration from YAML if provided, otherwise use defaults."""
    path = config_path or Path(__file__).resolve().parent.parent.parent / "config.yaml"
    if path.exists():
        data = yaml.safe_load(path.read_text()) or {}
    else:
        data = {}
    return Settings(**data)
