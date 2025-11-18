from __future__ import annotations

from pathlib import Path
from typing import List, Optional

import yaml
from pydantic import BaseModel, Field, field_validator


class ParserOptions(BaseModel):
    enable_extensions: bool = True
    include_doc_comments: bool = True


class Settings(BaseModel):
    repo_path: Path = Field(default_factory=lambda: Path(".").resolve())
    db_path: Path = Field(default_factory=lambda: Path("graphrag.db").resolve())
    feature_db_path: Path = Field(default_factory=lambda: Path("graphrag-feature.db").resolve())
    default_branch: str = "master"
    languages: List[str] = Field(default_factory=lambda: ["swift"])
    parser: dict[str, ParserOptions] = Field(default_factory=dict)

    @field_validator("repo_path", "db_path", "feature_db_path", mode="before")
    def _coerce_path(cls, value: str | Path) -> Path:
        return Path(value).expanduser().resolve()


def load_settings(config_path: Optional[Path] = None) -> Settings:
    """Load configuration from YAML if provided, otherwise use defaults."""

    path = config_path or Path(__file__).resolve().parent.parent.parent / "config.yaml"
    if path.exists():
        data = yaml.safe_load(path.read_text()) or {}
    else:
        data = {}

    parser_section = data.get("parser", {})
    parser_options: dict[str, ParserOptions] = {}
    for lang, opts in parser_section.items():
        parser_options[lang] = ParserOptions(**opts)
    data["parser"] = parser_options
    return Settings(**data)

