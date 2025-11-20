from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass(slots=True)
class MemberRecord:
    name: str
    kind: str
    signature: str
    code: str
    start_line: int
    end_line: int


@dataclass(slots=True)
class EntityRecord:
    name: str
    kind: str
    module: str
    language: str
    file_path: Path
    start_line: int
    end_line: int
    signature: str
    code: str
    stable_id: str
    docstring: Optional[str] = None
    extended_type: Optional[str] = None
    target_type: Optional[str] = None
    members: List[MemberRecord] = field(default_factory=list)

@dataclass(slots=True)
class RelationshipRecord:
    source_stable_id: str
    target_name: str
    edge_type: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    target_module: Optional[str] = None


@dataclass(slots=True)
class ParsedSource:
    entities: List[EntityRecord] = field(default_factory=list)
    relationships: List[RelationshipRecord] = field(default_factory=list)


