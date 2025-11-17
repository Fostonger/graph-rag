from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Iterable

from ..models.records import EntityRecord


class ParserAdapter(ABC):
    language: str

    @abstractmethod
    def parse(self, source: str, path: Path) -> Iterable[EntityRecord]:
        """Return entity records discovered in the given source."""


class ParserRegistry:
    def __init__(self) -> None:
        self._registry: dict[str, ParserAdapter] = {}

    def register(self, adapter: ParserAdapter) -> None:
        self._registry[adapter.language] = adapter

    def get(self, language: str) -> ParserAdapter:
        try:
            return self._registry[language]
        except KeyError as exc:
            raise ValueError(f"No parser registered for {language}") from exc

