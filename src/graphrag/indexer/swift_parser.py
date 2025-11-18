from __future__ import annotations

import re
from pathlib import Path
from typing import Iterable, List, Set

from tree_sitter import Language, Parser
from tree_sitter_swift import language as swift_language

from ..models.records import EntityRecord, MemberRecord, ParsedSource, RelationshipRecord
from .base import ParserAdapter
from .utils import ModuleResolver, compute_stable_id

ENTITY_NODE_TYPES = {
    "class_declaration",
    "struct_declaration",
    "enum_declaration",
    "protocol_declaration",
    "extension_declaration",
}

MEMBER_NODE_TYPES = {
    "function_declaration",
    "initializer_declaration",
    "deinitializer_declaration",
    "subscript_declaration",
    "variable_declaration",
    "property_declaration",
    "constant_declaration",
    "typealias_declaration",
}


PROPERTY_DECL_RE = re.compile(
    r"(?P<prefix>(?:weak|unowned)\s+)?(?:lazy\s+)?"
    r"(?:(?:private|fileprivate|internal|public)\s+)?"
    r"(?:var|let)\s+(?P<name>[A-Za-z_]\w*)\s*:\s*(?P<type>[A-Za-z_][\w?.<>, ]*)"
)

CREATE_EXPR_RE = re.compile(
    r"(?:=|return)\s+(?P<type>[A-Z][A-Za-z0-9_]*)\s*\(",
    re.MULTILINE,
)


class SwiftParser(ParserAdapter):
    language = "swift"

    def __init__(self, project_root: Path | None = None) -> None:
        self._language = Language(swift_language())
        self._parser = Parser(self._language)
        self._module_resolver = ModuleResolver(project_root)

    def parse(self, source: str, path: Path) -> ParsedSource:
        tree = self._parser.parse(source.encode("utf-8"))
        source_bytes = source.encode("utf-8")
        root = tree.root_node

        records: List[EntityRecord] = []
        relationships: List[RelationshipRecord] = []
        for node in self._iter_nodes(root):
            if node.type not in ENTITY_NODE_TYPES:
                continue
            entity_name = self._extract_name(node, source_bytes)
            if not entity_name:
                continue

            code = source_bytes[node.start_byte : node.end_byte].decode("utf-8")
            extended_type = None
            kind = _derive_kind(code, node.type)
            if kind == "extension" or node.type == "extension_declaration":
                extended_type = entity_name
                kind = "extension"

            start_line = node.start_point[0] + 1
            end_line = node.end_point[0] + 1
            file_path = path
            module = self._module_resolver.resolve(file_path)
            stable_name = (
                f"{entity_name}::extension::{path}:{start_line}"
                if kind == "extension"
                else entity_name
            )
            stable_id = compute_stable_id("swift", module, stable_name)

            members = list(
                self._extract_members(node, source_bytes, module, entity_name)
            )

            record = EntityRecord(
                name=entity_name,
                kind=kind,
                module=module,
                language="swift",
                file_path=file_path,
                start_line=start_line,
                end_line=end_line,
                signature=_signature(code),
                code=code,
                stable_id=stable_id,
                docstring=None,
                extended_type=extended_type,
                members=members,
            )
            records.append(record)
        for record in records:
            relationships.extend(self._derive_relationships(record))
        return ParsedSource(entities=records, relationships=relationships)

    def _iter_nodes(self, node):
        stack = [node]
        while stack:
            current = stack.pop()
            yield current
            stack.extend(current.children)

    def _extract_name(self, node, source_bytes: bytes) -> str | None:
        target = node.child_by_field_name("name") or node.child_by_field_name("type")
        if target:
            return source_bytes[target.start_byte : target.end_byte].decode("utf-8")
        # fallback: scan direct children
        for child in node.children:
            if child.type in {"identifier", "type_identifier", "simple_identifier"}:
                return source_bytes[child.start_byte : child.end_byte].decode("utf-8")
        return None

    def _extract_members(
        self,
        entity_node,
        source_bytes: bytes,
        module: str,
        entity_name: str,
    ) -> Iterable[MemberRecord]:
        for child in self._iter_nodes(entity_node):
            if child == entity_node:
                continue
            if child.type not in MEMBER_NODE_TYPES:
                continue
            name = self._extract_member_name(child, source_bytes)
            if not name:
                continue
            code = source_bytes[child.start_byte : child.end_byte].decode("utf-8")
            start_line = child.start_point[0] + 1
            end_line = child.end_point[0] + 1
            yield MemberRecord(
                name=name,
                kind=child.type.replace("_declaration", ""),
                signature=_signature(code),
                code=code,
                start_line=start_line,
                end_line=end_line,
            )

    def _extract_member_name(self, node, source_bytes: bytes) -> str | None:
        target = node.child_by_field_name("name")
        if target:
            return source_bytes[target.start_byte : target.end_byte].decode("utf-8")
        for child in node.children:
            if child.type in {"identifier", "simple_identifier", "type_identifier"}:
                return source_bytes[child.start_byte : child.end_byte].decode("utf-8")
        # fallback to signature text
        code = source_bytes[node.start_byte : node.end_byte].decode("utf-8")
        first_line = code.strip().splitlines()[0]
        tokens = first_line.split()
        return tokens[1] if len(tokens) > 1 else first_line

    def _derive_relationships(self, record: EntityRecord) -> List[RelationshipRecord]:
        rels: List[RelationshipRecord] = []
        rels.extend(self._relationships_from_properties(record))
        rels.extend(self._relationships_from_instantiations(record))
        return rels

    def _relationships_from_properties(
        self, record: EntityRecord
    ) -> List[RelationshipRecord]:
        rels: List[RelationshipRecord] = []
        for member in record.members:
            if member.kind not in {"variable", "property", "constant"}:
                continue
            match = PROPERTY_DECL_RE.search(member.code)
            if not match:
                continue
            target_type = self._normalize_type(match.group("type"))
            if not target_type or not target_type[0].isupper():
                continue
            is_weak = bool(match.group("prefix"))
            edge_type = "weakReference" if is_weak else "strongReference"
            metadata = {
                "member": member.name,
                "storage": "property",
                "accessor": member.kind,
                "strength": "weak" if is_weak else "strong",
            }
            rels.append(
                RelationshipRecord(
                    source_stable_id=record.stable_id,
                    target_name=target_type,
                    target_module=record.module,
                    edge_type=edge_type,
                    metadata=metadata,
                )
            )
        return rels

    def _relationships_from_instantiations(
        self, record: EntityRecord
    ) -> List[RelationshipRecord]:
        rels: List[RelationshipRecord] = []
        for member in record.members:
            if member.kind not in {"function", "initializer"}:
                continue
            created = self._find_created_types(member.code)
            for type_name in created:
                rels.append(
                    RelationshipRecord(
                        source_stable_id=record.stable_id,
                        target_name=type_name,
                        target_module=record.module,
                        edge_type="creates",
                        metadata={"member": member.name},
                    )
                )
        return rels

    def _find_created_types(self, code: str) -> Set[str]:
        types: Set[str] = set()
        for match in CREATE_EXPR_RE.finditer(code):
            type_name = match.group("type")
            if type_name and type_name[0].isupper():
                types.add(type_name)
        return types

    def _normalize_type(self, raw: str) -> str:
        candidate = raw.strip()
        candidate = candidate.replace("?", "").replace("!", "")
        candidate = candidate.split("<", 1)[0]
        candidate = candidate.replace("any ", "")
        candidate = candidate.strip()
        return candidate


def _signature(code: str) -> str:
    return code.strip().splitlines()[0][:240] if code.strip() else ""


def _derive_kind(code: str, node_type: str) -> str:
    stripped = code.lstrip()
    keyword = stripped.split(maxsplit=1)[0] if stripped else ""
    if keyword in {"struct", "class", "enum", "protocol", "extension"}:
        return keyword
    if node_type == "extension_declaration":
        return "extension"
    return node_type.replace("_declaration", "")

