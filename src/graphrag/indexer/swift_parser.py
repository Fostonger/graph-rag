from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Set, Tuple

from tree_sitter import Language, Node, Parser
from tree_sitter_swift import language as swift_language

from ..models.records import (
    EntityRecord,
    ExtensionRecord,
    MemberRecord,
    ParsedSource,
    RelationshipRecord,
)
from .base import ParserAdapter
from .dependencies import DependenciesWorker
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

# Pattern to extract visibility modifiers from declarations
VISIBILITY_KEYWORDS = {"public", "open", "internal", "fileprivate", "private"}


def _extract_visibility(code: str) -> Optional[str]:
    """Extract visibility modifier from a declaration."""
    tokens = code.strip().split()
    for token in tokens[:5]:  # Check first few tokens
        if token in VISIBILITY_KEYWORDS:
            return token
    return None  # Default is internal in Swift, but we return None for unspecified

@dataclass(slots=True)
class TypeMetadata:
    simple_name: str
    display_name: str
    kind: Optional[str] = None
    declarations: List[str] = field(default_factory=list)
    extensions: List[str] = field(default_factory=list)
    members: Dict[str, List[MemberRecord]] = field(default_factory=dict)
    conforms_to: Dict[str, str] = field(default_factory=dict)
    superclasses: Dict[str, str] = field(default_factory=dict)
    references: Dict[str, Set[str]] = field(default_factory=dict)

    def add_members(self, source_id: str, members: Iterable[MemberRecord]) -> None:
        members = list(members)
        if not members:
            return
        bucket = self.members.setdefault(source_id, [])
        bucket.extend(members)


class TypeRegistry:
    def __init__(self, simplify: Callable[[str], str]) -> None:
        self._simplify = simplify
        self._types: Dict[str, TypeMetadata] = {}

    def _key(self, name: Optional[str]) -> Optional[str]:
        if not name:
            return None
        simplified = self._simplify(name)
        return simplified or None

    def _ensure(self, name: Optional[str]) -> Optional[TypeMetadata]:
        key = self._key(name)
        if not key:
            return None
        metadata = self._types.get(key)
        if not metadata:
            display = name or key
            metadata = TypeMetadata(simple_name=key, display_name=display)
            self._types[key] = metadata
        return metadata

    def register_entity(self, record: EntityRecord) -> None:
        target_name = record.extended_type if record.kind == "extension" else record.name
        metadata = self._ensure(target_name)
        if not metadata:
            return
        if record.kind != "extension":
            if metadata.kind in (None, record.kind):
                metadata.kind = record.kind
            if record.stable_id not in metadata.declarations:
                metadata.declarations.append(record.stable_id)
        else:
            if record.stable_id not in metadata.extensions:
                metadata.extensions.append(record.stable_id)
        metadata.add_members(record.stable_id, record.members)

    def register_extension(self, record: ExtensionRecord) -> None:
        """Register an extension and its members with the extended type."""
        metadata = self._ensure(record.extended_type)
        if not metadata:
            return
        if record.stable_id not in metadata.extensions:
            metadata.extensions.append(record.stable_id)
        metadata.add_members(record.stable_id, record.members)

    def get_entity_stable_id(self, type_name: str) -> Optional[str]:
        """Get the stable_id of the primary declaration for a type."""
        key = self._key(type_name)
        if not key:
            return None
        metadata = self._types.get(key)
        if metadata and metadata.declarations:
            return metadata.declarations[0]
        return None

    def note_conformance(self, type_name: str, protocol_name: str) -> None:
        owner = self._ensure(type_name)
        proto_key = self._key(protocol_name)
        if not owner or not proto_key:
            return
        owner.conforms_to.setdefault(proto_key, protocol_name)
        self._ensure(protocol_name)

    def note_superclass(self, type_name: str, superclass_name: str) -> None:
        owner = self._ensure(type_name)
        superclass_key = self._key(superclass_name)
        if not owner or not superclass_key:
            return
        owner.superclasses.setdefault(superclass_key, superclass_name)
        self._ensure(superclass_name)

    def note_reference(self, type_name: str, source_id: str, context: str) -> None:
        metadata = self._ensure(type_name)
        if not metadata:
            return
        refs = metadata.references.setdefault(context, set())
        refs.add(source_id)

    def ensure_type(self, type_name: str) -> None:
        self._ensure(type_name)

    def get_kind(self, name: str) -> Optional[str]:
        key = self._key(name)
        if not key:
            return None
        metadata = self._types.get(key)
        return metadata.kind if metadata else None


class SwiftParser(ParserAdapter):
    language = "swift"

    def __init__(
        self,
        project_root: Path | None = None,
        dependencies: DependenciesWorker | None = None,
    ) -> None:
        self._language = Language(swift_language())
        self._parser = Parser(self._language)
        self._expr_parser = Parser(self._language)
        self._module_resolver = ModuleResolver(project_root, dependencies)
        self._type_registry = TypeRegistry(self._simplify_type_name)

    def parse(self, source: str, path: Path) -> ParsedSource:
        tree = self._parser.parse(source.encode("utf-8"))
        source_bytes = source.encode("utf-8")
        root = tree.root_node

        entity_records: List[EntityRecord] = []
        extension_records: List[ExtensionRecord] = []
        relationships: List[RelationshipRecord] = []

        # First pass: collect all entities (not extensions) to build type registry
        for node in self._iter_nodes(root):
            if node.type not in ENTITY_NODE_TYPES:
                continue
            entity_name = self._extract_name(node, source_bytes)
            if not entity_name:
                continue

            code = source_bytes[node.start_byte : node.end_byte].decode("utf-8")
            kind = _derive_kind(code, node.type)

            # Skip extensions in first pass
            if kind == "extension" or node.type == "extension_declaration":
                continue

            start_line = node.start_point[0] + 1
            end_line = node.end_point[0] + 1
            file_path = path
            module_meta = self._module_resolver.resolve_metadata(file_path)
            module = module_meta.module
            stable_id = compute_stable_id("swift", module, entity_name)
            visibility = _extract_visibility(code)

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
                extended_type=None,
                target_type=module_meta.target_type,
                visibility=visibility,
                members=members,
            )
            entity_records.append(record)

        # Register entities first so we can look up parent stable_ids
        self._register_entities(entity_records)

        # Second pass: collect extensions and link to parent entities
        for node in self._iter_nodes(root):
            if node.type not in ENTITY_NODE_TYPES:
                continue
            entity_name = self._extract_name(node, source_bytes)
            if not entity_name:
                continue

            code = source_bytes[node.start_byte : node.end_byte].decode("utf-8")
            kind = _derive_kind(code, node.type)

            # Only process extensions in second pass
            if kind != "extension" and node.type != "extension_declaration":
                continue

            extended_type = entity_name
            start_line = node.start_point[0] + 1
            end_line = node.end_point[0] + 1
            file_path = path
            module_meta = self._module_resolver.resolve_metadata(file_path)
            module = module_meta.module
            stable_name = f"{extended_type}::extension::{path}:{start_line}"
            stable_id = compute_stable_id("swift", module, stable_name)
            visibility = _extract_visibility(code)

            members = list(
                self._extract_members(node, source_bytes, module, extended_type)
            )

            # Extract conformances from extension signature
            conformances = self._parse_inherited_types(_signature(code))

            # Extract constraints from where clause if present
            constraints = self._extract_where_clause(code)

            ext_record = ExtensionRecord(
                stable_id=stable_id,
                extended_type=extended_type,
                module=module,
                language="swift",
                file_path=file_path,
                start_line=start_line,
                end_line=end_line,
                signature=_signature(code),
                code=code,
                constraints=constraints,
                visibility=visibility,
                target_type=module_meta.target_type,
                members=members,
                conformances=conformances,
            )
            extension_records.append(ext_record)
            self._type_registry.register_extension(ext_record)

        # Derive relationships from entities
        for record in entity_records:
            relationships.extend(self._derive_relationships(record))

        # Derive relationships from extensions (using parent entity's stable_id)
        for ext_record in extension_records:
            relationships.extend(self._derive_extension_relationships(ext_record))

        return ParsedSource(
            entities=entity_records,
            extensions=extension_records,
            relationships=relationships,
        )

    def _register_entities(self, entities: List[EntityRecord]) -> None:
        for record in entities:
            self._type_registry.register_entity(record)

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

    def _entity_type_name(self, record: EntityRecord) -> Optional[str]:
        if record.kind == "extension" and record.extended_type:
            return record.extended_type
        return record.name

    def _extract_where_clause(self, code: str) -> Optional[str]:
        """Extract the where clause from an extension declaration."""
        # Look for 'where' keyword in the first line (signature)
        first_line = code.strip().splitlines()[0] if code.strip() else ""
        where_idx = first_line.find(" where ")
        if where_idx == -1:
            return None
        # Extract from 'where' to the opening brace
        clause = first_line[where_idx + 7:]  # Skip ' where '
        brace_idx = clause.find("{")
        if brace_idx != -1:
            clause = clause[:brace_idx]
        return clause.strip() or None

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
        rels.extend(self._relationships_from_inheritance(record))
        return rels

    def _derive_extension_relationships(
        self, ext_record: ExtensionRecord
    ) -> List[RelationshipRecord]:
        """Derive relationships from an extension.
        
        Relationships use the parent entity's stable_id as source (if found),
        otherwise fall back to the extension's stable_id.
        """
        # Try to find the parent entity's stable_id
        parent_stable_id = self._type_registry.get_entity_stable_id(ext_record.extended_type)
        source_stable_id = parent_stable_id or ext_record.stable_id
        
        rels: List[RelationshipRecord] = []
        
        # Relationships from properties declared in extension
        for member in ext_record.members:
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
            self._type_registry.note_reference(
                target_type, source_stable_id, "property"
            )
            metadata = {
                "member": member.name,
                "storage": "property",
                "accessor": member.kind,
                "strength": "weak" if is_weak else "strong",
                "declaredVia": "extension",
            }
            rels.append(
                RelationshipRecord(
                    source_stable_id=source_stable_id,
                    target_name=target_type,
                    target_module=ext_record.module,
                    edge_type=edge_type,
                    metadata=metadata,
                )
            )
        
        # Relationships from instantiations in extension methods
        for member in ext_record.members:
            if member.kind not in {"function", "initializer"}:
                continue
            created = self._find_created_types(member.code)
            for type_name in created:
                self._type_registry.note_reference(
                    type_name, source_stable_id, "instantiation"
                )
                rels.append(
                    RelationshipRecord(
                        source_stable_id=source_stable_id,
                        target_name=type_name,
                        target_module=ext_record.module,
                        edge_type="creates",
                        metadata={"member": member.name, "declaredVia": "extension"},
                    )
                )
        
        # Conformances declared in extension
        for proto in ext_record.conformances:
            self._type_registry.note_conformance(ext_record.extended_type, proto)
            rels.append(
                RelationshipRecord(
                    source_stable_id=source_stable_id,
                    target_name=self._simplify_type_name(proto),
                    target_module=ext_record.module,
                    edge_type="conforms",
                    metadata={"declaredVia": "extension"},
                )
            )
        
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
            self._type_registry.note_reference(
                target_type, record.stable_id, "property"
            )
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
                self._type_registry.note_reference(
                    type_name, record.stable_id, "instantiation"
                )
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
        snippet = code.strip()
        if not snippet:
            return types
        source_bytes = snippet.encode("utf-8")
        tree = self._expr_parser.parse(source_bytes)
        for node in self._iter_nodes(tree.root_node):
            if node.type != "call_expression":
                continue
            target_name = self._call_target_name(node, source_bytes)
            if target_name and target_name[0].isupper():
                types.add(self._simplify_type_name(target_name))
        return types

    def _call_target_name(self, node: Node, source_bytes: bytes) -> Optional[str]:
        head = None
        for child in node.children:
            if child.type == "call_suffix":
                break
            if child.is_named:
                head = child
                break
        if head is None:
            return None
        if head.type in {"simple_identifier", "identifier", "type_identifier"}:
            return source_bytes[head.start_byte : head.end_byte].decode("utf-8")
        if head.type == "navigation_expression":
            if any(child.type == "call_expression" for child in head.children):
                return None
            text = source_bytes[head.start_byte : head.end_byte].decode("utf-8")
            segments = [segment for segment in text.split(".") if segment]
            while segments:
                candidate = segments.pop()
                if candidate and candidate[0].isupper():
                    return candidate
            return None
        return None

    def _relationships_from_inheritance(
        self, record: EntityRecord
    ) -> List[RelationshipRecord]:
        if record.kind not in {"class", "struct", "enum", "extension"}:
            return []
        type_name = self._entity_type_name(record)
        inherited = self._parse_inherited_types(record.signature)
        if not inherited:
            return []

        rels: List[RelationshipRecord] = []
        remaining = inherited
        assumed_superclass = False
        if record.kind == "class":
            superclass, remaining, assumed_superclass = self._select_superclass(
                inherited
            )
            if superclass:
                metadata: Dict[str, str] = {}
                if assumed_superclass:
                    metadata["assumed"] = "true"
                if type_name:
                    self._type_registry.note_superclass(type_name, superclass)
                rels.append(
                    RelationshipRecord(
                        source_stable_id=record.stable_id,
                        target_name=self._simplify_type_name(superclass),
                        target_module=record.module,
                        edge_type="superclass",
                        metadata=metadata,
                    )
                )

        for proto in remaining:
            metadata = {}
            if record.kind == "extension":
                metadata = {"declaredVia": "extension"}
            if type_name:
                self._type_registry.note_conformance(type_name, proto)
            rels.append(
                RelationshipRecord(
                    source_stable_id=record.stable_id,
                    target_name=self._simplify_type_name(proto),
                    target_module=record.module,
                    edge_type="conforms",
                    metadata=metadata,
                )
            )
        return rels

    def _parse_inherited_types(self, signature: str) -> List[str]:
        if ":" not in signature:
            return []
        clause = signature.split(":", 1)[1]
        for stopper in ("{", "where"):
            idx = clause.find(stopper)
            if idx != -1:
                clause = clause[:idx]
        clause = clause.strip()
        if not clause:
            return []
        parts: List[str] = []
        current: List[str] = []
        depth = 0
        for ch in clause:
            if ch == "<":
                depth += 1
                continue
            if ch == ">":
                if depth > 0:
                    depth -= 1
                continue
            if depth > 0:
                continue
            if ch == "," and depth == 0:
                token = "".join(current).strip()
                if token:
                    cleaned = self._clean_inherited_token(token)
                    if cleaned:
                        parts.append(cleaned)
                current = []
                continue
            current.append(ch)
        tail = "".join(current).strip()
        if tail:
            cleaned_tail = self._clean_inherited_token(tail)
            if cleaned_tail:
                parts.append(cleaned_tail)
        return parts

    def _clean_inherited_token(self, token: str) -> Optional[str]:
        candidate = token.strip()
        if not candidate:
            return None
        candidate = candidate.replace("?", "").replace("!", "")
        candidate = candidate.replace("any ", "")
        while candidate.endswith("{"):
            candidate = candidate[:-1].rstrip()
        return candidate or None

    def _select_superclass(
        self, inherited: List[str]
    ) -> Tuple[Optional[str], List[str], bool]:
        remaining = list(inherited)
        for idx, type_name in enumerate(inherited):
            classification = self._classify_inherited_type(type_name)
            if classification == "class":
                remaining.pop(idx)
                return type_name, remaining, False
        if inherited:
            first_classification = self._classify_inherited_type(inherited[0])
            if first_classification is None:
                return inherited[0], inherited[1:], True
        return None, inherited, False

    def _classify_inherited_type(self, type_name: str) -> Optional[str]:
        simplified = self._simplify_type_name(type_name)
        kind = self._type_registry.get_kind(simplified)
        if kind:
            return kind
        if simplified in {"AnyObject", "Sendable"}:
            return "protocol"
        return None

    def _simplify_type_name(self, name: str) -> str:
        simple = name.split("<", 1)[0]
        if "." in simple:
            simple = simple.split(".")[-1]
        return simple.strip()

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

