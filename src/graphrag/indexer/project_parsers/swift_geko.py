from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterator, List, Optional

from tree_sitter import Language, Node, Parser
from tree_sitter_swift import language as swift_language


@dataclass(slots=True)
class TestTargetMetadata:
    tests_type: str
    sources: List[str] = field(default_factory=list)
    dependencies: List[str] = field(default_factory=list)


@dataclass(slots=True)
class TargetMetadata:
    name: str
    target_type: str
    sources: List[str] = field(default_factory=list)
    tests: List[TestTargetMetadata] = field(default_factory=list)
    product: Optional[str] = None


@dataclass(slots=True)
class ProjectMetadata:
    name: str
    targets: List[TargetMetadata] = field(default_factory=list)


class SwiftGekoProjectParser:
    """Parse custom Geko/Tuist Project.swift files into structured metadata."""

    def __init__(self) -> None:
        self._language = Language(swift_language())
        self._parser = Parser(self._language)
        self._source_bytes: bytes = b""

    def parse(self, path: Path) -> ProjectMetadata:
        try:
            source_text = path.read_text(encoding="utf-8")
        except OSError as exc:
            raise ValueError(f"Unable to read {path}") from exc
        self._source_bytes = source_text.encode("utf-8")
        tree = self._parser.parse(self._source_bytes)

        module_call = self._find_first_call(tree.root_node, ".Module")
        if module_call is None:
            raise ValueError(f"Unable to locate module declaration inside {path}")

        args = self._collect_arguments(module_call)
        project_name = self._parse_string(args.get("name")) or path.parent.name
        targets_node = args.get("targets")
        targets = self._parse_targets_array(targets_node)
        return ProjectMetadata(name=project_name, targets=targets)

    # --- parsing helpers -------------------------------------------------
    def _parse_targets_array(self, node: Optional[Node]) -> List[TargetMetadata]:
        if node is None or node.type != "array_literal":
            return []
        targets: List[TargetMetadata] = []
        for child in node.named_children:
            if child.type != "call_expression":
                continue
            if not self._call_name_endswith(child, ".Target"):
                continue
            target = self._parse_target_call(child)
            if target:
                targets.append(target)
        return targets

    def _parse_target_call(self, node: Node) -> Optional[TargetMetadata]:
        args = self._collect_arguments(node)
        name = self._parse_string(args.get("name"))
        if not name:
            return None
        sources = self._parse_string_list(args.get("sources"))
        tests = self._parse_tests_array(args.get("tests"))
        product = self._parse_enum_value(args.get("product"))
        target_type = self._classify_target(name, product)
        return TargetMetadata(
            name=name,
            target_type=target_type,
            sources=self._normalize_sources(sources),
            tests=tests,
            product=product,
        )

    def _parse_tests_array(self, node: Optional[Node]) -> List[TestTargetMetadata]:
        if node is None or node.type != "array_literal":
            return []
        tests: List[TestTargetMetadata] = []
        for child in node.named_children:
            if child.type != "call_expression":
                continue
            if not self._call_name_endswith(child, ".Tests"):
                continue
            args = self._collect_arguments(child)
            tests_type = self._parse_enum_value(args.get("testsType")) or "unknown"
            sources = self._parse_string_list(args.get("sources"))
            dependencies = self._parse_dependency_names(args.get("dependencies"))
            tests.append(
                TestTargetMetadata(
                    tests_type=tests_type,
                    sources=sources,
                    dependencies=dependencies,
                )
            )
        return tests

    def _parse_dependency_names(self, node: Optional[Node]) -> List[str]:
        if node is None or node.type != "array_literal":
            return []
        deps: List[str] = []
        for child in node.named_children:
            if child.type != "call_expression":
                continue
            call_name = self._call_name(child)
            qualifier = call_name.split(".")[-1].lstrip(".")
            args = self._collect_arguments(child)
            dep_name = self._parse_string(args.get("name"))
            if dep_name:
                deps.append(f"{qualifier}:{dep_name}")
        return deps

    def _collect_arguments(self, node: Node) -> Dict[str, Node]:
        args: Dict[str, Node] = {}
        suffix = next((child for child in node.children if child.type == "call_suffix"), None)
        if suffix is None:
            return args
        value_args = next(
            (child for child in suffix.children if child.type == "value_arguments"), None
        )
        if value_args is None:
            return args
        for child in value_args.children:
            if child.type != "value_argument":
                continue
            label_node = next(
                (sub for sub in child.children if sub.type == "value_argument_label"),
                None,
            )
            value_node = next(
                (
                    sub
                    for sub in child.children
                    if sub.is_named and sub.type not in {"value_argument_label"}
                ),
                None,
            )
            if label_node is None or value_node is None:
                continue
            label = self._node_text(label_node).strip()
            args[label.rstrip(":")] = value_node
        return args

    def _parse_string_list(self, node: Optional[Node]) -> List[str]:
        if node is None or node.type != "array_literal":
            return []
        values: List[str] = []
        for child in node.named_children:
            parsed = self._parse_string(child)
            if parsed:
                values.append(parsed)
        return values

    def _parse_string(self, node: Optional[Node]) -> Optional[str]:
        if node is None:
            return None
        text = self._node_text(node).strip()
        if not text:
            return None
        if node.type == "line_string_literal" and len(text) >= 2:
            inner = text[1:-1]
            return bytes(inner, "utf-8").decode("unicode_escape")
        if node.type in {"simple_identifier", "identifier", "type_identifier"}:
            return text
        return text.strip('"') or None

    def _parse_enum_value(self, node: Optional[Node]) -> Optional[str]:
        if node is None:
            return None
        text = self._node_text(node).strip()
        return text.lstrip(".") or None

    def _call_name(self, node: Node) -> str:
        for child in node.children:
            if child.type == "call_suffix":
                break
            if child.is_named:
                return self._node_text(child)
        return ""

    def _call_name_endswith(self, node: Node, suffix: str) -> bool:
        return self._call_name(node).endswith(suffix)

    def _node_text(self, node: Node) -> str:
        return self._source_bytes[node.start_byte : node.end_byte].decode("utf-8")

    def _find_first_call(self, node: Node, suffix: str) -> Optional[Node]:
        for current in self._iterate(node):
            if current.type == "call_expression" and self._call_name_endswith(
                current, suffix
            ):
                return current
        return None

    def _iterate(self, node: Node) -> Iterator[Node]:
        stack = [node]
        while stack:
            current = stack.pop()
            yield current
            stack.extend(reversed(current.children))

    def _classify_target(self, name: str, product: Optional[str]) -> str:
        lowered = name.lower()
        if product and "test" in product.lower():
            return "test"
        if lowered.endswith("mock"):
            return "mock"
        if lowered.endswith("io") or lowered.endswith("interface") or lowered.endswith(
            "interfaces"
        ):
            return "interface"
        if lowered.endswith("tests"):
            return "test"
        return "app"

    def _normalize_sources(self, sources: List[str]) -> List[str]:
        normalized: List[str] = []
        for source in sources:
            cleaned = source.strip().replace("\\", "/")
            if cleaned:
                normalized.append(cleaned)
        return normalized

