"""Tests for SCIP-based code navigation queries."""

import sqlite3
from pathlib import Path

import pytest

from graphrag.db.schema import SymbolRole
from graphrag.db.scip_queries import (
    find_implementations,
    find_references,
    go_to_definition,
    search_symbols,
    _extract_name_from_symbol_id,
    _extract_module_from_symbol_id,
)

from conftest import create_external_indexer_db, seed_test_data


class TestSymbolIdParsing:
    """Tests for symbol ID parsing utilities."""

    def test_extract_name_from_class_symbol(self):
        assert _extract_name_from_symbol_id("swift MyModule MyClass#") == "MyClass"

    def test_extract_name_from_method_symbol(self):
        assert _extract_name_from_symbol_id("swift MyModule MyClass#doSomething().") == "doSomething"

    def test_extract_name_from_local_symbol(self):
        assert _extract_name_from_symbol_id("local 42") == "local_42"

    def test_extract_module_from_symbol(self):
        assert _extract_module_from_symbol_id("swift MyModule MyClass#") == "MyModule"

    def test_extract_module_from_local_returns_none(self):
        assert _extract_module_from_symbol_id("local 42") is None


class TestGoToDefinition:
    """Tests for go_to_definition query."""

    def test_finds_class_definition(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        conn = create_external_indexer_db(db_path)
        seed_test_data(conn)

        result = go_to_definition(conn, "MyClass")

        assert result is not None
        assert result["symbol"] == "MyClass"
        assert result["kind"] == "class"
        assert result["module"] == "MyModule"
        assert result["definition"]["file"] == "Sources/MyClass.swift"
        assert result["definition"]["line"] == 10
        assert "class MyClass" in result["definition"]["snippet"]
        conn.close()

    def test_includes_conformances(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        conn = create_external_indexer_db(db_path)
        seed_test_data(conn)

        result = go_to_definition(conn, "MyClass")

        assert "conformances" in result
        assert "IMyProtocol" in result["conformances"]
        conn.close()

    def test_includes_members(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        conn = create_external_indexer_db(db_path)
        seed_test_data(conn)

        result = go_to_definition(conn, "MyClass")

        assert "members" in result
        assert "doSomething()" in result["members"]
        conn.close()

    def test_includes_documentation(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        conn = create_external_indexer_db(db_path)
        seed_test_data(conn)

        result = go_to_definition(conn, "MyClass")

        assert result["documentation"] == "A sample class for testing."
        conn.close()

    def test_finds_protocol_definition(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        conn = create_external_indexer_db(db_path)
        seed_test_data(conn)

        result = go_to_definition(conn, "IMyProtocol")

        assert result is not None
        assert result["kind"] == "protocol"
        assert result["definition"]["file"] == "Sources/IMyProtocol.swift"
        conn.close()

    def test_returns_none_for_unknown_symbol(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        conn = create_external_indexer_db(db_path)
        seed_test_data(conn)

        result = go_to_definition(conn, "NonExistent")

        assert result is None
        conn.close()

    def test_finds_by_file_and_line_context(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        conn = create_external_indexer_db(db_path)
        seed_test_data(conn)

        # Should find the MyClass reference at line 15 in Assembly.swift
        result = go_to_definition(conn, "anything", file_path="Sources/Assembly.swift", line=15)

        assert result is not None
        assert result["symbol"] == "MyClass"
        conn.close()


class TestFindReferences:
    """Tests for find_references query."""

    def test_finds_all_references(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        conn = create_external_indexer_db(db_path)
        seed_test_data(conn)

        result = find_references(conn, "MyClass")

        assert result["symbol"] == "MyClass"
        assert result["reference_count"] == 2  # Excludes definition
        assert len(result["references"]) == 2
        conn.close()

    def test_includes_definitions_when_requested(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        conn = create_external_indexer_db(db_path)
        seed_test_data(conn)

        result = find_references(conn, "MyClass", include_definitions=True)

        assert result["reference_count"] == 3  # Includes definition
        conn.close()

    def test_includes_context_info(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        conn = create_external_indexer_db(db_path)
        seed_test_data(conn)

        result = find_references(conn, "MyClass")

        # At least one reference should have context
        refs_with_context = [r for r in result["references"] if "context" in r]
        assert len(refs_with_context) > 0
        conn.close()

    def test_groups_by_module(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        conn = create_external_indexer_db(db_path)
        seed_test_data(conn)

        result = find_references(conn, "MyClass")

        assert "grouped_by_module" in result
        assert "MyModule" in result["grouped_by_module"]
        conn.close()

    def test_respects_limit(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        conn = create_external_indexer_db(db_path)
        seed_test_data(conn)

        result = find_references(conn, "MyClass", limit=1)

        assert len(result["references"]) == 1
        assert result["reference_count"] == 2  # Total count is still correct
        conn.close()

    def test_returns_empty_for_unknown_symbol(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        conn = create_external_indexer_db(db_path)
        seed_test_data(conn)

        result = find_references(conn, "NonExistent")

        assert result["reference_count"] == 0
        assert len(result["references"]) == 0
        conn.close()


class TestFindImplementations:
    """Tests for find_implementations query."""

    def test_finds_protocol_implementations(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        conn = create_external_indexer_db(db_path)
        seed_test_data(conn)

        result = find_implementations(conn, "IMyProtocol")

        assert result["protocol"] == "IMyProtocol"
        assert result["implementation_count"] == 2

        impl_names = {i["name"] for i in result["implementations"]}
        assert "MyClass" in impl_names
        assert "MockMyClass" in impl_names
        conn.close()

    def test_includes_implementation_details(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        conn = create_external_indexer_db(db_path)
        seed_test_data(conn)

        result = find_implementations(conn, "IMyProtocol")

        my_class = next(i for i in result["implementations"] if i["name"] == "MyClass")
        assert my_class["kind"] == "class"
        assert my_class["module"] == "MyModule"
        assert my_class["file"] == "Sources/MyClass.swift"
        conn.close()

    def test_returns_empty_for_unknown_protocol(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        conn = create_external_indexer_db(db_path)
        seed_test_data(conn)

        result = find_implementations(conn, "NonExistent")

        assert result["implementation_count"] == 0
        conn.close()


class TestSearchSymbols:
    """Tests for search_symbols query."""

    def test_finds_exact_match(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        conn = create_external_indexer_db(db_path)
        seed_test_data(conn)

        results = search_symbols(conn, "MyClass")

        assert len(results) >= 1
        assert results[0]["name"] == "MyClass"
        conn.close()

    def test_finds_partial_match(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        conn = create_external_indexer_db(db_path)
        seed_test_data(conn)

        results = search_symbols(conn, "Mock")

        names = {r["name"] for r in results}
        assert "MockMyClass" in names
        conn.close()

    def test_wildcard_prefix(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        conn = create_external_indexer_db(db_path)
        seed_test_data(conn)

        results = search_symbols(conn, "*Protocol")

        names = {r["name"] for r in results}
        assert "IMyProtocol" in names
        conn.close()

    def test_wildcard_suffix(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        conn = create_external_indexer_db(db_path)
        seed_test_data(conn)

        results = search_symbols(conn, "My*")

        names = {r["name"] for r in results}
        assert "MyClass" in names
        conn.close()

    def test_filters_by_kind(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        conn = create_external_indexer_db(db_path)
        seed_test_data(conn)

        results = search_symbols(conn, "*", kind="protocol")

        assert all(r["kind"] == "protocol" for r in results)
        conn.close()

    def test_respects_limit(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        conn = create_external_indexer_db(db_path)
        seed_test_data(conn)

        results = search_symbols(conn, "*", limit=1)

        assert len(results) == 1
        conn.close()
