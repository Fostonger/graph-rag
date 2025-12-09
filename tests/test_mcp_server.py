"""Tests for MCP server tools."""

import asyncio
import json
from pathlib import Path

import pytest

from graphrag.config import Settings
from graphrag.mcp import server as mcp_server

from conftest import create_external_indexer_db, seed_test_data


def _seed_db(tmp_path: Path) -> Path:
    """Create and seed a test database."""
    db_path = tmp_path / "test.db"
    conn = create_external_indexer_db(db_path)
    seed_test_data(conn)
    conn.close()
    return db_path


class TestGoToDefinition:
    """Tests for go_to_definition MCP tool."""

    def test_finds_class_definition(self, tmp_path: Path):
        db_path = _seed_db(tmp_path)
        settings = Settings(db_path=db_path)
        
        original = mcp_server.runtime_settings
        mcp_server.runtime_settings = settings
        try:
            response = asyncio.run(
                mcp_server.handle_call_tool("go_to_definition", {"symbol": "MyClass"})
            )
            payload = json.loads(response[0].text)

            assert payload["symbol"] == "MyClass"
            assert payload["kind"] == "class"
            assert payload["module"] == "MyModule"
            assert payload["definition"]["file"] == "Sources/MyClass.swift"
            assert payload["definition"]["line"] == 10
            assert "IMyProtocol" in payload["conformances"]
            assert "doSomething()" in payload["members"]
        finally:
            mcp_server.runtime_settings = original

    def test_returns_error_for_unknown_symbol(self, tmp_path: Path):
        db_path = _seed_db(tmp_path)
        settings = Settings(db_path=db_path)
        
        original = mcp_server.runtime_settings
        mcp_server.runtime_settings = settings
        try:
            response = asyncio.run(
                mcp_server.handle_call_tool("go_to_definition", {"symbol": "NonExistent"})
            )
            payload = json.loads(response[0].text)

            assert "error" in payload
            assert "not found" in payload["error"]
        finally:
            mcp_server.runtime_settings = original

    def test_requires_symbol_argument(self, tmp_path: Path):
        db_path = _seed_db(tmp_path)
        settings = Settings(db_path=db_path)
        
        original = mcp_server.runtime_settings
        mcp_server.runtime_settings = settings
        try:
            with pytest.raises(ValueError) as exc_info:
                asyncio.run(
                    mcp_server.handle_call_tool("go_to_definition", {})
                )
            assert "symbol is required" in str(exc_info.value)
        finally:
            mcp_server.runtime_settings = original


class TestFindReferences:
    """Tests for find_references MCP tool."""

    def test_finds_references(self, tmp_path: Path):
        db_path = _seed_db(tmp_path)
        settings = Settings(db_path=db_path)
        
        original = mcp_server.runtime_settings
        mcp_server.runtime_settings = settings
        try:
            response = asyncio.run(
                mcp_server.handle_call_tool("find_references", {"symbol": "MyClass"})
            )
            payload = json.loads(response[0].text)

            assert payload["symbol"] == "MyClass"
            assert payload["reference_count"] == 2  # Excludes definition
            assert len(payload["references"]) == 2
        finally:
            mcp_server.runtime_settings = original

    def test_includes_definitions_when_requested(self, tmp_path: Path):
        db_path = _seed_db(tmp_path)
        settings = Settings(db_path=db_path)
        
        original = mcp_server.runtime_settings
        mcp_server.runtime_settings = settings
        try:
            response = asyncio.run(
                mcp_server.handle_call_tool(
                    "find_references",
                    {"symbol": "MyClass", "include_definitions": True}
                )
            )
            payload = json.loads(response[0].text)

            assert payload["reference_count"] == 3  # Includes definition
        finally:
            mcp_server.runtime_settings = original

    def test_respects_limit(self, tmp_path: Path):
        db_path = _seed_db(tmp_path)
        settings = Settings(db_path=db_path)
        
        original = mcp_server.runtime_settings
        mcp_server.runtime_settings = settings
        try:
            response = asyncio.run(
                mcp_server.handle_call_tool(
                    "find_references",
                    {"symbol": "MyClass", "limit": 1}
                )
            )
            payload = json.loads(response[0].text)

            assert len(payload["references"]) == 1
        finally:
            mcp_server.runtime_settings = original


class TestFindImplementations:
    """Tests for find_implementations MCP tool."""

    def test_finds_implementations(self, tmp_path: Path):
        db_path = _seed_db(tmp_path)
        settings = Settings(db_path=db_path)
        
        original = mcp_server.runtime_settings
        mcp_server.runtime_settings = settings
        try:
            response = asyncio.run(
                mcp_server.handle_call_tool(
                    "find_implementations",
                    {"protocol": "IMyProtocol"}
                )
            )
            payload = json.loads(response[0].text)

            assert payload["protocol"] == "IMyProtocol"
            assert payload["implementation_count"] == 2

            impl_names = {i["name"] for i in payload["implementations"]}
            assert "MyClass" in impl_names
            assert "MockMyClass" in impl_names
        finally:
            mcp_server.runtime_settings = original

    def test_includes_implementation_details(self, tmp_path: Path):
        db_path = _seed_db(tmp_path)
        settings = Settings(db_path=db_path)
        
        original = mcp_server.runtime_settings
        mcp_server.runtime_settings = settings
        try:
            response = asyncio.run(
                mcp_server.handle_call_tool(
                    "find_implementations",
                    {"protocol": "IMyProtocol"}
                )
            )
            payload = json.loads(response[0].text)

            my_class = next(
                i for i in payload["implementations"] if i["name"] == "MyClass"
            )
            assert my_class["kind"] == "class"
            assert my_class["module"] == "MyModule"
            assert my_class["file"] == "Sources/MyClass.swift"
        finally:
            mcp_server.runtime_settings = original


class TestSearchSymbols:
    """Tests for search_symbols MCP tool."""

    def test_searches_symbols(self, tmp_path: Path):
        db_path = _seed_db(tmp_path)
        settings = Settings(db_path=db_path)
        
        original = mcp_server.runtime_settings
        mcp_server.runtime_settings = settings
        try:
            response = asyncio.run(
                mcp_server.handle_call_tool("search_symbols", {"query": "My*"})
            )
            payload = json.loads(response[0].text)

            assert payload["count"] >= 1
            names = {s["name"] for s in payload["symbols"]}
            assert "MyClass" in names
        finally:
            mcp_server.runtime_settings = original

    def test_filters_by_kind(self, tmp_path: Path):
        db_path = _seed_db(tmp_path)
        settings = Settings(db_path=db_path)
        
        original = mcp_server.runtime_settings
        mcp_server.runtime_settings = settings
        try:
            response = asyncio.run(
                mcp_server.handle_call_tool(
                    "search_symbols",
                    {"query": "*", "kind": "protocol"}
                )
            )
            payload = json.loads(response[0].text)

            for symbol in payload["symbols"]:
                assert symbol["kind"] == "protocol"
        finally:
            mcp_server.runtime_settings = original

    def test_respects_limit(self, tmp_path: Path):
        db_path = _seed_db(tmp_path)
        settings = Settings(db_path=db_path)
        
        original = mcp_server.runtime_settings
        mcp_server.runtime_settings = settings
        try:
            response = asyncio.run(
                mcp_server.handle_call_tool(
                    "search_symbols",
                    {"query": "*", "limit": 1}
                )
            )
            payload = json.loads(response[0].text)

            assert len(payload["symbols"]) == 1
        finally:
            mcp_server.runtime_settings = original


class TestToolListing:
    """Tests for tool listing."""

    def test_lists_all_tools(self, tmp_path: Path):
        tools = asyncio.run(mcp_server.handle_list_tools())
        
        tool_names = {t.name for t in tools}
        
        assert "go_to_definition" in tool_names
        assert "find_references" in tool_names
        assert "find_implementations" in tool_names
        assert "search_symbols" in tool_names
        
        # Legacy tools should be removed
        assert "find_entities" not in tool_names
        assert "get_members" not in tool_names
        assert "get_graph" not in tool_names


class TestUnknownTool:
    """Tests for unknown tool handling."""

    def test_raises_for_unknown_tool(self, tmp_path: Path):
        db_path = _seed_db(tmp_path)
        settings = Settings(db_path=db_path)
        
        original = mcp_server.runtime_settings
        mcp_server.runtime_settings = settings
        try:
            with pytest.raises(ValueError) as exc_info:
                asyncio.run(
                    mcp_server.handle_call_tool("unknown_tool", {})
                )
            assert "Unknown tool" in str(exc_info.value)
        finally:
            mcp_server.runtime_settings = original
