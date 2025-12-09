"""MCP server for code navigation queries.

Provides IDE-like navigation tools over external indexer databases:
- go_to_definition: Find symbol definitions
- find_references: Find all symbol usages
- find_implementations: Find protocol implementations
- search_symbols: Search symbols by name
"""
from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from typing import Any, Optional

from mcp.server import NotificationOptions, Server
from mcp.server.models import InitializationOptions
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent

from ..config import Settings, load_settings
from ..db.query_service import QueryService

server = Server("graphrag-mcp")
runtime_settings: Optional[Settings] = None


def _resolve_settings(
    config: Optional[Path],
    repo: Optional[Path],
    db: Optional[Path],
) -> Settings:
    settings = load_settings(config)
    if repo:
        settings.repo_path = Path(repo).expanduser().resolve()
    if db:
        settings.db_path = Path(db).expanduser().resolve()
    return settings


def _json_text(payload: Any) -> TextContent:
    return TextContent(type="text", text=json.dumps(payload, indent=2, ensure_ascii=False))


def _get_query_service() -> QueryService:
    """Get QueryService instance with current settings."""
    if runtime_settings is None:
        raise RuntimeError("MCP server has not been initialized with settings.")
    return QueryService(runtime_settings)


@server.list_tools()
async def handle_list_tools() -> list[Tool]:
    return [
        Tool(
            name="go_to_definition",
            description="Find the definition of a symbol. Returns the file location, code snippet, inheritance info, and members.",
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {
                        "type": "string",
                        "description": "Symbol name to find (e.g., 'FeaturePresenter', 'viewDidLoad').",
                    },
                    "file": {
                        "type": "string",
                        "description": "Optional file path for context (helps disambiguate overloaded names).",
                    },
                    "line": {
                        "type": "integer",
                        "description": "Optional line number for context (finds the specific symbol at that location).",
                    },
                },
                "required": ["symbol"],
            },
        ),
        Tool(
            name="find_references",
            description="Find all usages/references of a symbol across the codebase.",
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {
                        "type": "string",
                        "description": "Symbol name to find references for.",
                    },
                    "include_definitions": {
                        "type": "boolean",
                        "description": "If true, include definition occurrences in results.",
                        "default": False,
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of references to return.",
                        "default": 50,
                        "minimum": 1,
                        "maximum": 200,
                    },
                },
                "required": ["symbol"],
            },
        ),
        Tool(
            name="find_implementations",
            description="Find all implementations of a protocol/interface. Shows which types conform to a protocol.",
            inputSchema={
                "type": "object",
                "properties": {
                    "protocol": {
                        "type": "string",
                        "description": "Protocol/interface name to find implementations for.",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of implementations to return.",
                        "default": 50,
                        "minimum": 1,
                        "maximum": 200,
                    },
                },
                "required": ["protocol"],
            },
        ),
        Tool(
            name="search_symbols",
            description="Search for symbols by name. Supports wildcards (*) for pattern matching.",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Search query. Use * for wildcards (e.g., 'Feature*', '*Presenter').",
                    },
                    "kind": {
                        "type": "string",
                        "enum": ["class", "struct", "protocol", "enum", "function", "property"],
                        "description": "Filter by symbol kind.",
                    },
                    "module": {
                        "type": "string",
                        "description": "Filter by module name.",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum results to return.",
                        "default": 25,
                        "minimum": 1,
                        "maximum": 100,
                    },
                },
                "required": ["query"],
            },
        ),
    ]


@server.call_tool()
async def handle_call_tool(
    name: str, arguments: dict[str, Any]
) -> list[TextContent]:
    service = _get_query_service()

    if name == "go_to_definition":
        symbol = arguments.get("symbol")
        if not symbol:
            raise ValueError("symbol is required")
        
        file_path = arguments.get("file")
        line = arguments.get("line")
        if line is not None:
            line = int(line)
        
        result = service.go_to_definition(symbol, file_path, line)
        
        if result is None:
            return [_json_text({"error": f"Symbol '{symbol}' not found"})]
        
        return [_json_text(result)]

    if name == "find_references":
        symbol = arguments.get("symbol")
        if not symbol:
            raise ValueError("symbol is required")
        
        include_definitions = bool(arguments.get("include_definitions", False))
        limit = int(arguments.get("limit", 50))
        
        result = service.find_references(symbol, include_definitions, limit)
        return [_json_text(result)]

    if name == "find_implementations":
        protocol = arguments.get("protocol")
        if not protocol:
            raise ValueError("protocol is required")
        
        limit = int(arguments.get("limit", 50))
        
        result = service.find_implementations(protocol, limit)
        return [_json_text(result)]

    if name == "search_symbols":
        query = arguments.get("query")
        if not query:
            raise ValueError("query is required")
        
        kind = arguments.get("kind")
        module = arguments.get("module")
        limit = int(arguments.get("limit", 25))
        
        results = service.search_symbols(query, kind, module, limit)
        return [_json_text({"count": len(results), "symbols": results})]

    raise ValueError(f"Unknown tool: {name}")


async def _main(settings: Settings) -> None:
    global runtime_settings
    runtime_settings = settings
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="graphrag-mcp",
                server_version="1.0.0",  # Major version bump for external indexer
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )


def run_server() -> None:
    parser = argparse.ArgumentParser(description="GraphRAG MCP server")
    parser.add_argument("--config", type=Path, help="Path to config.yaml")
    parser.add_argument("--repo", type=Path, help="Repository path override")
    parser.add_argument("--db", type=Path, help="Path to external indexer database")
    args = parser.parse_args()
    settings = _resolve_settings(args.config, args.repo, args.db)
    asyncio.run(_main(settings))
