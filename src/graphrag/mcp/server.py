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
    feature_db: Optional[Path],
) -> Settings:
    settings = load_settings(config)
    if repo:
        settings.repo_path = Path(repo).expanduser().resolve()
    if db:
        settings.db_path = Path(db).expanduser().resolve()
    if feature_db:
        settings.feature_db_path = Path(feature_db).expanduser().resolve()
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
            name="find_entities",
            description="Look up entities by name/module/path and return metadata (optionally code).",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Entity name or fragment to search for.",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max number of entities to return.",
                        "default": 25,
                        "minimum": 1,
                        "maximum": 200,
                    },
                    "include_code": {
                        "type": "boolean",
                        "description": "If true, include entity code snippets.",
                        "default": False,
                    },
                },
                "required": ["query"],
            },
        ),
        Tool(
            name="get_members",
            description="Fetch member details for specific entities (functions/properties/etc).",
            inputSchema={
                "type": "object",
                "properties": {
                    "entities": {
                        "type": "array",
                        "items": {"type": "string"},
                        "minItems": 1,
                        "description": "List of entity names.",
                    },
                    "members": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Subset of member names to filter (optional).",
                    },
                    "include_code": {
                        "type": "boolean",
                        "description": "Include member implementation code.",
                        "default": True,
                    },
                },
                "required": ["entities"],
            },
        ),
        Tool(
            name="get_graph",
            description="Return upstream/downstream dependency graph for a Swift entity.",
            inputSchema={
                "type": "object",
                "properties": {
                    "entity": {
                        "type": "string",
                        "description": "Entity name serving as the graph root.",
                    },
                    "stop_at": {
                        "type": "string",
                        "description": "Ancestor/superclass name that caps upstream traversal.",
                    },
                    "direction": {
                        "type": "string",
                        "enum": ["upstream", "downstream", "both"],
                        "default": "both",
                        "description": "Traversal direction relative to the root.",
                    },
                    "include_sibling_subgraphs": {
                        "type": "boolean",
                        "default": False,
                        "description": "Deprecated. Kept for compatibility.",
                    },
                    "max_hops": {
                        "type": "integer",
                        "minimum": 1,
                        "description": "Maximum traversal depth from the root entity.",
                    },
                    "targetType": {
                        "type": "string",
                        "enum": ["app", "test", "all"],
                        "default": "app",
                        "description": "Filter nodes by target type.",
                    },
                    "stop_at_module_boundary": {
                        "type": "boolean",
                        "default": False,
                        "description": "If true, entities from different modules become leaf nodes (not traversed further).",
                    },
                },
                "required": ["entity"],
            },
        ),
    ]


@server.call_tool()
async def handle_call_tool(
    name: str, arguments: dict[str, Any]
) -> list[TextContent]:
    service = _get_query_service()

    if name == "find_entities":
        query = arguments.get("query", "")
        limit = int(arguments.get("limit", 25))
        include_code = bool(arguments.get("include_code", False))

        rows = service.find_entities(query, limit=limit, include_code=include_code)
        return [_json_text({"tool": name, "count": len(rows), "entities": rows})]

    if name == "get_members":
        entities = arguments.get("entities") or []
        members = arguments.get("members") or []
        include_code = bool(arguments.get("include_code", True))

        if not entities:
            raise ValueError("entities array cannot be empty")

        rows = service.get_members(
            entity_names=entities,
            member_filters=members,
            include_code=include_code,
        )
        return [_json_text({"tool": name, "count": len(rows), "members": rows})]

    if name == "get_graph":
        entity = arguments.get("entity")
        if not entity:
            raise ValueError("entity is required")
        stop_at = arguments.get("stop_at")
        direction = arguments.get("direction", "both")
        include_siblings = bool(arguments.get("include_sibling_subgraphs", False))
        max_hops_arg = arguments.get("max_hops")
        if max_hops_arg is None:
            max_hops = runtime_settings.graph.max_hops if runtime_settings else None
        else:
            max_hops = int(max_hops_arg)
        target_type = (arguments.get("targetType") or "app").lower()
        stop_at_module_boundary = bool(arguments.get("stop_at_module_boundary", False))

        payload = service.get_graph(
            entity_name=entity,
            stop_name=stop_at,
            direction=direction,
            include_sibling_subgraphs=include_siblings,
            max_hops=max_hops,
            target_type=target_type,
            stop_at_module_boundary=stop_at_module_boundary,
        )
        return [_json_text({"tool": name, "graph": payload})]

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
                server_version="0.1.0",
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
    parser.add_argument("--db", type=Path, help="Database path override")
    parser.add_argument("--feature-db", type=Path, help="Feature database override")
    args = parser.parse_args()
    settings = _resolve_settings(args.config, args.repo, args.db, args.feature_db)
    asyncio.run(_main(settings))

