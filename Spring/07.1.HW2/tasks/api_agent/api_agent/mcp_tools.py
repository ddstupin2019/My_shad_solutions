import json
import logging
from contextlib import AsyncExitStack
from typing import Any

import httpx
from mcp.client.session import ClientSession
from mcp.client.streamable_http import streamable_http_client

BUILTIN_MCP_ID = "builtin-arithmetic"
BUILTIN_MCP_NAME = "builtin_arithmetic"
logger = logging.getLogger(__name__)


class MCPToolError(Exception):
    pass


def builtin_mcp_config() -> dict[str, Any]:
    return {
        "id": BUILTIN_MCP_ID,
        "user_id": "*",
        "name": BUILTIN_MCP_NAME,
        "url": "builtin://arithmetic",
        "created_at": "",
    }


def _prefixed_tool_name(mcp_name: str, tool_name: str) -> str:
    safe_mcp_name = "".join(
        ch if ch.isalnum() or ch in "_-" else "_" for ch in mcp_name
    )
    safe_tool_name = "".join(
        ch if ch.isalnum() or ch in "_-" else "_" for ch in tool_name
    )
    return f"{safe_mcp_name}__{safe_tool_name}"


def _split_tool_name(name: str) -> tuple[str, str]:
    if "__" not in name:
        raise MCPToolError(f"Unknown tool: {name}")
    return tuple(name.split("__", 1))  # type: ignore[return-value]


def _builtin_tools() -> list[dict[str, Any]]:
    return [
        {
            "type": "function",
            "function": {
                "name": _prefixed_tool_name(BUILTIN_MCP_NAME, "double"),
                "description": "Multiply a number by two.",
                "parameters": {
                    "type": "object",
                    "properties": {"value": {"type": "number"}},
                    "required": ["value"],
                    "additionalProperties": False,
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": _prefixed_tool_name(BUILTIN_MCP_NAME, "divide"),
                "description": "Divide numerator by denominator.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "numerator": {"type": "number"},
                        "denominator": {"type": "number"},
                    },
                    "required": ["numerator", "denominator"],
                    "additionalProperties": False,
                },
            },
        },
    ]


def _call_builtin_tool(tool_name: str, arguments: dict[str, Any]) -> str:
    if tool_name == "double":
        return json.dumps({"result": arguments["value"] * 2})
    if tool_name == "divide":
        denominator = arguments["denominator"]
        if denominator == 0:
            raise MCPToolError("division by zero")
        return json.dumps({"result": arguments["numerator"] / denominator})
    raise MCPToolError(f"Unknown builtin tool: {tool_name}")


async def _open_mcp_session(
    config: dict[str, Any], stack: AsyncExitStack
) -> ClientSession:
    headers = {"Authorization": f"Bearer {config['token']}"}
    http_client = httpx.AsyncClient(headers=headers, timeout=30)
    await stack.enter_async_context(http_client)
    read_stream, write_stream, _ = await stack.enter_async_context(
        streamable_http_client(config["url"], http_client=http_client)
    )
    session = ClientSession(read_stream, write_stream)
    await stack.enter_async_context(session)
    await session.initialize()
    return session


def _content_to_text(content: Any) -> str:
    if hasattr(content, "text"):
        return content.text
    if hasattr(content, "model_dump"):
        return json.dumps(content.model_dump(mode="json"), ensure_ascii=False)
    return str(content)


def _tool_result_to_text(result: Any) -> str:
    if getattr(result, "structuredContent", None) is not None:
        return json.dumps(result.structuredContent, ensure_ascii=False)
    content = getattr(result, "content", None)
    if content:
        return "\n".join(_content_to_text(item) for item in content)
    return ""


async def build_openai_tools(
    mcp_configs: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    tools: list[dict[str, Any]] = []
    for config in mcp_configs:
        if config["id"] == BUILTIN_MCP_ID:
            tools.extend(_builtin_tools())
            continue

        async with AsyncExitStack() as stack:
            session = await _open_mcp_session(config, stack)
            result = await session.list_tools()
            for tool in result.tools:
                tools.append(
                    {
                        "type": "function",
                        "function": {
                            "name": _prefixed_tool_name(config["name"], tool.name),
                            "description": tool.description or "",
                            "parameters": tool.inputSchema,
                        },
                    }
                )
    return tools


async def call_mcp_tool(
    tool_call_name: str,
    arguments: dict[str, Any],
    mcp_configs: list[dict[str, Any]],
) -> str:
    mcp_name, tool_name = _split_tool_name(tool_call_name)
    for config in mcp_configs:
        if config["name"] != mcp_name:
            continue
        logger.info("MCP tool function started: %s", tool_call_name)
        logger.debug("MCP tool arguments: %s", arguments)
        if config["id"] == BUILTIN_MCP_ID:
            return _call_builtin_tool(tool_name, arguments)

        async with AsyncExitStack() as stack:
            session = await _open_mcp_session(config, stack)
            result = await session.call_tool(tool_name, arguments)
            if result.isError:
                raise MCPToolError(_tool_result_to_text(result) or "MCP tool failed")
            return _tool_result_to_text(result)

    raise MCPToolError(f"Unknown tool provider: {mcp_name}")
