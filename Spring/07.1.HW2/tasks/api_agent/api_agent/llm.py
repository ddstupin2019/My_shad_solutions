import json
import logging
import re
from typing import Any

from openai import APIError, APIStatusError, APITimeoutError, AsyncOpenAI

from .mcp_tools import MCPToolError, call_mcp_tool

MAX_COMPLETIONS = 10
MAX_SAME_TOOL_CALLS_IN_ROW = 2
FENCED_JSON_RE = re.compile(r"^```(?:json)?\s*(?P<body>.*?)\s*```$", re.DOTALL)
logger = logging.getLogger(__name__)


async def complete_chat(
    llm_config: dict[str, Any],
    messages: list[dict[str, str]],
) -> str:
    message = await create_completion(llm_config, messages)
    content = message.get("content")
    if content is None:
        return ""
    return str(content)


async def create_completion(
    llm_config: dict[str, Any],
    messages: list[dict[str, Any]],
    tools: list[dict[str, Any]] | None = None,
    tool_choice: str | dict[str, Any] = "auto",
) -> dict[str, Any]:
    client = AsyncOpenAI(
        api_key=llm_config["api_key"],
        base_url=llm_config["base_url"],
    )
    kwargs: dict[str, Any] = {
        "model": llm_config["model"],
        "messages": messages,
    }
    if tools:
        kwargs["tools"] = tools
        kwargs["tool_choice"] = tool_choice
    response = await client.chat.completions.create(**kwargs)
    return response.choices[0].message.model_dump(mode="json", exclude_none=True)


def _tool_call_to_message_tool_call(tool_call: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": tool_call["id"],
        "type": "function",
        "function": {
            "name": tool_call["function"]["name"],
            "arguments": tool_call["function"].get("arguments") or "{}",
        },
    }


def _assistant_tool_call_message(
    assistant_message: dict[str, Any],
    tool_calls: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "role": "assistant",
        "content": assistant_message.get("content"),
        "tool_calls": [
            _tool_call_to_message_tool_call(tool_call) for tool_call in tool_calls
        ],
    }


def _tool_result_message(tool_call_id: str, result: str) -> dict[str, Any]:
    return {
        "role": "tool",
        "tool_call_id": tool_call_id,
        "content": result,
    }


def _tool_names(tools: list[dict[str, Any]]) -> set[str]:
    return {
        str(tool["function"]["name"])
        for tool in tools
        if tool.get("type") == "function" and isinstance(tool.get("function"), dict)
    }


def _load_json_object(content: str) -> dict[str, Any] | None:
    stripped = content.strip()
    match = FENCED_JSON_RE.fullmatch(stripped)
    if match:
        stripped = match.group("body").strip()

    try:
        value = json.loads(stripped)
    except json.JSONDecodeError:
        return None
    return value if isinstance(value, dict) else None


def _is_text_tool_call(content: str, available_tool_names: set[str]) -> bool:
    payload = _load_json_object(content)
    if payload is None:
        return False

    name = payload.get("name")
    arguments = payload.get("arguments")
    return (
        isinstance(name, str)
        and name in available_tool_names
        and isinstance(arguments, dict)
    )


def _history_messages(
    history: list[dict[str, Any]],
    available_tool_names: set[str],
) -> list[dict[str, str]]:
    messages = []
    for message in history:
        role = message["role"]
        content = message["content"]
        if role not in {"user", "assistant", "system"}:
            continue
        if role == "assistant" and _is_text_tool_call(content, available_tool_names):
            continue
        messages.append({"role": role, "content": content})
    return messages


def _parse_tool_arguments(tool_call: dict[str, Any]) -> dict[str, Any]:
    tool_name = tool_call["function"]["name"]
    raw_arguments = tool_call["function"].get("arguments") or "{}"
    try:
        arguments = json.loads(raw_arguments)
    except json.JSONDecodeError as error:
        raise MCPToolError(f"Invalid tool arguments for {tool_name}") from error
    if not isinstance(arguments, dict):
        raise MCPToolError(f"Invalid tool arguments for {tool_name}")
    return arguments


async def _run_tool_call(
    tool_call: dict[str, Any],
    available_tool_names: set[str],
    mcp_configs: list[dict[str, Any]],
) -> str:
    tool_name = tool_call["function"]["name"]
    if tool_name not in available_tool_names:
        raise MCPToolError(f"Unknown tool: {tool_name}")

    arguments = _parse_tool_arguments(tool_call)
    logger.info("Tool function started: %s", tool_name)
    logger.debug("Tool function arguments: %s", arguments)
    return await call_mcp_tool(tool_name, arguments, mcp_configs)


async def run_agent_iteration(
    llm_config: dict[str, Any],
    history: list[dict[str, Any]],
    tools: list[dict[str, Any]],
    mcp_configs: list[dict[str, Any]],
) -> str:
    last_tool_name: str | None = None
    same_tool_calls = 0
    available_tool_names = _tool_names(tools)
    messages = _history_messages(history, available_tool_names)

    for _ in range(MAX_COMPLETIONS):
        assistant_message = await create_completion(
            llm_config,
            messages,
            tools,
            tool_choice="auto",
        )
        tool_calls = assistant_message.get("tool_calls") or []
        if not tool_calls:
            content = str(assistant_message.get("content") or "")
            if _is_text_tool_call(content, available_tool_names):
                raise MCPToolError("LLM returned a tool call as text")
            return content

        messages.append(_assistant_tool_call_message(assistant_message, tool_calls))

        for tool_call in tool_calls:
            tool_name = tool_call["function"]["name"]
            if tool_name == last_tool_name:
                same_tool_calls += 1
            else:
                last_tool_name = tool_name
                same_tool_calls = 1
            if same_tool_calls > MAX_SAME_TOOL_CALLS_IN_ROW:
                raise MCPToolError(
                    f"Tool {tool_name} was called more than 2 times in a row"
                )

            result = await _run_tool_call(tool_call, available_tool_names, mcp_configs)
            messages.append(_tool_result_message(tool_call["id"], result))

    raise MCPToolError("Agent loop exceeded 10 completions")


def llm_error_message(error: Exception) -> str:
    if isinstance(error, APIStatusError):
        return f"LLM request failed with status {error.status_code}"
    if isinstance(error, APITimeoutError):
        return "LLM request timed out"
    if isinstance(error, APIError):
        return "LLM request failed"
    if isinstance(error, MCPToolError):
        return str(error)
    return "Unexpected LLM error"
