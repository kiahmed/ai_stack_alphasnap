"""
Swappable MCP client layer.

Reads mcp.config, handles auth, and exposes a single call_tool() method.
To point at a different MCP server, edit mcp/mcp.config — no Python changes needed.
"""
import os
import requests
from contextlib import asynccontextmanager
from mcp.client.streamable_http import streamablehttp_client
from mcp import ClientSession

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "mcp.config")


def _load_config(path: str = CONFIG_PATH) -> dict:
    cfg = {}
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                k, _, v = line.partition("=")
                cfg[k.strip()] = v.strip().strip('"').strip("'")
    return cfg


def _get_auth_headers(cfg: dict) -> dict:
    auth_type = cfg.get("MCP_AUTH_TYPE", "oauth2")

    if auth_type == "none":
        return {}

    if auth_type == "bearer":
        token = cfg.get("MCP_STATIC_TOKEN", "")
        if not token:
            raise ValueError("MCP_AUTH_TYPE=bearer but MCP_STATIC_TOKEN is empty")
        return {"Authorization": f"Bearer {token}"}

    # oauth2 — client_credentials grant
    token_url = cfg["MCP_OAUTH_TOKEN_URL"]
    client_id = cfg["MCP_OAUTH_CLIENT_ID"]
    client_secret = cfg["MCP_OAUTH_CLIENT_SECRET"]

    resp = requests.post(
        token_url,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        },
        timeout=15,
    )
    resp.raise_for_status()
    token = resp.json()["access_token"]
    return {"Authorization": f"Bearer {token}"}


class MCPClient:
    """Async context-manager that holds an MCP session open for tool calls."""

    def __init__(self, config_path: str = CONFIG_PATH):
        self._cfg = _load_config(config_path)
        self._session: ClientSession | None = None
        self._cm_stack = None

    async def __aenter__(self):
        headers = _get_auth_headers(self._cfg)
        url = self._cfg["MCP_SERVER_URL"]

        # streamablehttp_client is an async context-manager yielding (read, write, session_url)
        self._transport_cm = streamablehttp_client(url, headers=headers)
        read, write, _ = await self._transport_cm.__aenter__()

        self._session_cm = ClientSession(read, write)
        self._session = await self._session_cm.__aenter__()
        await self._session.initialize()
        return self

    async def __aexit__(self, *exc):
        if self._session_cm:
            await self._session_cm.__aexit__(*exc)
        if self._transport_cm:
            await self._transport_cm.__aexit__(*exc)

    async def list_tools(self) -> list[dict]:
        """Return list of {name, description, inputSchema} for all server tools."""
        result = await self._session.list_tools()
        return [
            {"name": t.name, "description": t.description, "inputSchema": t.inputSchema}
            for t in result.tools
        ]

    async def call_tool(self, tool_name: str, arguments: dict) -> dict:
        """Call a single MCP tool by name and return the result content."""
        result = await self._session.call_tool(tool_name, arguments)
        # result.content is a list of TextContent / ImageContent / etc.
        parts = []
        for item in result.content:
            if item.type == "text":
                parts.append({"type": "text", "text": item.text})
            elif item.type == "image":
                parts.append({"type": "image", "mimeType": item.mimeType, "data": item.data})
            else:
                parts.append({"type": item.type, "data": str(item)})
        return {"tool": tool_name, "isError": result.isError, "content": parts}
