"""
Test harness for Atlas MCP server integration.
Connects to the Atlas MCP endpoint, discovers available tools,
and runs a simple agent that can call them.

Usage:
    source ae_config.config
    export GOOGLE_APPLICATION_CREDENTIALS=$(pwd)/service_account.json
    python3 test_atlas_mcp.py
"""
import os
import asyncio
from google.adk.agents import Agent
from google.adk.tools.mcp_tool.mcp_toolset import McpToolset, StreamableHTTPConnectionParams
from google.adk.auth import AuthCredential, AuthCredentialTypes, OAuth2Auth
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types

# ── Load config from environment (sourced from ae_config.config) ──
ATLAS_MCP_URL = os.environ.get("ATLAS_MCP_URL", "https://atlastmcp.finmanagerai.com/mcp")
ATLAS_OAUTH_CLIENT_ID = os.environ.get("ATLAS_OAUTH_CLIENT_ID", "")
ATLAS_OAUTH_CLIENT_SECRET = os.environ.get("ATLAS_OAUTH_CLIENT_SECRET", "")
ATLAS_OAUTH_TOKEN_URL = os.environ.get("ATLAS_OAUTH_TOKEN_URL", "https://atlastmcp.finmanagerai.com/oauth/token")

PROJECT_ID = os.environ.get("PROJECT_ID", "marketresearch-agents")
LOCATION = os.environ.get("GOOGLE_CLOUD_LOCATION", "global")

os.environ["GOOGLE_CLOUD_PROJECT"] = PROJECT_ID
os.environ["GOOGLE_CLOUD_LOCATION"] = LOCATION
os.environ["GOOGLE_GENAI_USE_VERTEXAI"] = "TRUE"


def get_oauth_token() -> str:
    """Fetch an OAuth2 access token using client_credentials grant."""
    import requests
    resp = requests.post(ATLAS_OAUTH_TOKEN_URL, data={
        "grant_type": "client_credentials",
        "client_id": ATLAS_OAUTH_CLIENT_ID,
        "client_secret": ATLAS_OAUTH_CLIENT_SECRET,
    })
    resp.raise_for_status()
    token = resp.json().get("access_token")
    print(f"    Token obtained: {token[:20]}..." if token else "    ERROR: No access_token in response", flush=True)
    return token


def build_atlas_toolset() -> McpToolset:
    """Build the Atlas MCP toolset with OAuth2 Bearer token."""
    print("\n[1b] Fetching OAuth2 token...", flush=True)
    token = get_oauth_token()

    return McpToolset(
        connection_params=StreamableHTTPConnectionParams(
            url=ATLAS_MCP_URL,
            headers={"Authorization": f"Bearer {token}"},
        ),
    )


async def main():
    print("=" * 50)
    print("  ATLAS MCP SERVER TEST")
    print("=" * 50)

    # ── 1. Connect to Atlas MCP and discover tools ──
    print("\n[1] Connecting to Atlas MCP server...", flush=True)
    print(f"    URL:       {ATLAS_MCP_URL}", flush=True)
    print(f"    Client ID: {ATLAS_OAUTH_CLIENT_ID}", flush=True)
    print(f"    Token URL: {ATLAS_OAUTH_TOKEN_URL}", flush=True)

    atlas_toolset = build_atlas_toolset()
    tools = await atlas_toolset.get_tools()

    print(f"\n[2] Discovered {len(tools)} tools:", flush=True)
    for tool in tools:
        name = getattr(tool, 'name', str(tool))
        desc = getattr(tool, 'description', '')
        print(f"    - {name}: {desc[:120]}", flush=True)

    if not tools:
        print("\n    No tools discovered. Check server URL and auth credentials.")
        await atlas_toolset.close()
        return

    # ── 2. Create a test agent with the MCP tools ──
    print("\n[3] Creating test agent...", flush=True)

    test_agent = Agent(
        name="Atlas_MCP_Tester",
        model="gemini-3.1-pro-preview",
        generate_content_config=types.GenerateContentConfig(
            thinking_config=types.ThinkingConfig(thinking_budget=4096)
        ),
        tools=tools,
        instruction=(
            "You are a test agent for the Atlas MCP server. "
            "List the tools available to you and describe what each one does. "
            "Then try calling one of them with a simple test query."
        ),
    )

    # ── 3. Run the agent ──
    print("\n[4] Running test agent...\n", flush=True)

    session_service = InMemorySessionService()
    runner = Runner(
        agent=test_agent,
        app_name="atlas_mcp_test",
        session_service=session_service,
    )

    session = await session_service.create_session(
        app_name="atlas_mcp_test",
        user_id="test_user",
    )

    content = types.Content(
        role="user",
        parts=[types.Part(text="List your available tools, then run a simple test call on one of them.")]
    )

    async for event in runner.run_async(
        user_id="test_user",
        session_id=session.id,
        new_message=content,
    ):
        if event.content and event.content.parts:
            for part in event.content.parts:
                if hasattr(part, 'text') and part.text:
                    print(f"[AGENT] {part.text}", flush=True)
                elif hasattr(part, 'function_call') and part.function_call:
                    print(f"[TOOL CALL] {part.function_call.name}({dict(part.function_call.args)})", flush=True)
                elif hasattr(part, 'function_response') and part.function_response:
                    resp = part.function_response.response
                    preview = str(resp)[:300]
                    print(f"[TOOL RESPONSE] {part.function_response.name} → {preview}", flush=True)

    # ── 4. Cleanup ──
    try:
        await atlas_toolset.close()
    except RuntimeError as e:
        print(f"    (cleanup warning: {e})", flush=True)
    print("\n" + "=" * 50)
    print("  TEST COMPLETE")
    print("=" * 50)


if __name__ == "__main__":
    asyncio.run(main())
