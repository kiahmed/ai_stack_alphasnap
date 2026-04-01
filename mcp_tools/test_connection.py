"""
Test MCP server connectivity: auth, connect, list tools.

Usage (from project root):
    python3 -m mcp_tools.test_connection
"""
import sys
import asyncio

sys.path.insert(0, __file__.rsplit("/mcp_tools/", 1)[0])

from mcp_tools.client import MCPClient


async def main():
    print("=" * 55)
    print("  MCP SERVER CONNECTION TEST")
    print("=" * 55)

    print("\n[1] Connecting (auth + handshake)...", flush=True)
    async with MCPClient() as client:
        print("    Connected.", flush=True)

        print("\n[2] Listing tools...", flush=True)
        tools = await client.list_tools()
        print(f"    Discovered {len(tools)} tools:", flush=True)
        for t in tools:
            print(f"    - {t['name']}: {t['description'][:100]}", flush=True)

        if tools:
            print(f"\n[3] Smoke-test: calling Stock-Quote for NVDA...", flush=True)
            result = await client.call_tool("Stock-Quote", {"symbol": "NVDA"})
            for part in result.get("content", []):
                if part["type"] == "text":
                    print(f"    {part['text'][:300]}", flush=True)

    print("\n" + "=" * 55)
    print("  CONNECTION TEST PASSED")
    print("=" * 55)


if __name__ == "__main__":
    asyncio.run(main())
