"""
Test the reusable MCP tool functions against a live server.

Usage:
    python3 -m mcp_tools.test_tools              # default: NVDA
    python3 -m mcp_tools.test_tools SPY          # custom ticker
    python3 -m mcp_tools.test_tools NVDA --all   # include chart tools
"""
import sys
import json
import asyncio

sys.path.insert(0, __file__.rsplit("/mcp_tools/", 1)[0])

from mcp_tools.tools import MCPTools


def _preview(data, max_len=500):
    """Pretty-print preview of a result."""
    if isinstance(data, dict):
        text = json.dumps(data, indent=2, default=str)
    elif isinstance(data, list):
        text = json.dumps(data[:5], indent=2, default=str)
        if len(data) > 5:
            text += f"\n  ... ({len(data)} total items)"
    else:
        text = str(data)
    if len(text) > max_len:
        text = text[:max_len] + "\n  ... (truncated)"
    return text


async def main():
    symbol = sys.argv[1] if len(sys.argv) > 1 and not sys.argv[1].startswith("-") else "NVDA"
    run_charts = "--all" in sys.argv

    print("=" * 60)
    print(f"  MCP TOOLS TEST — {symbol}")
    print("=" * 60)

    async with MCPTools() as t:

        # 1. Stock Quote
        print(f"\n[1] Stock Quote ({symbol})", flush=True)
        quote = await t.stock_quote(symbol)
        print(_preview(quote))

        # 2. Greek Exposures
        print(f"\n[2] Greek Exposures ({symbol})", flush=True)
        greeks = await t.greek_exposures(symbol)
        print(_preview(greeks))

        # 3. Top Volume Contracts
        print(f"\n[3] Top Volume Contracts ({symbol}, top 5)", flush=True)
        top_vol = await t.top_volume_contracts(symbol, limit=5)
        print(_preview(top_vol))

        # 4. Top OI Contracts
        print(f"\n[4] Top OI Contracts ({symbol}, top 5)", flush=True)
        top_oi = await t.top_oi_contracts(symbol, limit=5)
        print(_preview(top_oi))

        # 5. Option Expirations
        print(f"\n[5] Option Expirations ({symbol})", flush=True)
        exps = await t.option_expirations(symbol)
        print(_preview(exps))

        # 6. OHLCV Price Data
        print(f"\n[6] Price OHLCV ({symbol}, 5d @ 1d)", flush=True)
        ohlcv = await t.price_ohlcv(symbol, interval="1d", period="5d")
        print(_preview(ohlcv))

        # 7. Full ticker snapshot
        print(f"\n[7] Ticker Snapshot ({symbol})", flush=True)
        snap = await t.ticker_snapshot(symbol)
        print(f"    Keys: {list(snap.keys())}")
        print(f"    Quote preview: {_preview(snap['quote'], 200)}")

        # 8. Charts (optional — returns image data)
        if run_charts:
            print(f"\n[8] NET GEX Chart ({symbol})", flush=True)
            gex = await t.net_gex_chart(symbol)
            for part in gex.get("content", []):
                if part["type"] == "image":
                    print(f"    Image: {part['mimeType']}, {len(part['data'])} bytes")
                elif part["type"] == "text":
                    print(f"    {part['text'][:200]}")

            print(f"\n[9] NET DEX Chart ({symbol})", flush=True)
            dex = await t.net_dex_chart(symbol)
            for part in dex.get("content", []):
                if part["type"] == "image":
                    print(f"    Image: {part['mimeType']}, {len(part['data'])} bytes")
                elif part["type"] == "text":
                    print(f"    {part['text'][:200]}")

    print("\n" + "=" * 60)
    print("  ALL TESTS PASSED")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
