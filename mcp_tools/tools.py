"""
Reusable MCP tool functions for ticker data.

All functions take a ticker symbol and return parsed results.
The underlying MCP server is configured in mcp/mcp.config — swap it without
touching this code.

Usage:
    from mcp.tools import MCPTools

    async with MCPTools() as t:
        quote = await t.stock_quote("NVDA")
        gex   = await t.greek_exposures("NVDA")
        top_v = await t.top_volume_contracts("SPY")
        top_oi = await t.top_oi_contracts("SPY")
"""
import json
from .client import MCPClient, CONFIG_PATH


def _parse_text(result: dict):
    """Extract and JSON-parse the first text part from a call_tool result."""
    for part in result.get("content", []):
        if part.get("type") == "text":
            text = part["text"]
            try:
                return json.loads(text)
            except (json.JSONDecodeError, TypeError):
                return text
    return result


class MCPTools:
    """High-level ticker data functions backed by MCP tool calls."""

    def __init__(self, config_path: str = CONFIG_PATH):
        self._client = MCPClient(config_path)

    async def __aenter__(self):
        await self._client.__aenter__()
        return self

    async def __aexit__(self, *exc):
        await self._client.__aexit__(*exc)

    # ── Raw call passthrough ─────────────────────────────────────────
    async def call(self, tool_name: str, **kwargs) -> dict:
        """Call any MCP tool by name. Returns raw result dict."""
        return await self._client.call_tool(tool_name, kwargs)

    async def list_tools(self) -> list[dict]:
        """List all available tools on the MCP server."""
        return await self._client.list_tools()

    # ── Quote & Price ────────────────────────────────────────────────
    async def stock_quote(self, symbol: str):
        """Real-time quote: price, change, open, high, low, volume."""
        result = await self._client.call_tool("Stock-Quote", {"symbol": symbol})
        return _parse_text(result)

    async def price_ohlcv(self, symbol: str, interval: str = "1d",
                          period: str = "3mo", start: str = None, end: str = None):
        """OHLCV price data. interval: 1d/1h/5m  period: 3mo/1y/5d."""
        args = {"symbol": symbol, "interval": interval, "period": period}
        if start:
            args["start"] = start
        if end:
            args["end"] = end
        result = await self._client.call_tool("Price-Data-OHLCV", args)
        return _parse_text(result)

    # ── Greek Exposures (data) ───────────────────────────────────────
    async def greek_exposures(self, symbol: str, num_expirations: int = 5):
        """Gamma, Delta, Vanna, Theta NET exposures across expirations."""
        result = await self._client.call_tool(
            "Analyze-Greek-Exposures",
            {"symbol": symbol, "num_expirations": num_expirations},
        )
        return _parse_text(result)

    # ── Top Volume & OI (data) ───────────────────────────────────────
    async def top_volume_contracts(self, symbol: str, limit: int = 20,
                                   expiration: str = None):
        """Highest-volume option contracts."""
        args = {"symbol": symbol, "sort_by": "volume", "limit": limit}
        if expiration:
            args["expiration"] = expiration
        result = await self._client.call_tool("Top-Volume-and-OI-Contracts", args)
        return _parse_text(result)

    async def top_oi_contracts(self, symbol: str, limit: int = 20,
                               expiration: str = None):
        """Highest-open-interest option contracts."""
        args = {"symbol": symbol, "sort_by": "open_interest", "limit": limit}
        if expiration:
            args["expiration"] = expiration
        result = await self._client.call_tool("Top-Volume-and-OI-Contracts", args)
        return _parse_text(result)

    # ── Options ──────────────────────────────────────────────────────
    async def option_expirations(self, symbol: str, filter: str = "next_10"):
        """Available option expiration dates."""
        result = await self._client.call_tool(
            "Option-Expiration-Dates", {"symbol": symbol, "filter": filter},
        )
        return _parse_text(result)

    async def options_chain(self, symbol: str, expiration: str):
        """Full options chain for a specific expiration date."""
        result = await self._client.call_tool(
            "Options-Chain", {"symbol": symbol, "expiration": expiration},
        )
        return _parse_text(result)

    # ── NET Exposure Charts (return image data) ──────────────────────
    async def net_gex_chart(self, symbol: str, strike_range: str = None):
        """Net Gamma Exposure bar chart (PNG image)."""
        args = {"symbol": symbol, "image_format": "png"}
        if strike_range:
            args["strike_range"] = strike_range
        return await self._client.call_tool("Net-Gamma-Exposure-Chart", args)

    async def net_dex_chart(self, symbol: str, strike_range: str = None):
        """Net Delta Exposure bar chart (PNG image)."""
        args = {"symbol": symbol, "image_format": "png"}
        if strike_range:
            args["strike_range"] = strike_range
        return await self._client.call_tool("Net-Delta-Exposure-Chart", args)

    async def net_vex_chart(self, symbol: str, strike_range: str = None):
        """Net Vanna Exposure bar chart (PNG image)."""
        args = {"symbol": symbol, "image_format": "png"}
        if strike_range:
            args["strike_range"] = strike_range
        return await self._client.call_tool("Net-Vanna-Exposure-Chart", args)

    async def net_tex_chart(self, symbol: str, strike_range: str = None):
        """Net Theta Exposure bar chart (PNG image)."""
        args = {"symbol": symbol, "image_format": "png"}
        if strike_range:
            args["strike_range"] = strike_range
        return await self._client.call_tool("Net-Theta-Exposure-Chart", args)

    # ── Convenience: all data for a ticker in one shot ───────────────
    async def ticker_snapshot(self, symbol: str) -> dict:
        """
        Pull quote + greek exposures + top volume + top OI for a ticker.
        Returns a dict with all four results — useful for DE agent grounding.
        """
        quote = await self.stock_quote(symbol)
        greeks = await self.greek_exposures(symbol)
        top_vol = await self.top_volume_contracts(symbol, limit=10)
        top_oi = await self.top_oi_contracts(symbol, limit=10)

        return {
            "symbol": symbol,
            "quote": quote,
            "greek_exposures": greeks,
            "top_volume_contracts": top_vol,
            "top_oi_contracts": top_oi,
        }
