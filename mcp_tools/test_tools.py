"""
Test the reusable MCP tool functions against a live server.

Usage:
    python3 -m mcp_tools.test_tools                       # default: NVDA, all steps
    python3 -m mcp_tools.test_tools SPY                   # custom ticker
    python3 -m mcp_tools.test_tools NVDA --all            # include chart tools
    python3 -m mcp_tools.test_tools GOOGL --only greeks   # run a single step

Step names: quote, greeks, top_vol, top_oi, exps, ohlcv, snapshot, gex, dex, vex, tex
Full untruncated output is always written to mcp_tools/tools_output.log.
"""
import sys
import json
import asyncio
from pathlib import Path

sys.path.insert(0, __file__.rsplit("/mcp_tools/", 1)[0])

from mcp_tools.tools import MCPTools


LOG_PATH = Path(__file__).parent / "tools_output.log"


def _preview(data, max_len=500):
    """Pretty-print preview of a result (truncated for screen)."""
    if isinstance(data, dict):
        text = json.dumps(data, indent=2, default=str)
    elif isinstance(data, list):
        text = json.dumps(data[:5], indent=2, default=str)
        if len(data) > 5:
            text += f"\n  ... ({len(data)} total items)"
    else:
        text = str(data)
    if len(text) > max_len:
        text = text[:max_len] + "\n  ... (truncated — see tools_output.log for full)"
    return text


def _full(data):
    """Full untruncated serialization for the log file."""
    if isinstance(data, (dict, list)):
        return json.dumps(data, indent=2, default=str)
    return str(data)


def _emit(log_fh, header: str, data):
    """Print a truncated preview to stdout and write the full payload to the log."""
    print(header, flush=True)
    print(_preview(data))
    log_fh.write(header + "\n")
    log_fh.write(_full(data) + "\n\n")
    log_fh.flush()


def _emit_chart(log_fh, header: str, result: dict):
    """Charts return image parts — describe on screen, base64 to log."""
    print(header, flush=True)
    log_fh.write(header + "\n")
    for part in result.get("content", []):
        if part["type"] == "image":
            line = f"    Image: {part['mimeType']}, {len(part['data'])} bytes"
            print(line)
            log_fh.write(line + "\n")
            log_fh.write(f"    data (base64): {part['data']}\n")
        elif part["type"] == "text":
            print(f"    {part['text'][:200]}")
            log_fh.write(part["text"] + "\n")
    log_fh.write("\n")
    log_fh.flush()


async def main():
    args = sys.argv[1:]
    symbol = args[0] if args and not args[0].startswith("-") else "NVDA"
    run_charts = "--all" in args

    only = None
    if "--only" in args:
        i = args.index("--only")
        if i + 1 >= len(args):
            print("error: --only requires a step name", file=sys.stderr)
            sys.exit(2)
        only = args[i + 1].lower()

    print("=" * 60)
    print(f"  MCP TOOLS TEST — {symbol}")
    if only:
        print(f"  step: {only}")
    print("=" * 60)
    print(f"  full log: {LOG_PATH}")

    with open(LOG_PATH, "w") as log_fh:
        log_fh.write(f"MCP TOOLS TEST — {symbol}\n")
        log_fh.write("=" * 60 + "\n\n")

        async with MCPTools() as t:

            steps = {
                "quote":    ("[1] Stock Quote",                    lambda: t.stock_quote(symbol)),
                "greeks":   ("[2] Greek Exposures",                lambda: t.greek_exposures(symbol)),
                "top_vol":  ("[3] Top Volume Contracts (top 5)",   lambda: t.top_volume_contracts(symbol, limit=5)),
                "top_oi":   ("[4] Top OI Contracts (top 5)",       lambda: t.top_oi_contracts(symbol, limit=5)),
                "exps":     ("[5] Option Expirations",             lambda: t.option_expirations(symbol)),
                "ohlcv":    ("[6] Price OHLCV (5d @ 1d)",          lambda: t.price_ohlcv(symbol, interval="1d", period="5d")),
                "snapshot": ("[7] Ticker Snapshot",                lambda: t.ticker_snapshot(symbol)),
            }
            chart_steps = {
                "gex": ("[8] NET GEX Chart",  lambda: t.net_gex_chart(symbol)),
                "dex": ("[9] NET DEX Chart",  lambda: t.net_dex_chart(symbol)),
                "vex": ("[10] NET VEX Chart", lambda: t.net_vex_chart(symbol)),
                "tex": ("[11] NET TEX Chart", lambda: t.net_tex_chart(symbol)),
            }

            if only:
                if only in steps:
                    header, runner = steps[only]
                    print(f"\n{header} ({symbol})", flush=True)
                    _emit(log_fh, f"\n{header} ({symbol})", await runner())
                elif only in chart_steps:
                    header, runner = chart_steps[only]
                    _emit_chart(log_fh, f"\n{header} ({symbol})", await runner())
                else:
                    valid = ", ".join(list(steps.keys()) + list(chart_steps.keys()))
                    print(f"error: unknown step '{only}'. valid: {valid}", file=sys.stderr)
                    sys.exit(2)
            else:
                for name, (header, runner) in steps.items():
                    result = await runner()
                    if name == "snapshot":
                        print(f"\n{header} ({symbol})", flush=True)
                        print(f"    Keys: {list(result.keys())}")
                        print(f"    Quote preview: {_preview(result['quote'], 200)}")
                        log_fh.write(f"\n{header} ({symbol})\n")
                        log_fh.write(_full(result) + "\n\n")
                        log_fh.flush()
                    else:
                        _emit(log_fh, f"\n{header} ({symbol})", result)

                if run_charts:
                    for header, runner in chart_steps.values():
                        _emit_chart(log_fh, f"\n{header} ({symbol})", await runner())

    print("\n" + "=" * 60)
    print("  ALL TESTS PASSED")
    print(f"  full output: {LOG_PATH}")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
