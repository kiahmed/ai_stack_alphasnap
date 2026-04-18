"""Run the Robotics scout alone and dump its raw output_key value.

Purpose: verify whether the scout already emits source URLs in its text output
(the text that flows into the Data Engineer's context via `{scout_name}_findings`).
If it does → dedup is the loss point. If it doesn't → scout prompt needs fixing first.

Run:
    python3 dev-utils/inspect_scout_output.py
"""
import os
import sys
import asyncio

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import market_team as mt
from vertexai import agent_engines


async def main():
    if not mt.check_auth():
        return

    pipelines = mt.build_sector_pipelines()
    robotics_pipeline = next((p for p in pipelines if "Robotics" in p.name), None)
    if not robotics_pipeline:
        print("ERROR: no Robotics pipeline")
        return

    # Extract just the scout (first sub-agent of the sequential pipeline)
    scout_agent = robotics_pipeline.sub_agents[0]
    print(f"Running scout-only: {scout_agent.name}")
    print(f"Output key: {scout_agent.output_key}\n")

    app = agent_engines.AdkApp(agent=scout_agent)

    final_text = []
    async for event in app.async_stream_query(
        user_id="inspect",
        message="Research new developments in Physical AI and Embodied Robotics from the last 24 hours."
    ):
        # Collect any text parts from scout events
        if isinstance(event, dict):
            content = event.get("content", {})
            parts = content.get("parts", []) if isinstance(content, dict) else []
            for p in parts:
                if isinstance(p, dict) and "text" in p and p["text"]:
                    final_text.append(p["text"])

    combined = "\n".join(final_text)
    print("\n" + "=" * 60)
    print("SCOUT TEXT OUTPUT (what the DE will see):")
    print("=" * 60)
    print(combined)
    print("=" * 60)

    # Quick URL sniff
    import re
    urls = re.findall(r"https?://\S+", combined)
    print(f"\n[ANALYSIS] URLs found in scout text: {len(urls)}")
    for u in urls[:10]:
        print(f"  - {u}")
    if not urls:
        print("  (none — scout is NOT emitting URLs in text output)")


if __name__ == "__main__":
    asyncio.run(main())
