import asyncio
import json
from market_team import build_sector_pipelines, check_auth
from vertexai import agent_engines

async def test_single():
    print("🧪 STARTING SINGLE SCOUT TEST: Robotics via AdkApp")
    
    if not check_auth():
        return

    # 1. Build only the pipelines
    pipelines = build_sector_pipelines()
    
    # 2. Extract the Robotics pipeline
    robotics_agent = next((p for p in pipelines if "Robotics" in p.name), None)
    
    if not robotics_agent:
        print("❌ Could not find Robotics pipeline")
        return

    # 3. Create a mini-app for JUST this agent
    app = agent_engines.AdkApp(agent=robotics_agent)

    # 4. Run the stream
    print(f"🚀 Launching {robotics_agent.name}...")
    try:
        async for event in app.async_stream_query(
            user_id="test_user",
            message="Research new developments in Physical AI and Embodied Robotics from the last 24 hours."
        ):
            print(event)
    except Exception as e:
        print(f"🔥 FATAL ERROR: {str(e)}")

if __name__ == "__main__":
    asyncio.run(test_single())
