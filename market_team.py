import os
import json
import yaml
from datetime import datetime
import asyncio
import vertexai
from vertexai import agent_engines
from google.adk.agents import Agent, SequentialAgent
from google.cloud import storage # Required for GCS support
from google.genai import types
from google.adk.tools import AgentTool, google_search, url_context
 

# Instantiate it and explicitly bypass the native grounding limits
safe_google_search = google_search

# Define the live web search tool
#google_search_tool = google_search
#google_search_tool = GoogleSearchTool(bypass_multi_tools_limit=True)
# 1. DEFINE THE THINKING CONFIGURATIONS
# ==========================================

# Standard config - Tool calling is currently incompatible with ThinkingConfig in many models
scout_config = types.GenerateContentConfig()
cio_config = types.GenerateContentConfig()

# ==========================================
# 2. LOAD CONFIGURATION
# ==========================================
with open("values.yaml", "r") as file:
    config = yaml.safe_load(file)

PROJECT_ID = config["gcp"]["project_id"]
LOCATION = config["gcp"]["location"]
MODEL_LOCATION = config["gcp"].get("model_location", LOCATION)
SUPERVISOR_MODEL = config["agents"]["supervisor_model"]
SUPERVISOR_NAME = config["agents"].get("supervisor_name", "Chief_Investment_Officer")
SUPERVISOR_VERSION = str(config["agents"].get("supervisor_version", "1.0"))
WORKER_MODEL = config["agents"]["worker_model"]
WORKER_VERSION = str(config["agents"].get("worker_version", "1.0"))

# 1. Set environment variables for the google-genai SDK (used by ADK models)
# This ensures models use the "global" endpoint if required.
os.environ["GOOGLE_CLOUD_PROJECT"] = PROJECT_ID
os.environ["GOOGLE_CLOUD_LOCATION"] = MODEL_LOCATION

# Storage Configuration
USE_GCS = config["storage"].get("use_gcs", False)
LOCAL_PATH = config["storage"]["local_path"]
GCS_PATH = config["storage"]["gcs_path"]

# Set the active memory file path for transparency in logs/tools
MEMORY_FILE = GCS_PATH if USE_GCS else LOCAL_PATH
MEMORY_LIMIT = config["storage"].get("memory_limit", 10)

vertexai.init(project=PROJECT_ID, location=LOCATION)

# ==========================================
# 2. HYBRID STORAGE LOGIC (Local & GCS)
# ==========================================
def _get_gcs_blob(gs_path: str):
    """Helper to get a blob from a gs:// URI."""
    parts = gs_path.replace("gs://", "").split("/", 1)
    bucket_name = parts[0]
    blob_name = parts[1]
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(bucket_name)
    return bucket.blob(blob_name)

def _normalize_category(input_string: str) -> str:
    """Soft map varying LLM strings (e.g. 'Power Energy') to true canonical categories ('Power & Energy')."""
    valid_categories = [cfg.get("category") for cfg in config.get("scouts", {}).values() if cfg.get("category")]
    normalized_input = input_string.lower().replace("&", "").replace("and", "").replace(" ", "").replace("_", "")
    
    for valid in valid_categories:
        normalized_valid = valid.lower().replace("&", "").replace("and", "").replace(" ", "").replace("_", "")
        if normalized_input == normalized_valid:
            return valid
    return input_string

def read_memory_log(category: str = "all", memory_limit: int = 10) -> str:
    """Reads the previous findings based on the active storage toggle.
    Args:
        category: The sector to filter by (e.g., 'Robotics', 'Crypto'). Use 'all' for a global baseline.
        memory_limit: Number of entries to return per category or in total.
    """
    data = []
    
    if USE_GCS:
        try:
            blob = _get_gcs_blob(GCS_PATH)
            if blob.exists():
                content = blob.download_as_text()
                data = json.loads(content)
            else:
                return "GCS memory file does not exist. Starting fresh."
        except Exception as e:
            return f"Error reading from GCS: {str(e)}"
    else:
        if os.path.exists(LOCAL_PATH):
            with open(LOCAL_PATH, "r") as f:
                try:
                    data = json.load(f)
                except json.JSONDecodeError:
                    return "Local memory file is corrupted."
        else:
            return "No local memory exists. Starting fresh."

    if not data:
        return "Memory log is empty."

    # Logic for Scouts: Filter by a specific sector
    if category != "all":
        category = _normalize_category(category)
        filtered = [e for e in data if e.get("category", "").lower() == category.lower()]
        return json.dumps(filtered[-memory_limit:], indent=2)
    
    # Logic for CIO: Establish per-category baseline
    else:
        # Dynamically determine active categories from config
        categories = [cfg.get("category") for cfg in config.get("scouts", {}).values() if cfg.get("enabled", True) and cfg.get("category")]
        baseline = {}
        for cat in categories:
            cat_data = [e for e in data if e.get("category", "").lower() == cat.lower() or cat.lower() in e.get("category", "").lower()]
            baseline[cat] = cat_data[-memory_limit:]
        return json.dumps(baseline, indent=2)

def append_to_memory_log(findings_timestamp: str, category: str, finding: str, insights_sentiment: str, guidance_play: str, price_levels: str) -> str:
    """Appends findings using the active storage toggle (Local or GCS).
    Args:
        findings_timestamp: The timestamp received from the scout.
        category: The canonical category (e.g., 'Robotics').
        finding: The raw finding text from the scout.
        insights_sentiment: Key investable takeaways and sentiment analysis.
        guidance_play: Near-term guidance and possible play.
        price_levels: Technical levels, pivots, and analyst PTs.
    """
    category = _normalize_category(category)
    
    entry = {
        "timestamp": findings_timestamp,
        "category": category,
        "finding": finding,
        "insights_sentiment": insights_sentiment,
        "guidance_play": guidance_play,
        "price_levels": price_levels
    }
    
    data = []
    
    if USE_GCS:
        try:
            blob = _get_gcs_blob(GCS_PATH)
            if blob.exists():
                data = json.loads(blob.download_as_text())
            data.append(entry)
            blob.upload_from_string(json.dumps(data, indent=4), content_type='application/json')
            return f"Logged finding for {category} to GCS bucket."
        except Exception as e:
            return f"Error writing to GCS: {str(e)}"
    else:
        if os.path.exists(LOCAL_PATH):
            with open(LOCAL_PATH, "r") as f:
                try:
                    data = json.load(f)
                except json.JSONDecodeError:
                    pass
        data.append(entry)
        with open(LOCAL_PATH, "w") as f:
            json.dump(data, f, indent=4)
        return f"Logged finding for {category} to local file."

# ==========================================
# 3. WORKER AGENTS (The Scouts)
# ==========================================
# Load instructions from configuration
SCOUT_BASE_PROMPT = config["prompts"]["scout_base"]
CIO_VAL = config["prompts"]["cio_instructions"]
CIO_INSTRUCTIONS = "\n".join(CIO_VAL) if isinstance(CIO_VAL, list) else CIO_VAL

# Dictionary to hold active scout agents
active_scouts = {}

print("--- Initializing Market Scouts ---", flush=True)
for scout_name, scout_cfg in config.get("scouts", {}).items():
    if scout_cfg.get("enabled", True):
        category = scout_cfg.get("category", "General")
        sector = scout_cfg.get("sector", scout_name.replace("_", " "))
        agent_instruction = f"<persona>\nYou are the {sector} sector analyst. Your canonical category for logging is '{category}'.\n{SCOUT_BASE_PROMPT}"
        agent = Agent(
            name=scout_name,
            version=WORKER_VERSION,
            model=WORKER_MODEL,
            tools=[
                read_memory_log, 
                safe_google_search, 
                url_context
            ],
            output_key=f"{scout_name}_findings",
            generate_content_config=scout_config, 
            instruction=agent_instruction
        )
        active_scouts[scout_name] = agent
        print(f"✅ Active: {scout_name} | Category: {category} | Sector: {sector}", flush=True)
    else:
        print(f"❌ Disabled: {scout_name}", flush=True)

# ==========================================
# 4. SUPERVISOR AGENT (The CIO)
# ==========================================
# Dynamically build the CIO's tool list starting with base tools (explicitly wrapped)
cio_tools = [
    read_memory_log, 
    append_to_memory_log,
    safe_google_search
]

# Dynamically construct CIO instruction to ingest output_keys
scout_findings_str = "=== SCOUT FINDINGS ===\n"
for scout_name in active_scouts.keys():
    scout_findings_str += f"{scout_name}: {{{scout_name}_findings}}\n"
scout_findings_str += "======================\n\n"

if "<task_instructions>" in CIO_INSTRUCTIONS:
    final_instruction = CIO_INSTRUCTIONS.replace("<task_instructions>", scout_findings_str + "<task_instructions>")
else:
    final_instruction = scout_findings_str + CIO_INSTRUCTIONS

Chief_Investment_Officer = Agent(
    name=SUPERVISOR_NAME,
    version=SUPERVISOR_VERSION,
    model=SUPERVISOR_MODEL,
    tools=cio_tools,
    generate_content_config=cio_config, 
    instruction=final_instruction
)

# 6. PIPELINE ORCHESTRATION
sub_agents = list(active_scouts.values()) + [Chief_Investment_Officer]
market_team = SequentialAgent(name="Market_Team", sub_agents=sub_agents)

app = agent_engines.AdkApp(agent=market_team)

# ==========================================
# 5. EXECUTION BLOCK (Pre-flight checks & Run)
# ==========================================
def check_auth():
    """Verify if we have a valid GCP session."""
    from google.auth import default
    try:
        credentials, project = default()
        print(f"📊 Auth Check: Using project {project}")
        return True
    except Exception as e:
        print(f"❌ AUTH ERROR: Your GCP session is invalid or missing.")
        print("Run: gcloud auth activate-service-account market-agent-sa@marketresearch-agents.iam.gserviceaccount.com --key-file=service_account.json")
        return False

if __name__ == "__main__":
    if not check_auth():
        exit(1)
        
    print(f"🚀 Initializing {Chief_Investment_Officer.name} with {SUPERVISOR_MODEL}...\n")
    
    async def run_report():
        async for event in app.async_stream_query(
            user_id="admin_user", 
            message="Execute the daily market sweep. Gather findings from scouts, log them, and print the tabular report."
        ):
            print(event)
            
    asyncio.run(run_report())