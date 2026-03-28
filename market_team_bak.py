import os
from google.cloud import storage
import json
import yaml
from datetime import datetime
import asyncio
import vertexai
from vertexai import agent_engines
from google.genai import types
from google.adk.agents import Agent, SequentialAgent, ParallelAgent
from google.adk.tools import AgentTool, google_search, url_context
 

# Instantiate it and explicitly bypass the native grounding limits
safe_google_search = google_search

# Define the live web search tool
#google_search_tool = google_search
#google_search_tool = GoogleSearchTool(bypass_multi_tools_limit=True)
# 1. DEFINE THE THINKING CONFIGURATIONS
# ==========================================

# Thinking budgets: light for Scouts/DEs (search + filter), high for Strategists (synthesis + judgment)
worker_config = types.GenerateContentConfig(
    thinking_config=types.ThinkingConfig(thinking_budget=4096)
)
strategist_config = types.GenerateContentConfig(
    thinking_config=types.ThinkingConfig(thinking_budget=16384)
)

# ==========================================
# 2. LOAD CONFIGURATION
# ==========================================
with open("values.yaml", "r") as file:
    config = yaml.safe_load(file)

PROJECT_ID = config["gcp"]["project_id"]
LOCATION = config["gcp"]["location"]
MODEL_LOCATION = config["gcp"].get("model_location", LOCATION)
SUPERVISOR_MODEL = config["agents"]["supervisor_model"]
WORKER_MODEL = config["agents"]["worker_model"]

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
        # Select the requested number of entries
        result = filtered[-memory_limit:]
        
        # 1. First dump the data its returning
        print(f"```json\n{json.dumps(result, indent=2)}\n```\n", flush=True)
        # 2. Then log right before return preceded with markdown
        print(f"### [Memory Baseline] Category: {category} | Entries: {len(result)}", flush=True)
        
        return json.dumps(result, indent=2)
    
    # Logic for CIO: Establish per-category baseline
    else:
        # Dynamically determine active categories from config
        categories = [cfg.get("category") for cfg in config.get("scouts", {}).values() if cfg.get("enabled", True) and cfg.get("category")]
        baseline = {}
        for cat in categories:
            cat_data = [e for e in data if e.get("category", "").lower() == cat.lower() or cat.lower() in e.get("category", "").lower()]
            baseline[cat] = cat_data[-memory_limit:]
        
        # 1. First dump the data its returning
        print(f"```json\n{json.dumps(baseline, indent=2)}\n```\n", flush=True)
        # 2. Then log right before return preceded with markdown
        total_entries = sum(len(v) for v in baseline.values())
        print(f"### [Global Baseline] Categories: {len(baseline)} | Total Entries: {total_entries}", flush=True)
        
        return json.dumps(baseline, indent=2)

def append_to_memory_log(findings_date: str, category: str, finding: str, insights_sentiment: str, guidance_play: str, price_levels: str) -> str:
    """Appends findings using the active storage toggle (Local or GCS).
    Args:
        findings_date: The date received from the scout.
        category: The canonical category (e.g., 'Robotics').
        finding: The raw finding text from the scout.
        insights_sentiment: Key investable takeaways and sentiment analysis.
        guidance_play: Near-term guidance and possible play.
        price_levels: Technical levels, pivots, and analyst PTs.
    """
    category = _normalize_category(category)
    
    entry = {
        "timestamp": findings_date,
        "category": category,
        "finding": finding,
        "insights_sentiment": insights_sentiment,
        "guidance_play": guidance_play,
        "price_levels": price_levels
    }
    print(f"\n### [Saving to Memory] Category: {category}", flush=True)
    print(f"```json\n{json.dumps(entry, indent=2)}\n```\n", flush=True)
    
    data = []
    
    if USE_GCS:
        # Use sector-specific filename to prevent race conditions during parallel runs
        sector_path = GCS_PATH.replace(".json", f"_{category}.json")
        try:
            blob = _get_gcs_blob(sector_path)
            # For sector snippets, we just overwrite/append to that specific sector's shard
            # This is safe because only one agent is ever writing to its own category's shard
            sharded_data = []
            if blob.exists():
                sharded_data = json.loads(blob.download_as_text())
            sharded_data.append(entry)
            blob.upload_from_string(json.dumps(sharded_data, indent=4), content_type='application/json')
            return f"Logged finding for {category} to GCS shard: {sector_path}"
        except Exception as e:
            return f"Error writing to GCS Shard: {str(e)}"
    else:
        # Local mirror
        local_sector_path = LOCAL_PATH.replace(".json", f"_{category}.json")
        data = []
        if os.path.exists(local_sector_path):
            with open(local_sector_path, "r") as f:
                try: data = json.load(f)
                except: pass
        data.append(entry)
        with open(local_sector_path, "w") as f:
            json.dump(data, f, indent=4)
        return f"Logged finding for {category} to local shard: {local_sector_path}"

# Global accumulation for local execution
TOKEN_METRICS = {"input": 0, "output": 0, "total": 0}

def _log_token_usage(callback_context, llm_response):
    """after_model_callback: prints token usage to stdout (captured by Cloud Logging when deployed)."""
    um = llm_response.usage_metadata
    if um:
        agent_name = getattr(callback_context, 'agent_name', 'unknown')
        inp = um.prompt_token_count or 0
        out = um.candidates_token_count or 0
        tot = um.total_token_count or 0
        TOKEN_METRICS["input"] += inp
        TOKEN_METRICS["output"] += out
        TOKEN_METRICS["total"] += tot
        print(f"[TOKEN_USAGE] {agent_name} | input={inp} | output={out} | total={tot}", flush=True)
    return None

def log_progress(message: str, searches: int = 0, topics: int = 0):
    """Log a timing or status marker to stdout with automated work metrics."""
    import time
    ts = time.strftime("%H:%M:%S")
    
    parts = []
    if searches > 0: parts.append(f"{searches} searches")
    if topics > 0: parts.append(f"{topics} topics")
    
    m_str = " | Metrics: " + ", ".join(parts) if parts else ""
    print(f"\n[PROGRESS] {ts} | {message}{m_str}", flush=True)
    return f"Progress logged: {message}{m_str}"

def batch_cooldown(seconds: int = 60):
    """Wait between batch executions to let the API rate-limit window reset."""
    import time
    print(f"\n[COOLDOWN] Waiting {seconds}s between batches to avoid 429...", flush=True)
    time.sleep(seconds)
    print(f"[COOLDOWN] Resuming after {seconds}s pause.", flush=True)
    return f"Cooldown complete after {seconds} seconds."

# ==========================================
# 3. AGENT BUILDING TOOLS & FACTORY
# ==========================================
def build_sector_pipelines():
    """Builds the list of individual sector sequential agents."""
    print("\n--- Building Sector Pipelines ---", flush=True)
    
    SCOUT_BASE_PROMPT = config["prompts"]["scout_base"]
    DE_INSTRUCTIONS = config["prompts"]["data_engineer_instructions"]
    ST_INSTRUCTIONS = config["prompts"]["strategist_instructions"]
    
    scouts_config = config.get("scouts", {})
    sector_pipelines = []

    for scout_name, scout_info in scouts_config.items():
        if not scout_info.get("enabled", True): continue
            
        category = scout_info.get("category", "General")
        sector = scout_info.get("sector", "Market")

        # SCOUT
        scout = Agent(
            name=scout_name,
            model=WORKER_MODEL,
            generate_content_config=worker_config,
            tools=[safe_google_search, url_context, log_progress],
            output_key=f"{scout_name}_findings",
            instruction=SCOUT_BASE_PROMPT.replace("{sector}", sector),
            after_model_callback=_log_token_usage
        )

        # DE
        data_engineer = Agent(
            name=f"{scout_name}_DE",
            model=WORKER_MODEL,
            generate_content_config=worker_config,
            tools=[read_memory_log, safe_google_search, log_progress],
            output_key=f"{scout_name}_analyzed",
            instruction=f"<persona>\nYou are the Data Engineer for {sector}.\nREQUIRED DATA: {{{scout_name}_findings}}\n\n" + DE_INSTRUCTIONS.replace("{category}", category).replace("{sector}", sector),
            after_model_callback=_log_token_usage
        )

        # STRATEGIST
        strategist = Agent(
            name=f"{scout_name}_Strategist",
            model=SUPERVISOR_MODEL,
            generate_content_config=strategist_config,
            tools=[append_to_memory_log, safe_google_search, log_progress],
            output_key=f"{scout_name}_report",
            instruction=f"<persona>\nYou are the Strategist for {sector}.\nREQUIRED DATA: {{{scout_name}_analyzed}}\n\n" + ST_INSTRUCTIONS.replace("{category}", category).replace("{sector}", sector),
            after_model_callback=_log_token_usage
        )

        sector_pipelines.append(SequentialAgent(
            name=f"{scout_name}_Pipeline",
            sub_agents=[scout, data_engineer, strategist]
        ))
        print(f"📦 Registered sector: {sector}", flush=True)
    return sector_pipelines

def merge_sector_shards() -> str:
    """Combines all sector-specific shard files into the master market_findings_log.
    Call this after all sector pipelines have completed to consolidate results."""
    all_findings = []
    try:
        scouts_cfg = config.get("scouts", {})
        for scout_name, info in scouts_cfg.items():
            cat = info.get("category", "General")
            if USE_GCS:
                shard_path = GCS_PATH.replace(".json", f"_{cat}.json")
                blob = _get_gcs_blob(shard_path)
                if blob.exists():
                    all_findings.extend(json.loads(blob.download_as_text()))
            else:
                shard_path = LOCAL_PATH.replace(".json", f"_{cat}.json")
                if os.path.exists(shard_path):
                    with open(shard_path, "r") as f:
                        all_findings.extend(json.load(f))

        all_findings.sort(key=lambda x: x.get("timestamp", ""), reverse=True)

        if USE_GCS:
            master_blob = _get_gcs_blob(GCS_PATH)
            master_blob.upload_from_string(json.dumps(all_findings, indent=4), content_type='application/json')
        else:
            with open(LOCAL_PATH, "w") as f:
                json.dump(all_findings, f, indent=4)

        # Cleanup shards
        for scout_name, info in scouts_cfg.items():
            cat = info.get("category", "General")
            if USE_GCS:
                shard_path = GCS_PATH.replace(".json", f"_{cat}.json")
                blob = _get_gcs_blob(shard_path)
                if blob.exists(): blob.delete()
            else:
                shard_path = LOCAL_PATH.replace(".json", f"_{cat}.json")
                if os.path.exists(shard_path): os.remove(shard_path)

        return f"Successfully merged {len(all_findings)} entries into master log."
    except Exception as e:
        return f"MERGE ERROR: {str(e)}"

_market_team_cache = None

def get_market_team():
    """Factory function for the parallel sweep orchestrator.
    Batches sectors in pairs (max 2 concurrent) to stay within API quota,
    then merges all shards into the master log.
    """
    global _market_team_cache
    if _market_team_cache:
        return _market_team_cache

    pipelines = build_sector_pipelines()

    # Batch pipelines in pairs to prevent 429 RESOURCE_EXHAUSTED on preview models.
    # Each batch runs 2 sectors concurrently via ParallelAgent; batches run sequentially
    # with a 60s cooldown between batches to let the RPM window reset.
    BATCH_SIZE = config["storage"].get("batch_size", 2)
    sweep_stages = []
    batch_count = 0
    for i in range(0, len(pipelines), BATCH_SIZE):
        # Insert cooldown agent between batches (not before the first one)
        if batch_count > 0:
            sweep_stages.append(Agent(
                name=f"Cooldown_{batch_count}",
                model=WORKER_MODEL,
                tools=[batch_cooldown],
                instruction="Call batch_cooldown(seconds=60) to pause before the next batch. Report when done.",
                after_model_callback=_log_token_usage
            ))
        batch = pipelines[i:i + BATCH_SIZE]
        if len(batch) == 1:
            sweep_stages.append(batch[0])
        else:
            sweep_stages.append(ParallelAgent(
                name=f"Batch_{batch_count + 1}",
                sub_agents=batch
            ))
        batch_count += 1

    # Merge agent consolidates all sector shards after all batches complete
    merge_agent = Agent(
        name="Shard_Merger",
        model=WORKER_MODEL,
        generate_content_config=worker_config,
        tools=[merge_sector_shards, log_progress],
        instruction=(
            "You are the final consolidation step of the market sweep. "
            "Call `merge_sector_shards()` to combine all sector findings into the master log. "
            "Then call `log_progress(message='Merge complete')`. Report the merge result."
        ),
        after_model_callback=_log_token_usage
    )

    # Root: sequential batches of parallel pairs, then merge
    _market_team_cache = SequentialAgent(
        name="Master_Market_Sweep",
        sub_agents=sweep_stages + [merge_agent]
    )
    return _market_team_cache

class MarketSweepApp:
    """Wrapper around AdkApp that adds fire-and-forget `trigger` and `kill` methods.
    Cloud Scheduler calls `trigger` (returns immediately, runs sweep in background)
    so the 30-minute attempt-deadline is never hit. `kill` cancels a running sweep.
    Direct API calls can still use `query`/`stream_query` for synchronous execution.
    """
    def __init__(self):
        # AdkApp holds _thread.lock internally — must NOT be created here or cloudpickle
        # will fail during deployment. Created in set_up() which runs after deserialization.
        self._app = None
        self._sweep_task = None
        self._stop_event = None
        self._sweep_started_at = None
        self._lock = None

    def register_operations(self):
        """Tells Vertex AI which methods to expose. Without this it only exposes 'query'."""
        return {
            "": ["query", "trigger", "kill", "status"],
            "stream": ["stream_query"],
        }

    def set_up(self):
        """Called by Vertex AI after deserialization. Safe to create unpicklable objects here."""
        import threading
        self._app = agent_engines.AdkApp(agent=get_market_team())
        self._stop_event = threading.Event()
        self._lock = threading.Lock()
        self._sweep_task = None

    def query(self, **kwargs):
        return self._app.query(**kwargs)

    def stream_query(self, **kwargs):
        return self._app.stream_query(**kwargs)

    def trigger(self, **kwargs):
        """Fire-and-forget: schedules the sweep on the server's uvicorn event loop and
        returns immediately. Using asyncio.run_coroutine_threadsafe keeps the coroutine
        alive on the running loop even after the HTTP response is sent, unlike a background
        thread which dies when the container tears down the request context."""
        import asyncio
        from datetime import datetime

        with self._lock:
            if self._sweep_task and not self._sweep_task.done():
                return {"status": "already_running",
                        "message": f"Sweep already in progress since {self._sweep_started_at}. "
                                   f"Call 'kill' first to stop it."}
            self._stop_event.clear()

        input_data = kwargs.get("input", {})
        user_id = input_data.get("user_id", "scheduler_async")
        message = input_data.get("message", "Execute the daily market sweep.")

        async def _async_run():
            try:
                print(f"\n[TRIGGER] Async sweep started for user={user_id}", flush=True)
                async for event in self._app.async_stream_query(user_id=user_id, message=message):
                    if self._stop_event.is_set():
                        print(f"\n[TRIGGER] Sweep CANCELLED by kill signal.", flush=True)
                        return
                print(f"\n[TRIGGER] Async sweep completed.", flush=True)
            except Exception as e:
                print(f"\n[TRIGGER] Async sweep FAILED: {e}", flush=True)

        with self._lock:
            self._sweep_started_at = datetime.utcnow().isoformat() + "Z"
            loop = asyncio.get_event_loop()
            self._sweep_task = asyncio.run_coroutine_threadsafe(_async_run(), loop)

        return {"status": "triggered", "message": "Sweep started in background",
                "started_at": self._sweep_started_at}

    def kill(self, **kwargs):
        """Signals a running sweep to stop. Returns immediately."""
        with self._lock:
            if not self._sweep_task or self._sweep_task.done():
                return {"status": "no_sweep_running", "message": "Nothing to kill."}

            self._stop_event.set()
            started = self._sweep_started_at

        return {
            "status": "kill_signal_sent",
            "message": f"Sweep started at {started} received kill signal (will stop at next checkpoint)."
        }

    def status(self, **kwargs):
        """Returns the current state of the sweep."""
        with self._lock:
            if self._sweep_task and not self._sweep_task.done():
                return {"status": "running", "started_at": self._sweep_started_at}
            return {"status": "idle"}

app = MarketSweepApp()

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

    # set_up() is normally called by Vertex AI after deserialization; call it manually for local runs
    app.set_up()

    print(f"🚀 Initializing Master Market Sweep...\n")

    async def run_report():
        async for event in app._app.async_stream_query(
            user_id="admin_user",
            message="Execute the daily market sweep. Gather findings from scouts, log them, and print the tabular report."
        ):
            # Events may arrive as dicts or Event objects depending on the runner
            if isinstance(event, dict):
                usage = event.get("usage_metadata") or event.get("usageMetadata")
                if usage:
                    TOKEN_METRICS["input"] += usage.get("prompt_token_count", 0) or 0
                    TOKEN_METRICS["output"] += usage.get("candidates_token_count", 0) or 0
                    TOKEN_METRICS["total"] += usage.get("total_token_count", 0) or 0
            elif hasattr(event, "usage_metadata") and event.usage_metadata:
                usage = event.usage_metadata
                TOKEN_METRICS["input"] += getattr(usage, "prompt_token_count", 0) or 0
                TOKEN_METRICS["output"] += getattr(usage, "candidates_token_count", 0) or 0
                TOKEN_METRICS["total"] += getattr(usage, "total_token_count", 0) or 0
            print(event)
        
        print("\n" + "="*40 + "\n📈 FINAL METRICS\n" + "="*40)
        print(f"🔹 Total Input:  {TOKEN_METRICS['input']:,}\n🔹 Total Output: {TOKEN_METRICS['output']:,}\n🔹 Total Tokens: {TOKEN_METRICS['total']:,}")
        print("="*40 + "\n")
            
    asyncio.run(run_report())