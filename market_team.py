import os
from google.cloud import storage
import json
import yaml
from datetime import datetime
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

def _shard_exists(category: str) -> bool:
    """Check if a sector's shard file exists (indicates the sector's strategist ran)."""
    if USE_GCS:
        shard_path = GCS_PATH.replace(".json", f"_{category}.json")
        try:
            return _get_gcs_blob(shard_path).exists()
        except Exception:
            return False
    else:
        return os.path.exists(LOCAL_PATH.replace(".json", f"_{category}.json"))

def _get_pipeline_category(pipeline) -> str:
    """Extract the category for a sector pipeline by matching its name to scouts config."""
    scouts_cfg = config.get("scouts", {})
    for scout_name, info in scouts_cfg.items():
        if scout_name in pipeline.name:
            return info.get("category", "General")
    return "General"

def merge_sector_shards() -> str:
    """Combines all sector-specific shard files into the master market_findings_log.
    Call this after all sector pipelines have completed to consolidate results."""
    new_findings = []
    try:
        scouts_cfg = config.get("scouts", {})
        for scout_name, info in scouts_cfg.items():
            cat = info.get("category", "General")
            if USE_GCS:
                shard_path = GCS_PATH.replace(".json", f"_{cat}.json")
                blob = _get_gcs_blob(shard_path)
                if blob.exists():
                    entries = json.loads(blob.download_as_text())
                    print(f"[MERGE] Shard '{cat}': {len(entries)} entries", flush=True)
                    new_findings.extend(entries)
                else:
                    print(f"[MERGE] Shard '{cat}': not found — skipped", flush=True)
            else:
                shard_path = LOCAL_PATH.replace(".json", f"_{cat}.json")
                if os.path.exists(shard_path):
                    with open(shard_path, "r") as f:
                        entries = json.load(f)
                    print(f"[MERGE] Shard '{cat}': {len(entries)} entries", flush=True)
                    new_findings.extend(entries)
                else:
                    print(f"[MERGE] Shard '{cat}': not found — skipped", flush=True)

        # Load existing master log and append — never overwrite history
        existing = []
        if USE_GCS:
            master_blob = _get_gcs_blob(GCS_PATH)
            if master_blob.exists():
                existing = json.loads(master_blob.download_as_text())
        else:
            if os.path.exists(LOCAL_PATH):
                with open(LOCAL_PATH, "r") as f:
                    try: existing = json.load(f)
                    except: pass

        all_findings = existing + new_findings
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

        result = f"Successfully merged {len(new_findings)} new entries into master log ({len(existing)} existing + {len(new_findings)} new = {len(all_findings)} total)."
        print(f"[MERGE] {result}", flush=True)
        return result
    except Exception as e:
        return f"MERGE ERROR: {str(e)}"

_market_batches_cache = None
_merge_agent_cache = None

def get_market_batches():
    """Factory function: Returns a list of parallel batches and the merge agent.
    No Master SequentialAgent! We handle orchestration natively in Python."""
    global _market_batches_cache, _merge_agent_cache
    if _market_batches_cache:
        return _market_batches_cache, _merge_agent_cache

    pipelines = build_sector_pipelines()
    BATCH_SIZE = config["storage"].get("batch_size", 2)
    
    batches = []
    batch_count = 1
    for i in range(0, len(pipelines), BATCH_SIZE):
        batch = pipelines[i:i + BATCH_SIZE]
        if len(batch) == 1:
            batches.append(batch[0]) # Just a sequential agent
        else:
            batches.append(ParallelAgent(
                name=f"Batch_{batch_count}",
                sub_agents=batch
            ))
        batch_count += 1

    # Define the merge agent separately
    merge_agent = Agent(
        name="Shard_Merger",
        model=WORKER_MODEL,
        tools=[merge_sector_shards, log_progress],
        instruction="Call `merge_sector_shards()` to combine all sector findings. Then call log_progress.",
        after_model_callback=_log_token_usage
    )

    _market_batches_cache = batches
    _merge_agent_cache = merge_agent
    return _market_batches_cache, _merge_agent_cache

class MarketSweepApp:
    """Vertex AI Agent Engine wrapper.
    Exposes stream_query as a sync generator — each batch gets its own
    short-lived AdkApp so sessions stay isolated and cloudpickle works.
    """

    def __init__(self):
        self.batches = None
        self.merge_agent = None

    def register_operations(self):
        return {"stream": ["stream_query"]}

    def set_up(self):
        self.batches, self.merge_agent = get_market_batches()

    def stream_query(self, **kwargs):
        import time
        user_id = kwargs.get("user_id", "scheduler")
        message = kwargs.get("message", "Execute your daily market sweep.")
        MAX_RETRIES = 3

        for i, batch_agent in enumerate(self.batches):
            print(f"\n{'='*40}\n  STARTING {batch_agent.name}\n{'='*40}", flush=True)

            # Track which pipelines still need to run
            if isinstance(batch_agent, ParallelAgent):
                remaining = list(batch_agent.sub_agents)
            else:
                remaining = [batch_agent]

            for attempt in range(1, MAX_RETRIES + 1):
                if not remaining:
                    break

                try:
                    # Build the agent to run: single pipeline or parallel group
                    if len(remaining) == 1:
                        run_agent = remaining[0]
                    elif attempt > 1:
                        run_agent = ParallelAgent(name=f"{batch_agent.name}_retry_{attempt}", sub_agents=remaining)
                    else:
                        run_agent = batch_agent

                    batch_app = agent_engines.AdkApp(agent=run_agent)
                    for event in batch_app.stream_query(user_id=user_id, message=message):
                        yield event
                    remaining = []
                    break

                except Exception as e:
                    err_str = str(e)
                    print(f"\n[ERROR] {batch_agent.name} attempt {attempt}/{MAX_RETRIES}: {e}", flush=True)

                    # Check which sectors completed via shard existence
                    still_pending = []
                    for pipeline in remaining:
                        cat = _get_pipeline_category(pipeline)
                        if _shard_exists(cat):
                            print(f"[RECOVERY] {pipeline.name} ({cat}) — shard exists, skipping.", flush=True)
                        else:
                            still_pending.append(pipeline)
                            print(f"[RECOVERY] {pipeline.name} ({cat}) — no shard, will retry.", flush=True)

                    remaining = still_pending

                    if not remaining:
                        print(f"[RECOVERY] All sectors in {batch_agent.name} completed despite error.", flush=True)
                        break

                    if attempt < MAX_RETRIES:
                        if "429" in err_str or "RESOURCE_EXHAUSTED" in err_str:
                            print(f"[COOLDOWN] 429 rate limit — waiting 60s...", flush=True)
                            time.sleep(60)
                        elif "503" in err_str or "502" in err_str or "UNAVAILABLE" in err_str:
                            print(f"[COOLDOWN] Server error — waiting 30s...", flush=True)
                            time.sleep(30)
                        else:
                            print(f"[COOLDOWN] Unexpected error — waiting 30s...", flush=True)
                            time.sleep(30)
                    else:
                        failed_names = [p.name for p in remaining]
                        print(f"[FATAL] {batch_agent.name} failed after {MAX_RETRIES} attempts. Lost: {failed_names}", flush=True)

            if i < len(self.batches) - 1:
                print(f"\n[COOLDOWN] Sleeping 60s between batches...", flush=True)
                time.sleep(60)

        print(f"\n{'='*40}\n  STARTING MERGE PHASE\n{'='*40}", flush=True)
        merge_app = agent_engines.AdkApp(agent=self.merge_agent)
        for event in merge_app.stream_query(user_id=user_id, message="Merge the shards."):
            yield event

        print(f"\nSweep fully completed.", flush=True)

app = MarketSweepApp()

# ==========================================
# 5. EXECUTION BLOCK (Local Testing)
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

    import time as _time
    _start = _time.time()

    app.set_up()
    print(f"Initializing Master Market Sweep...\n")

    for event in app.stream_query(
        user_id="admin_user",
        message="Execute the daily market sweep. Gather findings from scouts, log them, and print the tabular report."
    ):
        print(event)

    elapsed = _time.time() - _start
    print(f"\n{'='*40}")
    print(f"SWEEP COMPLETE — Total time: {int(elapsed // 3600)}h {int((elapsed % 3600) // 60)}m {int(elapsed % 60)}s")
    print(f"Token Usage — Input: {TOKEN_METRICS['input']:,} | Output: {TOKEN_METRICS['output']:,} | Total: {TOKEN_METRICS['total']:,}")
    print(f"{'='*40}\n")