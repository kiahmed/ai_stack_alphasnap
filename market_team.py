import os
import re
import math
import time
import fcntl
from collections import Counter
from google.cloud import storage
from google.api_core.exceptions import PreconditionFailed
import json
import yaml
from datetime import datetime
from dateutil.parser import parse as _parse_date
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

# Disable safety filters: market intel routinely covers defense, weapons, crypto,
# and biotech topics that the default Gemini filters silently drop (output=0, no
# tool calls). There is no end-user here to protect — the agent is the only consumer.
_SAFETY_OFF = [
    types.SafetySetting(category=cat, threshold=types.HarmBlockThreshold.OFF)
    for cat in (
        types.HarmCategory.HARM_CATEGORY_HATE_SPEECH,
        types.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
        types.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT,
        types.HarmCategory.HARM_CATEGORY_HARASSMENT,
    )
]

# Thinking budgets: light for Scouts/DEs (search + filter), high for Strategists (synthesis + judgment)
worker_config = types.GenerateContentConfig(
    thinking_config=types.ThinkingConfig(thinking_budget=4096),
    safety_settings=_SAFETY_OFF,
)
strategist_config = types.GenerateContentConfig(
    thinking_config=types.ThinkingConfig(thinking_budget=16384),
    safety_settings=_SAFETY_OFF,
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
_gcs_client = None

def _get_gcs_client():
    global _gcs_client
    if _gcs_client is None:
        _gcs_client = storage.Client(project=PROJECT_ID)
    return _gcs_client

def _get_gcs_blob(gs_path: str):
    """Helper to get a blob from a gs:// URI."""
    parts = gs_path.replace("gs://", "").split("/", 1)
    bucket_name = parts[0]
    blob_name = parts[1]
    client = _get_gcs_client()
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

# Locked 3-letter abbreviations for entry_id generation.
# If a new category is added, define its abbreviation here AND in proposed_alphasnap_changes.md.
CATEGORY_ABBR = {
    "Robotics": "ROB",
    "Crypto": "CRY",
    "AI Stack": "AIS",
    "Space & Defense": "SPD",
    "Power & Energy": "PWE",
    "Strategic Minerals": "STM",
}

def _reject_range_timestamp(ts: str) -> None:
    """Raise if ts looks like a range (e.g. 'March 21-22, 2026', '2026-03-23 to 2026-03-27').
    Strict ISO YYYY-MM-DD is short-circuited by the caller before this runs — so we only see
    mixed-format inputs here. We must not false-positive on ISO-with-time ('2026-04-15 14:23:00')."""
    if " to " in ts.lower() or "–" in ts or "—" in ts:
        raise ValueError(f"Range-shaped timestamp rejected: {ts!r}")
    # "<Month> <num>-<num>" style (requires leading letter to avoid matching the '-DD' inside ISO dates)
    if re.search(r"[A-Za-z]\w*\s+\d{1,2}\s*-\s*\d{1,2}", ts):
        raise ValueError(f"Range-shaped timestamp rejected: {ts!r}")

def _normalize_timestamp(ts) -> str:
    """Normalize any input timestamp to YYYY-MM-DD. Rejects ranges and unparseable input."""
    if ts is None or not str(ts).strip():
        raise ValueError("timestamp required")
    ts = str(ts).strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", ts):
        return ts
    _reject_range_timestamp(ts)
    try:
        return _parse_date(ts).strftime("%Y-%m-%d")
    except Exception as e:
        raise ValueError(f"Unparseable timestamp {ts!r}: {e}")

def _count_entries_for(category: str, date_iso: str) -> int:
    """Count existing entries matching (category, date_iso) across master log + current sector shard.
    Source-of-truth for the YYY counter in entry_id. Stateless — reads on every call."""
    count = 0
    # Master log
    try:
        if USE_GCS:
            blob = _get_gcs_blob(GCS_PATH)
            if blob.exists():
                data = json.loads(blob.download_as_text())
                count += sum(1 for e in data
                             if e.get("category") == category
                             and e.get("timestamp") == date_iso)
        else:
            if os.path.exists(LOCAL_PATH):
                with open(LOCAL_PATH, "r") as f:
                    data = json.load(f)
                count += sum(1 for e in data
                             if e.get("category") == category
                             and e.get("timestamp") == date_iso)
    except Exception as e:
        print(f"[WARN] _count_entries_for master log read failed: {e}", flush=True)

    # Current sector shard (enriched list already written this run)
    shard_base = GCS_PATH if USE_GCS else LOCAL_PATH
    shard_path = shard_base.replace(".json", f"_{category}.json")
    try:
        if USE_GCS:
            b = _get_gcs_blob(shard_path)
            raw = json.loads(b.download_as_text()) if b.exists() else None
        else:
            raw = json.load(open(shard_path)) if os.path.exists(shard_path) else None
        if isinstance(raw, dict):
            count += sum(1 for e in raw.get("enriched", [])
                         if e.get("timestamp") == date_iso)
    except Exception as e:
        print(f"[WARN] _count_entries_for shard read failed: {e}", flush=True)
    return count

def _next_entry_id(category: str, date_iso: str) -> str:
    """Generate the next entry_id for (category, date_iso). Format: XXX-MMDDYY-YYY."""
    abbr = CATEGORY_ABBR.get(category)
    if not abbr:
        raise ValueError(f"No CATEGORY_ABBR mapping for category {category!r}")
    mm, dd, yy = date_iso[5:7], date_iso[8:10], date_iso[2:4]
    return f"{abbr}-{mm}{dd}{yy}-{_count_entries_for(category, date_iso) + 1:03d}"

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

def append_to_memory_log(category: str, finding: str, timestamp: str,
                         sentiment_takeaways: str, guidance_play: str,
                         price_levels: str, source_url: str = None) -> str:
    """Appends a single finding to the per-sector shard with a unique entry_id.

    Args:
        category: Canonical category (e.g., 'Robotics').
        finding: Raw finding text from the scout.
        timestamp: Finding date. Accepts YYYY-MM-DD, ISO with time, or human-readable
                   (e.g., 'March 24, 2026'). Range-shaped values (e.g., 'March 21-22, 2026')
                   are rejected — pass a single date only.
        sentiment_takeaways: Sentiment label (Very Bullish/Bullish/Neutral/Bearish/Very Bearish) + layered takeaways.
        guidance_play: Near-term trade idea.
        price_levels: Tickers, PTs, institutional levels.
        source_url: Primary source URL for this finding. Pass None if unavailable.

    Returns:
        The generated entry_id (e.g., 'ROB-041726-003').
    """
    category = _normalize_category(category)
    date_iso = _normalize_timestamp(timestamp)

    base_entry = {
        "timestamp": date_iso,
        "category": category,
        "finding": finding,
        "sentiment_takeaways": sentiment_takeaways,
        "guidance_play": guidance_play,
        "price_levels": price_levels,
        "source_url": source_url,
    }

    if USE_GCS:
        return _append_gcs(category, date_iso, base_entry)
    return _append_local(category, date_iso, base_entry)

# Max retries when a concurrent writer bumps the shard generation (GCS) or holds
# the flock (local) between our read and our write. 5 attempts absorbs realistic
# Strategist-parallel-batch contention (usually 2-6 appends per turn).
_APPEND_MAX_RETRIES = 5

def _append_gcs(category: str, date_iso: str, base_entry: dict) -> str:
    """GCS write path with if_generation_match retry. Closes the parallel-append race."""
    sector_path = GCS_PATH.replace(".json", f"_{category}.json")
    blob = _get_gcs_blob(sector_path)

    for attempt in range(1, _APPEND_MAX_RETRIES + 1):
        # 1. Capture current shard state + its generation number.
        #    generation=0 on the upload means "only succeed if the object does not yet exist"
        #    — the right precondition for a first-time create.
        shard_data = {"deduped": [], "enriched": []}
        generation = 0
        if blob.exists():
            blob.reload()
            generation = blob.generation
            raw = json.loads(blob.download_as_text())
            if isinstance(raw, dict) and "deduped" in raw:
                shard_data = raw
            else:
                shard_data["enriched"] = raw  # legacy flat list migrate

        # 2. Recompute entry_id against the FRESHLY read shard + master. This must be
        #    inside the retry loop — if a concurrent writer landed an entry since our
        #    last read, our counter advances too.
        entry_id = _next_entry_id(category, date_iso)
        entry = {"entry_id": entry_id, **base_entry}
        shard_data["enriched"].append(entry)

        # 3. Conditional upload. PreconditionFailed (HTTP 412) means the shard's
        #    generation moved under us — another writer landed between our read
        #    and our write. Retry with fresh state.
        try:
            blob.upload_from_string(
                json.dumps(shard_data, indent=4),
                content_type='application/json',
                if_generation_match=generation,
            )
        except PreconditionFailed:
            print(f"[APPEND] {category} | generation-match miss on attempt {attempt}/{_APPEND_MAX_RETRIES} — retrying", flush=True)
            continue
        except Exception as e:
            return f"Error writing to GCS Shard: {str(e)}"

        if attempt > 1:
            print(f"[APPEND] {category} | {entry_id} landed after {attempt} attempts (contention resolved)", flush=True)
        print(f"\n### [Saving to Memory] Category: {category} | ID: {entry_id}", flush=True)
        print(f"```json\n{json.dumps(entry, indent=2)}\n```\n", flush=True)
        enriched_n = len(shard_data["enriched"])
        deduped_n = len(shard_data["deduped"])
        print(f"[ENRICH] {category}: {enriched_n}/{deduped_n} findings enriched.", flush=True)
        return entry_id

    raise RuntimeError(
        f"append_to_memory_log: exceeded {_APPEND_MAX_RETRIES} retries on shard contention for {category}. "
        f"Check for stuck concurrent writers."
    )

def _append_local(category: str, date_iso: str, base_entry: dict) -> str:
    """Local-mode write path with advisory flock — serializes in-process concurrent writers."""
    local_sector_path = LOCAL_PATH.replace(".json", f"_{category}.json")
    lock_path = local_sector_path + ".lock"

    with open(lock_path, "w") as lockfile:
        fcntl.flock(lockfile, fcntl.LOCK_EX)
        try:
            shard_data = {"deduped": [], "enriched": []}
            if os.path.exists(local_sector_path):
                with open(local_sector_path, "r") as f:
                    try:
                        raw = json.load(f)
                        if isinstance(raw, dict) and "deduped" in raw:
                            shard_data = raw
                        else:
                            shard_data["enriched"] = raw
                    except Exception as e:
                        print(f"[WARN] Could not read local shard {local_sector_path}: {e}", flush=True)

            entry_id = _next_entry_id(category, date_iso)
            entry = {"entry_id": entry_id, **base_entry}
            shard_data["enriched"].append(entry)

            with open(local_sector_path, "w") as f:
                json.dump(shard_data, f, indent=4)

            print(f"\n### [Saving to Memory] Category: {category} | ID: {entry_id}", flush=True)
            print(f"```json\n{json.dumps(entry, indent=2)}\n```\n", flush=True)
            enriched_n = len(shard_data["enriched"])
            deduped_n = len(shard_data["deduped"])
            print(f"[ENRICH] {category}: {enriched_n}/{deduped_n} findings enriched.", flush=True)
            return entry_id
        finally:
            fcntl.flock(lockfile, fcntl.LOCK_UN)

# ==========================================
# 3. DETERMINISTIC DEDUP ENGINE (TF-IDF + Entity Overlap)
# ==========================================
DEDUP_CFG = config.get("dedup", {})
TFIDF_THRESHOLD = DEDUP_CFG.get("tfidf_threshold", 0.45)
ENTITY_THRESHOLD = DEDUP_CFG.get("entity_threshold", 0.6)
NOVELTY_MIN = DEDUP_CFG.get("novelty_min_entities", 2)

# Common uppercase words to exclude from ticker detection
_STOP_UPPER = {
    'THE', 'AND', 'FOR', 'BUT', 'NOT', 'YOU', 'ALL', 'CAN', 'HER', 'WAS',
    'ONE', 'OUR', 'OUT', 'ARE', 'HAS', 'HIS', 'HOW', 'ITS', 'MAY', 'NEW',
    'NOW', 'OLD', 'SEE', 'WAY', 'WHO', 'DID', 'GET', 'HIM', 'LET', 'SAY',
    'SHE', 'TOO', 'USE', 'CEO', 'CFO', 'CTO', 'COO', 'IPO', 'ETF', 'GDP',
    'API', 'USA', 'USD', 'EUR', 'GBP', 'WITH', 'THIS', 'THAT', 'FROM',
    'THEY', 'BEEN', 'HAVE', 'WILL', 'EACH', 'MAKE', 'LIKE', 'LONG', 'VERY',
    'WHEN', 'WHAT', 'YOUR', 'SOME', 'THEM', 'THAN', 'MOST', 'ALSO', 'INTO',
    'OVER', 'SUCH', 'JUST', 'NEAR', 'TERM', 'PER', 'VIA', 'KEY', 'PRE',
    'PRO', 'BOTH', 'ONLY', 'SAME', 'MORE', 'LESS', 'FULL', 'HIGH', 'LOW',
    'NEXT', 'LAST', 'WEEK', 'YEAR', 'NEWS', 'PLUS', 'DEAL'
}

def _tokenize(text):
    return re.findall(r"[a-z0-9]+(?:'[a-z]+)?", text.lower())

def _build_idf(documents):
    n = len(documents)
    df = Counter()
    for doc in documents:
        df.update(set(doc))
    return {term: math.log((n + 1) / (count + 1)) + 1 for term, count in df.items()}

def _tfidf_vector(tokens, idf):
    tf = Counter(tokens)
    return {term: freq * idf.get(term, 1.0) for term, freq in tf.items()}

def _cosine_sim(vec_a, vec_b):
    common = set(vec_a.keys()) & set(vec_b.keys())
    dot = sum(vec_a[k] * vec_b[k] for k in common)
    mag_a = math.sqrt(sum(v * v for v in vec_a.values()))
    mag_b = math.sqrt(sum(v * v for v in vec_b.values()))
    if mag_a == 0 or mag_b == 0:
        return 0.0
    return dot / (mag_a * mag_b)

def _tfidf_similarity(a, b, idf):
    vec_a = _tfidf_vector(_tokenize(a), idf)
    vec_b = _tfidf_vector(_tokenize(b), idf)
    return _cosine_sim(vec_a, vec_b)

def _extract_entities(text):
    entities = set()
    tickers = set()
    # Dollar amounts: $40B, $30 billion, $500M
    for m in re.findall(r'\$[\d,.]+\s*[BMKTbmkt](?:illion|rillion)?', text):
        entities.add(m.strip().upper())
    # Percentages and multipliers
    entities.update(re.findall(r'[\d,.]+%', text))
    entities.update(re.findall(r'[\d,.]+[xX]\b', text))
    # Explicit $TICKER format
    tickers.update(re.findall(r'\$([A-Z]{1,5})\b', text))
    # Uppercase 2-5 char words (likely tickers), exclude stopwords
    for t in re.findall(r'\b([A-Z]{2,5})\b', text):
        if t not in _STOP_UPPER:
            tickers.add(t)
    # Multi-word capitalized names (company/product names)
    entities.update(re.findall(r'[A-Z][a-z]+(?:\s+[A-Z][a-z]+)+', text))
    # All entities includes tickers (for overlap scoring), but tickers tracked separately for novelty
    all_entities = entities | tickers
    return all_entities, tickers

def _merge_substring_entities(entities):
    """Merge entities where one is a substring of another (e.g. 'Red Cat' and 'Red Cat Holdings')."""
    merged = set()
    sorted_ents = sorted(entities, key=len, reverse=True)
    for ent in sorted_ents:
        ent_lower = ent.lower()
        if not any(ent_lower in existing.lower() for existing in merged):
            merged.add(ent)
    return merged

def _entity_overlap(entities_a, entities_b):
    if not entities_a or not entities_b:
        return 0.0
    # Merge substring variants before comparing
    merged_a = _merge_substring_entities(entities_a)
    merged_b = _merge_substring_entities(entities_b)
    intersection = merged_a & merged_b
    # Also count partial matches: "Red Cat" in A matches "Red Cat Holdings" in B
    for ea in merged_a:
        for eb in merged_b:
            if ea != eb and (ea.lower() in eb.lower() or eb.lower() in ea.lower()):
                intersection.add(ea)
    smaller = min(len(merged_a), len(merged_b))
    return len(intersection) / smaller

def _coerce_scout_item(item):
    """Accept either a bare finding string (legacy) or {finding, source_url} object.
    Returns (finding_text, source_url_or_None)."""
    if isinstance(item, dict):
        return str(item.get("finding", "")).strip(), (item.get("source_url") or None)
    return str(item).strip(), None

def dedup_findings(scout_findings_json: str, category: str) -> str:
    """Deterministic dedup: compares scout findings against the memory baseline.

    Three-layer matching (any layer → duplicate, short-circuit):
      1. URL equality — same source_url as a baseline entry (both non-null)
      2. TF-IDF cosine on finding text
      3. Entity overlap on tickers/names/amounts

    Args:
        scout_findings_json: JSON array. Each element is either:
          - an object: {"finding": "...", "source_url": "https://..."}  (preferred)
          - a bare string: "finding text"  (legacy, source_url treated as None)
        category: Sector category (e.g. 'Robotics', 'AI Stack').

    Returns:
        JSON with shape:
          {
            "kept":    [{"finding": "...", "source_url": "..." | null}, ...],
            "dropped": [{"title": "...", "matched_entry_id": "..." | null,
                         "reason": "url_match" | "tfidf" | "entity_overlap" | "intra_batch",
                         "scores": {"tfidf": 0.52, "entity": 0.71}}, ...]
          }
    """
    category = _normalize_category(category)

    # Parse scout findings — tolerate bare strings as legacy shorthand
    try:
        raw = json.loads(scout_findings_json)
        if not isinstance(raw, list):
            raw = [raw]
    except json.JSONDecodeError:
        raw = [scout_findings_json]

    scout_items = [_coerce_scout_item(x) for x in raw]   # [(text, url), ...]
    scout_texts = [t for t, _ in scout_items]

    # Load baseline from master log — capture entry_id + source_url alongside text
    baseline_entries = []
    mem_limit = config.get("storage", {}).get("memory_limit", 10)
    if USE_GCS:
        try:
            blob = _get_gcs_blob(GCS_PATH)
            if blob.exists():
                data = json.loads(blob.download_as_text())
                baseline_entries = [e for e in data if e.get("category", "").lower() == category.lower()]
                baseline_entries = baseline_entries[-mem_limit:]
        except Exception:
            pass
    else:
        if os.path.exists(LOCAL_PATH):
            try:
                with open(LOCAL_PATH, "r") as f:
                    data = json.load(f)
                baseline_entries = [e for e in data if e.get("category", "").lower() == category.lower()]
                baseline_entries = baseline_entries[-mem_limit:]
            except Exception:
                pass

    baseline_texts = [e.get("finding", "") for e in baseline_entries]
    baseline_urls = [(e.get("source_url") or None) for e in baseline_entries]
    baseline_ids = [e.get("entry_id") for e in baseline_entries]

    kept = []       # list of {"finding", "source_url"} dicts
    dropped = []    # list of drop-report dicts

    def _drop(text, url, reason, matched_id, tfidf_s=None, ent_s=None):
        dropped.append({
            "title": text[:80],
            "matched_entry_id": matched_id,
            "reason": reason,
            "scores": {
                "tfidf": round(tfidf_s, 2) if tfidf_s is not None else None,
                "entity": round(ent_s, 2) if ent_s is not None else None,
            },
        })
        print(f"[DEDUP] {category} | DROP ({reason}) matched={matched_id}", flush=True)
        print(f"  scout    : {text[:140]}", flush=True)

    # No baseline → nothing to compare against; still run intra-batch check
    if not baseline_texts:
        print(f"[DEDUP] {category}: No baseline — checking intra-batch only.", flush=True)

    # Build IDF corpus only if we have baseline or multiple scout items (for intra-batch)
    need_tfidf = bool(baseline_texts) or len(scout_texts) > 1
    if need_tfidf:
        corpus = baseline_texts + scout_texts
        idf = _build_idf([_tokenize(t) for t in corpus])
        baseline_entity_sets = [_extract_entities(t) for t in baseline_texts]

    for idx, (finding_text, finding_url) in enumerate(scout_items):
        is_duplicate = False
        finding_entities, finding_tickers = _extract_entities(finding_text) if need_tfidf else (set(), set())

        # ── Layer 1: URL equality fast-path ──
        if finding_url:
            for i, b_url in enumerate(baseline_urls):
                if b_url and b_url == finding_url:
                    _drop(finding_text, finding_url, "url_match", baseline_ids[i])
                    is_duplicate = True
                    break

        # ── Layer 2+3: TF-IDF + entity overlap vs baseline ──
        if not is_duplicate and baseline_texts:
            for i, base_text in enumerate(baseline_texts):
                base_entities, _ = baseline_entity_sets[i]
                tfidf_score = _tfidf_similarity(finding_text, base_text, idf)
                ent_score = _entity_overlap(finding_entities, base_entities)

                if tfidf_score >= TFIDF_THRESHOLD or ent_score >= ENTITY_THRESHOLD:
                    # Novelty check: exclude tickers — they're alt identifiers, not new info
                    novel = (finding_entities - base_entities) - finding_tickers
                    if len(novel) >= NOVELTY_MIN:
                        continue  # update on same topic — keep

                    reason = "tfidf" if tfidf_score >= TFIDF_THRESHOLD else "entity_overlap"
                    _drop(finding_text, finding_url, reason, baseline_ids[i], tfidf_score, ent_score)
                    is_duplicate = True
                    break

        # ── Intra-batch dedup ──
        if not is_duplicate and kept:
            for k in kept:
                k_text = k["finding"]
                k_url = k.get("source_url")
                # URL fast-path within batch
                if finding_url and k_url and finding_url == k_url:
                    _drop(finding_text, finding_url, "intra_batch", None)
                    is_duplicate = True
                    break
                k_entities, _ = _extract_entities(k_text)
                tfidf_score = _tfidf_similarity(finding_text, k_text, idf)
                ent_score = _entity_overlap(finding_entities, k_entities)
                if tfidf_score >= TFIDF_THRESHOLD or ent_score >= ENTITY_THRESHOLD:
                    _drop(finding_text, finding_url, "intra_batch", None, tfidf_score, ent_score)
                    is_duplicate = True
                    break

        if not is_duplicate:
            kept.append({"finding": finding_text, "source_url": finding_url})

    print(f"[DEDUP] {category}: {len(kept)} unique, {len(dropped)} duplicates removed "
          f"(from {len(scout_items)} scout findings vs {len(baseline_texts)} baseline).",
          flush=True)

    # Persist deduped manifest to sector shard — source of truth for the Strategist
    shard_data = {"deduped": kept, "enriched": []}
    if USE_GCS:
        shard_path = GCS_PATH.replace(".json", f"_{category}.json")
        try:
            blob = _get_gcs_blob(shard_path)
            blob.upload_from_string(json.dumps(shard_data, indent=4), content_type='application/json')
            print(f"[DEDUP] Wrote {len(kept)} deduped findings to shard: {shard_path}", flush=True)
        except Exception as e:
            print(f"[DEDUP] WARNING: Failed to write shard manifest: {e}", flush=True)
    else:
        shard_path = LOCAL_PATH.replace(".json", f"_{category}.json")
        with open(shard_path, "w") as f:
            json.dump(shard_data, f, indent=4)
        print(f"[DEDUP] Wrote {len(kept)} deduped findings to shard: {shard_path}", flush=True)

    return json.dumps({"kept": kept, "dropped": dropped}, indent=2)

# Global accumulation for local execution
TOKEN_METRICS = {"input": 0, "output": 0, "total": 0}

# Per-topic JSON schema expected from the DE. Callback validates against this —
# missing keys get WARNed but we still render what we have so a partial blob
# doesn't kill the pipeline.
_DE_REQUIRED_KEYS = ("finding", "date", "source_url", "sentiment",
                     "direct", "indirect", "market_dynamics", "price_levels")

_SCOUT_REQUIRED_KEYS = ("finding", "date", "source_url")

# Gemini's built-in google_search tool exposes only these proxy URLs to the LLM —
# the real source URL is hidden behind a 302 redirect. The tokens expire after
# ~a few days (community-reported, not documented), so persisting them makes
# downstream entries decay. Resolve eagerly before rendering.
_GROUNDING_REDIRECT_PREFIX = "https://vertexaisearch.cloud.google.com/grounding-api-redirect/"


def _resolve_grounding_url(url, timeout=4.0):
    """Follow a vertexaisearch grounding-api-redirect URL to its real destination.
    Returns the resolved URL on success, the original on any failure (network,
    timeout, non-redirect response) so we degrade gracefully."""
    try:
        import requests
        resp = requests.head(url, allow_redirects=True, timeout=timeout)
        final = resp.url or url
        if final.startswith(_GROUNDING_REDIRECT_PREFIX):
            return url
        return final
    except Exception as e:
        print(f"[SCOUT_URL_RESOLVE] HEAD failed for grounding redirect: {type(e).__name__}", flush=True)
        return url


def _resolve_scout_urls(items):
    """Walk scout findings in-place and resolve any grounding-redirect URLs to
    their real destinations. Non-proxy URLs and missing URLs are untouched.
    Returns (resolved_count, skipped_non_proxy, failed_count)."""
    resolved = failed = skipped = 0
    for it in items:
        u = it.get("source_url")
        if not u or not isinstance(u, str):
            continue
        if not u.startswith(_GROUNDING_REDIRECT_PREFIX):
            skipped += 1
            continue
        new_u = _resolve_grounding_url(u)
        if new_u == u:
            failed += 1
        else:
            it["source_url"] = new_u
            resolved += 1
    return resolved, skipped, failed


# ── Grounding-chunk capture + null-URL fallback ──
# When the scout LLM returns source_url: null (or omits it) for a finding, we
# can still recover a URL via Gemini's grounding metadata: every search turn
# attaches `grounding_chunks[*].web.uri` (proxy URLs) and `grounding_supports`
# that map text character ranges to the chunks that support them. By finding
# which grounding_support ranges overlap each finding's JSON span in the raw
# model output, we can pick the citing chunk and resolve its proxy.

_SCOUT_GROUNDING_KEY = "_scout_grounding"


def _capture_grounding(callback_context, llm_response):
    """after_model_callback for the scout. Piggybacks on _log_token_usage, then
    stashes the most recent grounding_metadata (chunks + supports) into session
    state for _validate_and_render_scout_output to use as a null-URL fallback.

    Gemini's grounding metadata rides on the turn where search happened, which
    may not be the final text-generation turn — so we overwrite only when the
    incoming turn has non-empty chunks. Last non-empty wins."""
    _log_token_usage(callback_context, llm_response)

    try:
        cands = getattr(llm_response, "candidates", None) or []
        if not cands:
            return None
        gm = getattr(cands[0], "grounding_metadata", None)
        if gm is None:
            return None
        chunks = getattr(gm, "grounding_chunks", None) or []
        if not chunks:
            return None

        chunk_dicts = []
        for c in chunks:
            web = getattr(c, "web", None)
            chunk_dicts.append({
                "uri": getattr(web, "uri", None) if web else None,
                "domain": getattr(web, "domain", None) if web else None,
                "title": getattr(web, "title", None) if web else None,
            })

        support_dicts = []
        for s in (getattr(gm, "grounding_supports", None) or []):
            seg = getattr(s, "segment", None)
            support_dicts.append({
                "start": getattr(seg, "start_index", None) if seg else None,
                "end": getattr(seg, "end_index", None) if seg else None,
                "chunk_indices": list(getattr(s, "grounding_chunk_indices", None) or []),
            })

        _write_state(callback_context, _SCOUT_GROUNDING_KEY, {
            "chunks": chunk_dicts,
            "supports": support_dicts,
        })
    except Exception as e:
        print(f"[SCOUT_GROUNDING] capture failed: {type(e).__name__}: {e}", flush=True)
    return None


def _finding_spans_in_blob(blob):
    """Return [(start, end), ...] for each `"finding"` key occurrence in the raw
    JSON blob. Finding N covers chars [spans[N].start, spans[N+1].start), or to
    end-of-blob for the last one. These offsets align with grounding_supports
    segment indices because the blob IS the raw model text output."""
    spans = []
    for m in re.finditer(r'"finding"\s*:', blob):
        spans.append(m.start())
    ranges = []
    for i, s in enumerate(spans):
        e = spans[i + 1] if i + 1 < len(spans) else len(blob)
        ranges.append((s, e))
    return ranges


def _fill_null_urls_from_grounding(items, blob, grounding):
    """For each item with null/empty source_url, find grounding_supports whose
    segment range falls within that finding's blob span, pick the first cited
    chunk, resolve its proxy via HEAD. Only substitutes if resolution succeeds
    (we don't want to fill nulls with proxies that may decay).
    Returns (filled, attempted_but_failed)."""
    if not grounding or not items:
        return 0, 0
    chunks = grounding.get("chunks") or []
    supports = grounding.get("supports") or []
    if not chunks or not supports:
        return 0, 0

    ranges = _finding_spans_in_blob(blob)
    filled = 0
    failed = 0
    for i, it in enumerate(items):
        u = it.get("source_url")
        if u:  # already has a URL — don't override
            continue
        if i >= len(ranges):
            continue
        start, end = ranges[i]
        chunk_idx = None
        for sup in supports:
            s_start = sup.get("start")
            if s_start is None:
                continue
            if start <= s_start < end:
                for ci in sup.get("chunk_indices", []):
                    if 0 <= ci < len(chunks):
                        chunk_idx = ci
                        break
                if chunk_idx is not None:
                    break
        if chunk_idx is None:
            continue
        proxy = chunks[chunk_idx].get("uri")
        if not proxy or not proxy.startswith(_GROUNDING_REDIRECT_PREFIX):
            continue
        resolved = _resolve_grounding_url(proxy)
        if resolved == proxy:
            failed += 1
            continue
        it["source_url"] = resolved
        filled += 1
    return filled, failed


def _parse_json_array(blob):
    """Tolerant JSON-array parse. Strips ```json ...``` or ``` ...``` fences the
    LLM sometimes wraps around JSON even when instructed not to. Returns a list
    of dicts, or None if unrecoverable."""
    s = str(blob).strip()
    if s.startswith("```"):
        first_nl = s.find("\n")
        if first_nl > 0:
            s = s[first_nl + 1:]
        if s.rstrip().endswith("```"):
            s = s.rstrip()[:-3].rstrip()
    try:
        data = json.loads(s)
    except json.JSONDecodeError:
        return None
    if not isinstance(data, list):
        return None
    return [x for x in data if isinstance(x, dict)]


def _fmt_field(v):
    """Render None / empty → "None" (matches the Python literal the Strategist
    prompt tells it to pass through). All other values → str()."""
    if v is None or v == "":
        return "None"
    return str(v)


def _render_de_canonical(topics):
    """Render validated topic dicts into labelled `=== TOPIC N ===` blocks.
    Strategist reads these directly — sentiment_takeaways is pre-composed as a
    single one-liner matching CLAUDE.md's schema so the Strategist can copy it
    verbatim into append_to_memory_log without re-synthesizing."""
    if not topics:
        return "No new findings."
    blocks = []
    for i, t in enumerate(topics, start=1):
        sentiment_takeaways = (
            f"Sentiment: {_fmt_field(t.get('sentiment'))} | "
            f"Direct: {_fmt_field(t.get('direct'))} | "
            f"Indirect: {_fmt_field(t.get('indirect'))} | "
            f"Market Dynamics: {_fmt_field(t.get('market_dynamics'))}"
        )
        blocks.append(
            f"=== TOPIC {i} ===\n"
            f"finding: {_fmt_field(t.get('finding'))}\n"
            f"date: {_fmt_field(t.get('date'))}\n"
            f"source_url: {_fmt_field(t.get('source_url'))}\n"
            f"sentiment_takeaways: {sentiment_takeaways}\n"
            f"price_levels: {_fmt_field(t.get('price_levels'))}"
        )
    return "\n\n".join(blocks)


def _resolve_output_key(callback_context, agent_name):
    """Derive the agent's output_key. Primary: pull from the bound agent. Fallback:
    infer from the agent_name suffix (`_DE` → `_analyzed`, else `_findings`)."""
    try:
        k = callback_context._invocation_context.agent.output_key
        if k:
            return k
    except Exception:
        pass
    if agent_name.endswith("_DE"):
        return f"{agent_name[:-3]}_analyzed"
    return f"{agent_name}_findings"


def _render_scout_canonical(items):
    """Render validated scout finding dicts into `=== FINDING N ===` blocks.
    The DE reads these directly — no URL-hunting in prose, no date-line parsing."""
    if not items:
        return "No new findings."
    blocks = []
    for i, it in enumerate(items, start=1):
        blocks.append(
            f"=== FINDING {i} ===\n"
            f"finding: {_fmt_field(it.get('finding'))}\n"
            f"date: {_fmt_field(it.get('date'))}\n"
            f"source_url: {_fmt_field(it.get('source_url'))}"
        )
    return "\n\n".join(blocks)


def _validate_and_render_scout_output(callback_context):
    """after_agent_callback for the Scout agent.

    Flow mirrors `_validate_and_render_de_output`:
      1. Read Scout's raw output from state[output_key] — should be a JSON array.
      2. Parse (tolerant to ```json fences).
      3. Validate per-finding schema — missing keys WARNed, items without
         `finding` dropped.
      4. Render survivors as `=== FINDING N ===` blocks and write back to
         state[output_key] so the DE sees a stable labelled structure.
      5. Delegate to `_dump_agent_output` so the AGENT_RAW dump still lands in
         Cloud Logging.

    On unrecoverable parse failure the raw blob is left in place so the DE can
    still do a best-effort prose pass — same as pre-schema behaviour.
    """
    agent_name = getattr(callback_context, 'agent_name', 'unknown')
    key = _resolve_output_key(callback_context, agent_name)

    try:
        blob = callback_context.state.get(key)
    except Exception:
        blob = None

    if not blob:
        print(f"[SCOUT_VALIDATE] {agent_name}: state[{key}] empty — leaving as-is.", flush=True)
        return _dump_agent_output(callback_context)

    items = _parse_json_array(blob)
    if items is None:
        print(
            f"[SCOUT_VALIDATE] {agent_name}: output NOT valid JSON array — leaving raw blob "
            f"in place (DE will attempt prose fallback).",
            flush=True,
        )
        return _dump_agent_output(callback_context)

    validated = []
    for i, it in enumerate(items, start=1):
        missing = [k for k in _SCOUT_REQUIRED_KEYS if k not in it]
        if missing:
            print(f"[SCOUT_VALIDATE] {agent_name}: WARN finding #{i} missing keys: {missing}", flush=True)
        if not it.get("finding"):
            print(f"[SCOUT_VALIDATE] {agent_name}: WARN finding #{i} has no `finding` — dropping.", flush=True)
            continue
        validated.append(it)

    try:
        grounding = callback_context.state.get(_SCOUT_GROUNDING_KEY)
    except Exception:
        grounding = None
    if grounding:
        filled, fill_failed = _fill_null_urls_from_grounding(validated, str(blob), grounding)
        if filled or fill_failed:
            print(
                f"[SCOUT_URL_FILL] {agent_name}: filled {filled} null URLs from grounding chunks "
                f"({fill_failed} failed).",
                flush=True,
            )

    resolved, skipped, failed = _resolve_scout_urls(validated)
    if resolved or failed:
        print(
            f"[SCOUT_URL_RESOLVE] {agent_name}: resolved {resolved} grounding redirects "
            f"({failed} failed, {skipped} non-proxy URLs untouched).",
            flush=True,
        )

    rendered = _render_scout_canonical(validated)
    _write_state(callback_context, key, rendered)
    print(
        f"[SCOUT_VALIDATE] {agent_name}: parsed {len(items)} findings → rendered {len(validated)} canonical blocks to state[{key}].",
        flush=True,
    )
    return _dump_agent_output(callback_context)


def _write_state(callback_context, key, value):
    """Write a value back into session state. Prefer `callback_context.state[key]`
    (the standard ADK API); fall back to `_invocation_context.session.state` if
    the wrapper doesn't accept __setitem__ on this ADK version."""
    try:
        callback_context.state[key] = value
        return True
    except Exception:
        pass
    try:
        callback_context._invocation_context.session.state[key] = value
        return True
    except Exception as e:
        print(f"[DE_VALIDATE] FAILED to write state[{key}]: {e}", flush=True)
        return False


def _validate_and_render_de_output(callback_context):
    """after_agent_callback for the DE agent.

    Flow:
      1. Read DE's raw output from state[output_key] — should be a JSON array.
      2. Parse (tolerant to ```json fences).
      3. Validate per-topic schema — missing keys logged as WARN, topics
         without `finding` dropped as unusable.
      4. Render the survivors into `=== TOPIC N ===` blocks and write back to
         state[output_key] so the Strategist sees a stable labelled structure.
      5. Delegate to `_dump_agent_output` so the AGENT_RAW dump still lands in
         Cloud Logging (now showing the rendered canonical form, not raw JSON).

    On any irrecoverable parse failure we leave the raw blob intact — Strategist
    will do its best with prose, same as pre-schema behaviour — and surface
    a [DE_VALIDATE] warning so the failure is visible in logs.
    """
    agent_name = getattr(callback_context, 'agent_name', 'unknown')
    key = _resolve_output_key(callback_context, agent_name)

    try:
        blob = callback_context.state.get(key)
    except Exception:
        blob = None

    if not blob:
        print(f"[DE_VALIDATE] {agent_name}: state[{key}] empty — leaving as-is.", flush=True)
        return _dump_agent_output(callback_context)

    topics = _parse_json_array(blob)
    if topics is None:
        print(
            f"[DE_VALIDATE] {agent_name}: output NOT valid JSON array — leaving raw blob "
            f"in place (Strategist will attempt prose fallback).",
            flush=True,
        )
        return _dump_agent_output(callback_context)

    validated = []
    for i, t in enumerate(topics, start=1):
        missing = [k for k in _DE_REQUIRED_KEYS if k not in t]
        if missing:
            print(f"[DE_VALIDATE] {agent_name}: WARN topic #{i} missing keys: {missing}", flush=True)
        if not t.get("finding"):
            print(f"[DE_VALIDATE] {agent_name}: WARN topic #{i} has no `finding` — dropping.", flush=True)
            continue
        validated.append(t)

    rendered = _render_de_canonical(validated)
    _write_state(callback_context, key, rendered)
    print(
        f"[DE_VALIDATE] {agent_name}: parsed {len(topics)} topics → rendered {len(validated)} canonical blocks to state[{key}].",
        flush=True,
    )
    return _dump_agent_output(callback_context)


def _dump_agent_output(callback_context):
    """after_agent_callback: prints the agent's raw output blob (session.state[output_key])
    to stdout so we can audit the exact format Gemini produced at each stage. Lands
    in Cloud Logging when deployed, framed by BEGIN/END markers for easy grep.

    Works for any agent with a discoverable output_key. Derivation order:
      1. Direct lookup via callback_context._invocation_context.agent.output_key
      2. Fallback: infer from agent_name suffix (`_DE` → `_analyzed`,
         `_Strategist` → `_report`, otherwise `_findings`).
    """
    agent_name = getattr(callback_context, 'agent_name', 'unknown')

    key = None
    try:
        key = callback_context._invocation_context.agent.output_key
    except Exception:
        pass
    if not key:
        if agent_name.endswith("_DE"):
            key = f"{agent_name[:-3]}_analyzed"
        elif agent_name.endswith("_Strategist"):
            key = f"{agent_name[:-len('_Strategist')]}_report"
        else:
            key = f"{agent_name}_findings"

    try:
        blob = callback_context.state.get(key)
    except Exception:
        blob = None
    if not blob:
        print(f"[AGENT_RAW] {agent_name}: <empty or missing state[{key}]>", flush=True)
        return None
    print(f"[AGENT_RAW_BEGIN] {agent_name} | key={key} | len={len(str(blob))}", flush=True)
    print(str(blob), flush=True)
    print(f"[AGENT_RAW_END] {agent_name}", flush=True)
    return None


def _log_token_usage(callback_context, llm_response):
    """after_model_callback: prints token usage to stdout (captured by Cloud Logging when deployed)."""
    um = llm_response.usage_metadata
    agent_name = getattr(callback_context, 'agent_name', 'unknown')
    if um:
        inp = um.prompt_token_count or 0
        out = um.candidates_token_count or 0
        tot = um.total_token_count or 0
        TOKEN_METRICS["input"] += inp
        TOKEN_METRICS["output"] += out
        TOKEN_METRICS["total"] += tot
        print(f"[TOKEN_USAGE] {agent_name} | input={inp} | output={out} | total={tot}", flush=True)

        # Surface silent model failures (safety blocks, transient empty completions).
        # Without this they look like a successful turn — agent exits, shard stays partial.
        if out == 0:
            finish = "?"
            block = "?"
            try:
                cands = getattr(llm_response, "candidates", None) or []
                if cands:
                    finish = getattr(cands[0], "finish_reason", "?")
                pf = getattr(llm_response, "prompt_feedback", None)
                if pf is not None:
                    block = getattr(pf, "block_reason", "?")
            except Exception:
                pass
            print(f"[WARN] {agent_name} returned EMPTY response (output=0). finish_reason={finish}, block_reason={block}", flush=True)
    return None

def log_progress(message: str, searches: int = 0, topics: int = 0):
    """Log a timing or status marker to stdout with automated work metrics."""
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
            after_model_callback=_capture_grounding,
            after_agent_callback=_validate_and_render_scout_output
        )

        # DE
        data_engineer = Agent(
            name=f"{scout_name}_DE",
            model=WORKER_MODEL,
            generate_content_config=worker_config,
            tools=[dedup_findings, safe_google_search, log_progress],
            output_key=f"{scout_name}_analyzed",
            instruction=f"<persona>\nYou are the Data Engineer for {sector}.\nREQUIRED DATA: {{{scout_name}_findings}}\n\n" + DE_INSTRUCTIONS.replace("{category}", category).replace("{sector}", sector),
            after_model_callback=_log_token_usage,
            after_agent_callback=_validate_and_render_de_output
        )

        # STRATEGIST
        strategist = Agent(
            name=f"{scout_name}_Strategist",
            model=SUPERVISOR_MODEL,
            generate_content_config=strategist_config,
            tools=[append_to_memory_log, safe_google_search, log_progress],
            output_key=f"{scout_name}_report",
            instruction=f"<persona>\nYou are the Strategist for {sector}.\nREQUIRED DATA: {{{scout_name}_analyzed}}\n\n" + ST_INSTRUCTIONS.replace("{category}", category).replace("{sector}", sector),
            after_model_callback=_log_token_usage,
            after_agent_callback=_dump_agent_output
        )

        sector_pipelines.append(SequentialAgent(
            name=f"{scout_name}_Pipeline",
            sub_agents=[scout, data_engineer, strategist]
        ))
        print(f"📦 Registered sector: {sector}", flush=True)
    return sector_pipelines

def _shard_valid(category: str) -> bool:
    """Check if a sector's shard is complete: enriched count matches deduped count.
    Returns False (and deletes partial shard) if incomplete, triggering retry."""
    shard_data = None
    shard_path = None

    if USE_GCS:
        shard_path = GCS_PATH.replace(".json", f"_{category}.json")
        try:
            blob = _get_gcs_blob(shard_path)
            if not blob.exists():
                return False
            raw = json.loads(blob.download_as_text())
            if isinstance(raw, dict) and "deduped" in raw:
                shard_data = raw
            else:
                # Legacy flat list — treat as complete (no manifest to compare)
                return True
        except Exception:
            return False
    else:
        shard_path = LOCAL_PATH.replace(".json", f"_{category}.json")
        if not os.path.exists(shard_path):
            return False
        try:
            with open(shard_path, "r") as f:
                raw = json.load(f)
            if isinstance(raw, dict) and "deduped" in raw:
                shard_data = raw
            else:
                return True
        except Exception:
            return False

    deduped_n = len(shard_data.get("deduped", []))
    enriched_n = len(shard_data.get("enriched", []))

    if deduped_n == 0:
        # DE found nothing new — valid (empty sector)
        print(f"[SHARD] {category}: dedup found 0 new findings — shard valid (empty).", flush=True)
        return True

    if enriched_n >= deduped_n:
        print(f"[SHARD] {category}: {enriched_n}/{deduped_n} enriched — complete.", flush=True)
        return True

    # Partial write — keep shard intact for strategist-only retry
    print(f"[SHARD] {category}: {enriched_n}/{deduped_n} enriched — INCOMPLETE, needs retry.", flush=True)
    return False

def _get_pipeline_category(pipeline) -> str:
    """Extract the category for a sector pipeline by matching its name to scouts config."""
    scouts_cfg = config.get("scouts", {})
    for scout_name, info in scouts_cfg.items():
        if scout_name in pipeline.name:
            return info.get("category", "General")
    return "General"

def _rebuild_pipeline(pipeline) -> "SequentialAgent":
    """Build a fresh copy of a sector pipeline (new Agent objects, no parent binding).
    ADK Pydantic enforces single-parent — reusing agents in a new ParallelAgent fails."""
    SCOUT_BASE_PROMPT = config["prompts"]["scout_base"]
    DE_INSTRUCTIONS = config["prompts"]["data_engineer_instructions"]
    ST_INSTRUCTIONS = config["prompts"]["strategist_instructions"]
    scouts_cfg = config.get("scouts", {})
    for scout_name, info in scouts_cfg.items():
        if scout_name in pipeline.name:
            category = info.get("category", "General")
            sector = info.get("sector", "Market")
            scout = Agent(
                name=scout_name,
                model=WORKER_MODEL,
                generate_content_config=worker_config,
                tools=[safe_google_search, url_context, log_progress],
                output_key=f"{scout_name}_findings",
                instruction=SCOUT_BASE_PROMPT.replace("{sector}", sector),
                after_model_callback=_capture_grounding,
                after_agent_callback=_validate_and_render_scout_output
            )
            data_engineer = Agent(
                name=f"{scout_name}_DE",
                model=WORKER_MODEL,
                generate_content_config=worker_config,
                tools=[dedup_findings, safe_google_search, log_progress],
                output_key=f"{scout_name}_analyzed",
                instruction=f"<persona>\nYou are the Data Engineer for {sector}.\nREQUIRED DATA: {{{scout_name}_findings}}\n\n" + DE_INSTRUCTIONS.replace("{category}", category).replace("{sector}", sector),
                after_model_callback=_log_token_usage,
                after_agent_callback=_validate_and_render_de_output
            )
            strategist = Agent(
                name=f"{scout_name}_Strategist",
                model=SUPERVISOR_MODEL,
                generate_content_config=strategist_config,
                tools=[append_to_memory_log, safe_google_search, log_progress],
                output_key=f"{scout_name}_report",
                instruction=f"<persona>\nYou are the Strategist for {sector}.\nREQUIRED DATA: {{{scout_name}_analyzed}}\n\n" + ST_INSTRUCTIONS.replace("{category}", category).replace("{sector}", sector),
                after_model_callback=_log_token_usage,
                after_agent_callback=_dump_agent_output
            )
            return SequentialAgent(
                name=f"{scout_name}_Pipeline",
                sub_agents=[scout, data_engineer, strategist]
            )
    raise ValueError(f"Could not find config for pipeline: {pipeline.name}")

def _get_unenriched_findings(category: str) -> list:
    """Read the shard and return findings from deduped that aren't yet in enriched."""
    shard_data = None
    if USE_GCS:
        shard_path = GCS_PATH.replace(".json", f"_{category}.json")
        try:
            blob = _get_gcs_blob(shard_path)
            if blob.exists():
                raw = json.loads(blob.download_as_text())
                if isinstance(raw, dict):
                    shard_data = raw
        except Exception:
            pass
    else:
        shard_path = LOCAL_PATH.replace(".json", f"_{category}.json")
        if os.path.exists(shard_path):
            try:
                with open(shard_path, "r") as f:
                    shard_data = json.load(f)
            except Exception:
                pass

    if not shard_data or not isinstance(shard_data, dict):
        return []

    deduped = shard_data.get("deduped", [])
    enriched_findings = {e.get("finding", "") for e in shard_data.get("enriched", [])}
    return [f for f in deduped if f not in enriched_findings]


def _build_strategist_retry(pipeline) -> "Agent":
    """Build a standalone Strategist agent to process only unenriched findings from the shard."""
    scouts_cfg = config.get("scouts", {})
    ST_INSTRUCTIONS = config["prompts"]["strategist_instructions"]

    for scout_name, info in scouts_cfg.items():
        if scout_name in pipeline.name:
            category = info.get("category", "General")
            sector = info.get("sector", "Market")
            unenriched = _get_unenriched_findings(category)

            if not unenriched:
                return None

            findings_block = "\n".join(f"- {f}" for f in unenriched)
            instruction = (
                f"<persona>\nYou are the Strategist for {sector}.\n"
                f"The following findings need analysis and logging. Process ALL of them.\n\n"
                f"FINDINGS TO PROCESS:\n{findings_block}\n\n"
                + ST_INSTRUCTIONS.replace("{category}", category).replace("{sector}", sector)
            )

            return Agent(
                name=f"{scout_name}_Strategist_Retry",
                model=SUPERVISOR_MODEL,
                generate_content_config=strategist_config,
                tools=[append_to_memory_log, safe_google_search, log_progress],
                instruction=instruction,
                after_model_callback=_log_token_usage
            )
    return None


def merge_sector_shards() -> str:
    """Combines all sector-specific shard files into the master market_findings_log.
    Call this after all sector pipelines have completed to consolidate results."""
    new_findings = []
    merge_stats = []  # Per-category stats: (category, deduped, merged)
    try:
        scouts_cfg = config.get("scouts", {})
        for scout_name, info in scouts_cfg.items():
            cat = info.get("category", "General")
            raw = None
            if USE_GCS:
                shard_path = GCS_PATH.replace(".json", f"_{cat}.json")
                blob = _get_gcs_blob(shard_path)
                if blob.exists():
                    raw = json.loads(blob.download_as_text())
                else:
                    print(f"[MERGE] Shard '{cat}': not found — skipped", flush=True)
                    merge_stats.append((cat, 0, 0))
                    continue
            else:
                shard_path = LOCAL_PATH.replace(".json", f"_{cat}.json")
                if os.path.exists(shard_path):
                    with open(shard_path, "r") as f:
                        raw = json.load(f)
                else:
                    print(f"[MERGE] Shard '{cat}': not found — skipped", flush=True)
                    merge_stats.append((cat, 0, 0))
                    continue

            # Handle new structured format {"deduped": [...], "enriched": [...]}
            if isinstance(raw, dict) and "deduped" in raw:
                enriched = raw.get("enriched", [])
                deduped = raw.get("deduped", [])
                deduped_n = len(deduped)
                enriched_n = len(enriched)
                if enriched:
                    status = "complete" if enriched_n >= deduped_n else "PARTIAL"
                    print(f"[MERGE] Shard '{cat}': {enriched_n}/{deduped_n} enriched — {status}", flush=True)
                    new_findings.extend(enriched)
                else:
                    print(f"[MERGE] Shard '{cat}': 0/{deduped_n} enriched — no data to merge", flush=True)
                merge_stats.append((cat, deduped_n, enriched_n))
            else:
                # Legacy flat list format
                print(f"[MERGE] Shard '{cat}': {len(raw)} entries (legacy format)", flush=True)
                new_findings.extend(raw)
                merge_stats.append((cat, len(raw), len(raw)))

        # Load existing master log and append — never overwrite history
        existing = []
        if USE_GCS:
            master_blob = _get_gcs_blob(GCS_PATH)
            if master_blob.exists():
                try:
                    existing = json.loads(master_blob.download_as_text())
                except Exception as e:
                    print(f"[ERROR] GCS master log corrupted, refusing to overwrite: {e}", flush=True)
                    return f"MERGE ABORTED: master log at {GCS_PATH} is corrupted — fix manually before re-running."
        else:
            if os.path.exists(LOCAL_PATH):
                with open(LOCAL_PATH, "r") as f:
                    try:
                        existing = json.load(f)
                    except Exception as e:
                        print(f"[ERROR] Master log corrupted, refusing to overwrite: {e}", flush=True)
                        return f"MERGE ABORTED: master log at {LOCAL_PATH} is corrupted — fix manually before re-running."

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

        # Print per-category merge stats
        print(f"\n{'='*40}", flush=True)
        print(f"  MERGE STATS (per category)", flush=True)
        print(f"{'='*40}", flush=True)
        total_deduped = 0
        total_merged = 0
        for cat, deduped_n, merged_n in merge_stats:
            flag = "" if merged_n >= deduped_n else " ⚠ INCOMPLETE"
            print(f"  {cat:<22} deduped: {deduped_n}  merged: {merged_n}{flag}", flush=True)
            total_deduped += deduped_n
            total_merged += merged_n
        print(f"  {'─'*38}", flush=True)
        print(f"  {'TOTAL':<22} deduped: {total_deduped}  merged: {total_merged}", flush=True)
        if total_merged < total_deduped:
            print(f"  ⚠ {total_deduped - total_merged} findings missed due to incomplete strategist runs.", flush=True)
        print(f"{'='*40}\n", flush=True)

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
        sweep_start = time.time()
        user_id = kwargs.get("user_id", "scheduler")
        message = kwargs.get("message", "Execute your daily market sweep.")
        MAX_RETRIES = 3

        for i, batch_agent in enumerate(self.batches):
            print(f"\n{'='*40}\n  STARTING {batch_agent.name}\n{'='*40}", flush=True)

            # Track which pipelines are in this batch
            if isinstance(batch_agent, ParallelAgent):
                pipelines_in_batch = list(batch_agent.sub_agents)
            else:
                pipelines_in_batch = [batch_agent]

            # --- Attempt 1: run the batch as-is (parallel if multiple) ---
            try:
                batch_app = agent_engines.AdkApp(agent=batch_agent)
                for event in batch_app.stream_query(user_id=user_id, message=message):
                    yield event
            except Exception as e:
                print(f"\n[ERROR] {batch_agent.name}: {e}", flush=True)

            # --- Check which sectors need retry ---
            # ADK ParallelAgent swallows 429s in background threads,
            # so the generator can complete "normally" with missing/partial sectors.
            incomplete_sectors = []
            no_shard_sectors = []
            for pipeline in pipelines_in_batch:
                cat = _get_pipeline_category(pipeline)
                if _shard_valid(cat):
                    print(f"[CHECK] {pipeline.name} ({cat}) — shard OK.", flush=True)
                else:
                    # Distinguish partial shard (strategist died mid-way) vs no shard (full failure)
                    shard_path = (GCS_PATH if USE_GCS else LOCAL_PATH).replace(".json", f"_{cat}.json")
                    try:
                        has_shard = (_get_gcs_blob(shard_path).exists() if USE_GCS else os.path.exists(shard_path))
                    except Exception as e:
                        print(f"[WARN] Could not check shard for {cat}, assuming full retry needed: {e}", flush=True)
                        has_shard = False
                    if has_shard:
                        incomplete_sectors.append(pipeline)
                        print(f"[CHECK] {pipeline.name} ({cat}) — partial shard, strategist retry needed.", flush=True)
                    else:
                        no_shard_sectors.append(pipeline)
                        print(f"[CHECK] {pipeline.name} ({cat}) — no shard, full retry needed.", flush=True)

            # --- Strategist-only retry for partial shards ---
            for pipeline in incomplete_sectors:
                cat = _get_pipeline_category(pipeline)
                for attempt in range(2, MAX_RETRIES + 1):
                    print(f"\n[RETRY-STRATEGIST] {cat} — attempt {attempt}/{MAX_RETRIES}, waiting 60s...", flush=True)
                    time.sleep(60)
                    try:
                        strategist_agent = _build_strategist_retry(pipeline)
                        if strategist_agent is None:
                            print(f"[RETRY-STRATEGIST] {cat} — no unenriched findings left, skipping.", flush=True)
                            break
                        retry_app = agent_engines.AdkApp(agent=strategist_agent)
                        for event in retry_app.stream_query(user_id=user_id, message=f"Process the remaining unenriched findings for {cat}."):
                            yield event
                    except Exception as e:
                        print(f"[ERROR] {cat} strategist retry {attempt}/{MAX_RETRIES}: {e}", flush=True)

                    if _shard_valid(cat):
                        print(f"[RETRY-STRATEGIST] {cat} — completed on attempt {attempt}.", flush=True)
                        break
                else:
                    print(f"[PARTIAL] {cat} — strategist retries exhausted, merging what we have.", flush=True)

            # --- Full pipeline retry for sectors with no shard at all ---
            for pipeline in no_shard_sectors:
                cat = _get_pipeline_category(pipeline)
                for attempt in range(2, MAX_RETRIES + 1):
                    print(f"\n[RETRY-FULL] {cat} — attempt {attempt}/{MAX_RETRIES}, waiting 60s...", flush=True)
                    time.sleep(60)
                    try:
                        fresh = _rebuild_pipeline(pipeline)
                        retry_app = agent_engines.AdkApp(agent=fresh)
                        for event in retry_app.stream_query(user_id=user_id, message=message):
                            yield event
                    except Exception as e:
                        print(f"[ERROR] {cat} full retry {attempt}/{MAX_RETRIES}: {e}", flush=True)

                    if _shard_valid(cat):
                        print(f"[RETRY-FULL] {cat} — succeeded on attempt {attempt}.", flush=True)
                        break
                else:
                    print(f"[FATAL] {cat} — failed after {MAX_RETRIES} attempts, no data.", flush=True)

            if i < len(self.batches) - 1:
                print(f"\n[COOLDOWN] Sleeping 60s between batches...", flush=True)
                time.sleep(60)

        print(f"\n{'='*40}\n  STARTING MERGE PHASE\n{'='*40}", flush=True)
        merge_success = False
        for merge_attempt in range(1, MAX_RETRIES + 1):
            try:
                merge_app = agent_engines.AdkApp(agent=self.merge_agent)
                for event in merge_app.stream_query(user_id=user_id, message="Merge the shards."):
                    yield event
                merge_success = True
                break
            except Exception as e:
                print(f"[ERROR] Merge attempt {merge_attempt}/{MAX_RETRIES}: {e}", flush=True)
                if merge_attempt < MAX_RETRIES:
                    print(f"[RETRY-MERGE] Waiting 60s before retry...", flush=True)
                    time.sleep(60)

        if not merge_success:
            print(f"[RETRY-MERGE] Agent retries exhausted — falling back to direct merge.", flush=True)
            try:
                result = merge_sector_shards()
                print(f"[RETRY-MERGE] Direct merge result: {result}", flush=True)
            except Exception as e:
                print(f"[FATAL] Direct merge also failed: {e}", flush=True)

        elapsed = time.time() - sweep_start
        print(f"\n{'='*40}", flush=True)
        print(f"SWEEP COMPLETE — Total time: {int(elapsed // 3600)}h {int((elapsed % 3600) // 60)}m {int(elapsed % 60)}s", flush=True)
        print(f"Token Usage — Input: {TOKEN_METRICS['input']:,} | Output: {TOKEN_METRICS['output']:,} | Total: {TOKEN_METRICS['total']:,}", flush=True)
        print(f"{'='*40}\n", flush=True)

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

    app.set_up()
    print(f"Initializing Master Market Sweep...\n")

    for event in app.stream_query(
        user_id="admin_user",
        message="Execute the daily market sweep. Gather findings from scouts, log them, and print the tabular report."
    ):
        print(event)