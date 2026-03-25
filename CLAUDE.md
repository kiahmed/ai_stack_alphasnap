# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is AlphaSnap

AlphaSnap is an AI-powered daily market intelligence system deployed on Google Cloud's Vertex AI Agent Engines (ADK framework). It runs 6 parallel sector pipelines — Robotics, Crypto, AI Stack, Space & Defense, Power & Energy, and Strategic Minerals — each using a three-stage Scout → Data Engineer → Strategist pipeline to gather, deduplicate, and report market findings. Findings are persisted to a rolling JSON log (local or GCS) for next-day deduplication.

## Common Commands

### Run locally
```bash
# Authenticate first
source ae_config.config
export GOOGLE_APPLICATION_CREDENTIALS=$(pwd)/service_account.json

python3 market_team.py
```

### Test a single scout in isolation
```bash
python3 test_single_scout.py   # Tests Robotics pipeline only
```

### Deploy to Vertex AI Agent Engines
```bash
python3 deploy_agent.py        # Creates remote engine, writes ENGINE_ID back to ae_config.config
```

### Set up / update Cloud Scheduler
```bash
./setup_scheduler.sh           # Creates "market-team-daily-sweeper" job (7:15 AM EST daily)
```

### Manually trigger a run
```bash
./trigger_scheduler.sh         # Uses TRIGGER_MODE from ae_config.config: "scheduler" or "api"
```

### Deduplicate the findings log
```bash
python3 dedupe_lts.py          # 80% similarity threshold via SequenceMatcher, per-category
```

### Dev environment setup
```bash
./make_dev_evn_ready.sh        # Sets up GCP auth and quota project config
./apply_iam_roles.sh           # Grants IAM roles to the service account
```

## Architecture

### Configuration split
- `ae_config.config` — Infrastructure: PROJECT_ID, ENGINE_ID, SA_EMAIL, STAGING_BUCKET, scheduler config, Python requirements for deployment
- `values.yaml` — Everything runtime: agent models, enabled scouts, `use_gcs` toggle, `memory_limit`, and **all agent prompts** (Scout, Data Engineer, Strategist personas). Edit prompts here without touching Python.

### Three-stage pipeline per sector (market_team.py)
Each enabled scout in `values.yaml` gets its own `SequentialAgent` chain:

1. **Scout** (worker model) — `safe_google_search` + `url_context` → raw findings from the last 24 hours
2. **Data Engineer** (worker model) — `read_memory_log` (fetches last N entries as dedup baseline) + `safe_google_search` → filters duplicates, extracts Direct/Indirect/Market Dynamics insights with sentiment
3. **Strategist** (supervisor model) — `append_to_memory_log` + `log_progress` → appends each finding as a separate entry (one entry per topic, not one per run), generates markdown sector report

### Parallel orchestration
`ParallelMarketSweep` (inherits `SequentialAgent`) runs all sector pipelines concurrently with `asyncio.Semaphore(2)` to avoid API quota exhaustion and a 10-second stagger between sector starts. Each sector writes to a category shard (`market_findings_log_{Category}.json`), then all shards are merged into the master log.

### Hybrid storage
Toggled by `storage.use_gcs` in `values.yaml`:
- **GCS mode**: reads/writes `gs://marketresearch-agents/market_findings_log.json` + category shards
- **Local mode**: reads/writes local JSON files with identical schema

Both modes use the same `read_memory_log` / `append_to_memory_log` tool interface.

### Memory entry schema
```json
{
  "timestamp": "ISO-8601",
  "category": "Robotics",
  "finding": "...",
  "insights_sentiment": "Direct: ... | Indirect: ... | Sentiment: Bullish",
  "guidance_play": "...",
  "price_levels": "..."
}
```

### Deployment flow
`deploy_agent.py` → Vertex AI Reasoning Engine (host) → Cloud Scheduler triggers `streamQuery` daily → `AdkApp` dispatches to `ParallelMarketSweep` orchestrator.

## Known Issue: Pydantic Validation Error

The `ParallelMarketSweep` class (lines ~265–313 in `market_team.py`) causes a Pydantic `ValidationError` for `InvocationContext` because the custom async orchestrator is not recognized as a valid `BaseAgent` instance by the ADK framework. The last stable git checkpoint is tagged in the commit history as *"last change before breaking CIO architecture"*. Fixing this requires ensuring `ParallelMarketSweep` fully conforms to the ADK `BaseAgent` / `SequentialAgent` Pydantic contract.

## Key Dependencies
- `google-cloud-aiplatform[agent_engines,adk]` — ADK agent framework and Vertex AI
- `google-cloud-storage` — GCS hybrid storage
- `pyyaml` — values.yaml loading
- GCP project: `marketresearch-agents`, region: `us-central1`, Preview models use `model_location: "global"`
