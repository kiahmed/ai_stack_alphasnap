# Agent Configuration and Storage Toggle Walkthrough

I have successfully updated the agentic stack to support hybrid storage, externalized prompts, and high-performance model configuration with separated locations.

## Changes Made

### 1. Hybrid Storage Toggle
- **Feature:** Seamlessly switch between local JSON storage and Google Cloud Storage (GCS) for findings.
- **Config:** Added `use_gcs` toggle in `values.yaml` along with `local_path` and `gcs_path`.
- **Logic:** `market_team.py` dynamically selects the storage backend based on the toggle.

### 2. Externalized Prompts
- **Feature:** Manage all agent instructions from `values.yaml` without touching Python code.
- **Fix:** Joined multi-line instructions and corrected Agent parameter names (`instruction`, `sub_agents`, `generate_content_config`).

### 3. Gemini 3.1 Pro & Split Locations
- **Feature:** Support for the latest **Gemini 3.1 Pro Preview** model.
- **Fix:** Discovered that these models require the `global` location endpoint for programmatic generation.
- **Design:** Implemented "Split Location" configuration. The model uses `global` via environment variable overrides, while all other resources (storage, deployment) remain pinned to your regional location (`us-central1`).

### 4. Ruthless CIO & Detailed Instructions
- **Feature:** Added a "Ruthless" persona to the CIO to ensure high data quality and avoid hallucinations.
- **Logic:** Implemented a critical filtering step where the CIO cross-references scout findings against the memory log to discard duplicates.
- **Scout Update:** Configured scouts with a high-volume data gathering persona to provide raw dumps for the CIO to filter.

### 5. Parameterized Memory Log
- **Feature:** `read_memory_log` now accepts a `memory_limit` parameter.
- **Usage:** Scouts use a default limit of 10 to stay focused on recent events, while the CIO is instructed to use `memory_limit=50` for a much deeper historical baseline check during filtering.

### 6. ADK-Native Google Search
- **Feature:** Correctly integrated the `google.adk.tools.google_search` tool for the scouts.
- **Fix:** Resolved Pydantic validation errors by switching from generic GenAI grounding to the official ADK search tool, enabling live web access for Robotics and Crypto analysis.

### 7. Agent-as-Tool Refactor
- **Feature:** Decoupled scouts from `sub_agents` and integrated them directly into the CIO's `tools` array using the `AgentTool` wrapper.
- **Benefit:** This allows the CIO to "consult" scouts as discrete data sources without formal control transfers, keeping the Supervisor-Coordinator logic clean and centralized.

### 8. Targeted Search for Cost Efficiency
- **Feature:** Scouts are now configured for **targeted** data gathering specifically aimed at catalysts and technicals.
- **Enhanced Sources:** Explicitly queries alternative sources like **FinTwit**, **Reddit**, and specialist blogs alongside mainstream news to catch early-stage developments and M&A rumors while minimizing generic search noise.
- **Ticker Precision:** Mandates searching for today's technical data (buy/sell walls, pivots, gamma flips) only when relevant tickers are identified, optimizing for relevance and token usage.

### 9. Fine-Tuned CIO Analysis
- **Feature:** Refined the CIO's persona to be a "Ruthless Quantitative Lead," focusing on perfect integrity between news and speculation.
- **Enhanced Logic:** Implemented an "UPDATE" rule for the memory log—if a finding has new data (e.g., a price move or partnership confirmation), it is kept and updated even if the core event was previously logged.
- **Sector Alignment:** Consolidated reports now strictly follow the **Physical AI & Embodied Robotics** and **Crypto & Blockchain** sectors.
- **Reporting Standards:** Mandated sector-specific ETF links (e.g., BOTZ, BITO) for macro findings to provide immediate investment context.

### 10. Dynamic Scout Management & Toggles
- **Feature:** Introduced a scalable agent architecture where scouts are defined in `values.yaml` rather than hard-coded in Python.
- **Toggle System:** Each scout (Robotics, Crypto, AI Stack, Space/Defense, Power/Energy, Strategic Minerals) has an `enabled: true/false` flag.
- **Dynamic CIO:** The Chief Investment Officer automatically detects which specialists are online during initialization and adds them to its toolset, allowing for a flexible, cost-optimized research roster.

## Verification Results

### Agent-as-Tool Coordination Test
- Verified via trace logs that the **Chief Investment Officer** successfully triggers `Robotics_Scout` and `Crypto_Scout` as direct tool calls.
- Confirmed that the findings from these "tool-scouts" are correctly returned to the CIO for filtering and logging.

### Parameterized Tool Call Test
- Verified via trace logs that the **Chief Investment Officer** successfully passes `memory_limit: 50` when establishing its baseline.
- Verified that the simulation runs without validation errors after switching to the native ADK search tool.

### Storage Test
- Verified successful read/write to both local `market_findings_log.json` and the `gs://marketresearch-agents/` bucket.

### Model Execution
- Successfully ran `market_team.py` with `gemini-3.1-pro-preview`.
- Confirmed that **Chief Investment Officer** correctly delegated tasks to **Crypto Scout** and **Robotics Scout** using the global endpoint.

```
🚀 Initializing Chief_Investment_Officer with gemini-3.1-pro-preview...
{'author': 'Chief_Investment_Officer', 'actions': {'transfer_to_agent': 'Crypto_Scout'}}
```

## How to Use
1. Edit `values.yaml` to change prompts or toggle storage.
2. Run `python3 market_team.py` to start the simulation.
