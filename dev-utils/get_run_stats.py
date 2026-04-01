#!/usr/bin/env python3
"""Post-run token usage utility for AlphaSnap Agent Engine.

Pulls stats from three sources:
  1. Cloud Monitoring — Gemini publisher token metrics (all locations)
  2. Cloud Logging   — Agent Engine stdout (pipeline progress, memory ops)
  3. Cloud Logging   — Vertex AI prediction logs (generateContent usage metadata)

Usage:
  python3 get_run_stats.py                    # Stats from the last 2 hours
  python3 get_run_stats.py --hours 24         # Stats from the last 24 hours
  python3 get_run_stats.py --date 2026-03-24  # Stats for a specific date
  python3 get_run_stats.py --discover         # List all available aiplatform metrics
"""

import argparse
import json
import re
from datetime import datetime, timedelta, timezone

from google.cloud import monitoring_v3
from google.cloud import logging as cloud_logging

# ── Configuration (from ae_config.config) ──
PROJECT_ID = "marketresearch-agents"
LOCATION = "us-central1"
ENGINE_ID = "2391587873749991424"
MODEL_ID = "gemini-3.1-pro-preview"

# Terminal colors
CYAN = "\033[96m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
BOLD = "\033[1m"
DIM = "\033[2m"
RESET = "\033[0m"


# ══════════════════════════════════════════════════════════════
# SOURCE 1: Cloud Monitoring (publisher model metrics)
# ══════════════════════════════════════════════════════════════

def get_token_metrics(start_time, end_time):
    """Query Cloud Monitoring for Gemini token usage — no location filter."""
    client = monitoring_v3.MetricServiceClient()
    project_name = f"projects/{PROJECT_ID}"

    interval = monitoring_v3.TimeInterval(
        start_time=start_time,
        end_time=end_time,
    )

    # No location filter — Agent Engine may route through global or us-central1
    results = client.list_time_series(
        request={
            "name": project_name,
            "filter": 'metric.type = "aiplatform.googleapis.com/publisher/online_serving/token_count"',
            "interval": interval,
            "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
        }
    )

    input_tokens = 0
    output_tokens = 0
    models_seen = set()

    for ts in results:
        token_type = ts.metric.labels.get("token_type", "unknown")
        model = ts.resource.labels.get("model_id", "unknown")
        location = ts.resource.labels.get("location", "unknown")
        models_seen.add(f"{model} ({location})")

        total = sum(point.value.int64_value or int(point.value.double_value) for point in ts.points)

        if token_type == "input":
            input_tokens += total
        elif token_type == "output":
            output_tokens += total

    return input_tokens, output_tokens, models_seen


def get_model_invocations(start_time, end_time):
    """Query Cloud Monitoring for model invocation count — no location filter."""
    client = monitoring_v3.MetricServiceClient()
    project_name = f"projects/{PROJECT_ID}"

    interval = monitoring_v3.TimeInterval(
        start_time=start_time,
        end_time=end_time,
    )

    try:
        results = client.list_time_series(
            request={
                "name": project_name,
                "filter": 'metric.type = "aiplatform.googleapis.com/publisher/online_serving/model_invocation_count"',
                "interval": interval,
                "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
            }
        )
        total = 0
        for ts in results:
            total += sum(point.value.int64_value or int(point.value.double_value) for point in ts.points)
        return total
    except Exception:
        return None


def discover_metrics():
    """List all aiplatform metrics available in the project."""
    client = monitoring_v3.MetricServiceClient()
    project_name = f"projects/{PROJECT_ID}"

    print(f"\n{BOLD}{CYAN}Discovering aiplatform metrics in {PROJECT_ID}...{RESET}\n")

    descriptors = client.list_metric_descriptors(
        request={
            "name": project_name,
            "filter": 'metric.type = starts_with("aiplatform.googleapis.com/")',
        }
    )

    found = []
    for desc in descriptors:
        found.append({
            "type": desc.type,
            "display": desc.display_name,
            "kind": str(desc.metric_kind),
            "labels": [l.key for l in desc.labels],
        })

    if not found:
        print(f"  {YELLOW}No aiplatform metrics found. The project may not have any active model endpoints.{RESET}")
        return

    # Group by prefix
    groups = {}
    for m in found:
        prefix = "/".join(m["type"].split("/")[:3])
        groups.setdefault(prefix, []).append(m)

    for prefix, metrics in sorted(groups.items()):
        print(f"  {CYAN}{prefix}{RESET}")
        for m in metrics:
            labels_str = ", ".join(m["labels"]) if m["labels"] else "none"
            print(f"    {m['type'].split('/')[-1]:40} labels: {DIM}{labels_str}{RESET}")
    print(f"\n  {GREEN}Total: {len(found)} metrics{RESET}\n")


# ══════════════════════════════════════════════════════════════
# SOURCE 2: Cloud Logging — Agent Engine stdout
# ══════════════════════════════════════════════════════════════

def get_agent_logs(start_time, end_time):
    """Pull Agent Engine stdout logs and extract pipeline markers."""
    client = cloud_logging.Client(project=PROJECT_ID)

    start_str = start_time.isoformat("T") + "Z" if not start_time.tzinfo else start_time.isoformat()
    end_str = end_time.isoformat("T") + "Z" if not end_time.tzinfo else end_time.isoformat()

    filter_str = (
        f'resource.type="aiplatform.googleapis.com/ReasoningEngine" '
        f'AND resource.labels.location="{LOCATION}" '
        f'AND resource.labels.reasoning_engine_id="{ENGINE_ID}" '
        f'AND timestamp>="{start_str}" '
        f'AND timestamp<="{end_str}"'
    )

    entries = list(client.list_entries(filter_=filter_str, order_by=cloud_logging.ASCENDING))

    progress_lines = []
    memory_ops = []
    errors = []
    token_lines = []

    for entry in entries:
        text = entry.payload if isinstance(entry.payload, str) else str(entry.payload)

        if "[PROGRESS]" in text:
            progress_lines.append(text.strip())
        if "[TOKEN_USAGE]" in text:
            token_lines.append(text.strip())
        if "[Saving to Memory]" in text or "[Memory Baseline]" in text or "[Global Baseline]" in text:
            memory_ops.append(text.strip())
        if "ERROR" in text or "Traceback" in text:
            errors.append(text.strip())

    # Extract timing
    run_start = None
    run_end = None
    time_re = re.compile(r"\[PROGRESS\]\s+(\d{2}:\d{2}:\d{2})")
    for line in progress_lines:
        m = time_re.search(line)
        if m:
            if run_start is None:
                run_start = m.group(1)
            run_end = m.group(1)

    # Parse token usage from [TOKEN_USAGE] lines
    token_re = re.compile(r"\[TOKEN_USAGE\]\s+(\S+)\s+\|\s+input=(\d+)\s+\|\s+output=(\d+)\s+\|\s+total=(\d+)")
    total_input = 0
    total_output = 0
    total_total = 0
    per_agent = {}
    for line in token_lines:
        m = token_re.search(line)
        if m:
            agent, inp, out, tot = m.group(1), int(m.group(2)), int(m.group(3)), int(m.group(4))
            total_input += inp
            total_output += out
            total_total += tot
            if agent not in per_agent:
                per_agent[agent] = {"input": 0, "output": 0, "total": 0, "calls": 0}
            per_agent[agent]["input"] += inp
            per_agent[agent]["output"] += out
            per_agent[agent]["total"] += tot
            per_agent[agent]["calls"] += 1

    sector_completions = [l for l in progress_lines if "complete" in l.lower()]

    return {
        "total_log_entries": len(entries),
        "progress_lines": progress_lines,
        "memory_ops_count": len(memory_ops),
        "errors": errors,
        "token_usage": {
            "input": total_input,
            "output": total_output,
            "total": total_total,
            "per_agent": per_agent,
            "llm_calls": len(token_lines),
        },
        "run_start": run_start,
        "run_end": run_end,
        "scouts_completed": len([l for l in sector_completions if "Scout" in l]),
        "data_engineers_completed": len([l for l in sector_completions if "DE " in l]),
        "strategists_completed": len([l for l in sector_completions if "Strategist" in l]),
        "merge_completed": any("Merge" in l for l in sector_completions),
    }


# ══════════════════════════════════════════════════════════════
# SOURCE 3: Cloud Logging — Vertex AI prediction request logs
# ══════════════════════════════════════════════════════════════

def get_prediction_token_usage(start_time, end_time):
    """Search Vertex AI prediction/LLM request logs for token usage metadata.

    The Agent Engine makes internal generateContent calls to Gemini.
    These may be logged under different log names depending on config.
    """
    client = cloud_logging.Client(project=PROJECT_ID)

    start_str = start_time.isoformat("T") + "Z" if not start_time.tzinfo else start_time.isoformat()
    end_str = end_time.isoformat("T") + "Z" if not end_time.tzinfo else end_time.isoformat()

    # Try multiple log sources where token usage might appear
    filters = [
        # Gemini API / Vertex AI prediction logs
        (
            f'resource.type="aiplatform.googleapis.com/ReasoningEngine" '
            f'AND resource.labels.reasoning_engine_id="{ENGINE_ID}" '
            f'AND (textPayload:"token_count" OR textPayload:"tokenCount" OR textPayload:"usage_metadata" OR textPayload:"usageMetadata") '
            f'AND timestamp>="{start_str}" AND timestamp<="{end_str}"'
        ),
        # General Vertex AI prediction logs with usage
        (
            f'(resource.type="aiplatform.googleapis.com/Endpoint" OR resource.type="audited_resource") '
            f'AND (jsonPayload.response.usageMetadata:* OR jsonPayload.usageMetadata:*) '
            f'AND timestamp>="{start_str}" AND timestamp<="{end_str}"'
        ),
    ]

    total_input = 0
    total_output = 0
    total_calls = 0
    source_found = None

    for i, filter_str in enumerate(filters):
        try:
            entries = list(client.list_entries(filter_=filter_str, page_size=500))
            if not entries:
                continue

            source_found = f"filter_{i+1}"

            for entry in entries:
                payload = entry.payload if isinstance(entry.payload, str) else ""
                json_payload = entry.payload if isinstance(entry.payload, dict) else {}

                # Try structured JSON payload first
                usage = (
                    json_payload.get("response", {}).get("usageMetadata")
                    or json_payload.get("usageMetadata")
                    or json_payload.get("usage_metadata")
                )

                if usage and isinstance(usage, dict):
                    total_input += usage.get("promptTokenCount", 0) or usage.get("prompt_token_count", 0) or 0
                    total_output += usage.get("candidatesTokenCount", 0) or usage.get("candidates_token_count", 0) or 0
                    total_calls += 1
                    continue

                # Try parsing from textPayload
                if isinstance(payload, str) and ("token_count" in payload or "tokenCount" in payload):
                    # Try to extract JSON from the text
                    for pattern in [r'\{[^{}]*token[^{}]*\}', r'\{[^{}]*Token[^{}]*\}']:
                        m = re.search(pattern, payload)
                        if m:
                            try:
                                data = json.loads(m.group())
                                inp = data.get("promptTokenCount", 0) or data.get("prompt_token_count", 0) or 0
                                out = data.get("candidatesTokenCount", 0) or data.get("candidates_token_count", 0) or 0
                                if inp or out:
                                    total_input += inp
                                    total_output += out
                                    total_calls += 1
                            except json.JSONDecodeError:
                                pass

            if total_calls > 0:
                break  # Found data, no need to try other filters

        except Exception as e:
            continue

    return total_input, total_output, total_calls, source_found


# ══════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════

def parse_args():
    parser = argparse.ArgumentParser(description="AlphaSnap post-run token usage stats")
    parser.add_argument("--hours", type=float, default=2, help="Look back N hours (default: 2)")
    parser.add_argument("--date", type=str, help="Specific date YYYY-MM-DD (overrides --hours)")
    parser.add_argument("--discover", action="store_true", help="List all available aiplatform metrics")
    return parser.parse_args()


def main():
    args = parse_args()

    if args.discover:
        discover_metrics()
        return

    if args.date:
        day = datetime.strptime(args.date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        start_time = day
        end_time = day + timedelta(days=1)
        window_label = args.date
    else:
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=args.hours)
        window_label = f"last {args.hours}h"

    print(f"\n{BOLD}{CYAN}{'='*60}")
    print(f"  AlphaSnap Run Stats — {window_label}")
    print(f"  Engine: {ENGINE_ID} | Project: {PROJECT_ID}")
    print(f"{'='*60}{RESET}\n")

    # ── 1. Cloud Monitoring: Publisher Token Metrics ──
    print(f"{BOLD}{CYAN}📊 Token Usage — Cloud Monitoring (Publisher){RESET}")
    print(f"{CYAN}{'-'*50}{RESET}")
    monitoring_found = False
    try:
        input_tokens, output_tokens, models_seen = get_token_metrics(start_time, end_time)
        total_tokens = input_tokens + output_tokens
        invocations = get_model_invocations(start_time, end_time)

        if total_tokens > 0:
            monitoring_found = True
            print(f"  Input Tokens:    {GREEN}{input_tokens:>12,}{RESET}")
            print(f"  Output Tokens:   {GREEN}{output_tokens:>12,}{RESET}")
            print(f"  Total Tokens:    {BOLD}{GREEN}{total_tokens:>12,}{RESET}")
            if invocations:
                print(f"  Model Calls:     {GREEN}{invocations:>12,}{RESET}")
                avg = total_tokens // invocations if invocations else 0
                print(f"  Avg Tokens/Call: {GREEN}{avg:>12,}{RESET}")
            if models_seen:
                print(f"  Models:          {DIM}{', '.join(models_seen)}{RESET}")
        else:
            print(f"  {YELLOW}No publisher metrics found.{RESET}")
            print(f"  {DIM}(Agent Engine internal calls may not emit publisher metrics){RESET}")
    except Exception as e:
        print(f"  {RED}Error: {e}{RESET}")

    # ── 2. Cloud Logging: Prediction Token Usage ──
    print(f"\n{BOLD}{CYAN}📊 Token Usage — Prediction Logs{RESET}")
    print(f"{CYAN}{'-'*50}{RESET}")
    try:
        pred_input, pred_output, pred_calls, pred_source = get_prediction_token_usage(start_time, end_time)

        if pred_calls > 0:
            pred_total = pred_input + pred_output
            print(f"  Input Tokens:    {GREEN}{pred_input:>12,}{RESET}")
            print(f"  Output Tokens:   {GREEN}{pred_output:>12,}{RESET}")
            print(f"  Total Tokens:    {BOLD}{GREEN}{pred_total:>12,}{RESET}")
            print(f"  LLM Calls:       {GREEN}{pred_calls:>12,}{RESET}")
            avg = pred_total // pred_calls if pred_calls else 0
            print(f"  Avg Tokens/Call: {GREEN}{avg:>12,}{RESET}")
        else:
            print(f"  {YELLOW}No token data found in prediction logs.{RESET}")
            if not monitoring_found:
                print(f"  {DIM}Tip: Run with --discover to see what metrics exist.{RESET}")
                print(f"  {DIM}Consider enabling request-response logging or the{RESET}")
                print(f"  {DIM}BigQuery Agent Analytics Plugin for per-run tracking.{RESET}")
    except Exception as e:
        print(f"  {RED}Error: {e}{RESET}")

    # ── 3. Cloud Logging: Agent Pipeline Activity ──
    print(f"\n{BOLD}{CYAN}📋 Pipeline Activity — Agent Engine Logs{RESET}")
    print(f"{CYAN}{'-'*50}{RESET}")
    try:
        logs = get_agent_logs(start_time, end_time)

        if logs["total_log_entries"] == 0:
            print(f"  {YELLOW}No agent logs found for this window.{RESET}")
        else:
            print(f"  Log Entries:     {logs['total_log_entries']:>8,}")
            if logs["run_start"] and logs["run_end"]:
                print(f"  Run Window:      {logs['run_start']} → {logs['run_end']} UTC")
            print(f"  Scouts:          {logs['scouts_completed']:>8}")
            print(f"  Data Engineers:  {logs['data_engineers_completed']:>8}")
            print(f"  Strategists:     {logs['strategists_completed']:>8}")
            print(f"  Merge:           {'✅' if logs['merge_completed'] else '❌':>8}")
            print(f"  Memory Ops:      {logs['memory_ops_count']:>8}")

            # Token usage from after_model_callback
            tu = logs["token_usage"]
            if tu["llm_calls"] > 0:
                print(f"\n{BOLD}{CYAN}📊 Token Usage — Agent Callbacks{RESET}")
                print(f"{CYAN}{'-'*50}{RESET}")
                print(f"  Input Tokens:    {GREEN}{tu['input']:>12,}{RESET}")
                print(f"  Output Tokens:   {GREEN}{tu['output']:>12,}{RESET}")
                print(f"  Total Tokens:    {BOLD}{GREEN}{tu['total']:>12,}{RESET}")
                print(f"  LLM Calls:       {GREEN}{tu['llm_calls']:>12,}{RESET}")
                avg = tu['total'] // tu['llm_calls'] if tu['llm_calls'] else 0
                print(f"  Avg Tokens/Call: {GREEN}{avg:>12,}{RESET}")

                if tu["per_agent"]:
                    print(f"\n  {CYAN}Per-Agent Breakdown:{RESET}")
                    print(f"  {'Agent':<30} {'Input':>10} {'Output':>10} {'Total':>10} {'Calls':>6}")
                    print(f"  {'-'*70}")
                    for agent, stats in sorted(tu["per_agent"].items(), key=lambda x: x[1]["total"], reverse=True):
                        print(f"  {agent:<30} {stats['input']:>10,} {stats['output']:>10,} {stats['total']:>10,} {stats['calls']:>6}")

            if logs["errors"]:
                print(f"\n  {RED}⚠ Errors ({len(logs['errors'])}):{RESET}")
                for err in logs["errors"][:5]:
                    print(f"    {RED}{err[:120]}{RESET}")

            if logs["progress_lines"]:
                print(f"\n  {CYAN}Timeline:{RESET}")
                for line in logs["progress_lines"]:
                    print(f"    {line}")

    except Exception as e:
        print(f"  {RED}Error: {e}{RESET}")

    print(f"\n{CYAN}{'='*60}{RESET}\n")


if __name__ == "__main__":
    main()
