"""
Recovery script for broken / incomplete market sweep runs.

Checks all sector shards (GCS or local), re-runs any missing or partial
sectors, then merges everything into the master log.

Usage:
    # Diagnose only — show shard status, don't fix anything
    python3 fix_broken_run.py --dry-run

    # Re-run missing sectors + merge
    python3 fix_broken_run.py

    # Re-run a specific sector only (skip shard check for others)
    python3 fix_broken_run.py --sector "Space & Defense"

    # Skip sector re-runs, just force the merge
    python3 fix_broken_run.py --merge-only
"""
import argparse
import asyncio
import json
import sys
import os
import time

# Ensure parent dir is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from market_team import (
    check_auth, config, build_sector_pipelines,
    _shard_valid, _get_pipeline_category, _rebuild_pipeline,
    _build_strategist_retry, _get_unenriched_findings,
    merge_sector_shards,
    USE_GCS, GCS_PATH, LOCAL_PATH, _get_gcs_blob,
)
from vertexai import agent_engines


MAX_RETRIES = 3
RETRY_WAIT = 60  # seconds


# ------------------------------------------------------------------
# Diagnostics
# ------------------------------------------------------------------
def diagnose_shards():
    """Return a dict of {category: status} for every enabled scout."""
    scouts_cfg = config.get("scouts", {})
    report = {}

    for scout_name, info in scouts_cfg.items():
        if not info.get("enabled", True):
            continue
        cat = info.get("category", "General")

        # Does a shard exist at all?
        shard_path = (GCS_PATH if USE_GCS else LOCAL_PATH).replace(".json", f"_{cat}.json")
        try:
            has_shard = (_get_gcs_blob(shard_path).exists() if USE_GCS else os.path.exists(shard_path))
        except Exception:
            has_shard = False

        if not has_shard:
            report[cat] = {"status": "MISSING", "deduped": 0, "enriched": 0, "pipeline_name": f"{scout_name}_Pipeline"}
            continue

        # Read shard contents
        try:
            if USE_GCS:
                raw = json.loads(_get_gcs_blob(shard_path).download_as_text())
            else:
                with open(shard_path, "r") as f:
                    raw = json.load(f)
        except Exception as e:
            report[cat] = {"status": f"READ_ERROR: {e}", "deduped": 0, "enriched": 0, "pipeline_name": f"{scout_name}_Pipeline"}
            continue

        if isinstance(raw, dict) and "deduped" in raw:
            d = len(raw.get("deduped", []))
            e = len(raw.get("enriched", []))
            if d == 0:
                st = "EMPTY"
            elif e >= d:
                st = "COMPLETE"
            else:
                st = f"PARTIAL ({e}/{d})"
        else:
            st = f"LEGACY ({len(raw)} entries)"
            d, e = len(raw), len(raw)

        report[cat] = {"status": st, "deduped": d, "enriched": e, "pipeline_name": f"{scout_name}_Pipeline"}

    return report


def print_report(report):
    print(f"\n{'='*60}")
    print(f"  SHARD STATUS ({'GCS' if USE_GCS else 'Local'})")
    print(f"{'='*60}")
    for cat, info in report.items():
        flag = "" if info["status"] == "COMPLETE" else " <-- needs fix"
        print(f"  {cat:<22} {info['status']:<24}{flag}")
    print(f"{'='*60}\n")


# ------------------------------------------------------------------
# Recovery: re-run a single sector pipeline via local AdkApp
# ------------------------------------------------------------------
def run_sector_pipeline(pipeline, cat):
    """Run a full sector pipeline (Scout -> DE -> Strategist) locally."""
    print(f"\n[RECOVERY] Running full pipeline for {cat}...")
    app = agent_engines.AdkApp(agent=pipeline)
    for event in app.stream_query(
        user_id="recovery",
        message=f"Execute your daily market sweep for {cat}."
    ):
        pass  # events consumed; agents write to shard via tools
    print(f"[RECOVERY] Pipeline for {cat} finished.")


def run_strategist_retry(pipeline, cat):
    """Run just the strategist on unenriched findings from a partial shard."""
    unenriched = _get_unenriched_findings(cat)
    if not unenriched:
        print(f"[RECOVERY] {cat}: no unenriched findings — already complete.")
        return

    print(f"[RECOVERY] {cat}: {len(unenriched)} unenriched findings, running strategist retry...")
    strategist = _build_strategist_retry(pipeline)
    if strategist is None:
        print(f"[RECOVERY] {cat}: _build_strategist_retry returned None — skipping.")
        return

    app = agent_engines.AdkApp(agent=strategist)
    for event in app.stream_query(
        user_id="recovery",
        message=f"Process the remaining unenriched findings for {cat}."
    ):
        pass
    print(f"[RECOVERY] Strategist retry for {cat} finished.")


def recover_sector(cat, info, pipelines_by_name):
    """Attempt to recover a single sector. Returns True if shard is now valid."""
    status = info["status"]
    pipeline_name = info["pipeline_name"]

    # Find the matching pipeline object for rebuilds
    original = pipelines_by_name.get(pipeline_name)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            if status == "MISSING":
                # Full pipeline needed
                fresh = _rebuild_pipeline(original) if original else None
                if fresh is None:
                    print(f"[ERROR] Cannot rebuild pipeline for {cat} — no matching config.")
                    return False
                run_sector_pipeline(fresh, cat)
            elif "PARTIAL" in status:
                # Strategist-only retry
                fresh = _rebuild_pipeline(original) if original else None
                if fresh is None:
                    print(f"[ERROR] Cannot rebuild pipeline for {cat}.")
                    return False
                run_strategist_retry(fresh, cat)
            else:
                return True  # COMPLETE, EMPTY, LEGACY — nothing to do

            if _shard_valid(cat):
                print(f"[RECOVERY] {cat} — fixed on attempt {attempt}.")
                return True
            else:
                print(f"[RECOVERY] {cat} — still incomplete after attempt {attempt}/{MAX_RETRIES}.")

        except Exception as e:
            print(f"[ERROR] {cat} recovery attempt {attempt}/{MAX_RETRIES}: {e}")

        if attempt < MAX_RETRIES:
            print(f"[RECOVERY] Waiting {RETRY_WAIT}s before retry...")
            time.sleep(RETRY_WAIT)

    print(f"[FATAL] {cat} — could not recover after {MAX_RETRIES} attempts.")
    return False


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Fix broken market sweep runs.")
    parser.add_argument("--dry-run", action="store_true", help="Diagnose only, don't fix anything.")
    parser.add_argument("--merge-only", action="store_true", help="Skip sector recovery, just run merge.")
    parser.add_argument("--sector", type=str, default=None, help="Recover a specific sector only (e.g. 'Space & Defense').")
    args = parser.parse_args()

    if not check_auth():
        sys.exit(1)

    # --- Diagnose ---
    report = diagnose_shards()
    print_report(report)

    needs_fix = {cat: info for cat, info in report.items() if info["status"] not in ("COMPLETE", "EMPTY", "LEGACY")}

    if args.sector:
        if args.sector not in report:
            print(f"[ERROR] Sector '{args.sector}' not found. Available: {list(report.keys())}")
            sys.exit(1)
        needs_fix = {args.sector: report[args.sector]}

    if args.dry_run:
        if needs_fix:
            print(f"Would recover: {list(needs_fix.keys())}")
        else:
            print("All shards look good. Nothing to recover.")
        print("Would then run merge_sector_shards().")
        return

    # --- Recover missing/partial sectors ---
    if not args.merge_only and needs_fix:
        print(f"\n{'='*60}")
        print(f"  RECOVERING {len(needs_fix)} SECTOR(S)")
        print(f"{'='*60}")

        # Build pipelines once so _rebuild_pipeline has objects to match against
        pipelines = build_sector_pipelines()
        pipelines_by_name = {p.name: p for p in pipelines}

        for cat, info in needs_fix.items():
            recover_sector(cat, info, pipelines_by_name)

        # Re-diagnose
        print("\n--- Post-recovery status ---")
        report = diagnose_shards()
        print_report(report)

    elif not args.merge_only:
        print("All sectors complete — skipping recovery.\n")

    # --- Merge ---
    print(f"{'='*60}")
    print(f"  RUNNING MERGE")
    print(f"{'='*60}")
    try:
        result = merge_sector_shards()
        print(f"\n[RESULT] {result}")
    except Exception as e:
        print(f"\n[FATAL] Merge failed: {e}")
        sys.exit(1)

    print(f"\nDone.")


if __name__ == "__main__":
    main()
