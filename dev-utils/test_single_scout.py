"""Run a single sector pipeline end-to-end.

Exercises:
  1. Scout → DE → Strategist live run (via AdkApp)
  2. append_to_memory_log writes enriched entries to the sector shard
  3. dedup_findings consults the master log as baseline
  4. entry_id generation (XXX-MMDDYY-YYY) for the run date
  5. merge_sector_shards copies enriched into the master log

Modes:
  default    — master log copied to a tempdir, USE_GCS forced False. Real logs untouched.
  --use-gcs  — respects values.yaml. If use_gcs=true, runs live against GCS master + shards.

Usage:
    cd arboryx.ai
    python3 dev-utils/test_single_scout.py                                          # Robotics, isolated
    python3 dev-utils/test_single_scout.py --scout Power_Energy_Scout               # Power & Energy, isolated
    python3 dev-utils/test_single_scout.py --scout Power_Energy_Scout --use-gcs     # Power & Energy, live GCS
"""
import argparse
import asyncio
import json
import os
import shutil
import sys
import tempfile
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import market_team as mt
from vertexai import agent_engines


def parse_args():
    ap = argparse.ArgumentParser(description="Run one sector pipeline with verification")
    ap.add_argument("--scout", default="Robotics_Scout",
                    help="Scout name from values.yaml (e.g. Power_Energy_Scout)")
    ap.add_argument("--message", default=None, help="Override the user prompt")
    ap.add_argument("--use-gcs", action="store_true",
                    help="Respect values.yaml storage config (live GCS). Default: isolated tempdir local mode.")
    return ap.parse_args()


def setup_isolation():
    """Copy master log into a tempdir and force mt into local mode pointing at it."""
    real_log = os.path.join(os.path.dirname(os.path.abspath(__file__)), "market_findings_log.json")
    tmp = tempfile.mkdtemp(prefix="arboryx_sector_test_")
    iso_log = os.path.join(tmp, "market_findings_log.json")
    if os.path.exists(real_log):
        shutil.copy(real_log, iso_log)
    else:
        with open(iso_log, "w") as f:
            json.dump([], f)
    mt.USE_GCS = False
    mt.LOCAL_PATH = iso_log
    mt.MEMORY_FILE = iso_log
    print(f"[test_single_scout] Isolated log: {iso_log} (seeded from {real_log})", flush=True)
    return tmp, iso_log


def _load_master():
    """Read master log as a list of entries, from GCS or local depending on mt.USE_GCS."""
    if mt.USE_GCS:
        blob = mt._get_gcs_blob(mt.GCS_PATH)
        if not blob.exists():
            return []
        return json.loads(blob.download_as_text())
    if not os.path.exists(mt.LOCAL_PATH):
        return []
    with open(mt.LOCAL_PATH) as f:
        return json.load(f)


def _load_shard(category: str):
    """Read sector shard dict {deduped, enriched} from GCS or local. Returns None if missing."""
    base = mt.GCS_PATH if mt.USE_GCS else mt.LOCAL_PATH
    shard_path = base.replace(".json", f"_{category}.json")
    if mt.USE_GCS:
        blob = mt._get_gcs_blob(shard_path)
        if not blob.exists():
            return None, shard_path
        return json.loads(blob.download_as_text()), shard_path
    if not os.path.exists(shard_path):
        return None, shard_path
    with open(shard_path) as f:
        return json.load(f), shard_path


async def run_pipeline(scout_name: str, message: str):
    if not mt.check_auth():
        return None
    pipelines = mt.build_sector_pipelines()
    pipeline = next((p for p in pipelines if scout_name in p.name), None)
    if pipeline is None:
        print(f"❌ No pipeline for {scout_name!r}. Available: {[p.name for p in pipelines]}")
        return None
    app = agent_engines.AdkApp(agent=pipeline)
    print(f"🚀 Launching {pipeline.name} (USE_GCS={mt.USE_GCS})…", flush=True)
    try:
        async for event in app.async_stream_query(user_id="test_user", message=message):
            print(event, flush=True)
    except Exception as e:
        print(f"🔥 FATAL ERROR: {e}", flush=True)
    return pipeline


def verify(category: str) -> bool:
    abbr = mt.CATEGORY_ABBR.get(category, "???")
    pre_master = _load_master()
    shard, shard_path = _load_shard(category)

    print(f"\n{'='*60}\n  VERIFICATION: {category} (abbr={abbr})\n{'='*60}", flush=True)
    print(f"Shard path: {shard_path}", flush=True)

    if shard is None:
        print(f"❌ FAIL: shard not written", flush=True)
        return False
    deduped, enriched = shard.get("deduped", []), shard.get("enriched", [])
    print(f"✓ Shard: {len(deduped)} deduped, {len(enriched)} enriched", flush=True)

    if not enriched:
        print("⚠️  No enriched entries — sector produced zero new findings this run.", flush=True)
        return True

    run_date = enriched[0].get("timestamp")
    try:
        run_mmddyy = date.fromisoformat(run_date).strftime("%m%d%y")
    except Exception:
        print(f"  ❌ first entry timestamp {run_date!r} is not ISO YYYY-MM-DD", flush=True)
        return False
    print(f"  Run date (from Strategist): {run_date}", flush=True)

    ids = [e.get("entry_id") for e in enriched]
    print(f"  Entry IDs (shard): {ids}", flush=True)

    # Pre-existing entries for this category on run_date in the master before merge.
    # Entry_ids must continue from len(pre_today)+1.
    pre_today = [e for e in pre_master if e.get("category") == category and e.get("timestamp") == run_date]
    start = len(pre_today) + 1
    print(f"  Pre-existing master entries for {category} on {run_date}: {len(pre_today)} "
          f"(entry_ids should be {abbr}-{run_mmddyy}-{start:03d}..{start + len(ids) - 1:03d})", flush=True)

    ok = True
    for i, e in enumerate(enriched):
        expected_id = f"{abbr}-{run_mmddyy}-{start + i:03d}"
        if e.get("entry_id") != expected_id:
            print(f"  ❌ entry_id[{i+1}]={e.get('entry_id')!r} expected {expected_id!r}", flush=True)
            ok = False
        if e.get("timestamp") != run_date:
            print(f"  ❌ timestamp[{i+1}]={e.get('timestamp')!r} != run_date {run_date!r}", flush=True)
            ok = False
        if e.get("category") != category:
            print(f"  ❌ category[{i+1}]={e.get('category')!r} != {category!r}", flush=True)
            ok = False
    if ok:
        print(f"✓ Entry IDs sequential from {abbr}-{run_mmddyy}-{start:03d}, all stamped {run_date}", flush=True)

    print(f"\n{'='*60}\n  MERGE SHARD → MASTER\n{'='*60}", flush=True)
    merge_result = mt.merge_sector_shards()
    print(f"  merge_sector_shards() → {merge_result}", flush=True)

    master = _load_master()
    todays = [e for e in master if e.get("category") == category and e.get("timestamp") == run_date]
    master_ids = [e.get("entry_id") for e in todays]
    print(f"  Master now has {len(todays)} {category} entries for {run_date}: {master_ids}", flush=True)
    if set(ids).issubset(set(master_ids)):
        print("✓ All shard entry_ids present in master after merge", flush=True)
    else:
        missing = set(ids) - set(master_ids)
        print(f"  ❌ missing in master: {missing}", flush=True)
        ok = False

    pre_existing = [e for e in pre_master if e.get("category") == category and e.get("timestamp") != run_date]
    mem_limit = mt.config.get("storage", {}).get("memory_limit", 10)
    print(f"  Baseline available for dedup: {len(pre_existing)} prior {category} entries "
          f"(memory_limit={mem_limit}, last-N consulted by dedup_findings)", flush=True)
    return ok


def main():
    args = parse_args()
    scouts_cfg = mt.config.get("scouts", {})
    if args.scout not in scouts_cfg:
        print(f"❌ Scout {args.scout!r} not in values.yaml. Available: {list(scouts_cfg.keys())}")
        sys.exit(1)
    info = scouts_cfg[args.scout]
    category, sector = info["category"], info["sector"]
    message = args.message or f"Research new developments in {sector} from the last 24 hours."

    tmp = None
    if args.use_gcs:
        print(f"[test_single_scout] LIVE GCS mode — USE_GCS={mt.USE_GCS} MEMORY_FILE={mt.MEMORY_FILE}", flush=True)
    else:
        tmp, _ = setup_isolation()

    print(f"🧪 SINGLE SCOUT TEST: {args.scout}")
    print(f"   Category: {category} | Sector: {sector}")
    print(f"   Message:  {message}\n", flush=True)

    asyncio.run(run_pipeline(args.scout, message))
    ok = verify(category)

    if tmp is not None:
        print(f"\n[test_single_scout] Cleanup: {tmp}")
        shutil.rmtree(tmp, ignore_errors=True)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
