"""
Validation test for the DE → Strategist shard pipeline.
Tests dedup_findings manifest write, append_to_memory_log enriched write,
_shard_valid completeness check, strategist-only retry via _get_unenriched_findings,
and merge with per-category stats.

Uses real data from market_findings_log.json as baseline.

Usage:
    python3 test_shard_pipeline.py
"""
import os
import json
import sys

# Force local storage mode for testing
os.environ["GOOGLE_CLOUD_PROJECT"] = "marketresearch-agents"
os.environ["GOOGLE_CLOUD_LOCATION"] = "global"

# Force local storage for testing
import market_team
market_team.USE_GCS = False
from market_team import (
    dedup_findings, append_to_memory_log, _shard_valid,
    _get_unenriched_findings, merge_sector_shards,
    LOCAL_PATH, config
)

TEST_CATEGORY = "Robotics"
SHARD_PATH = LOCAL_PATH.replace(".json", f"_{TEST_CATEGORY}.json")

# Real scout findings — mix of new + duplicate from existing log
SCOUT_FINDINGS = [
    "Boston Dynamics unveiled Atlas 2.0 with upgraded manipulation capabilities for warehouse logistics.",
    "Amazon confirmed the acquisition of Fauna Robotics and its 42-inch bipedal home humanoid 'Sprout', running on Nvidia Jetson Orin and Amazon Titan AI models.",  # duplicate from existing log
    "Figure AI raised $1.5B Series C to scale humanoid robot production for BMW and Amazon deployments.",
]

def cleanup():
    """Remove any test shard files."""
    for scout_name, info in config.get("scouts", {}).items():
        cat = info.get("category", "General")
        path = LOCAL_PATH.replace(".json", f"_{cat}.json")
        if os.path.exists(path):
            os.remove(path)

def read_shard():
    """Read the test shard file."""
    if os.path.exists(SHARD_PATH):
        with open(SHARD_PATH, "r") as f:
            return json.load(f)
    return None

def test_dedup_writes_manifest():
    """Test 1: dedup_findings should write a manifest with deduped findings to shard."""
    print("\n" + "=" * 60)
    print("TEST 1: DE dedup_findings writes manifest to shard")
    print("=" * 60)

    cleanup()

    result = dedup_findings(
        scout_findings_json=json.dumps(SCOUT_FINDINGS),
        category=TEST_CATEGORY
    )

    unique = json.loads(result)
    shard = read_shard()

    assert shard is not None, "FAIL: Shard file was not created"
    assert "deduped" in shard, "FAIL: Shard missing 'deduped' key"
    assert "enriched" in shard, "FAIL: Shard missing 'enriched' key"
    assert len(shard["deduped"]) == len(unique), f"FAIL: deduped count mismatch ({len(shard['deduped'])} vs {len(unique)})"
    assert len(shard["enriched"]) == 0, "FAIL: enriched should be empty after DE"

    print(f"\n  PASS: Shard created with {len(shard['deduped'])} deduped, 0 enriched")
    print(f"  Unique findings: {len(unique)}")
    for i, f in enumerate(unique):
        print(f"    [{i+1}] {f[:100]}...")
    return unique


def test_strategist_enriches(unique_findings):
    """Test 2: append_to_memory_log should write into enriched array."""
    print("\n" + "=" * 60)
    print("TEST 2: Strategist append_to_memory_log writes to enriched")
    print("=" * 60)

    for i, finding in enumerate(unique_findings):
        result = append_to_memory_log(
            findings_date="2026-03-31",
            category=TEST_CATEGORY,
            finding=finding,
            insights_sentiment=f"Bullish. Test insight #{i+1}.",
            guidance_play=f"Test guidance #{i+1}.",
            price_levels=f"Test levels #{i+1}."
        )
        print(f"  Enriched [{i+1}]: {result}")

    shard = read_shard()
    deduped_n = len(shard["deduped"])
    enriched_n = len(shard["enriched"])

    assert enriched_n == deduped_n, f"FAIL: enriched ({enriched_n}) != deduped ({deduped_n})"

    for entry in shard["enriched"]:
        for key in ["timestamp", "category", "finding", "insights_sentiment", "guidance_play", "price_levels"]:
            assert key in entry, f"FAIL: enriched entry missing '{key}'"

    print(f"\n  PASS: {enriched_n}/{deduped_n} enriched — complete shard")


def test_shard_valid_complete():
    """Test 3: _shard_valid should return True for a complete shard."""
    print("\n" + "=" * 60)
    print("TEST 3: _shard_valid on complete shard")
    print("=" * 60)

    result = _shard_valid(TEST_CATEGORY)
    assert result is True, "FAIL: Complete shard should be valid"
    print("  PASS: Complete shard validated as True")


def test_shard_valid_partial():
    """Test 4: _shard_valid should return False and KEEP partial shard."""
    print("\n" + "=" * 60)
    print("TEST 4: _shard_valid on partial shard (simulated 429 mid-strategist)")
    print("=" * 60)

    partial_shard = {
        "deduped": ["finding A", "finding B", "finding C"],
        "enriched": [
            {"timestamp": "2026-03-31", "category": TEST_CATEGORY, "finding": "finding A",
             "insights_sentiment": "Bullish", "guidance_play": "test", "price_levels": "test"}
        ]
    }
    with open(SHARD_PATH, "w") as f:
        json.dump(partial_shard, f)

    result = _shard_valid(TEST_CATEGORY)
    assert result is False, "FAIL: Partial shard should be invalid"
    assert os.path.exists(SHARD_PATH), "FAIL: Partial shard should be KEPT (not deleted)"
    print("  PASS: Partial shard (1/3) detected as incomplete, file preserved")


def test_unenriched_findings():
    """Test 5: _get_unenriched_findings returns only the missing findings."""
    print("\n" + "=" * 60)
    print("TEST 5: _get_unenriched_findings on partial shard")
    print("=" * 60)

    # Shard from test 4 should still be there: 3 deduped, 1 enriched (finding A)
    unenriched = _get_unenriched_findings(TEST_CATEGORY)
    assert len(unenriched) == 2, f"FAIL: Expected 2 unenriched, got {len(unenriched)}"
    assert "finding B" in unenriched, "FAIL: 'finding B' should be unenriched"
    assert "finding C" in unenriched, "FAIL: 'finding C' should be unenriched"
    assert "finding A" not in unenriched, "FAIL: 'finding A' should already be enriched"
    print(f"  PASS: {len(unenriched)} unenriched findings identified: {unenriched}")

    # Now simulate strategist retry enriching the remaining 2
    for finding in unenriched:
        append_to_memory_log(
            findings_date="2026-03-31",
            category=TEST_CATEGORY,
            finding=finding,
            insights_sentiment="Bullish retry",
            guidance_play="retry guidance",
            price_levels="retry levels"
        )

    # Should now be valid
    assert _shard_valid(TEST_CATEGORY) is True, "FAIL: Shard should be complete after retry enrichment"
    remaining = _get_unenriched_findings(TEST_CATEGORY)
    assert len(remaining) == 0, f"FAIL: Expected 0 unenriched after retry, got {len(remaining)}"
    print("  PASS: After retry enrichment, shard is complete (3/3)")


def test_shard_valid_empty_dedup():
    """Test 6: _shard_valid should return True when dedup found 0 new findings."""
    print("\n" + "=" * 60)
    print("TEST 6: _shard_valid on empty dedup (no new findings)")
    print("=" * 60)

    empty_shard = {"deduped": [], "enriched": []}
    with open(SHARD_PATH, "w") as f:
        json.dump(empty_shard, f)

    result = _shard_valid(TEST_CATEGORY)
    assert result is True, "FAIL: Empty dedup shard should be valid"

    os.remove(SHARD_PATH)
    print("  PASS: Empty dedup shard validated as True")


def test_merge_complete_with_stats():
    """Test 7: merge_sector_shards reads enriched, prints per-category stats."""
    print("\n" + "=" * 60)
    print("TEST 7: merge_sector_shards with complete shard + stats output")
    print("=" * 60)

    cleanup()

    complete_shard = {
        "deduped": ["Finding Alpha", "Finding Beta"],
        "enriched": [
            {"timestamp": "2026-03-31", "category": "Robotics", "finding": "Finding Alpha",
             "insights_sentiment": "Bullish", "guidance_play": "Buy AMZN", "price_levels": "$210"},
            {"timestamp": "2026-03-31", "category": "Robotics", "finding": "Finding Beta",
             "insights_sentiment": "Very Bullish", "guidance_play": "Buy NVDA", "price_levels": "$165"},
        ]
    }
    with open(SHARD_PATH, "w") as f:
        json.dump(complete_shard, f)

    result = merge_sector_shards()
    print(f"  Merge result: {result}")

    assert "2" in result or "new entries" in result, f"FAIL: Expected 2 entries merged, got: {result}"
    print("  PASS: Merge correctly read enriched entries + printed stats")


def test_merge_partial_no_salvage():
    """Test 8: merge only takes enriched, does NOT salvage raw deduped."""
    print("\n" + "=" * 60)
    print("TEST 8: merge partial shard — only enriched merged, no salvage")
    print("=" * 60)

    cleanup()

    # Partial shard: 3 deduped, 1 enriched
    partial_shard = {
        "deduped": ["Finding X", "Finding Y", "Finding Z"],
        "enriched": [
            {"timestamp": "2026-03-31", "category": "Robotics", "finding": "Finding X",
             "insights_sentiment": "Bullish", "guidance_play": "test", "price_levels": "test"}
        ]
    }
    with open(SHARD_PATH, "w") as f:
        json.dump(partial_shard, f)

    result = merge_sector_shards()
    print(f"  Merge result: {result}")

    # The merge result string tells us exactly how many were merged this round
    assert "1 new entries" in result, f"FAIL: Expected 1 entry merged (enriched only), got: {result}"
    # Verify no "Pending analysis" salvage entries were created
    if os.path.exists(LOCAL_PATH):
        with open(LOCAL_PATH, "r") as f:
            master = json.load(f)
        salvaged = [e for e in master if "Pending analysis" in e.get("insights_sentiment", "")]
        assert len(salvaged) == 0, f"FAIL: Should NOT salvage raw deduped, found {len(salvaged)}"
    print(f"  PASS: Only 1 enriched entry merged, 2 raw deduped correctly skipped")

    cleanup()


if __name__ == "__main__":
    print("=" * 60)
    print("  SHARD PIPELINE VALIDATION TEST")
    print("=" * 60)

    # Backup master log if it exists
    master_backup = None
    if os.path.exists(LOCAL_PATH):
        with open(LOCAL_PATH, "r") as f:
            master_backup = f.read()

    try:
        unique = test_dedup_writes_manifest()
        test_strategist_enriches(unique)
        test_shard_valid_complete()
        test_shard_valid_partial()
        test_unenriched_findings()
        test_shard_valid_empty_dedup()
        test_merge_complete_with_stats()
        test_merge_partial_no_salvage()

        print("\n" + "=" * 60)
        print("  ALL 8 TESTS PASSED")
        print("=" * 60)

    except AssertionError as e:
        print(f"\n  FAILED: {e}")
        sys.exit(1)

    finally:
        # Restore master log
        cleanup()
        if master_backup is not None:
            with open(LOCAL_PATH, "w") as f:
                f.write(master_backup)
            print(f"\n  Restored original {LOCAL_PATH}")
