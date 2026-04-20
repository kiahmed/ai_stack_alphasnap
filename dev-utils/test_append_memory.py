"""Acceptance tests for the hardened `append_to_memory_log`.

Runs the 8 acceptance tests from
../catalyst-knowledge-graph/docs/proposed_alphasnap_changes.md against a
temporary local-mode path (never touches GCS or the real master log).

Run:
    cd alphasnap
    python3 dev-utils/test_append_memory.py

Writes pass/fail report to stdout (tee into dev-utils/output.log).
"""
import os
import sys
import json
import shutil
import tempfile

# Import the module under test before monkey-patching paths
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import market_team as mt


# ── Isolation: redirect the module to a throwaway local path ──
_TMP = tempfile.mkdtemp(prefix="alphasnap_test_")
_LOG = os.path.join(_TMP, "market_findings_log.json")

mt.USE_GCS = False
mt.LOCAL_PATH = _LOG
mt.MEMORY_FILE = _LOG


def _reset():
    """Wipe temp dir between tests so each case starts clean."""
    for name in os.listdir(_TMP):
        os.remove(os.path.join(_TMP, name))


def _read_shard_enriched(category):
    path = _LOG.replace(".json", f"_{category}.json")
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return json.load(f).get("enriched", [])


def _last_entry(category):
    entries = _read_shard_enriched(category)
    return entries[-1] if entries else None


# ── Test cases ──
def test_1_id_uniqueness_within_date():
    _reset()
    id1 = mt.append_to_memory_log("Robotics", "finding 1", "2026-04-15", "...", "...", "...", "https://a.com")
    id2 = mt.append_to_memory_log("Robotics", "finding 2", "2026-04-15", "...", "...", "...", None)
    id3 = mt.append_to_memory_log("Robotics", "finding 3", "2026-04-15", "...", "...", "...", "https://c.com")
    assert id1 == "ROB-041526-001", f"got {id1!r}"
    assert id2 == "ROB-041526-002", f"got {id2!r}"
    assert id3 == "ROB-041526-003", f"got {id3!r}"


def test_2_counter_resets_on_new_date():
    _reset()
    mt.append_to_memory_log("Robotics", "f1", "2026-04-15", "...", "...", "...", None)
    mt.append_to_memory_log("Robotics", "f2", "2026-04-15", "...", "...", "...", None)
    mt.append_to_memory_log("Robotics", "f3", "2026-04-15", "...", "...", "...", None)
    id4 = mt.append_to_memory_log("Robotics", "f4", "2026-04-16", "...", "...", "...", None)
    assert id4 == "ROB-041626-001", f"got {id4!r}"


def test_3_counter_is_per_category():
    _reset()
    mt.append_to_memory_log("Robotics", "f1", "2026-04-15", "...", "...", "...", None)
    id5 = mt.append_to_memory_log("Crypto", "f5", "2026-04-15", "...", "...", "...", None)
    assert id5 == "CRY-041526-001", f"got {id5!r}"


def test_4_timestamp_normalization():
    _reset()
    id6 = mt.append_to_memory_log("Robotics", "f6", "March 24, 2026", "...", "...", "...", None)
    entry = _last_entry("Robotics")
    assert entry["timestamp"] == "2026-03-24", f"got {entry['timestamp']!r}"
    assert entry["entry_id"] == "ROB-032426-001", f"got {entry['entry_id']!r}"
    assert id6 == "ROB-032426-001"

    # ISO with time should also strip to date
    _reset()
    mt.append_to_memory_log("Robotics", "f", "2026-04-15T14:23:00Z", "...", "...", "...", None)
    assert _last_entry("Robotics")["timestamp"] == "2026-04-15"

    # Space-separated time
    _reset()
    mt.append_to_memory_log("Robotics", "f", "2026-04-15 14:23:00", "...", "...", "...", None)
    assert _last_entry("Robotics")["timestamp"] == "2026-04-15"


def test_5_schema_completeness():
    _reset()
    mt.append_to_memory_log("Robotics", "f", "2026-04-17", "sent", "play", "lvl", "https://src")
    entry = _last_entry("Robotics")
    required = {"entry_id", "timestamp", "category", "finding",
                "sentiment_takeaways", "guidance_play", "price_levels", "source_url"}
    assert set(entry.keys()) == required, f"keys={set(entry.keys())}, missing={required - set(entry.keys())}"


def test_6_missing_source_url_stores_null():
    _reset()
    # Call without source_url kwarg — default should be None
    mt.append_to_memory_log("Robotics", "f7", "2026-04-17", "...", "...", "...")
    entry = _last_entry("Robotics")
    assert entry["source_url"] is None, f"got {entry['source_url']!r}"


def test_7_unparseable_timestamp_raises():
    _reset()
    try:
        mt.append_to_memory_log("Robotics", "f", "not a date", "...", "...", "...", None)
        assert False, "should have raised ValueError"
    except ValueError:
        pass


def test_8_range_shaped_timestamp_rejected():
    for bad_ts in ["March 21-22, 2026", "March 18–19, 2026", "2026-03-23 to 2026-03-27"]:
        _reset()
        try:
            mt.append_to_memory_log("Robotics", "f", bad_ts, "...", "...", "...", None)
            assert False, f"should have raised for {bad_ts!r}"
        except ValueError:
            pass


# Bonus: master log + shard counting together (edge case not in proposal's tests)
def test_9_master_log_plus_shard_counter():
    _reset()
    # Pre-seed master log with 2 Robotics entries on the target date
    pre = [
        {"entry_id": "ROB-041526-001", "timestamp": "2026-04-15", "category": "Robotics", "finding": "old1"},
        {"entry_id": "ROB-041526-002", "timestamp": "2026-04-15", "category": "Robotics", "finding": "old2"},
    ]
    with open(_LOG, "w") as f:
        json.dump(pre, f)
    # Now append — should continue from 003
    new_id = mt.append_to_memory_log("Robotics", "f", "2026-04-15", "...", "...", "...", None)
    assert new_id == "ROB-041526-003", f"got {new_id!r}"
    # Second call same run — shard already has 1 entry, master has 2 → 004
    new_id2 = mt.append_to_memory_log("Robotics", "f", "2026-04-15", "...", "...", "...", None)
    assert new_id2 == "ROB-041526-004", f"got {new_id2!r}"


TESTS = [
    ("Test 1: ID uniqueness within a date", test_1_id_uniqueness_within_date),
    ("Test 2: Counter resets on new date", test_2_counter_resets_on_new_date),
    ("Test 3: Counter is per-category", test_3_counter_is_per_category),
    ("Test 4: Timestamp normalization", test_4_timestamp_normalization),
    ("Test 5: Schema completeness (8 fields)", test_5_schema_completeness),
    ("Test 6: Missing source_url stores null", test_6_missing_source_url_stores_null),
    ("Test 7: Unparseable timestamp raises", test_7_unparseable_timestamp_raises),
    ("Test 8: Range-shaped timestamp rejected", test_8_range_shaped_timestamp_rejected),
    ("Test 9: Master log + shard combined counter", test_9_master_log_plus_shard_counter),
]


def main():
    passed = 0
    failed = []
    print(f"[test_append_memory] Using temp dir: {_TMP}\n", flush=True)
    for name, fn in TESTS:
        try:
            fn()
            print(f"  PASS  {name}", flush=True)
            passed += 1
        except Exception as e:
            print(f"  FAIL  {name}: {e}", flush=True)
            failed.append((name, str(e)))
    print(f"\n[test_append_memory] Results: {passed}/{len(TESTS)} passed", flush=True)
    if failed:
        print("\nFailures:", flush=True)
        for name, err in failed:
            print(f"  - {name}: {err}", flush=True)
    shutil.rmtree(_TMP, ignore_errors=True)
    sys.exit(0 if not failed else 1)


if __name__ == "__main__":
    main()
