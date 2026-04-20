"""Acceptance tests for the 3-layer `dedup_findings` with URL fast-path.

Verifies:
 1. Layer 1 (URL match) — same URL as baseline entry → dropped with reason=url_match
 2. Layer 2/3 (tfidf/entity) — different URL but semantically same story → dropped
 3. New URL + novel content → kept
 4. Return shape: {"kept": [...], "dropped": [...]}
 5. Backward compat — bare string input treated as {finding: s, source_url: null}
 6. source_url preserved verbatim on kept findings
 7. Intra-batch URL dedup
 8. Historical entries without entry_id / source_url still get matched via tfidf

Run:
    cd alphasnap
    python3 dev-utils/test_dedup_url.py
"""
import os
import sys
import json
import shutil
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import market_team as mt

_TMP = tempfile.mkdtemp(prefix="alphasnap_dedup_test_")
_LOG = os.path.join(_TMP, "market_findings_log.json")

mt.USE_GCS = False
mt.LOCAL_PATH = _LOG
mt.MEMORY_FILE = _LOG


def _seed_baseline(entries):
    """Write a list of entries directly to the local master log."""
    with open(_LOG, "w") as f:
        json.dump(entries, f, indent=2)


def _reset():
    for name in os.listdir(_TMP):
        os.remove(os.path.join(_TMP, name))


# ── Fixture baseline: one Robotics entry with full metadata ──
_BASELINE_ENTRY = {
    "entry_id": "ROB-041526-015",
    "timestamp": "2026-04-15",
    "category": "Robotics",
    "finding": "Accenture Ventures announced a strategic investment in General Robotics to accelerate "
               "AI-powered robotic automation in manufacturing and logistics, integrating with NVIDIA "
               "Isaac Sim and Omniverse.",
    "sentiment_takeaways": "Very Bullish.",
    "guidance_play": "Accumulate ACN",
    "price_levels": "NVDA",
    "source_url": "https://newsroom.accenture.com/news/2026/accenture-invests-in-general-robotics",
}


def test_1_url_fast_path_match():
    """Same URL as baseline → must be dropped with reason=url_match, matched_entry_id set."""
    _reset()
    _seed_baseline([_BASELINE_ENTRY])
    scout = json.dumps([
        {
            "finding": "Totally different prose about a completely unrelated robotics topic from the deep sea.",
            "source_url": "https://newsroom.accenture.com/news/2026/accenture-invests-in-general-robotics",
        }
    ])
    result = json.loads(mt.dedup_findings(scout, "Robotics"))
    assert len(result["kept"]) == 0, f"expected 0 kept, got {len(result['kept'])}"
    assert len(result["dropped"]) == 1
    drop = result["dropped"][0]
    assert drop["reason"] == "url_match", f"got reason={drop['reason']!r}"
    assert drop["matched_entry_id"] == "ROB-041526-015", f"got {drop['matched_entry_id']!r}"


def test_2_different_url_caught_by_tfidf_or_entity():
    """Different URL but same story → dropped via tfidf/entity overlap."""
    _reset()
    _seed_baseline([_BASELINE_ENTRY])
    scout = json.dumps([
        {
            # Near-identical to baseline but rephrased, different URL
            "finding": "Accenture Ventures invested strategically in General Robotics to scale AI-powered "
                       "robotic automation across manufacturing and logistics using NVIDIA Isaac Sim.",
            "source_url": "https://www.reuters.com/technology/accenture-general-robotics-deal-2026",
        }
    ])
    result = json.loads(mt.dedup_findings(scout, "Robotics"))
    assert len(result["kept"]) == 0, f"expected 0 kept, got {len(result['kept'])}: {result['kept']}"
    assert len(result["dropped"]) == 1
    drop = result["dropped"][0]
    assert drop["reason"] in ("tfidf", "entity_overlap"), f"got reason={drop['reason']!r}"
    assert drop["matched_entry_id"] == "ROB-041526-015"
    # Scores should be populated for this layer
    assert drop["scores"]["tfidf"] is not None or drop["scores"]["entity"] is not None


def test_3_novel_finding_kept():
    """Unrelated finding → kept in output, URL preserved verbatim."""
    _reset()
    _seed_baseline([_BASELINE_ENTRY])
    scout = json.dumps([
        {
            "finding": "AgiBot Labs launched Maniformer Subsidiary, a new venture from Shanghai Tech to tackle physical data bottlenecks for embodied systems.",
            "source_url": "https://techcrunch.com/2026/04/17/agibot-maniformer/",
        }
    ])
    result = json.loads(mt.dedup_findings(scout, "Robotics"))
    assert len(result["kept"]) == 1, f"expected 1 kept, got {len(result['kept'])}: {result}"
    kept = result["kept"][0]
    assert kept["source_url"] == "https://techcrunch.com/2026/04/17/agibot-maniformer/"
    assert "Maniformer" in kept["finding"]
    assert len(result["dropped"]) == 0


def test_4_return_shape():
    """Return is always valid JSON with exactly keys {kept, dropped}."""
    _reset()
    _seed_baseline([_BASELINE_ENTRY])
    result = json.loads(mt.dedup_findings('[]', "Robotics"))
    assert set(result.keys()) == {"kept", "dropped"}, f"got keys={set(result.keys())}"
    assert isinstance(result["kept"], list)
    assert isinstance(result["dropped"], list)


def test_5_backward_compat_bare_string_input():
    """Legacy bare-string input still works — source_url treated as None."""
    _reset()
    _seed_baseline([_BASELINE_ENTRY])
    scout = json.dumps([
        "A completely novel finding about Antioch raising $8.5M seed for physical AI sim tools.",
    ])
    result = json.loads(mt.dedup_findings(scout, "Robotics"))
    assert len(result["kept"]) == 1
    kept = result["kept"][0]
    assert kept["source_url"] is None
    assert "Antioch" in kept["finding"]


def test_6_url_preserved_verbatim():
    """Exact URL string (including query params) preserved end-to-end."""
    _reset()
    _seed_baseline([])
    weird_url = "https://www.forbes.com/sites/test/2026/04/16/story?ref=twitter&utm=feed"
    scout = json.dumps([
        {"finding": "A fresh finding about Antioch closing an $8.5M seed round.", "source_url": weird_url},
    ])
    result = json.loads(mt.dedup_findings(scout, "Robotics"))
    assert result["kept"][0]["source_url"] == weird_url


def test_7_intra_batch_url_dedup():
    """Two scout findings with the same URL in one batch → second dropped as intra_batch."""
    _reset()
    _seed_baseline([])
    same_url = "https://techcrunch.com/2026/04/17/same-story/"
    scout = json.dumps([
        {"finding": "First phrasing of the launch.", "source_url": same_url},
        {"finding": "Slightly different phrasing of the same launch event.", "source_url": same_url},
    ])
    result = json.loads(mt.dedup_findings(scout, "Robotics"))
    assert len(result["kept"]) == 1, f"expected 1 kept, got {len(result['kept'])}"
    assert len(result["dropped"]) == 1
    assert result["dropped"][0]["reason"] == "intra_batch"


def test_8_historical_entry_without_entry_id():
    """Pre-Phase-1 entry (no entry_id, no source_url) still catchable via tfidf/entity."""
    _reset()
    historical = {
        "timestamp": "2026-04-10",
        "category": "Robotics",
        "finding": "Accenture Ventures announced investment in General Robotics for AI automation in manufacturing.",
        # no entry_id, no source_url — the pre-Phase-1 shape
    }
    _seed_baseline([historical])
    scout = json.dumps([
        {
            "finding": "Accenture Ventures made a strategic investment in General Robotics to scale AI automation in manufacturing logistics.",
            "source_url": "https://somewhere.com/story",
        }
    ])
    result = json.loads(mt.dedup_findings(scout, "Robotics"))
    assert len(result["kept"]) == 0, f"expected 0 kept, got {result}"
    drop = result["dropped"][0]
    assert drop["matched_entry_id"] is None, f"historical entry has no entry_id, expected None, got {drop['matched_entry_id']!r}"
    assert drop["reason"] in ("tfidf", "entity_overlap")


def test_9_mixed_input_objects_and_strings():
    """Mixed input: some objects, some bare strings — both handled."""
    _reset()
    _seed_baseline([])
    scout = json.dumps([
        {"finding": "Object-style finding about SpaceX launch.", "source_url": "https://spacex.com/news/1"},
        "Bare-string finding about a totally unrelated industrial robot."
    ])
    result = json.loads(mt.dedup_findings(scout, "Robotics"))
    assert len(result["kept"]) == 2
    urls = [k["source_url"] for k in result["kept"]]
    assert "https://spacex.com/news/1" in urls
    assert None in urls


TESTS = [
    ("Test 1: URL fast-path matches baseline", test_1_url_fast_path_match),
    ("Test 2: Different URL caught by tfidf/entity", test_2_different_url_caught_by_tfidf_or_entity),
    ("Test 3: Novel finding kept w/ URL preserved", test_3_novel_finding_kept),
    ("Test 4: Return shape is {kept, dropped}", test_4_return_shape),
    ("Test 5: Backward compat — bare string input", test_5_backward_compat_bare_string_input),
    ("Test 6: URL preserved verbatim (query params)", test_6_url_preserved_verbatim),
    ("Test 7: Intra-batch URL dedup", test_7_intra_batch_url_dedup),
    ("Test 8: Historical entry (no entry_id) matchable", test_8_historical_entry_without_entry_id),
    ("Test 9: Mixed input: objects + bare strings", test_9_mixed_input_objects_and_strings),
]


def main():
    passed = 0
    failed = []
    print(f"[test_dedup_url] Using temp dir: {_TMP}\n", flush=True)
    for name, fn in TESTS:
        try:
            fn()
            print(f"  PASS  {name}", flush=True)
            passed += 1
        except Exception as e:
            print(f"  FAIL  {name}: {e}", flush=True)
            failed.append((name, str(e)))
    print(f"\n[test_dedup_url] Results: {passed}/{len(TESTS)} passed", flush=True)
    if failed:
        print("\nFailures:", flush=True)
        for name, err in failed:
            print(f"  - {name}: {err}", flush=True)
    shutil.rmtree(_TMP, ignore_errors=True)
    sys.exit(0 if not failed else 1)


if __name__ == "__main__":
    main()
