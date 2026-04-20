"""Race simulation for `append_to_memory_log` — verifies GCS if_generation_match
retry prevents silent data loss when multiple strategist calls append in parallel.

Setup:
 - MockBlob: in-memory object that mimics real GCS blob semantics (generation
   counter, conditional upload, raises PreconditionFailed on mismatch).
 - 5 threads synchronized on a Barrier to guarantee they all read the shard at
   the same state, then race each other to upload.

Expected with the fix:
 - All 5 entries land in the shard (no data loss)
 - All 5 entry_ids are unique
 - Several "generation-match miss" retry log lines fire (proves the race actually
   triggered and the retry logic absorbed it — if no retries print, the test
   did not exercise the race path)

Run:
    cd alphasnap
    python3 dev-utils/test_append_race.py
"""
import os
import sys
import json
import threading
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import market_team as mt
from google.api_core.exceptions import PreconditionFailed


# ── MockBlob — in-memory GCS blob that enforces if_generation_match ──
class _MockStore:
    """Shared in-memory object store keyed by blob name."""
    def __init__(self):
        self._data = {}          # name -> (bytes, generation)
        self._lock = threading.Lock()
        self._gen_counter = 0

    def exists(self, name):
        with self._lock:
            return name in self._data

    def read(self, name):
        with self._lock:
            return self._data.get(name)  # (bytes, generation) or None

    def write(self, name, data_bytes, if_generation_match):
        with self._lock:
            current = self._data.get(name)
            current_gen = current[1] if current else 0
            if if_generation_match is not None and if_generation_match != current_gen:
                raise PreconditionFailed(
                    f"Generation mismatch for {name!r}: expected {if_generation_match}, got {current_gen}"
                )
            self._gen_counter += 1
            self._data[name] = (data_bytes, self._gen_counter)
            return self._gen_counter


_STORE = _MockStore()


# Barrier that forces all append threads to pause before their FIRST upload,
# so they all collide on the same generation. Retry uploads skip the barrier so
# the retry path can make progress.
_UPLOAD_BARRIER = None
_BARRIER_PASSED = set()         # thread ids that already crossed the barrier
_BARRIER_PASSED_LOCK = threading.Lock()


class MockBlob:
    def __init__(self, name):
        self.name = name
        self.generation = 0  # cached; .reload() refreshes

    def exists(self):
        return _STORE.exists(self.name)

    def reload(self):
        rec = _STORE.read(self.name)
        if rec is not None:
            self.generation = rec[1]

    def download_as_text(self):
        rec = _STORE.read(self.name)
        if rec is None:
            raise FileNotFoundError(self.name)
        return rec[0].decode("utf-8")

    def upload_from_string(self, data, content_type=None, if_generation_match=None):
        # On the FIRST upload attempt per thread, pause at the barrier so all
        # writers converge with the same pre-read generation. Retry uploads
        # skip the barrier so the retry path can make forward progress.
        global _UPLOAD_BARRIER
        if _UPLOAD_BARRIER is not None and self.name.endswith("_Robotics.json"):
            tid = threading.get_ident()
            with _BARRIER_PASSED_LOCK:
                first_time = tid not in _BARRIER_PASSED
                if first_time:
                    _BARRIER_PASSED.add(tid)
            if first_time:
                try:
                    _UPLOAD_BARRIER.wait(timeout=10)
                except threading.BrokenBarrierError:
                    pass
        if isinstance(data, str):
            data = data.encode("utf-8")
        new_gen = _STORE.write(self.name, data, if_generation_match)
        self.generation = new_gen


def _mock_get_gcs_blob(gs_path):
    # Mirror real layout: strip gs:// prefix, use the full path after bucket as name.
    parts = gs_path.replace("gs://", "").split("/", 1)
    blob_name = parts[1] if len(parts) > 1 else parts[0]
    return MockBlob(blob_name)


# ── Test harness ──
def _reset_store():
    global _UPLOAD_BARRIER
    with _STORE._lock:
        _STORE._data.clear()
        _STORE._gen_counter = 0
    with _BARRIER_PASSED_LOCK:
        _BARRIER_PASSED.clear()
    _UPLOAD_BARRIER = None


def test_parallel_append_no_data_loss():
    """5 threads append simultaneously; all 5 entries must land with unique IDs."""
    global _UPLOAD_BARRIER
    _reset_store()

    # Configure market_team to use GCS code path with mocked blobs
    mt.USE_GCS = True
    mt.GCS_PATH = "gs://test-bucket/market_findings_log.json"
    mt._get_gcs_blob = _mock_get_gcs_blob

    # Seed the shard with one entry so blob.exists() is True on first read and
    # all threads capture the same initial generation. Without this seed the
    # first thread creates the shard (generation=0 path) and its write advances
    # the generation, losing the "all read at the same generation" setup.
    seed_blob = _mock_get_gcs_blob("gs://test-bucket/market_findings_log_Robotics.json")
    seed_blob.upload_from_string(
        json.dumps({"deduped": [], "enriched": []}),
        content_type='application/json',
        if_generation_match=0,
    )

    N = 5
    results = [None] * N
    errors = [None] * N

    # Hold all N threads at the upload step until they've all read the shard.
    # This guarantees a generation-race is actually triggered.
    _UPLOAD_BARRIER = threading.Barrier(N)

    def worker(i):
        try:
            entry_id = mt.append_to_memory_log(
                category="Robotics",
                finding=f"thread-{i} finding — unique content about topic {i}",
                timestamp="2026-04-17",
                sentiment_takeaways="Direct: X. Indirect: Y. Market Dynamics: Z. Sentiment: Neutral",
                guidance_play=f"play for topic {i}",
                price_levels=f"PL-{i}",
                source_url=f"https://example.com/article-{i}",
            )
            results[i] = entry_id
        except Exception as e:
            errors[i] = e

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(N)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Report errors
    for i, e in enumerate(errors):
        assert e is None, f"thread {i} raised: {e!r}"

    # 1. All threads returned an entry_id
    for i, rid in enumerate(results):
        assert isinstance(rid, str) and rid.startswith("ROB-"), \
            f"thread {i} got non-id return: {rid!r}"

    # 2. All entry_ids are unique — no collision
    assert len(set(results)) == N, \
        f"duplicate entry_ids detected: {results}"

    # 3. Shard in mock storage contains all N entries — no data loss.
    #    Mock store is keyed by blob name only (bucket is stripped), matching real
    #    `_get_gcs_blob` behavior which discards bucket_name and keys on blob_name.
    shard_name = "market_findings_log_Robotics.json"
    rec = _STORE.read(shard_name)
    assert rec is not None, f"shard was never written; store keys={list(_STORE._data.keys())}"
    shard_data = json.loads(rec[0].decode("utf-8"))
    enriched = shard_data.get("enriched", [])
    assert len(enriched) == N, \
        f"expected {N} entries in shard, got {len(enriched)}: ids={[e['entry_id'] for e in enriched]}"

    # 4. Entry ids in shard match what each thread got back
    shard_ids = {e["entry_id"] for e in enriched}
    returned_ids = set(results)
    assert shard_ids == returned_ids, \
        f"mismatch: returned={returned_ids}, shard={shard_ids}"

    # 5. IDs form the exact sequence 001..005 for this (category, date)
    expected = {f"ROB-041726-{i:03d}" for i in range(1, N + 1)}
    assert shard_ids == expected, f"id sequence wrong: {sorted(shard_ids)} != {sorted(expected)}"

    print(f"  ✓ all {N} entries landed with unique sequential IDs: {sorted(shard_ids)}")


def test_single_append_still_works():
    """Sanity: single append with no contention still works, generation=0 on create."""
    _reset_store()
    mt.USE_GCS = True
    mt.GCS_PATH = "gs://test-bucket/market_findings_log.json"
    mt._get_gcs_blob = _mock_get_gcs_blob

    entry_id = mt.append_to_memory_log(
        category="Robotics",
        finding="solo finding",
        timestamp="2026-04-17",
        sentiment_takeaways="Direct: X. Indirect: Y. Market Dynamics: Z. Sentiment: Neutral",
        guidance_play="solo play",
        price_levels="X",
        source_url="https://example.com/solo",
    )
    assert entry_id == "ROB-041726-001", f"got {entry_id!r}"


TESTS = [
    ("Test 1: Single append creates shard with generation=0", test_single_append_still_works),
    ("Test 2: 5 parallel appends all land, unique IDs, no data loss", test_parallel_append_no_data_loss),
]


def main():
    passed = 0
    failed = []
    print("[test_append_race] Starting race simulation\n", flush=True)
    for name, fn in TESTS:
        try:
            fn()
            print(f"  PASS  {name}", flush=True)
            passed += 1
        except Exception as e:
            print(f"  FAIL  {name}: {e}", flush=True)
            failed.append((name, str(e)))
    print(f"\n[test_append_race] Results: {passed}/{len(TESTS)} passed", flush=True)
    if failed:
        print("\nFailures:", flush=True)
        for n, err in failed:
            print(f"  - {n}: {err}", flush=True)
    sys.exit(0 if not failed else 1)


if __name__ == "__main__":
    main()
