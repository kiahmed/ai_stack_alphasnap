# AlphaSnap workbench — design decisions (living doc)

Captures decisions made while implementing [proposed_alphasnap_changes.md](../../catalyst-knowledge-graph/docs/proposed_alphasnap_changes.md) from the Arbor project, plus enhancements proposed back to Arbor.

Format: each decision records the options considered, trade-offs, and the final choice. New context gets appended — do not rewrite history.

---

## 2026-04-17 — Phase 1: `append_to_memory_log` hardening

### Scope locked from proposal
- Add `source_url` param (optional, `None` allowed)
- Normalize `timestamp` to `YYYY-MM-DD`, reject range-shaped inputs
- Generate unique `entry_id` (`XXX-MMDDYY-YYY`), stateless counter derived from master log + current shard
- Rename `findings_date` → `timestamp`

### Decision A1 — return value
**Options:**
1. Return the long descriptive string (current: `"Logged finding for Robotics to GCS shard (3/3 enriched): ..."`)
2. Return just `entry_id`
3. Return `None`

**Chose #2.** The strategist prompt never references the return value — it's a pure confirmation. The long string wastes ~20 tokens × 20-30 calls/sweep. Returning `entry_id` keeps the signal useful for test scripts and any future programmatic callers (Arbor ingestion, backfill). Rich progress info remains in `print()` → stdout → Cloud Logging (zero token cost).

### Decision A2 — counter derivation source
**Options:**
1. Count from master log only
2. Count from master log + current sector shard's `enriched` list
3. Count from just the shard

**Chose #2.** AlphaSnap writes to per-category shards mid-sweep, then merges to master at the end. If we only count the master log, two writes in the same sweep both get `-001`. Counting shard's `enriched` too keeps IDs unique across the full sweep. Matches the spec's "log is source of truth" semantics — the "log" is the fully-merged view, and shard is the transient pre-merge version of that view.

### Decision A3 — range-detection regex
Initial regex `\b\d{1,2}\s*-\s*\d{1,2}\b` was too aggressive — matched `04-15` inside `2026-04-15 14:23:00`. Replaced with `[A-Za-z]\w*\s+\d{1,2}\s*-\s*\d{1,2}` (requires a leading word like "March"). Combined with `" to "` and em/en dash literal checks.

### Result
All 9 acceptance tests pass (8 from proposal + 1 edge case for master-log+shard combined counter). Live single-scout run against local mode succeeded — `ROB-041526-001` + `ROB-041526-002` written with correct schema.

---

## 2026-04-17 — Phase 2: source_url plumbing (propagation through dedup)

### Problem observed
After Phase 1, `source_url` was reliably `null` for every live-run entry despite the scout clearly emitting canonical URLs (Bloomberg, Reuters, TechCrunch, etc.) in its text output — confirmed via `dev-utils/inspect_scout_output.py`.

### Root cause
The DE prompt tells the Data Engineer to call:
```python
dedup_findings(scout_findings_json='["finding 1 text", "finding 2 text", ...]', category=...)
```
This contract takes a JSON array of **strings**, so the LLM parses URLs out of scout prose before the tool call. The structured URL data is lost before dedup even sees it, and the strategist has nothing to pass as `source_url`.

### Options considered

**Option 1 — Pass structured objects through dedup, URLs preserved deterministically (Python-side)**
- `dedup_findings` accepts `[{"finding": "...", "source_url": "..."}]`
- Python keeps URL stuck to finding through the filter
- Returns `{"kept": [...full objects...], "dropped": [...report...]}`
- Enables URL-equality fast-path dedup as a free signal
- Cost: ~200 extra tokens per dedup call × 6 sectors = ~1200 tokens/day ≈ $0.02/day

**Option 2 — DE re-attaches URLs after dedup (LLM-side bookkeeping)**
- Dedup input/output stay as strings
- DE holds URL→finding map in its context, re-attaches post-filter
- Fragile: relies on LLM to correctly match URLs to surviving findings by ordering/reasoning

**Option 3 — Slim index report from dedup (LLM-side bookkeeping, tokenefficient)**
- Dedup returns `{kept_indices: [0,2,3], dropped: [{matched_entry_id, title}]}`
- DE filters its own context list by index, attaches URLs
- Saves ~80% of dedup-return tokens
- Still relies on LLM to do correct index-to-URL mapping — same fragility as Option 2

### Chose Option 1.
**Reason:** LLM bookkeeping is fragile; determinism beats cleverness. The token cost of Option 1 (~$0.60/month) is negligible compared to the reliability win. Options 2 and 3 move the work to the LLM where a single hallucination can mismatch URLs to findings silently — the kind of bug you only catch months later during audit.

Secondary benefit: Option 1 unlocks a URL-equality fast-path dedup for free (Python compares URL strings before running tfidf/entity math).

### Additional design — 3-layer dedup

Current: TF-IDF cosine OR entity overlap → duplicate.
New layering:
1. **URL match** (new, first) — if both sides have non-null URLs and they match, mark duplicate immediately, skip steps 2-3
2. **TF-IDF cosine** (existing) — catches rephrasings
3. **Entity overlap** (existing) — catches same-story-different-words via shared tickers/names

Step 1 is essentially free (string compare). Steps 2-3 unchanged.

### Handling historical entries without `entry_id` / `source_url`
Pre-Phase-1 entries in the master log lack both fields. URL fast-path simply skips them (baseline URL is null → can't match). `matched_entry_id` returned as `null` for those. Fall through to tfidf/entity, which works fine.

### Backward compat during rollout
`dedup_findings` accepts bare strings in the input array as shorthand for `{"finding": str, "source_url": null}`. Protects against mid-rollout prompt/tool version skew.

### Return shape

```json
{
  "kept": [
    {"finding": "...", "source_url": "https://..."},
    {"finding": "...", "source_url": null}
  ],
  "dropped": [
    {
      "title": "first ~80 chars of scout finding",
      "matched_entry_id": "ROB-041526-015",
      "reason": "url_match | tfidf | entity_overlap",
      "scores": {"tfidf": 0.52, "entity": 0.71}
    }
  ]
}
```

### Enhancement proposed back to Arbor
The original proposal suggests adding `source_url` to `append_to_memory_log` but implicitly assumes the strategist receives URLs in its context. It doesn't address the DE-level loss point. The enhancement: dedup must accept structured objects so URLs survive the scout→DE→strategist handoff. To be logged as `[Proposed and enhanced by alphasnap]` against the relevant proposal section after acceptance tests pass.

### Observed edge case — entity-sparse findings (not fixed now)
During Test 3 of `test_dedup_url.py`, an entity-sparse scout text (single shared entity `"AI"` as the only extractable token) triggered `_entity_overlap = 1.0` (intersection 1 / smaller-set 1) and was false-dropped against a baseline on a completely unrelated topic. Pre-existing behavior, unchanged here to keep Phase-2 scope tight; noted as a Phase-3 candidate. Two mitigations worth considering later: (a) require intersection size ≥ 2 before trusting the ratio, or (b) weight entity overlap by the rarer-side multi-word entities only (tickers / single caps tokens like `AI` are too common to anchor).

### Test results — 9/9 passed
`python3 dev-utils/test_dedup_url.py` → 9/9 after tightening Test 3's scout text to include multi-word distinct entities ("AgiBot Labs", "Maniformer Subsidiary", "Shanghai Tech") so the overlap ratio is meaningful. Run captured in `dev-utils/output.log`.

### Proposal file annotated
Edited `../catalyst-knowledge-graph/docs/proposed_alphasnap_changes.md` — struck through the original one-paragraph "Strategist prompt change" and added an `[Proposed and enhanced by alphasnap]` block documenting: the DE-level URL loss point, the structured-object contract change, the 3-layer dedup design, the `{kept, dropped}` return shape, the intra-batch dedup free-win, and pointers to the tests + this workbench.

---

## 2026-04-17 — Phase 3: Live scout validation

### Plan
Verify the structured-object dedup contract end-to-end through the real LLM pipeline, not just unit tests. Unit tests prove the Python logic; live test proves the Data Engineer LLM actually calls `dedup_findings` with `[{finding, source_url}]` shape (prompt-compliance question).

### Steps
1. Flip `storage.use_gcs` → `false` in `values.yaml` (local mode — GCS master untouched).
2. Seed a local master log with one mock Robotics entry carrying a plausible URL the scout might re-surface (e.g., a recent Accenture/General Robotics / NVIDIA headline).
3. Run `python3 dev-utils/test_single_scout.py` — Robotics pipeline only.
4. Inspect the `[DEDUP]` print lines from `market_team.py` — expect at least one `DROP (url_match)` or `DROP (tfidf)` against the seeded entry if the scout hits adjacent territory; otherwise a clean fresh-run with URLs populated on every kept entry.
5. Check the resulting shard (`market_findings_log_Robotics.json`) — every `deduped` entry should have a non-null `source_url` if the DE is complying with the new contract.
6. Revert `use_gcs` → `true`, clean up local shards, archive run log to `dev-utils/run-logs/`.

### Pre-run archival (per workbench rule)
- Last command: `python3 dev-utils/test_dedup_url.py` → 9/9 pass (summarized above).
- Last action: proposal file annotation (summarized above).
- OK to proceed.

### Live run results — ALL VALIDATED
Run: `dev-utils/run-logs/live_phase2_20260417_195852.log` (Robotics scout, local mode, baseline seeded with `ROB-041626-001` Accenture/GeneralRobotics + `ROB-041626-002` Figure AI $1.5B).

**Outcome — 4/4 Phase-2 goals confirmed:**

1. **URL preservation end-to-end.** Both enriched entries in `market_findings_log_Robotics.json` carried correct canonical URLs (`intellectia.ai/...`, `restofworld.org/...`). Proves the DE LLM is calling `dedup_findings` with structured `{finding, source_url}` objects per the new prompt — otherwise URLs would be null.

2. **Dedup fired correctly.** `[DEDUP] Robotics: 2 unique, 1 duplicates removed (from 3 scout findings vs 2 baseline)`. The dropped finding hit Layer 3 entity_overlap against `ROB-041626-001`. Confirmed reason line: `[DEDUP] Robotics | DROP (entity_overlap) matched=ROB-041626-001`.

3. **entry_id counter respects seeded baseline.** New IDs `ROB-041626-003` (dated 2026-04-16, continues from seed's 001/002) and `ROB-041726-001` (dated 2026-04-17, fresh bucket). Confirms `_count_entries_for` correctly scans master log before write.

4. **Schema complete.** All 8 fields on both enriched entries: `entry_id`, `timestamp`, `category`, `finding`, `insights_sentiment`, `guidance_play`, `price_levels`, `source_url`.

**Note on scout behavior:** this run did NOT trigger Layer 1 (URL fast-path) because the scout surfaced genuinely different stories than the seed (intellectia.ai/restofworld.org vs newsroom.accenture.com). The Accenture seed was still caught via entity overlap on shared entities ("NVIDIA", "AI") — so Layer 3 compensated. To deterministically exercise Layer 1 in a live test, you'd have to seed with a URL the scout already found today, which isn't knowable ahead of time. Layer 1 remains unit-tested (test_1).

### Cleanup post-run
- `values.yaml: use_gcs` reverted to `true`.
- Local artifacts removed: `market_findings_log.json`, `market_findings_log_Robotics.json`.
- Run log archived: `dev-utils/run-logs/live_phase2_20260417_195852.log`.
- GCS master log untouched throughout (`USE_GCS=False` meant local-only writes).

### Phase 2 — DONE
Implementation validated via 9/9 unit tests + live pipeline run. Proposal annotated with `[Proposed and enhanced by alphasnap]`. Ready to deploy when you give the word — `python3 deploy_agent.py`.

---

## 2026-04-17 — Phase 3 (partial): Concurrency audit

### Question raised
What if multiple sector pipelines run in parallel, or two sweep invocations overlap? Where do `dedup_findings`, `append_to_memory_log`, and the `entry_id` counter race?

### Analysis — what's safe
- **Cross-sector parallelism within one sweep:** safe by design. Each sector writes only its own shard (`market_findings_log_{Category}.json`); entry_id counters are category-filtered in `_count_entries_for`, so ROB and CRY counters are independent. Master log is read-only during sector work; `merge_sector_shards` runs once at the end, serial.
- **Intra-sector agent ordering:** safe. `SequentialAgent(sub_agents=[scout, de, strategist])` means DE fully finishes (including dedup) before Strategist starts (including appends). Confirmed by reading `build_sector_pipelines` at market_team.py:692.

### Risks identified
1. **Strategist parallel `append_to_memory_log` calls** (OPEN) — values.yaml:128 says "call append_to_memory_log separately" for each topic. Gemini can batch function calls in a single response. Two concurrent appends on the same shard ⇒ both read shard at state N, both compute the same entry_id (e.g. `ROB-041726-006`), both upload — the second upload wins and the first entry is lost, survivor has a duplicate-looking ID. Not fixed yet. Mitigation candidates: GCS `if_generation_match` on shard upload with retry; post-generation id-collision check in `_next_entry_id`.
2. **Cross-invocation overlap** (FIXED, this phase) — if a manual trigger fires while the scheduler's run is still in flight, two Cloud Run instances both call `streamQuery` on the Agent Engine, both derive entry_ids from the same master-log snapshot, ID collision. Fixed by Cloud Run instance cap (see below).

### Fix #3 — Cloud Run instance cap
Edited `deploy_cloud_func_pipeline.sh`:
- Added `--max-instances=1` and `--concurrency=1` to the `gcloud functions deploy` call for `market-sweep-runner`.
- Second caller now gets HTTP 429 instead of launching a parallel sweep.
- Scheduler layer already had `--max-retry-attempts=0` (prevents retry flake) — belt-and-braces.

### Still open
Risk #1 (strategist parallel appends) is the remaining concurrency hole. Observable as a duplicate `entry_id` in the master log if it ever triggers. Revisit in a future phase with GCS conditional writes or an id-collision retry loop.

---

## 2026-04-17 — Phase 3 (fix #1): GCS conditional write for append

### Decision
Implement `if_generation_match` retry in `append_to_memory_log` to close risk #1 (parallel strategist appends racing on same shard).

**Why write-time, not merge-time:** GCS (and local JSON files) don't support atomic-append. `blob.upload_from_string(json.dumps(shard))` is a whole-object replace. Two parallel appends each download state N, each locally append, each upload their full N+1 version — whoever writes second wins and the loser's entry is **destroyed before it ever lands in storage**. There is nothing for merge-time logic to detect or bump because the lost entry never existed in any persistent form. See earlier discussion.

### Implementation plan
1. Wrap the GCS read→modify→write in a retry loop that:
   - `blob.reload()` to capture current generation
   - Downloads shard
   - Recomputes `entry_id` from fresh shard+master state (this is critical — counter must be fresh per attempt)
   - Appends entry, uploads with `if_generation_match=generation`
   - On `google.api_core.exceptions.PreconditionFailed` (HTTP 412), retry up to N times
2. Apply same pattern to local-mode (use POSIX `flock` or atomic-rename via `os.replace`) so dev and prod behave identically.
3. Test with a `MockBlob` that simulates GCS generation semantics + two threads racing through `threading.Barrier`, verify both entries land with unique IDs.

### Implementation completed
- `market_team.py`: imports `fcntl` and `google.api_core.exceptions.PreconditionFailed`. `append_to_memory_log` now delegates to `_append_gcs` (conditional-write retry loop, `_APPEND_MAX_RETRIES=5`) or `_append_local` (advisory flock). `entry_id` generation moved INSIDE the retry loop so the counter is fresh against post-contention shard state.
- `dev-utils/test_append_race.py`: 2 tests. MockBlob simulates real GCS generation semantics; a two-stage barrier holds all N writers at the upload point on their *first attempt only* (retries bypass) to force a genuine race.

### Test results — 2/2 pass, race genuinely triggered
Run: `python3 dev-utils/test_append_race.py`
- Test 1 (solo append): PASS. Verifies `generation=0` creation path still works.
- Test 2 (5 parallel appends): PASS. Log shows:
  - `[APPEND] generation-match miss on attempt 1/5 — retrying` × 4
  - `[APPEND] ROB-041726-00N landed after 2 attempts (contention resolved)` × 4
  - All 5 entries present in shard with sequential IDs `ROB-041726-001`..`005`
  - Zero data loss, zero duplicate IDs
- Proves both (a) the retry path is actually exercised, and (b) it correctly recomputes counters to produce a contiguous ID sequence under real race conditions.
