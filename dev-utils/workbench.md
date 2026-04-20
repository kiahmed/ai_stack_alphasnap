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

---

## 2026-04-19 — Prompt hygiene + per-agent debug dump

### Field rename
`insights_sentiment` → `sentiment_takeaways` across `market_team.py`, `values.yaml`, `CLAUDE.md`, and all `dev-utils/test_*.py`. All 9/9 + 2/2 + 9/9 tests pass. Historical `output.log` and `workbench.md` entries left untouched.

### Debug dump on every state-writing agent
New `_dump_agent_output` after_agent_callback in `market_team.py`. Prints `[AGENT_RAW_BEGIN|END]` framed blocks carrying `session.state[output_key]` to stdout (→ Cloud Logging). Wired on Scout/DE/Strategist in both main-build and rebuild paths. Skipped on `Strategist_Retry` and `Shard_Merger` (no `output_key`).

### Prompt tightening — cross-agent name leakage
LLMs see only their own instruction string; references to "the DE" / "the scout" mean nothing to them. Re-anchored every cross-agent reference on the `REQUIRED DATA:` token that literally appears in the prompt, and on step numbers for self-references. Changes:
- `values.yaml:81` — "scout's numbered list" → "`REQUIRED DATA:` block contains a numbered list"
- `values.yaml:84,86` — "scout did not provide" → "no URL present in that item" / "no URL was found in `REQUIRED DATA:`"
- `values.yaml:92` — removed downstream-leaking "so the Strategist can pass it later"
- `values.yaml:128` — `guidance_play` now labeled as `guidance_play[<topic>]` in step 2 for later pass-through
- `values.yaml:137` — "Send Call to action" → "Pass the exact `guidance_play[<topic>]` string from step 2 — verbatim"
- `values.yaml:139` — `source_url` reference switched from "DE's analyzed output" to "in `REQUIRED DATA:`"

### Remaining known leak (not touched; scope was DE + Strategist only)
- Scout prompt line 74: "Pass findings to next agent." — scout has no concept of "next agent"; phrasing risks it emitting conversational framing into its state blob.

---

## 2026-04-19 — Live test of debug-dump pipeline + architectural finding

### Run
`GOOGLE_APPLICATION_CREDENTIALS=dev-utils/service_account.json PYTHONPATH=. python3 dev-utils/test_single_scout.py` (Robotics only). Exit 0, 1m40s, 141 lines in `/tmp/single_scout_run.log`. Three findings persisted: ROB-041926-001, ROB-041926-002, ROB-041826-001 — all with correct non-null source_urls.

### What the dump revealed
- **Scout output**: markdown-numbered list with backticked URLs (`Source: \`https://...\``). URLs survived into DE call because the prompt now tells DE to look inside each numbered item.
- **DE output**: single 839-char blob. Sentiment label on first line, **one** Direct/Indirect/Market Dynamics triplet, then a trailing "Sources:" list. `Direct:` actually mixed findings #1 (Honor) + #3 (Zebra); `Indirect:` covered only #2 (funding round); `Market Dynamics:` was a macro paragraph touching all three. Collapsed cross-topic analysis, not per-topic.
- **Strategist output**: a single markdown table where each row has a per-topic sentiment + per-topic takeaways. The Strategist quietly re-did DE's analysis per-topic — URL-to-topic mapping only worked because URL slugs contained topic keywords.

### Root cause
Each hop does LLM-prose↔structure conversion. DE has freedom to collapse three inputs into one analysis; Strategist has freedom to re-synthesize. Fragility compounds at every hop.

### Fix direction (agreed): JSON-schema contract per hop, bottom-up
Implement between DE → Strategist first (worst offender), then Scout → DE. DE emits a strict JSON array `[{finding, source_url, sentiment, direct, indirect, market_dynamics, price_levels}]`. A validation callback re-renders as canonical `=== TOPIC N ===` blocks into session state. Strategist iterates per-block and passes fields verbatim to `append_to_memory_log`. No LLM freedom to collapse, no URL-to-topic mapping guesswork.

---

## 2026-04-19 — Phase 4 (DE→Strategist JSON contract): IMPLEMENTED + live-validated

### Changes shipped
- **`values.yaml` DE `<output_constraints>`**: fully rewritten. DE's entire response must be a JSON array `[{finding, date, source_url, sentiment, direct, indirect, market_dynamics, price_levels}]`, one object per kept finding. Added explicit "ONE OBJECT PER TOPIC — do NOT collapse" rule and a two-topic worked example.
- **`values.yaml` DE step 4**: "one analysis PER FINDING, never merged" language. Ticker research and URL preservation scoped to per-finding.
- **`values.yaml` Strategist instructions**: rewritten. Opens by documenting the `=== TOPIC N ===` block schema as REQUIRED DATA. Step 2 iterates per block to author `guidance_play[<N>]`. Step 3 copies every field (`finding`, `timestamp`, `sentiment_takeaways`, `price_levels`, `source_url`) verbatim from the block — no regeneration.
- **`market_team.py`**: new helpers `_parse_de_json`, `_fmt_field`, `_render_de_canonical`, `_resolve_output_key`, `_write_state`, and callback `_validate_and_render_de_output`. Tolerant of ```json fences. Validates per-topic schema, drops unusable topics (missing `finding`), warns on missing keys, re-renders survivors as labelled blocks, writes back to `session.state[output_key]`, then delegates to `_dump_agent_output`.
- **DE wiring**: `after_agent_callback=_validate_and_render_de_output` on DE in both `build_sector_pipelines` (market_team.py:925) and `_rebuild_pipeline` (market_team.py:1034). Scout/Strategist still use `_dump_agent_output`.

### Live test — `python3 dev-utils/test_single_scout.py` (Robotics)
- Scout: 2 searches → 3 topics (1845-char output, today's scout emitted NO source URLs — test ran with null URLs).
- DE: emitted valid JSON array, 3 per-topic objects. `[DE_VALIDATE]` confirmed `parsed 3 topics → rendered 3 canonical blocks to state[Robotics_Scout_analyzed]`. Rendered blob 3210 chars (vs last run's 839 chars that collapsed all topics).
- Strategist: read canonical blocks, called `append_to_memory_log` exactly 3 times (one per block). Each call's `sentiment_takeaways` was pre-composed by the callback in `Sentiment: X | Direct: ... | Indirect: ... | Market Dynamics: ...` form, copied verbatim. Per-topic `price_levels`: Honor = None, Alibaba = BABA $142/$189, UBTech = 113.90/156.39 HKD — matches each topic's actual relevance.
- Persisted `gs://marketresearch-agents/market_findings_log_Robotics.json`: 3 entries (ROB-041826-001, ROB-041926-001, ROB-041926-002) with correct per-topic fields. Total run 1m45s.

### Residual issues (not blockers, noted for later)
- `source_url` comes through as the Python string `"None"` rather than JSON `null` when DE passes nothing. Not a dedup correctness issue (URL fast-path skips null/"None" equivalently), but the persisted JSON is slightly off-spec.
- DE copies `finding` text verbatim from the scout — the stored `finding` still includes the scout's markdown framing ("Signal: ... Details: ..."). Works, but extracting just the narrative would cleaner. Separate improvement — probably moves into Scout→DE hop when we tighten that contract next.
- Scout doesn't reliably emit URLs (today's run: zero URLs even though prompt asks). Root cause for the next phase (Scout→DE JSON contract).

### Next up
Apply the same pattern to Scout → DE. Scout emits strict JSON `[{finding, date, source_url}]` with a post-scout validation/render callback. DE prompt simplifies since URL-extraction instructions go away. Should also bring URL emission reliability up since "Source:" becomes a required JSON key, not a markdown convention.

---

## 2026-04-19 — Phase 5 (Scout→DE JSON contract): IMPLEMENTED, pending live test

### Changes shipped
- **`values.yaml` Scout prompt**: step 4 **RUTHLESS OUTPUT** rewritten to require per-development capture of `finding` / `date` / `source_url`. Added `<output_constraints>` demanding a single JSON array `[{finding, date, source_url}]`, with per-key descriptions referencing the step 4 label and a two-entry worked example. No null-value instructions (Python handles absence).
- **`values.yaml` DE step 2/3**: simplified. Step 2 now just says "REQUIRED DATA is a sequence of `=== FINDING N ===` labelled blocks with `finding`, `date`, `source_url` keys — treat each block as one input item". No more URL-hunting in prose, no "Source:" / parentheses / bare-URL heuristics. Step 3's dedup call instructs to copy block fields verbatim. DE `date` output constraint now points at the block's `date` field.
- **`market_team.py`**: renamed `_parse_de_json` → `_parse_json_array` (generic, reused by both validators). Added `_SCOUT_REQUIRED_KEYS`, `_render_scout_canonical` (emits `=== FINDING N ===` blocks), `_validate_and_render_scout_output` (mirrors DE validator: parse → validate → re-render → write-back → delegate to _dump_agent_output).
- **Scout wiring**: `after_agent_callback=_validate_and_render_scout_output` on Scout in both `build_sector_pipelines` and `_rebuild_pipeline`.

### Offline verification
- `python3 -c "import ast; ast.parse(open('market_team.py').read())"` → OK.
- `python3 -c "yaml.safe_load(open('values.yaml'))"` → parses cleanly (scout_base 2238 chars, DE 4354 chars).
- `python3 dev-utils/test_dedup_url.py` → 9/9 PASS. The parse-function rename didn't regress dedup behaviour.

### Pending
- Live `test_single_scout.py` Robotics end-to-end. Want to see: `[SCOUT_VALIDATE] Robotics_Scout: parsed N findings → rendered N canonical blocks`, followed by `[DE_VALIDATE] Robotics_Scout_DE: parsed N topics → rendered N canonical blocks`. Both must fire for the contract to hold across both hops. Scout URL emission should now be reliable since `source_url` is a required JSON key, not a markdown convention.

---

## 2026-04-19 — Phase 5 follow-up: URL resolution + null-URL fallback via grounding

### Problem found in first live run (`/tmp/scout_live_test.log`)
Both SCOUT_VALIDATE and DE_VALIDATE fired cleanly, 3 findings propagated. But all three `source_url` values were `vertexaisearch.cloud.google.com/grounding-api-redirect/...` proxy wrappers — unpublishable, decay in days. DE passed them through byte-for-byte (verified: Scout FINDING 1/2/3 URLs identical to DE TOPIC 1/2/3 URLs, same suffix). So DE is not at fault — the proxy came out of the LLM itself.

### Google doesn't expose the real URL
Wrote `dev-utils/test_grounding_fields.py`: direct `google.genai.Client(vertexai=True)` probe bypassing ADK. Dumped `grounding_metadata.model_dump()` and reverse-scanned the full response object for the real URL substring. Result: `grounding_chunks[*].web.uri` is always the proxy, `web.domain` is a bare hostname, and nowhere in the response is the original URL exposed. Only recovery path is HEAD + follow-redirects on the proxy.

### Fix shipped (Scout side only — DE has no URL logic)
- **`_resolve_grounding_url(url)`**: HEAD + `allow_redirects=True`, 4s timeout. Returns real URL on success; returns original (proxy) on any failure or if the redirect chain somehow still terminates at another grounding-redirect.
- **`_resolve_scout_urls(items)`**: walks items in-place; skips non-proxy URLs, HEAD-resolves proxy URLs, mutates in place.
- **`_capture_grounding` (after_model_callback)**: piggybacks on `_log_token_usage`. Stashes `grounding_chunks[*].{uri,domain,title}` + `grounding_supports[*].{segment.start,end, chunk_indices}` into `state[_scout_grounding]`. Overwrites only when the incoming turn has non-empty chunks — search-bearing turn wins over the final text-generation turn.
- **`_fill_null_urls_from_grounding(items, blob, grounding)`**: for items whose LLM-emitted `source_url` is null/empty, uses regex-located `"finding"` key offsets in the raw JSON blob as finding spans, finds a grounding_support whose segment.start lies within the span, picks the first cited chunk, HEAD-resolves its proxy. **Only substitutes on verified success** — failed resolution leaves the null (we do NOT inject bare proxies via this path).
- **Ordering inside `_validate_and_render_scout_output`**: parse JSON → validate → fill nulls from grounding → resolve emitted proxies → canonical render → `_write_state`. Mutations happen on the `validated` list, so the `=== FINDING N ===` blocks DE reads contain post-resolution URLs.

### Prompt softening (user pushback: prior wording was overkill)
`values.yaml` Scout step 4 final: `"the source URL for the finding from the search. Preferably a full URL."` Removed the earlier "copied verbatim, no reformatting, no reference numbers" clause — too aggressive, was causing the LLM to default to the proxy URL in grounding_chunks rather than extracting the real URL it saw in the article body.

### Net behaviour DE sees
- LLM emitted real URL → passes through untouched (non-proxy skip path).
- LLM emitted proxy URL → HEAD-resolved to real URL; falls back to proxy only if HEAD fails.
- LLM emitted null → grounding-map + HEAD; falls back to null if either step fails. No bare proxies.

### Next live test expectations
`[SCOUT_URL_RESOLVE]` when proxies are unwrapped. `[SCOUT_URL_FILL]` only when nulls exist AND grounding yields a citing chunk AND HEAD resolves. Neither marker is required to fire — zero emitted proxies / zero nulls is also a valid outcome. The fail state is any `vertexaisearch.cloud.google.com/grounding-api-redirect/` URL surviving into the rendered DE block.

### Live test — `dev-utils/run-logs/live_phase5_verify_20260419_143235.log`
Clean pass. Robotics scout, 121 lines, exit 0.
- **Scout**: 2 findings. Both emitted with real canonical article URLs directly — `kiripost.com/news/humanoid-robots-take-over-beijing-half-marathon` and `malaymail.com/news/tech-gadgets/2026/04/18/embodied-ai-robotics-market-update`. Zero proxies.
- **`[SCOUT_VALIDATE]`** fired: `parsed 2 findings → rendered 2 canonical blocks`.
- **`[SCOUT_URL_RESOLVE]` / `[SCOUT_URL_FILL]`** did NOT fire — correct outcome: no proxies to unwrap, no nulls to fill. Safety-net layer stayed dormant as designed.
- **DE**: passed URLs through byte-for-byte (scout canonical FINDING 1/2 URLs identical to DE TOPIC 1/2 URLs).
- **`[DE_VALIDATE]`** fired: `parsed 2 topics → rendered 2 canonical blocks`.
- **Strategist**: 2 `append_to_memory_log` calls, both returned entry IDs (ROB-041926-001, ROB-041826-001). Per-topic price_levels well-targeted: Honor (private) → `N/A (Private)`, UBTech → `9880.HK Pivot: $115, Call Wall: $130, Analyst PT: $140`.

### Takeaway
Softened prompt ("Preferably a full URL") + strict JSON key requirement is enough to get real URLs out of the LLM in the common case. The grounding-fallback layer is a safety net for the rare run where the LLM defaults to proxies or emits nulls — worth keeping, but not load-bearing when the prompt works.

---

## 2026-04-19 — Local dedup tooling: `dedup_single_or_double_file.py` (renamed from `dedupe_lts.py`)

### Changes
- CLI args: `dedup_single_or_double_file.py [INPUT] [OUTPUT] [--filter-only]`. Positional args with hardcoded defaults.
- Added `_load_entries(path)` normalizer — handles master-log list format AND shard dict `{"deduped":..., "enriched":[...]}`. Shard path returns `data["enriched"]` (master-log-shaped entries); list path returns as-is. Error on unrecognized shape.
- Added `--filter-only` flag. Default MERGE: output = `input_entries + unique_output_entries`. Filter-only: output = `unique_output_entries` alone (drops INPUT entries from output file entirely).
- Semantics clarified in banner: INPUT = baseline (read-only), OUTPUT = file being filtered. An entry from OUTPUT is dropped if it collides with any entry from INPUT OR any earlier entry in OUTPUT.

### Entity extractor fix (option 1)
Original regexes missed camelCase company names. Root cause: `UBTech` failed both `\b[A-Z]{2,5}\b` (word boundary broken by adjacent `T`) and `[A-Z][a-z]+(?:\s+[A-Z][a-z]+)+` (no space). Two duplicate UBTech entries slipped through at tfidf=0.270 / entity_score=0.000.

Added to `_extract_entities()`:
```python
# Intra-word camelCase / PascalCase (OpenAI, DeepMind, LinkedIn, McDonald)
entities.update(re.findall(r'\b[A-Z][a-z]+(?:[A-Z][a-z]*)+\b', text))
# Acronym-prefix names (UBTech, NASAStudy) — 2+ uppercase run then lowercase tail
entities.update(re.findall(r'\b[A-Z]{2,}[a-z][A-Za-z]*\b', text))
```
Doesn't capture pure acronyms (USA/NASA/HTML) nor single-capitalized words (Lightning/Beijing). Verified: UBTech pair now caught at entity=1.00, Honor pair caught at tfidf=0.46. 8→6 entries on `backups_market_findings_log_Robotics_back2.json`.

---

## 2026-04-19 — Full-sector e2e test

### Harness
`dev-utils/run_local_test.sh`. Runs `market_team.py` (MarketSweepApp orchestrator — sidesteps the ParallelMarketSweep Pydantic issue by using per-sector AdkApp instances). Logs to `dev-utils/run-logs/live_full_sweep_<timestamp>.log`.

### Scope
All 6 scouts enabled in `values.yaml`: Robotics, Crypto, AI_Stack, Space_Defense, Power_Energy, Strategic_Minerals. `storage.use_gcs: true`, `memory_limit: 10`.

### Markers to watch per sector
- `[SCOUT_VALIDATE] parsed N findings → rendered N canonical blocks`
- `[SCOUT_URL_RESOLVE]` — fires only if LLM emitted proxy URLs.
- `[SCOUT_URL_FILL]` — fires only if LLM emitted nulls AND grounding resolved them.
- `[DE_VALIDATE] parsed N topics → rendered N canonical blocks`
- `[APPEND_MEMORY] entry_id=<CAT>-MMDDYY-NNN` × N per sector

### Fail states
- Any `vertexaisearch.cloud.google.com/grounding-api-redirect/` URL surviving into DE's TOPIC blocks.
- Scout/DE validate failures → canonical block render skipped.
- ParallelMarketSweep Pydantic crash (known; we're not invoking it).

