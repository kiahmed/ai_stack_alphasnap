# Proposal 0001 — Firestore as the source of truth for findings

**Status:** Draft
**Authors:** imran (with Claude assistance)
**Date:** 2026-04-30
**Companion repo:** `arboryx-admin/` (the API + UI)

## Context

Today the master findings log lives in GCS at
`gs://marketresearch-agents/market_findings_log.json`. Each agent run writes
per-category shards (`market_findings_log_{Category}.json`), then
`merge_sector_shards()` consolidates them into the master JSON at end of run.
A separate `cloud_function_dedup` rewrites the master log periodically with
backups.

The companion `arboryx-admin/` API now reads from **Firestore** (config flag
`FINDINGS_BACKEND=firestore`, validated live 2026-04-29). Firestore is
populated by a one-shot mirror script
(`arboryx-admin/dev-utils/sync_gcs_to_firestore.py`) that diffs the GCS log
against the `findings/` collection on demand. So the read path is already
Firestore-native; the **write origin is still GCS**, with manual sync as the
hand-off.

That sync hand-off is the last piece of GCS-rooted plumbing. Until it goes,
freshly produced findings are not visible to the API until someone runs the
sync script. We want writes to land directly in Firestore so the API serves
live data with no second step.

## Goal

Make `arboryx.ai` write findings directly to Firestore so that:

1. The API in `arboryx-admin/` serves live data with no manual sync.
2. The GCS log + per-category shards + `merge_sector_shards()` step go away.
3. The `sync_gcs_to_firestore.py` mirror in the admin repo retires.
4. GCS is repurposed as a periodic-backup target (Firestore export → GCS),
   not the system of record.

## Non-goals

- Changing the agent pipeline architecture or prompts.
- Changing the entry schema visible to the API or UI.
- Migrating *historical* GCS data — it is already mirrored into Firestore by
  the sync script and that mirror is current. We freeze the GCS log as an
  archive on cutover, then back up Firestore from there forward.

## Current write surface (what changes)

The Firestore-write path replaces these load-bearing pieces in
`market_team.py`:

| Function | What it does today | After cutover |
|---|---|---|
| `_append_gcs(category, date_iso, base_entry)` (lines ~333-387) | Reads per-category shard, computes next `entry_id`, conditional upload with `if_generation_match` retry on parallel-writer contention. | Replaced by `_append_firestore(...)`. Firestore handles concurrent writes natively (transactions for the entry_id counter). |
| `_append_local(category, date_iso, base_entry)` (lines ~389+) | Same, but local-disk + flock. | **Keep** — it's a useful offline dev path. Selected via `storage.backend: local`. |
| `read_memory_log(category, memory_limit)` (lines ~192-256) | Downloads the master GCS JSON, filters by category, returns top-N. | Replaced by Firestore query (`where category == X order by timestamp desc limit N`). Result shape unchanged. |
| `merge_sector_shards()` (lines ~1475-1572) | Reads each category shard, appends to master log, sorts, uploads master, deletes shards. | **Removed.** Each finding is its own Firestore doc; no shards, no merge step. |
| `_get_gcs_client` / `_get_gcs_blob` (lines ~83-98) | Singleton GCS client. | Removed when `use_gcs` is dropped. |

And in the dedup function (`cloud_function_dedup/main.py`):

- Today: downloads master JSON from GCS, runs TF-IDF + entity dedup, backs
  up old JSON to `gs://<bucket>/backups/`, uploads cleaned JSON, emails
  report.
- After: reads `findings/` collection from Firestore, runs the same dedup,
  **deletes** the duplicate docs (Firestore is the index of record), emails
  the same report. Backup is no longer the dedup function's job — see
  "Backup story" below.

The shape of `cloud_function_dedup` stays similar (same dedup math, same
email report); only the IO layer changes.

## Proposed Firestore schema

Same fields as today's JSON entries, with one Firestore-side meta field:

```json
{
  "entry_id": "ROB-041926-001",         // doc id == entry_id
  "timestamp": "2026-04-19",
  "category": "Robotics",
  "finding": "...",
  "sentiment_takeaways": "Sentiment: Bullish | Direct: ... | Indirect: ... | Market Dynamics: ...",
  "guidance_play": "...",
  "price_levels": "...",
  "source_url": "...",
  "tooltip": "...",
  "_synced_at": <Firestore server timestamp>   // set on every create/update
}
```

- **Doc ID = `entry_id`** (e.g. `ROB-041926-001`). Makes idempotent upserts
  trivial and avoids a secondary lookup on the API side.
- **Single collection: `findings/`**. No sub-collections. Every existing API
  query (filter by category, range by timestamp, paginate) is a flat
  collection query. Firestore-admin already serves this collection — we are
  not changing what the admin API reads, just who populates it.
- `_synced_at` is set with `firestore.SERVER_TIMESTAMP` on each write. The
  admin API derives its `data_generation` cache key from
  `max(_synced_at_ms) * 100_000 + len(data)` — that math keeps working.
- The `_hash` field used by the sync script is **dropped**. It was a
  diff-marker for the GCS→Firestore mirror; with direct writes there's no
  diff to compute.

## Concurrency: entry_id allocation

Today's GCS path serializes parallel appends via `if_generation_match`
(HTTP 412 → reload → retry up to 5 times). The race we have to close:
two scouts in the same category landing on the same date both compute
`ROB-041926-003` from a stale read.

Two viable Firestore patterns:

### Option A — Counter doc per (category, date), updated in a transaction

```
entry_counters/ROB-041926   { next: 4 }
findings/ROB-041926-003     { ... }
```

Allocation:

```python
@firestore.transactional
def allocate(tx):
    counter_ref = db.collection("entry_counters").document(f"{prefix}-{date_compact}")
    snap = counter_ref.get(transaction=tx)
    n = (snap.to_dict() or {}).get("next", 1) if snap.exists else 1
    tx.set(counter_ref, {"next": n + 1}, merge=True)
    return n
```

- Pros: deterministic IDs, matches today's format exactly, no scans.
- Cons: one extra small write per finding (~$0.18/M, negligible at 30/day).

### Option B — Use a server-time-derived suffix, drop strict counters

```
findings/ROB-041926-152345781   # epoch-ms suffix
```

- Pros: no counter doc, no transaction.
- Cons: `entry_id` shape changes; the UI and any saved bookmarks break.

**Recommendation: Option A.** Keeps the `entry_id` contract, costs are
trivial, and the transaction semantics are simpler than what `_append_gcs`
already does.

## Config surface

Add a `storage.backend` toggle to `values.yaml` (replaces the binary
`use_gcs`):

```yaml
storage:
  backend: firestore        # firestore | gcs | local
  memory_limit: 10
  batch_size: 3

  # only consulted when backend == local
  local_path: "market_findings_log.json"

  # only consulted when backend == gcs (legacy; will be removed in a follow-up)
  gcs_path: "gs://marketresearch-agents/market_findings_log.json"
```

This keeps every legacy path runnable for the cutover window, and lets a
dev flip `backend: local` for offline runs.

## Backup story

GCS becomes the **backup destination**, not the source of truth.

- One-time: cap the existing master log (rename to
  `gs://marketresearch-agents/archive/market_findings_log_pre-firestore-cutover-YYYYMMDD.json`)
  so it stays as a frozen archive.
- Recurring: Cloud Scheduler → Firestore managed export → GCS bucket,
  weekly. Firestore exports land as protobuf-format directories; restore is
  via `gcloud firestore import`. This is supported natively, no custom code.
- Retention: 4 weekly exports (~1 month rolling) is plenty for a per-day
  intelligence log. Lifecycle rule on the bucket auto-deletes older.

The `cloud_function_dedup` function continues to send the dedup report
email, but its "backup the master JSON before mutation" step disappears
because (a) Firestore deletions are independently recoverable from the
weekly export and (b) we don't have a master JSON to clobber any more.

## Cutover plan

Each step is independently revertible — no Big Bang.

1. **Add** `_append_firestore` + `read_memory_log_firestore` behind a
   `storage.backend == "firestore"` switch. `gcs` and `local` paths stay
   intact. Deploy to Vertex AI; run one cycle with `backend: gcs` to
   confirm zero-diff regression.
2. **Run a parallel-write cycle** with `backend: firestore` in a non-prod
   trigger. Confirm:
   - All 6 categories produce findings.
   - Entry IDs are dense (no gaps, no collisions) per category-day.
   - The admin API serves the new findings within seconds of the run
     completing.
3. **Flip prod** by setting `backend: firestore` in `values.yaml` and
   redeploying via `deploy_arboryx.ai_engine.py`. Run one full daily cycle.
4. **Rewrite** `cloud_function_dedup/main.py` to read/delete via Firestore
   instead of mutating the GCS JSON. Deploy. Run one cleanup cycle and
   confirm the email report matches what the JSON-flavored function would
   have produced.
5. **Stand up the backup pipeline:** Cloud Scheduler → Firestore export
   → `gs://marketresearch-agents/firestore-exports/`. Validate the first
   export round-trips via `gcloud firestore import` into a scratch project.
6. **Freeze GCS:** rename the master JSON to
   `archive/market_findings_log_pre-firestore-cutover-<date>.json`. Drop the
   `gcs_path` config. Remove `_append_gcs`, `merge_sector_shards`,
   `_get_gcs_client/_get_gcs_blob` from `market_team.py`. Drop
   `google-cloud-storage` from `requirements.txt` if nothing else uses it.
7. **Retire admin-side artifacts:** remove
   `arboryx-admin/dev-utils/sync_gcs_to_firestore.py`, drop the
   `_load_findings_gcs` branch, drop the `FINDINGS_BACKEND` flag, redeploy
   `arboryx-admin-api`.

Steps 1-3 can land in one PR (the Firestore writer + config switch).
Steps 4-7 are separate, smaller PRs that follow once step 3 has soaked.

## Risks / open questions

- **Entry ID transaction throughput:** If two scouts in the same category
  produce findings within the same transaction window, one will retry. At
  current volume (~5 findings/category/day, two-scout concurrency) this is
  effectively zero contention. Worth a load-test stub before flipping prod.
- **Firestore index on `(category, timestamp desc)`:** required for the
  read-memory-log query. The admin API already exercises this index — no
  new index work needed if we reuse the same shape.
- **Cost ceiling:** ~30 finding writes/day × ~6 reads/run × 1 dedup pass.
  Well inside the Firestore free tier even at 10× growth. Confirmed
  separately on the admin side.
- **Dedup function scope:** rewriting `cloud_function_dedup` is non-trivial
  (TF-IDF + entity overlap + email pipeline are reused, but the IO layer is
  new). Keep this as its own follow-up PR after step 3 has soaked.
- **`use_gcs: bool` deprecation:** anything in `dev-utils/` that hard-codes
  `use_gcs` (e.g. `master_log_corrector.py`, the various `test_*.py`) needs
  to be audited and either updated to read `storage.backend` or marked as
  GCS-archive-only utilities. Audit pass goes with step 6.

## Acceptance criteria

- A daily run with `storage.backend: firestore` lands every finding in
  `findings/` with a valid `entry_id` and no shard files in GCS.
- `arboryx-admin-api`'s `?action=stats` and `?action=findings` return live
  data within seconds of the run completing — no manual sync.
- The dedup function operates on Firestore, sends the same email report
  format, and removes duplicates from `findings/`.
- A weekly Firestore export lands in
  `gs://marketresearch-agents/firestore-exports/` and round-trips into a
  scratch import.
- `dev-utils/sync_gcs_to_firestore.py` is deleted from `arboryx-admin/`.
- `FINDINGS_BACKEND` is removed from `arboryx_admin_backend.config` and the
  Cloud Function code.

## Out of scope (future proposals)

- User auth (Firebase Auth + `users/{uid}` collection) — already on the
  admin-side roadmap.
- Server-side cursor pagination for the API — deferred until volume
  demands it.
- Cross-region Firestore replication — current single-region (`nam5`) is
  fine for this workload.
