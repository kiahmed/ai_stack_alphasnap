"""arboryx-master-log-dedup — bi-weekly Cloud Function that scans the master
findings log for duplicates that slipped past the runtime DE dedup, removes
the newer occurrence (keeping the original entry_id intact for downstream
references), and emails a report to arborix.platform@gmail.com only if
duplicates were actually found.

Triggered by Cloud Scheduler (HTTP POST). Each invocation:
  1. Downloads gs://<bucket>/market_findings_log.json
  2. Per-category, walks chronologically. For each entry it computes the
     newest MEMORY_LIMIT prior same-category entries (same baseline shape the
     runtime dedup uses) and runs the same matching layers:
       - Layer 1: URL equality
       - Layer 2: TF-IDF cosine on finding text
       - Layer 3: entity overlap on tickers/companies/place-names/$amounts
     With a novelty escape (case-insensitive set diff) and a TFIDF_FLOOR for
     entity-only drops.
  3. If any duplicates were found, backs the master log up to
     gs://<bucket>/backups/master_findings_log.backup-<UTC stamp>.json,
     writes the cleaned log back, and emails a report.
  4. Logs a JSON summary in all cases. NO email on the empty path.

Retry policy: every GCS read/write/email send is wrapped in a 3-attempt loop
with 5s sleeps. A failure on the first attempt does NOT short-circuit the
function — it just logs and retries.

Algorithm primitives (entity extraction, TF-IDF, novelty escape) are kept in
sync with market_team.py:_extract_entities / _entity_overlap / dedup_findings.
If you change one, change both — there is no shared module yet.
"""

from __future__ import annotations

import json
import logging
import math
import os
import re
import smtplib
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from email.mime.text import MIMEText
from typing import Any

from google.cloud import storage

# ──────────────────────────────────────────────────────────────────────────
# Config (env vars set by deploy.sh)
# ──────────────────────────────────────────────────────────────────────────
PROJECT_ID = os.environ["PROJECT_ID"]
GCS_BUCKET = os.environ["GCS_BUCKET"]
GCS_OBJECT = os.environ.get("GCS_OBJECT", "market_findings_log.json")
BACKUP_PREFIX = os.environ.get("BACKUP_PREFIX", "backups/")

GMAIL_FROM = os.environ.get("GMAIL_FROM", "arborix.platform@gmail.com")
GMAIL_TO = os.environ.get("GMAIL_TO", "arborix.platform@gmail.com")
# Mounted by --set-secrets in deploy.sh. The function only reads it at send
# time so a missing secret fails the email step, not the dedup itself.
GMAIL_APP_PASSWORD_ENV = "GMAIL_APP_PASSWORD"

DRY_RUN_DEFAULT = os.environ.get("DEFAULT_DRY_RUN", "false").lower() == "true"

# Thresholds — kept aligned with values.yaml + market_team.py defaults.
TFIDF_THRESHOLD = float(os.environ.get("TFIDF_THRESHOLD", "0.35"))
ENTITY_THRESHOLD = float(os.environ.get("ENTITY_THRESHOLD", "0.6"))
NOVELTY_MIN = int(os.environ.get("NOVELTY_MIN", "3"))
TFIDF_FLOOR_FOR_ENTITY = float(os.environ.get("TFIDF_FLOOR_FOR_ENTITY", "0.15"))
MEMORY_LIMIT = int(os.environ.get("MEMORY_LIMIT", "10"))

RETRY_ATTEMPTS = int(os.environ.get("RETRY_ATTEMPTS", "3"))
RETRY_DELAY_SEC = int(os.environ.get("RETRY_DELAY_SEC", "5"))

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("dedup")


# ──────────────────────────────────────────────────────────────────────────
# Retry helper
# ──────────────────────────────────────────────────────────────────────────
def with_retry(label: str, fn, attempts: int = RETRY_ATTEMPTS, delay: int = RETRY_DELAY_SEC):
    last_exc: Exception | None = None
    for i in range(1, attempts + 1):
        try:
            return fn()
        except Exception as exc:
            last_exc = exc
            log.warning("[%s] attempt %d/%d failed: %s", label, i, attempts, exc)
            if i < attempts:
                time.sleep(delay)
    raise RuntimeError(f"{label} exhausted {attempts} attempts") from last_exc


# ──────────────────────────────────────────────────────────────────────────
# Dedup primitives — port of market_team.py helpers
# ──────────────────────────────────────────────────────────────────────────
_STOP_UPPER = {
    'THE','AND','FOR','BUT','NOT','YOU','ALL','CAN','HER','WAS','ONE','OUR',
    'OUT','ARE','HAS','HIS','HOW','ITS','MAY','NEW','NOW','OLD','SEE','WAY',
    'WHO','DID','GET','HIM','LET','SAY','SHE','TOO','USE','CEO','CFO','CTO',
    'COO','IPO','ETF','GDP','API','USA','USD','EUR','GBP','WITH','THIS','THAT',
    'FROM','THEY','BEEN','HAVE','WILL','EACH','MAKE','LIKE','LONG','VERY',
    'WHEN','WHAT','YOUR','SOME','THEM','THAN','MOST','ALSO','INTO','OVER',
    'SUCH','JUST','NEAR','TERM','PER','VIA','KEY','PRE','PRO','BOTH','ONLY',
    'SAME','MORE','LESS','FULL','HIGH','LOW','NEXT','LAST','WEEK','YEAR',
    'NEWS','PLUS','DEAL',
}

_CAP_STOP = {
    'the','this','that','these','those','an','a','and','or','but','of','to',
    'in','on','at','by','for','with','from','into','as','is','was','were',
    'will','would','could','should','can','may','might','must','it','its',
    'their','they','them','our','us','we','he','she','his','her','him',
    'while','though','although','because','since','until','before','after',
    'recent','recently','new','newer','newest','near','far','more','most',
    'first','last','later','earlier','today','yesterday','tomorrow',
    'monday','tuesday','wednesday','thursday','friday','saturday','sunday',
    'january','february','march','april','june','july','august',
    'september','october','november','december',
    'inc','corp','co','ltd','llc','plc','group','holdings','company','companies',
}


def _tokenize(text: str) -> list[str]:
    return re.findall(r"[a-z0-9]+(?:'[a-z]+)?", text.lower())


def _build_idf(documents: list[list[str]]) -> dict[str, float]:
    n = len(documents)
    df: Counter = Counter()
    for doc in documents:
        df.update(set(doc))
    return {term: math.log((n + 1) / (count + 1)) + 1 for term, count in df.items()}


def _tfidf_vector(tokens: list[str], idf: dict[str, float]) -> dict[str, float]:
    tf = Counter(tokens)
    return {term: freq * idf.get(term, 1.0) for term, freq in tf.items()}


def _cosine_sim(a: dict[str, float], b: dict[str, float]) -> float:
    common = set(a.keys()) & set(b.keys())
    dot = sum(a[k] * b[k] for k in common)
    mag_a = math.sqrt(sum(v * v for v in a.values()))
    mag_b = math.sqrt(sum(v * v for v in b.values()))
    if mag_a == 0 or mag_b == 0:
        return 0.0
    return dot / (mag_a * mag_b)


def _tfidf_similarity(a: str, b: str, idf: dict[str, float]) -> float:
    return _cosine_sim(_tfidf_vector(_tokenize(a), idf), _tfidf_vector(_tokenize(b), idf))


def _extract_entities(text: str) -> tuple[set[str], set[str]]:
    entities: set[str] = set()
    tickers: set[str] = set()
    for m in re.findall(r'\$[\d,.]+\s*[BMKTbmkt](?:illion|rillion)?', text):
        entities.add(m.strip().upper())
    entities.update(re.findall(r'[\d,.]+%', text))
    entities.update(re.findall(r'[\d,.]+[xX]\b', text))
    tickers.update(re.findall(r'\$([A-Z]{1,5})\b', text))
    for t in re.findall(r'\b([A-Z]{2,5})\b', text):
        if t not in _STOP_UPPER:
            tickers.add(t)
    entities.update(re.findall(r'[A-Z][a-z]+(?:\s+[A-Z][a-z]+)+', text))
    entities.update(re.findall(r'\b[A-Z][a-z]+(?:[A-Z][a-z]*)+\b', text))
    entities.update(re.findall(r'\b[A-Z]{2,}[a-z][A-Za-z]*\b', text))
    entities.update(re.findall(r'\b(?:[A-Z]\.){2,}', text))
    multiword_tokens = {tok for ent in entities for tok in ent.split()}
    for t in re.findall(r'\b([A-Z][a-z]{3,})\b', text):
        if t in multiword_tokens: continue
        if t.upper() in _STOP_UPPER: continue
        if t.lower() in _CAP_STOP: continue
        entities.add(t)
    return entities | tickers, tickers


def _merge_substring_entities(entities: set[str]) -> set[str]:
    merged: set[str] = set()
    for ent in sorted(entities, key=len, reverse=True):
        ent_l = ent.lower()
        if not any(ent_l in existing.lower() for existing in merged):
            merged.add(ent)
    return merged


def _entity_overlap(a: set[str], b: set[str]) -> float:
    if not a or not b:
        return 0.0
    ma, mb = _merge_substring_entities(a), _merge_substring_entities(b)
    intersection = ma & mb
    for ea in ma:
        for eb in mb:
            if ea != eb and (ea.lower() in eb.lower() or eb.lower() in ea.lower()):
                intersection.add(ea)
    smaller = min(len(ma), len(mb))
    return len(intersection) / smaller if smaller > 0 else 0.0


# ──────────────────────────────────────────────────────────────────────────
# Sweep — chronological per-category walk identical to the runtime baseline
# ──────────────────────────────────────────────────────────────────────────
def find_duplicates(entries: list[dict]) -> list[dict]:
    """Return a list of duplicate-pair records. Each record:
        {target_id, target_ts, target_finding, target_url,
         matched_id,  matched_ts,  matched_finding, matched_url,
         category, reason, scores: {tfidf, entity}}
    `target` is the NEWER entry that will be removed; `matched` is the older
    original kept in place.
    """
    by_cat: dict[str, list[dict]] = defaultdict(list)
    for e in entries:
        cat = e.get("category", "Unknown")
        by_cat[cat].append(e)
    for cat in by_cat:
        by_cat[cat].sort(key=lambda x: (x.get("timestamp", ""), x.get("entry_id", "")))

    dups: list[dict] = []
    for cat, group in by_cat.items():
        for i, target in enumerate(group):
            if i == 0:
                continue
            prior = group[:i]
            # Newest mem_limit first — same shape dedup_findings now sees.
            baseline = sorted(prior, key=lambda x: x.get("timestamp", ""), reverse=True)[:MEMORY_LIMIT]
            if not baseline:
                continue

            t_text = target.get("finding", "") or ""
            t_url = target.get("source_url") or None
            b_texts = [b.get("finding", "") or "" for b in baseline]
            b_urls = [(b.get("source_url") or None) for b in baseline]

            # Layer 1: URL equality
            matched_idx: int | None = None
            reason: str | None = None
            scores: dict[str, float | None] = {"tfidf": None, "entity": None}

            if t_url:
                for j, bu in enumerate(b_urls):
                    if bu and bu == t_url:
                        matched_idx, reason = j, "url_match"
                        break

            # Layer 2+3: tfidf / entity_overlap with novelty escape + tfidf floor
            if matched_idx is None:
                idf = _build_idf([_tokenize(t) for t in (b_texts + [t_text])])
                t_ent, t_tk = _extract_entities(t_text)
                for j, b_text in enumerate(b_texts):
                    b_ent, _ = _extract_entities(b_text)
                    tfidf = _tfidf_similarity(t_text, b_text, idf)
                    ent = _entity_overlap(t_ent, b_ent)
                    tfidf_hit = tfidf >= TFIDF_THRESHOLD
                    entity_hit = ent >= ENTITY_THRESHOLD and tfidf >= TFIDF_FLOOR_FOR_ENTITY
                    if not (tfidf_hit or entity_hit):
                        continue
                    base_lc = {e.lower() for e in b_ent}
                    tk_lc = {t.lower() for t in t_tk}
                    novel = {e for e in t_ent if e.lower() not in base_lc and e.lower() not in tk_lc}
                    if len(novel) >= NOVELTY_MIN:
                        continue   # follow-up story — keep
                    matched_idx = j
                    reason = "tfidf" if tfidf_hit else "entity_overlap"
                    scores = {"tfidf": round(tfidf, 3), "entity": round(ent, 3)}
                    break

            if matched_idx is None:
                continue

            m = baseline[matched_idx]
            dups.append({
                "category": cat,
                "target_id": target.get("entry_id"),
                "target_ts": target.get("timestamp"),
                "target_finding": t_text,
                "target_url": t_url,
                "matched_id": m.get("entry_id"),
                "matched_ts": m.get("timestamp"),
                "matched_finding": m.get("finding", "") or "",
                "matched_url": m.get("source_url"),
                "reason": reason,
                "scores": scores,
            })
    return dups


# ──────────────────────────────────────────────────────────────────────────
# GCS IO
# ──────────────────────────────────────────────────────────────────────────
def _client() -> storage.Client:
    return storage.Client(project=PROJECT_ID)


def download_master() -> tuple[list[dict], int]:
    def _do():
        blob = _client().bucket(GCS_BUCKET).blob(GCS_OBJECT)
        blob.reload()
        return json.loads(blob.download_as_text()), blob.generation
    return with_retry("gcs_download", _do)


def backup_master() -> str:
    def _do():
        client = _client()
        src = client.bucket(GCS_BUCKET).blob(GCS_OBJECT)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        dst_name = f"{BACKUP_PREFIX}market_findings_log.backup-{stamp}.json"
        client.bucket(GCS_BUCKET).copy_blob(src, client.bucket(GCS_BUCKET), dst_name)
        return f"gs://{GCS_BUCKET}/{dst_name}"
    return with_retry("gcs_backup", _do)


def upload_master(entries: list[dict], if_generation_match: int) -> None:
    def _do():
        blob = _client().bucket(GCS_BUCKET).blob(GCS_OBJECT)
        blob.upload_from_string(
            json.dumps(entries, indent=4),
            content_type="application/json",
            if_generation_match=if_generation_match,
        )
    with_retry("gcs_upload", _do)


# ──────────────────────────────────────────────────────────────────────────
# Email — only sent when dups were found
# ──────────────────────────────────────────────────────────────────────────
def _format_report(dups: list[dict], backup_uri: str | None, removed: int) -> str:
    lines = [
        f"Arboryx master-log dedup sweep — {datetime.now(timezone.utc).isoformat()}",
        f"Bucket: gs://{GCS_BUCKET}/{GCS_OBJECT}",
        f"Backup: {backup_uri or '(none — dry run)'}",
        f"Duplicates removed: {removed}",
        "",
        "=" * 78,
    ]
    by_cat: dict[str, list[dict]] = defaultdict(list)
    for d in dups:
        by_cat[d["category"]].append(d)
    for cat, ds in sorted(by_cat.items()):
        lines.append(f"\n[{cat}] {len(ds)} duplicate(s)")
        lines.append("-" * 78)
        for d in ds:
            lines.append(f"  REMOVED   {d['target_id']}  ({d['target_ts']})")
            lines.append(f"    finding : {d['target_finding'][:240]}")
            lines.append(f"    url     : {d['target_url']}")
            lines.append(f"  KEPT      {d['matched_id']}  ({d['matched_ts']})")
            lines.append(f"    finding : {d['matched_finding'][:240]}")
            lines.append(f"    url     : {d['matched_url']}")
            lines.append(f"  match     reason={d['reason']}  scores={d['scores']}")
            lines.append("")
    return "\n".join(lines)


def send_email_report(subject: str, body: str) -> None:
    password = os.environ.get(GMAIL_APP_PASSWORD_ENV)
    if not password:
        raise RuntimeError(f"{GMAIL_APP_PASSWORD_ENV} env var not set — secret not mounted")

    def _do():
        msg = MIMEText(body, "plain", "utf-8")
        msg["Subject"] = subject
        msg["From"] = GMAIL_FROM
        msg["To"] = GMAIL_TO
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=30) as smtp:
            smtp.login(GMAIL_FROM, password)
            smtp.sendmail(GMAIL_FROM, [GMAIL_TO], msg.as_string())
    with_retry("smtp_send", _do)


# ──────────────────────────────────────────────────────────────────────────
# HTTP entry point
# ──────────────────────────────────────────────────────────────────────────
def dedup_handler(request):
    started = datetime.now(timezone.utc)
    # ?dry_run=true forces a no-write run (still emails if dups found)
    dry_run = DRY_RUN_DEFAULT
    try:
        if hasattr(request, "args") and request.args.get("dry_run"):
            dry_run = request.args.get("dry_run").lower() == "true"
    except Exception:
        pass

    log.info("dedup_start dry_run=%s thresholds={tfidf=%s, entity=%s, novelty=%s, floor=%s, mem=%s}",
             dry_run, TFIDF_THRESHOLD, ENTITY_THRESHOLD, NOVELTY_MIN,
             TFIDF_FLOOR_FOR_ENTITY, MEMORY_LIMIT)

    try:
        entries, generation = download_master()
        log.info("downloaded %d entries (generation=%s)", len(entries), generation)

        dups = find_duplicates(entries)
        log.info("found %d duplicate pair(s)", len(dups))

        backup_uri: str | None = None
        removed = 0
        if dups and not dry_run:
            backup_uri = backup_master()
            log.info("backup → %s", backup_uri)
            drop_ids = {d["target_id"] for d in dups}
            cleaned = [e for e in entries if e.get("entry_id") not in drop_ids]
            removed = len(entries) - len(cleaned)
            upload_master(cleaned, if_generation_match=generation)
            log.info("uploaded cleaned log: %d → %d entries (-%d)", len(entries), len(cleaned), removed)

        if dups:
            subject = f"[Arboryx Dedup] {len(dups)} duplicate(s) {'detected (dry-run)' if dry_run else 'removed'} — {datetime.now(timezone.utc).strftime('%Y-%m-%d')}"
            body = _format_report(dups, backup_uri, removed)
            try:
                send_email_report(subject, body)
                log.info("email sent → %s", GMAIL_TO)
            except Exception as exc:
                # Don't fail the whole function if email fails — the cleanup already
                # happened. Log loudly so it's visible in the Cloud Function logs.
                log.exception("email send failed (cleanup already applied): %s", exc)

        elapsed_ms = int((datetime.now(timezone.utc) - started).total_seconds() * 1000)
        result: dict[str, Any] = {
            "status": "ok",
            "dry_run": dry_run,
            "scanned": len(entries),
            "duplicates": len(dups),
            "removed": removed,
            "backup": backup_uri,
            "started_at": started.isoformat(),
            "elapsed_ms": elapsed_ms,
        }
        log.info("dedup_complete %s", json.dumps(result))
        return (json.dumps(result), 200, {"Content-Type": "application/json"})

    except Exception as exc:
        log.exception("dedup_failed")
        body = {"status": "error", "error": str(exc)}
        return (json.dumps(body), 500, {"Content-Type": "application/json"})
