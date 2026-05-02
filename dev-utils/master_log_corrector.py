"""Master log corrector for Arboryx.

Two modes:

1. Canonicalize (default) — normalize timestamps to YYYY-MM-DD, assign unique
   entry_ids using the same stateless derivation market_team.py uses on forward
   writes, re-sort newest-first, normalize sentiment prose. Idempotent.

2. Sentiment-fix (--fix-sentiment / --fix-sentiment-file) — re-evaluate the
   sentiment label for a targeted set of entry_ids by asking Gemini (via
   google-genai, Vertex backend) to classify the primary subject entity based
   on finding + existing takeaways + guidance. Only the targeted entries are
   patched and written to `dev-utils/updated_sentiment.json` (or --output).
   The master log is NEVER modified in this mode — merge the patched file back
   manually after review.

Source selection:
    --source PATH_OR_URI   — local path OR gs://bucket/object (single unified flag)
    --local PATH           — legacy alias for a local file source
    (default)              — gs://marketresearch-agents/market_findings_log.json

Usage (canonicalize):
    python dev-utils/master_log_corrector.py --source dev-utils/market_findings_log.json
    python dev-utils/master_log_corrector.py --source gs://marketresearch-agents/market_findings_log.json --dry-run
    python dev-utils/master_log_corrector.py --apply                           # uses default GCS source

Usage (sentiment fix):
    python dev-utils/master_log_corrector.py --fix-sentiment PWE-042026-004,PWE-042126-002
    python dev-utils/master_log_corrector.py --fix-sentiment-file ids.txt --output fixed.json
    python dev-utils/master_log_corrector.py --source log.json --fix-sentiment ROB-041926-001

Dependencies: google-cloud-storage, python-dateutil, google-genai, pyyaml
    pip install google-cloud-storage python-dateutil google-genai pyyaml

This script must remain coherent with the entry_id algorithm in market_team.py
(`_next_entry_id` / `_count_entries_for`). If that spec changes, update both.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from dateutil import parser as date_parser

DEFAULT_BUCKET = "marketresearch-agents"
DEFAULT_OBJECT = "market_findings_log.json"
BACKUP_PREFIX = "backups/"

CATEGORY_ABBR = {
    "Robotics": "ROB",
    "Crypto": "CRY",
    "AI Stack": "AIS",
    "Space & Defense": "SPD",
    "Power & Energy": "PWE",
    "Strategic Minerals": "STM",
}

CANONICAL_FIELDS = (
    "entry_id",
    "timestamp",
    "category",
    "finding",
    "sentiment_takeaways",
    "guidance_play",
    "price_levels",
    "source_url",
    "tooltip",
)

# Canonical sentiment vocabulary per values.yaml:86 (data_engineer_instructions).
# The LLM sometimes emits strength-qualifier synonyms (extremely/highly/strongly
# bullish etc.); these collapse to the Very Bullish / Very Bearish extreme.
# Ordered: stronger qualifier first so "Very Extremely Bullish" (if it ever appears)
# wouldn't double-match. Case-insensitive; replacement preserves canonical capitalization.
_STRENGTH_VARIANTS = "extremely|highly|strongly|heavily|drastically"
SENTIMENT_NORMALIZATIONS = [
    (re.compile(rf"\b(?:{_STRENGTH_VARIANTS})\s+bullish\b", re.IGNORECASE), "Very Bullish"),
    (re.compile(rf"\b(?:{_STRENGTH_VARIANTS})\s+bearish\b", re.IGNORECASE), "Very Bearish"),
]

_CANONICAL_LABEL_ALT = (
    rf"(?:{_STRENGTH_VARIANTS})\s+(?:bullish|bearish)"
    r"|very\s+bullish|very\s+bearish|bullish|bearish|neutral"
)

_PIPE_SECTION_RE = re.compile(
    r"\s*\|\s*(?=(?:Direct|Indirect|Market Dynamics|Sentiment)\s*:)",
    re.IGNORECASE,
)

_SENTIMENT_ANY_RE = re.compile(
    rf"\bSentiment\s*:\s*({_CANONICAL_LABEL_ALT})\s*\.?\s*",
    re.IGNORECASE,
)

_TRAILING_CANONICAL_RE = re.compile(
    rf"[\s.]+({_CANONICAL_LABEL_ALT})\s*[🟢🔴🟡]?\s*\.?\s*$",
    re.IGNORECASE,
)

_LEADING_CANONICAL_RE = re.compile(
    rf"^\s*(?:{_CANONICAL_LABEL_ALT})\b",
    re.IGNORECASE,
)

_FUZZY_PREFIX_PATTERNS: list[tuple[re.Pattern, str, bool]] = [
    (re.compile(r"^\s*highly\s+positive\s*\.\s*", re.IGNORECASE), "Very Bullish", True),
    (re.compile(r"^\s*positive\s+sentiment\s*\.\s*", re.IGNORECASE), "Bullish", True),
    (re.compile(r"^\s*positive\s*\.\s*", re.IGNORECASE), "Bullish", True),
    (re.compile(r"^\s*constructive\s*\.\s*", re.IGNORECASE), "Bullish", True),
    (re.compile(r"^\s*highly\s+negative\s*\.\s*", re.IGNORECASE), "Very Bearish", True),
    (re.compile(r"^\s*negative\s+sentiment\s*\.\s*", re.IGNORECASE), "Bearish", True),
    (re.compile(r"^\s*negative\s*\.\s*", re.IGNORECASE), "Bearish", True),
    (re.compile(r"^\s*highly\s+positive\s+(?:for|on|signal|tailwinds?)\b", re.IGNORECASE), "Very Bullish", False),
    (re.compile(r"^\s*positive\s+(?:sentiment\s+)?(?:for|on|signal|tailwinds?)\b", re.IGNORECASE), "Bullish", False),
    (re.compile(r"^\s*positive\s+signal\b", re.IGNORECASE), "Bullish", False),
    (re.compile(r"^\s*constructive\s+(?:for|on|in)\b", re.IGNORECASE), "Bullish", False),
    (re.compile(r"^\s*highly\s+negative\s+(?:for|on|signal|headwinds?)\b", re.IGNORECASE), "Very Bearish", False),
    (re.compile(r"^\s*negative\s+(?:sentiment\s+)?(?:for|on|signal|headwinds?)\b", re.IGNORECASE), "Bearish", False),
    (re.compile(r"^\s*mixed\s*(?:\([^)]*\))?\s*[.,]", re.IGNORECASE), "Neutral", False),
    (re.compile(r"^\s*volatile\s*(?:/\s*actionable)?\s*[.,]", re.IGNORECASE), "Neutral", False),
    (re.compile(r"^\s*highly\s+speculative\s*(?:/\s*volatile)?\s*[.,]", re.IGNORECASE), "Neutral", False),
]

_QUALIFIED_LABEL_RE = re.compile(
    rf"^\s*(?:[A-Za-z][\w-]*\s+){{1,2}}({_CANONICAL_LABEL_ALT})\s*\.\s*",
    re.IGNORECASE,
)


def _canonicalize_label(raw: str) -> str:
    s = raw.strip().lower()
    if re.match(rf"(?:{_STRENGTH_VARIANTS})\s+bullish$", s):
        return "Very Bullish"
    if re.match(rf"(?:{_STRENGTH_VARIANTS})\s+bearish$", s):
        return "Very Bearish"
    mapping = {
        "very bullish": "Very Bullish",
        "very bearish": "Very Bearish",
        "bullish": "Bullish",
        "bearish": "Bearish",
        "neutral": "Neutral",
    }
    return mapping.get(s, "Neutral")


def _starts_with_canonical(text: str) -> bool:
    return bool(_LEADING_CANONICAL_RE.match(text))


def _prepend_label(label: str, body: str) -> str:
    body = body.strip()
    if not body:
        return f"{label}."
    return f"{label}. {body}"


def _normalize_pipe_separators(text: str) -> tuple[str, bool]:
    after_pipe = _PIPE_SECTION_RE.sub(". ", text)
    piped = after_pipe != text
    new = re.sub(r"\.{2,}(\s+|$)", r".\1", after_pipe)
    return new, piped


def _promote_explicit_sentiment(text: str) -> tuple[str, str | None]:
    m = _SENTIMENT_ANY_RE.search(text)
    if not m:
        return text, None
    label = _canonicalize_label(m.group(1))
    stripped = _SENTIMENT_ANY_RE.sub("", text).strip()
    stripped = re.sub(r"\s+([.!?])", r"\1", stripped)
    if _starts_with_canonical(stripped):
        return stripped, label
    return _prepend_label(label, stripped), label


def _promote_trailing_canonical(text: str) -> tuple[str, str | None]:
    if _starts_with_canonical(text):
        return text, None
    m = _TRAILING_CANONICAL_RE.search(text)
    if not m:
        return text, None
    label = _canonicalize_label(m.group(1))
    body = text[: m.start()].rstrip()
    if not body:
        return f"{label}.", label
    if not body.endswith((".", "!", "?")):
        body += "."
    return _prepend_label(label, body), label


def _promote_fuzzy_prefix(text: str) -> tuple[str, str | None]:
    if _starts_with_canonical(text):
        return text, None
    for pattern, label, strip in _FUZZY_PREFIX_PATTERNS:
        m = pattern.match(text)
        if not m:
            continue
        if strip:
            remainder = text[m.end():].lstrip()
            return _prepend_label(label, remainder), label
        return _prepend_label(label, text.lstrip()), label
    m_q = _QUALIFIED_LABEL_RE.match(text)
    if m_q:
        label = _canonicalize_label(m_q.group(1))
        remainder = text[m_q.end():].lstrip()
        return _prepend_label(label, remainder), label
    return text, None


def normalize_sentiment(text: str) -> tuple[str, list[tuple[str, str]]]:
    if not text:
        return text, []
    applied: list[tuple[str, str]] = []
    out = text
    for pattern, replacement in SENTIMENT_NORMALIZATIONS:
        def _sub(m: re.Match) -> str:
            applied.append((m.group(0), replacement))
            return replacement
        out = pattern.sub(_sub, out)
    return out, applied


_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_ISO_DATETIME_RE = re.compile(r"^\d{4}-\d{2}-\d{2}[ T]")
_DIGIT_HYPHEN_RE = re.compile(r"(\d+)\s*-\s*(\d+)")
_RANGE_SEPS = ("–", "—", " to ", " - ")


def _split_range(s: str) -> tuple[str, str] | None:
    for sep in _RANGE_SEPS:
        if sep in s:
            left, right = s.split(sep, 1)
            return left.strip(), right.strip()
    m = _DIGIT_HYPHEN_RE.search(s)
    if m:
        hyphen_idx = s.find("-", m.start())
        return s[:hyphen_idx].strip(), s[hyphen_idx + 1:].strip()
    return None


def normalize_timestamp(raw: str | None) -> tuple[str, bool]:
    if raw is None or not str(raw).strip():
        raise ValueError("timestamp required")
    s = str(raw).strip()
    if _ISO_DATE_RE.match(s):
        return s, False
    if _ISO_DATETIME_RE.match(s):
        return s[:10], False
    split = _split_range(s)
    if split is not None:
        left, right = split
        try:
            right_parsed = date_parser.parse(right)
        except (date_parser.ParserError, ValueError, OverflowError):
            right_parsed = None
        try:
            if right_parsed is not None:
                left_parsed = date_parser.parse(left, default=right_parsed)
            else:
                left_parsed = date_parser.parse(left)
            return left_parsed.strftime("%Y-%m-%d"), True
        except (date_parser.ParserError, ValueError, OverflowError) as ex:
            raise ValueError(f"unparseable range timestamp: {raw!r}") from ex
    try:
        return date_parser.parse(s).strftime("%Y-%m-%d"), False
    except (date_parser.ParserError, ValueError, OverflowError) as ex:
        raise ValueError(f"unparseable timestamp: {raw!r}") from ex


def entry_id_for(category: str, date_iso: str, nth: int) -> str:
    if category not in CATEGORY_ABBR:
        raise ValueError(
            f"unknown category {category!r}; add to CATEGORY_ABBR (and to market_team.py)"
        )
    mm, dd, yy = date_iso[5:7], date_iso[8:10], date_iso[2:4]
    return f"{CATEGORY_ABBR[category]}-{mm}{dd}{yy}-{nth:03d}"


def _restructure_sentiment_text(text: str) -> tuple[str, dict[str, str | bool]]:
    actions: dict[str, str | bool] = {
        "pipe": False, "explicit": None, "trailing": None, "fuzzy": None,
    }
    if not text:
        return text, actions
    out, piped = _normalize_pipe_separators(text)
    actions["pipe"] = piped
    out, lbl = _promote_explicit_sentiment(out)
    if lbl is not None:
        actions["explicit"] = lbl
    else:
        out, lbl = _promote_trailing_canonical(out)
        if lbl is not None:
            actions["trailing"] = lbl
        else:
            out, lbl = _promote_fuzzy_prefix(out)
            if lbl is not None:
                actions["fuzzy"] = lbl
    return out, actions


def canonicalize(entry: dict, entry_id: str, date_iso: str) -> tuple[dict, list[tuple[str, str]], dict]:
    restructured, actions = _restructure_sentiment_text(entry.get("sentiment_takeaways", ""))
    cleaned_sentiment, applied = normalize_sentiment(restructured)
    out = {
        "entry_id": entry_id,
        "timestamp": date_iso,
        "category": entry["category"],
        "finding": entry.get("finding", ""),
        "sentiment_takeaways": cleaned_sentiment,
        "guidance_play": entry.get("guidance_play", ""),
        "price_levels": entry.get("price_levels", ""),
        "source_url": entry.get("source_url"),
        "tooltip": entry.get("tooltip", ""),
    }
    extras = set(entry.keys()) - set(CANONICAL_FIELDS)
    if extras:
        print(f"  warn: dropping unexpected fields {sorted(extras)} on {entry_id}", file=sys.stderr)
    return out, applied, actions


def correct_log(entries: list[dict]) -> tuple[list[dict], dict]:
    stats = {
        "input_count": len(entries),
        "timestamp_reformat": 0,
        "ranges_collapsed": 0,
        "id_assigned": 0,
        "id_changed": 0,
        "sentiment_normalizations": 0,
        "sentiment_replacements": Counter(),
        "unknown_categories": Counter(),
        "range_samples": [],
        "pipe_normalizations": 0,
        "sentiment_promoted_explicit": 0,
        "sentiment_promoted_trailing": 0,
        "sentiment_inferred_fuzzy": 0,
        "sentiment_residual_ids": [],
        "sentiment_promotion_labels": Counter(),
    }

    normalized: list[tuple[int, dict, str]] = []
    for idx, entry in enumerate(entries):
        raw_ts = entry.get("timestamp")
        date_iso, was_range = normalize_timestamp(raw_ts)
        if date_iso != raw_ts:
            stats["timestamp_reformat"] += 1
        if was_range:
            stats["ranges_collapsed"] += 1
            if len(stats["range_samples"]) < 10:
                stats["range_samples"].append((raw_ts, date_iso))
        cat = entry.get("category")
        if cat not in CATEGORY_ABBR:
            stats["unknown_categories"][cat] += 1
        normalized.append((idx, entry, date_iso))

    if stats["unknown_categories"]:
        unknown = dict(stats["unknown_categories"])
        raise ValueError(f"unknown categories in log: {unknown}")

    normalized.sort(key=lambda t: (t[2], -t[0]), reverse=True)

    counters: Counter = Counter()
    corrected: list[dict] = []
    for _, entry, date_iso in normalized:
        cat = entry["category"]
        counters[(cat, date_iso)] += 1
        new_id = entry_id_for(cat, date_iso, counters[(cat, date_iso)])
        old_id = entry.get("entry_id")
        if old_id is None:
            stats["id_assigned"] += 1
        elif old_id != new_id:
            stats["id_changed"] += 1
        out_entry, applied, actions = canonicalize(entry, new_id, date_iso)
        if applied:
            stats["sentiment_normalizations"] += 1
            for matched, repl in applied:
                stats["sentiment_replacements"][(matched.lower(), repl)] += 1
        if actions.get("pipe"):
            stats["pipe_normalizations"] += 1
        if actions.get("explicit"):
            stats["sentiment_promoted_explicit"] += 1
            stats["sentiment_promotion_labels"][("explicit", actions["explicit"])] += 1
        if actions.get("trailing"):
            stats["sentiment_promoted_trailing"] += 1
            stats["sentiment_promotion_labels"][("trailing", actions["trailing"])] += 1
        if actions.get("fuzzy"):
            stats["sentiment_inferred_fuzzy"] += 1
            stats["sentiment_promotion_labels"][("fuzzy", actions["fuzzy"])] += 1
        st = (out_entry.get("sentiment_takeaways") or "").lstrip()
        if st and not _starts_with_canonical(st):
            stats["sentiment_residual_ids"].append(new_id)
        corrected.append(out_entry)

    return corrected, stats


def verify_idempotent(corrected: list[dict]) -> None:
    again, _ = correct_log([dict(e) for e in corrected])
    if again != corrected:
        raise AssertionError("corrector is not idempotent — output differs on second pass")


def summarize(stats: dict, corrected: list[dict]) -> None:
    cat_counts = Counter(e["category"] for e in corrected)
    date_range = (
        min(e["timestamp"] for e in corrected),
        max(e["timestamp"] for e in corrected),
    ) if corrected else (None, None)

    print("=" * 60)
    print("Master log correction summary")
    print("=" * 60)
    print(f"  Entries in:             {stats['input_count']}")
    print(f"  Entries out:            {len(corrected)}")
    print(f"  Timestamps reformatted: {stats['timestamp_reformat']}")
    print(f"  Date ranges collapsed:  {stats['ranges_collapsed']}")
    if stats["range_samples"]:
        print(f"  Range → start-date samples:")
        for raw, iso in stats["range_samples"]:
            print(f"    {raw!r:<40} -> {iso}")
    print(f"  IDs newly assigned:     {stats['id_assigned']}")
    print(f"  IDs changed:            {stats['id_changed']}")
    print(f"  Sentiment normalizations: {stats['sentiment_normalizations']} entries, "
          f"{sum(stats['sentiment_replacements'].values())} replacements")
    if stats["sentiment_replacements"]:
        print(f"  Sentiment replacement breakdown:")
        for (matched, repl), n in stats["sentiment_replacements"].most_common():
            print(f"    {n:>4}x  {matched!r} -> {repl!r}")
    print(f"  Pipe-separator fixes:   {stats['pipe_normalizations']}")
    print(f"  Sentiment promoted (explicit `Sentiment: X`): {stats['sentiment_promoted_explicit']}")
    print(f"  Sentiment promoted (trailing bare label):    {stats['sentiment_promoted_trailing']}")
    print(f"  Sentiment inferred from fuzzy prose prefix:  {stats['sentiment_inferred_fuzzy']}")
    if stats["sentiment_promotion_labels"]:
        print(f"  Promotion breakdown (source, label → count):")
        for (src, lbl), n in stats["sentiment_promotion_labels"].most_common():
            print(f"    {n:>4}x  {src:<9} -> {lbl}")
    residual = stats["sentiment_residual_ids"]
    print(f"  Entries without `Sentiment:` marker (manual review): {len(residual)}")
    if residual:
        for rid in residual:
            print(f"    - {rid}")
    print(f"  Date range:           {date_range[0]} → {date_range[1]}")
    print(f"  Per-category counts:")
    for cat, n in sorted(cat_counts.items(), key=lambda x: -x[1]):
        print(f"    {CATEGORY_ABBR.get(cat, '???')}  {cat:<20} {n}")
    if corrected:
        print("\n  First entry (newest):")
        print(f"    {corrected[0]['entry_id']}  {corrected[0]['timestamp']}  {corrected[0]['category']}")
        print(f"    {corrected[0]['finding'][:100]}...")
        print("  Last entry (oldest):")
        print(f"    {corrected[-1]['entry_id']}  {corrected[-1]['timestamp']}  {corrected[-1]['category']}")


def load_local(path: Path) -> list[dict]:
    return json.loads(path.read_text())


def save_local(path: Path, entries: list[dict]) -> None:
    path.write_text(json.dumps(entries, indent=2, ensure_ascii=False))


def gcs_download(bucket: str, obj: str) -> list[dict]:
    from google.cloud import storage
    client = storage.Client()
    blob = client.bucket(bucket).blob(obj)
    return json.loads(blob.download_as_text())


def gcs_backup(bucket: str, obj: str) -> str:
    from google.cloud import storage
    client = storage.Client()
    src = client.bucket(bucket).blob(obj)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup_name = f"{BACKUP_PREFIX}{Path(obj).stem}.backup-{stamp}.json"
    client.bucket(bucket).copy_blob(src, client.bucket(bucket), backup_name)
    return f"gs://{bucket}/{backup_name}"


def gcs_upload(bucket: str, obj: str, entries: list[dict]) -> None:
    from google.cloud import storage
    client = storage.Client()
    blob = client.bucket(bucket).blob(obj)
    blob.upload_from_string(
        json.dumps(entries, indent=2, ensure_ascii=False),
        content_type="application/json",
    )


# ==========================================================================
# Sentiment-fix mode: re-classify sentiment for specific entry_ids via Gemini.
# ==========================================================================

_SENTIMENT_FIX_PROMPT = """\
You are a precise financial sentiment classifier. Classify the sentiment of the \
following market finding TOWARD ITS PRIMARY SUBJECT ENTITY OR ENTITIES — the \
specific company, ticker, asset, or project the finding is about. Do NOT classify \
overall market mood; classify the directional impact on those entities' price / \
outlook over the near-term (days to weeks).

CATEGORY: {category}

FINDING TITLE / BODY:
{finding}

EXISTING TAKEAWAYS (context only — do not repeat in your output):
{sentiment_takeaways}

GUIDANCE / PLAY:
{guidance_play}

Valid sentiment labels (use EXACTLY these spellings): Very Bullish, Bullish, Neutral, Bearish, Very Bearish.

Produce a SINGLE SHORT SENTENCE that will be PREPENDED to the existing takeaways. \
Form:
  - One dominant entity   → `<Label>.`                       (e.g. `Very Bullish.`)
  - Multiple entities     → `<Entity> <Label>, <Entity> <Label>.`  \
(e.g. `MSFT Neutral, GOOGL Bullish.`)
The sentence must end with a single period. Use ticker symbols when obvious \
(MSFT, NVDA, BTC), otherwise the company/asset name. Do not include any other \
words or punctuation in the PREPEND sentence.

HARD RULES (do not violate):
  - NEVER ask clarifying questions. NEVER output anything other than the two \
required lines below.
  - If the finding names multiple entities but only some have price/guidance \
coverage, classify ONLY the entities that receive direct impact from the news. \
If every entity is affected similarly, classify them all.
  - If still ambiguous, emit a single `<Label>.` covering the dominant theme.
  - Existing takeaways may already include a sentiment word — IGNORE it and \
classify from the finding + guidance yourself.

Respond in this EXACT format, two lines only, no extra prose:

PREPEND: <the single sentence to prepend, ending with a period>
REASONING: <one sentence explaining the classification>
"""

_SENTIMENT_FIX_RETRY_PROMPT = """\
Your previous response did not follow the required format. Respond NOW with \
exactly two lines and nothing else:

PREPEND: <single sentence, ending with a period, containing one of: \
Very Bullish, Bullish, Neutral, Bearish, Very Bearish>
REASONING: <one sentence>

Do not ask questions. Do not add any other text. Classify the finding below:

CATEGORY: {category}
FINDING: {finding}
GUIDANCE: {guidance_play}
"""

_CLIENT = None


def _load_project_config() -> dict:
    """Load values.yaml from the arboryx.ai project root (next to this file's parent)."""
    import yaml
    root = Path(__file__).resolve().parent.parent
    with open(root / "values.yaml") as f:
        return yaml.safe_load(f)


def _get_genai_client():
    global _CLIENT
    if _CLIENT is not None:
        return _CLIENT
    from google import genai
    cfg = _load_project_config()
    project = cfg["gcp"]["project_id"]
    model_location = cfg["gcp"].get("model_location", cfg["gcp"]["location"])
    os.environ.setdefault("GOOGLE_CLOUD_PROJECT", project)
    os.environ.setdefault("GOOGLE_CLOUD_LOCATION", model_location)
    _CLIENT = genai.Client(vertexai=True, project=project, location=model_location)
    return _CLIENT


def _get_worker_model() -> str:
    cfg = _load_project_config()
    return cfg["agents"]["worker_model"]


_LLM_PREPEND_RE = re.compile(r"^\s*PREPEND\s*:\s*(.+?)\s*$", re.IGNORECASE | re.MULTILINE)
_LLM_REASONING_RE = re.compile(r"^\s*REASONING\s*:\s*(.+?)\s*$", re.IGNORECASE | re.MULTILINE | re.DOTALL)
_CANONICAL_LABEL_ANY_RE = re.compile(rf"\b({_CANONICAL_LABEL_ALT})\b", re.IGNORECASE)

# Hard caps / forbidden substrings for the PREPEND line. The contract is a
# single short sentence of labels — reject anything that looks like a rewrite
# of the takeaways body. The entry's existing `sentiment_takeaways` is the
# only source of takeaway prose; the LLM's response body is never used.
_PREPEND_MAX_LEN = 200
_PREPEND_FORBIDDEN_MARKERS = (
    "direct:", "indirect:", "market dynamics:",
    "sentiment:", "primary_entity", "reasoning:",
)


def _canonicalize_labels_in_sentence(text: str) -> str:
    """Normalize the case/spelling of every canonical label occurrence in `text`
    (e.g. 'bullish' → 'Bullish', 'extremely bullish' → 'Very Bullish'). Leaves
    surrounding prose (tickers, entity names, commas) untouched."""
    return _CANONICAL_LABEL_ANY_RE.sub(lambda m: _canonicalize_label(m.group(1)), text)


def _parse_llm_response(text: str) -> tuple[str | None, str | None]:
    """Extract ONLY the PREPEND line and the REASONING line from the LLM
    response. The PREPEND line is collapsed to its first physical line, stripped
    of wrapping quotes/backticks, canonicalized for label casing, and guaranteed
    to end with a single period. Returns (prepend_sentence, reasoning)."""
    if not text:
        return None, None
    prepend = None
    reasoning = None
    m = _LLM_PREPEND_RE.search(text)
    if m:
        raw = m.group(1).strip().splitlines()[0].strip()
        raw = raw.strip("`\"'")
        raw = _canonicalize_labels_in_sentence(raw)
        raw = raw.rstrip(" .") + "."
        prepend = raw
    m = _LLM_REASONING_RE.search(text)
    if m:
        reasoning = m.group(1).strip().splitlines()[0].strip()
    return prepend, reasoning


def _validate_prepend(prepend: str) -> None:
    """Raise ValueError if the PREPEND line doesn't meet the contract:
      - non-empty
      - ≤ _PREPEND_MAX_LEN chars
      - contains at least one canonical label
      - contains no structural markers from the takeaways body
      - is a single sentence (at most one period, which is the trailing one)
    """
    if not prepend:
        raise ValueError("PREPEND line is empty")
    if len(prepend) > _PREPEND_MAX_LEN:
        raise ValueError(f"PREPEND too long ({len(prepend)} > {_PREPEND_MAX_LEN}): {prepend!r}")
    if not _CANONICAL_LABEL_ANY_RE.search(prepend):
        raise ValueError(f"PREPEND has no canonical sentiment label: {prepend!r}")
    low = prepend.lower()
    for marker in _PREPEND_FORBIDDEN_MARKERS:
        if marker in low:
            raise ValueError(f"PREPEND contains disallowed marker {marker!r}: {prepend!r}")
    # Single sentence: the only period must be the trailing one. Allow common
    # abbreviations inside a ticker like "T.ROWE" by only checking for period
    # followed by whitespace (sentence break).
    if re.search(r"\.\s+\S", prepend[:-1] if prepend.endswith(".") else prepend):
        raise ValueError(f"PREPEND looks like multiple sentences: {prepend!r}")


def _call_llm(client, model: str, prompt: str) -> str:
    from google.genai import types as gt
    resp = client.models.generate_content(
        model=model,
        contents=prompt,
        config=gt.GenerateContentConfig(temperature=0.0, max_output_tokens=512),
    )
    return (resp.text or "").strip()


def classify_sentiment_llm(entry: dict, model: str, client) -> tuple[str, str, str]:
    """Call the LLM, extract and validate ONLY the PREPEND line, and return it.
    The existing `sentiment_takeaways` on `entry` is never modified here and
    nothing else from the LLM response is used downstream.

    Retries once with a terse strict-format reminder if the first response
    fails to parse (e.g. the model asked a clarifying question, wrapped the
    output in prose, or produced multi-sentence output).

    Returns (prepend_sentence, reasoning, raw_text). Raises ValueError on any
    contract violation after the retry (missing/empty PREPEND, no canonical
    label, too long, forbidden markers, multi-sentence)."""
    prompt = _SENTIMENT_FIX_PROMPT.format(
        category=entry.get("category", "(unknown)"),
        finding=entry.get("finding", "") or "(empty)",
        sentiment_takeaways=entry.get("sentiment_takeaways", "") or "(empty)",
        guidance_play=entry.get("guidance_play", "") or "(empty)",
    )
    raw = _call_llm(client, model, prompt)
    prepend, reasoning = _parse_llm_response(raw)
    first_error: str | None = None
    try:
        _validate_prepend(prepend or "")
    except ValueError as ex:
        first_error = str(ex)

    if first_error is not None:
        retry_prompt = _SENTIMENT_FIX_RETRY_PROMPT.format(
            category=entry.get("category", "(unknown)"),
            finding=entry.get("finding", "") or "(empty)",
            guidance_play=entry.get("guidance_play", "") or "(empty)",
        )
        print(f"    ⟳ retrying (first attempt failed: {first_error.splitlines()[0]})", flush=True)
        raw = _call_llm(client, model, retry_prompt)
        prepend, reasoning = _parse_llm_response(raw)
        try:
            _validate_prepend(prepend or "")
        except ValueError as ex:
            raise ValueError(
                f"LLM failed format on both attempts. Last error: {ex}\n"
                f"First error: {first_error}\nraw (retry):\n{raw}"
            ) from None

    return prepend, reasoning or "(no reasoning)", raw


def _prepend_sentiment_sentence(sentence: str, original: str) -> str:
    """Concatenate `sentence` in front of `original`. The `original` is the
    entry's existing `sentiment_takeaways` value — it is NEVER modified in any
    way (no stripping of leading labels, no canonicalization, no reformatting).
    The LLM's response body is not involved here; the only LLM output that
    reaches this function is the validated PREPEND line.

    Output: '<sentence> <original>' (single space separator) or just
    '<sentence>' if original is empty/whitespace."""
    s = sentence.strip()
    if not s.endswith("."):
        s = s + "."
    if not original or not original.strip():
        return s
    return f"{s} {original.lstrip()}"


def _extract_first_canonical(text: str) -> str | None:
    """For audit only — return the first canonical label found in `text`, or None."""
    m = _CANONICAL_LABEL_ANY_RE.search(text or "")
    return _canonicalize_label(m.group(1)) if m else None


def _parse_ids(arg_values: list[str] | None, id_file: Path | None) -> list[str]:
    """Split each --fix-sentiment value on ANY mix of commas and whitespace
    (spaces, tabs, newlines) so a multi-line pasted list works. Strip YAML /
    Markdown list bullets (`-`, `*`, `•`) off leading tokens, drop empties,
    dedup preserving first-seen order."""
    raw_tokens: list[str] = []
    if arg_values:
        for v in arg_values:
            raw_tokens.extend(re.split(r"[,\s]+", v))
    if id_file is not None:
        for line in id_file.read_text().splitlines():
            s = line.split("#", 1)[0]
            raw_tokens.extend(re.split(r"[,\s]+", s))

    ids: list[str] = []
    for tok in raw_tokens:
        tok = tok.lstrip("-*•").strip()
        if tok:
            ids.append(tok)

    seen = set()
    dedup: list[str] = []
    for i in ids:
        if i not in seen:
            seen.add(i)
            dedup.append(i)
    return dedup


def fix_sentiments(entries: list[dict], target_ids: list[str],
                   delay_seconds: float = 1.5) -> tuple[list[dict], list[dict]]:
    """Look up each target_id in entries, call the LLM, splice the new sentiment
    label into sentiment_takeaways. Sleeps `delay_seconds` BETWEEN calls (not
    before the first, not after the last) to stay under Vertex per-minute quota.
    Returns (patched_entries, audit_rows). audit_rows includes old/new labels,
    primary entity, reasoning, and any errors (entry unchanged on error)."""
    by_id = {e.get("entry_id"): e for e in entries}
    missing = [i for i in target_ids if i not in by_id]
    if missing:
        print(f"⚠️  {len(missing)} entry_id(s) not found in log: {missing}", file=sys.stderr)

    client = _get_genai_client()
    model = _get_worker_model()
    print(f"Using model: {model}  |  inter-call delay: {delay_seconds}s")

    patched: list[dict] = []
    audit: list[dict] = []
    for i, eid in enumerate((i for i in target_ids if i in by_id), 1):
        if i > 1 and delay_seconds > 0:
            time.sleep(delay_seconds)
        entry = dict(by_id[eid])
        before_text = entry.get("sentiment_takeaways", "") or ""
        m_old = _LEADING_CANONICAL_RE.match(_restructure_sentiment_text(before_text)[0])
        old_label = _canonicalize_label(m_old.group(0)) if m_old else None

        print(f"[{i}/{len(target_ids)}] {eid} — calling LLM…", flush=True)
        try:
            prepend, reasoning, _raw = classify_sentiment_llm(entry, model, client)
        except Exception as ex:
            print(f"  ❌ LLM error for {eid}: {ex}", file=sys.stderr)
            audit.append({
                "entry_id": eid, "old_label": old_label, "new_label": None,
                "prepend": None, "reasoning": None, "error": str(ex),
            })
            continue

        new_label = _extract_first_canonical(prepend)
        new_text = _prepend_sentiment_sentence(prepend, before_text)
        entry["sentiment_takeaways"] = new_text
        patched.append(entry)
        audit.append({
            "entry_id": eid, "old_label": old_label, "new_label": new_label,
            "prepend": prepend, "reasoning": reasoning, "error": None,
        })
        print(f"  → prepend: {prepend}   ({old_label} → {new_label})")
        print(f"    reason: {reasoning}")
    return patched, audit


def _strip_substrings(value, subs: tuple[str, ...]):
    """Recursively apply `.replace(s, "")` for every `s` in `subs` to every
    string found within `value`. dicts, lists, and non-string primitives are
    walked through unchanged in shape. Exact, case-sensitive match."""
    if isinstance(value, str):
        for s in subs:
            if s:
                value = value.replace(s, "")
        return value
    if isinstance(value, dict):
        return {k: _strip_substrings(v, subs) for k, v in value.items()}
    if isinstance(value, list):
        return [_strip_substrings(v, subs) for v in value]
    return value


def strip_unwanted_everywhere(entries: list[dict], substrings: list[str]) -> tuple[list[dict], dict, int]:
    """Remove each case-sensitive substring from every string value (recursively)
    in every entry. Returns (new_entries, per_sub_count, entries_changed).
    per_sub_count maps each substring to the total occurrences removed across
    all entries."""
    subs = tuple(s for s in (substrings or []) if s)
    if not subs:
        return list(entries), {}, 0
    per_sub_counts: dict[str, int] = {s: 0 for s in subs}
    changed = 0
    out: list[dict] = []
    for e in entries:
        before_blob = json.dumps(e, ensure_ascii=False)
        stripped = _strip_substrings(e, subs)
        after_blob = json.dumps(stripped, ensure_ascii=False)
        if before_blob != after_blob:
            changed += 1
        for s in subs:
            per_sub_counts[s] += before_blob.count(s) - after_blob.count(s)
        out.append(stripped)
    return out, per_sub_counts, changed


def _print_strip_report(per_sub: dict, entries_changed: int, total_entries: int) -> None:
    print(f"  Removed unwanted substrings from {entries_changed}/{total_entries} entries:")
    for s, n in per_sub.items():
        print(f"    {s!r:<20}  {n} occurrences")


def summarize_sentiment_fix(audit: list[dict], patched: list[dict], out_path: Path) -> None:
    ok = [r for r in audit if r["error"] is None]
    changed = [r for r in ok if r["old_label"] != r["new_label"]]
    unchanged = [r for r in ok if r["old_label"] == r["new_label"]]
    errored = [r for r in audit if r["error"] is not None]

    print("=" * 60)
    print("Sentiment-fix summary")
    print("=" * 60)
    print(f"  Targeted:  {len(audit)}")
    print(f"  Succeeded: {len(ok)}  (changed={len(changed)}, unchanged={len(unchanged)})")
    print(f"  Errored:   {len(errored)}")
    if changed:
        print("  Label changes:")
        for r in changed:
            print(f"    {r['entry_id']}  {r['old_label']} → {r['new_label']}  prepend: {r['prepend']!r}")
    if errored:
        print("  Errors:")
        for r in errored:
            print(f"    {r['entry_id']}  {r['error']}")
    print(f"  Output:    {out_path}  ({len(patched)} entries)")


# ==========================================================================
# Push mode: upload a local file to GCS verbatim via `gcloud storage cp`.
# No canonicalize, no strip, no LLM. The destination object is ALWAYS
# `market_findings_log.json` at the configured bucket, and always replaces
# an existing object (gcloud storage cp default).
# ==========================================================================

PUSH_DEST_OBJECT = "market_findings_log.json"


def _resolve_push_source(source_arg: str | None, local_arg: Path | None) -> Path:
    """Resolve which LOCAL file to push. Priority:
      1. --source (must be a local path, not a gs:// URI)
      2. --local (legacy)
      3. values.yaml `storage.local_path` (relative to project root)
      4. dev-utils/market_findings_log.json
    Raises ValueError for a gs:// --source, FileNotFoundError if nothing resolves."""
    if source_arg:
        if source_arg.startswith("gs://"):
            raise ValueError(f"--push requires a local source; got remote URI {source_arg!r}.")
        p = Path(source_arg)
        if not p.exists():
            raise FileNotFoundError(f"Push source not found: {p}")
        return p.resolve()
    if local_arg is not None:
        p = Path(local_arg)
        if not p.exists():
            raise FileNotFoundError(f"Push source (--local) not found: {p}")
        return p.resolve()

    root = Path(__file__).resolve().parent.parent
    candidates: list[Path] = []
    try:
        cfg = _load_project_config()
        cfg_path = root / cfg["storage"]["local_path"]
        candidates.append(cfg_path)
    except Exception:
        pass
    candidates.append(root / "dev-utils" / PUSH_DEST_OBJECT)

    for c in candidates:
        if c.exists():
            return c.resolve()
    tried = "\n  ".join(str(c) for c in candidates)
    raise FileNotFoundError(
        f"No default {PUSH_DEST_OBJECT} found. Tried:\n  {tried}\n"
        f"Pass --source <path> explicitly."
    )


def push_to_gcs(local_path: Path, bucket: str, dry_run: bool = False) -> int:
    """Upload `local_path` to gs://{bucket}/{PUSH_DEST_OBJECT} using `gcloud storage cp`.
    Always replaces the destination object. Returns the gcloud exit code (0 on success)."""
    import shutil
    import subprocess
    dest = f"gs://{bucket}/{PUSH_DEST_OBJECT}"
    cmd = ["gcloud", "storage", "cp", str(local_path), dest]
    print("=" * 60)
    print("Push mode")
    print("=" * 60)
    print(f"  Source: {local_path}")
    print(f"  Dest:   {dest}")
    print(f"  $ {' '.join(cmd)}")
    if dry_run:
        print("  (dry-run: command not executed)")
        return 0
    if shutil.which("gcloud") is None:
        print("  ❌ `gcloud` not found on PATH. Install Google Cloud SDK or add it to PATH.", file=sys.stderr)
        return 127
    try:
        result = subprocess.run(cmd, check=False)
    except OSError as ex:
        print(f"  ❌ Failed to invoke gcloud: {ex}", file=sys.stderr)
        return 1
    if result.returncode == 0:
        print(f"  ✅ Uploaded {local_path.stat().st_size} bytes to {dest}")
    else:
        print(f"  ❌ gcloud exited with code {result.returncode}", file=sys.stderr)
    return result.returncode


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="Canonicalize mode: download + process, do not upload.")
    mode.add_argument("--apply", action="store_true", help="Canonicalize mode: backup + upload corrected log.")
    ap.add_argument("--source", type=str, default=None, metavar="PATH_OR_GS_URI",
                    help="Entries source. Either a local JSON path or a gs://bucket/object URI. "
                         "Takes precedence over --local and --bucket/--object. If omitted, "
                         f"defaults to gs://{DEFAULT_BUCKET}/{DEFAULT_OBJECT}.")
    ap.add_argument("--local", type=Path, help="[legacy] Process a local JSON file instead of GCS. Equivalent to --source PATH.")
    ap.add_argument("--output", type=Path, help="Write result to this path. Canonicalize: full corrected log. Fix-sentiment: only patched entries (default: dev-utils/updated_sentiment.json).")
    ap.add_argument("--bucket", default=DEFAULT_BUCKET, help=f"GCS bucket (default: {DEFAULT_BUCKET}). Ignored if --source is a gs:// URI.")
    ap.add_argument("--object", default=DEFAULT_OBJECT, help=f"GCS object path (default: {DEFAULT_OBJECT}). Ignored if --source is a gs:// URI.")
    ap.add_argument("--fix-sentiment", action="append", default=None,
                    help="Comma-separated entry_ids to re-classify via LLM. Repeatable. Triggers sentiment-fix mode.")
    ap.add_argument("--fix-sentiment-file", type=Path, default=None,
                    help="Path to a text file of entry_ids (one per line, '#' comments ok). Triggers sentiment-fix mode.")
    ap.add_argument("--llm-delay", type=float, default=1.5,
                    help="Seconds to wait between consecutive LLM calls in fix-sentiment mode (default 1.5). Set 0 to disable.")
    ap.add_argument("--remove-unwanted", action="append", default=None, metavar="STRING",
                    help="Exact case-sensitive string to remove from every text field in every entry. "
                         "Repeatable. Standalone (no other mode): writes all cleaned entries to "
                         "dev-utils/corrected_entries.json (or --output). Combined with --fix-sentiment "
                         "or canonicalize: post-processes that mode's output before saving.")
    ap.add_argument("--push", action="store_true",
                    help=f"Upload a local file to gs://<bucket>/{PUSH_DEST_OBJECT} via `gcloud storage cp`. "
                         "Always replaces the destination. No canonicalize, no strip, no LLM. "
                         "Source: --source (must be local) > --local > values.yaml storage.local_path > "
                         f"dev-utils/{PUSH_DEST_OBJECT}. Combine with --dry-run to print the command "
                         "without executing. --fix-sentiment / --remove-unwanted are ignored in push mode.")
    args = ap.parse_args()

    # Push mode short-circuits before any entries are loaded.
    if args.push:
        if args.fix_sentiment or args.fix_sentiment_file or args.remove_unwanted or args.apply:
            print("⚠️  --push ignores --fix-sentiment / --remove-unwanted / --apply. Upload only.", file=sys.stderr)
        try:
            src = _resolve_push_source(args.source, args.local)
        except (FileNotFoundError, ValueError) as ex:
            print(f"❌ {ex}", file=sys.stderr)
            return 2
        return push_to_gcs(src, args.bucket, dry_run=args.dry_run)

    # Source resolution (priority: --source > --local > GCS defaults).
    # `is_local` drives the --apply gate downstream; GCS upload is only valid
    # when the source itself was GCS.
    is_local = False
    if args.source:
        if args.source.startswith("gs://"):
            parts = args.source[5:].split("/", 1)
            if len(parts) != 2 or not parts[0] or not parts[1]:
                print(f"Bad --source URI (expected gs://<bucket>/<object>): {args.source!r}", file=sys.stderr)
                return 2
            args.bucket, args.object = parts
            entries = gcs_download(args.bucket, args.object)
            source_desc = args.source
        else:
            is_local = True
            src_path = Path(args.source)
            entries = load_local(src_path)
            source_desc = str(src_path)
    elif args.local:
        is_local = True
        entries = load_local(args.local)
        source_desc = str(args.local)
    else:
        entries = gcs_download(args.bucket, args.object)
        source_desc = f"gs://{args.bucket}/{args.object}"
    print(f"Loaded {len(entries)} entries from {source_desc}")

    remove_subs = args.remove_unwanted or []
    target_ids = _parse_ids(args.fix_sentiment, args.fix_sentiment_file)

    # Sentiment-fix mode takes precedence if any id was specified
    if target_ids:
        print(f"Sentiment-fix mode: {len(target_ids)} target entry_id(s).")
        patched, audit = fix_sentiments(entries, target_ids, delay_seconds=args.llm_delay)
        if remove_subs:
            patched, per_sub, changed = strip_unwanted_everywhere(patched, remove_subs)
            print()
            _print_strip_report(per_sub, changed, len(patched))
        out_path = args.output or (Path(__file__).parent / "updated_sentiment.json")
        save_local(out_path, patched)
        summarize_sentiment_fix(audit, patched, out_path)
        return 0 if all(r["error"] is None for r in audit) else 1

    # Remove-unwanted standalone mode: no fix-sentiment, no canonicalize intent
    # (neither --dry-run nor --apply). Skip the canonicalize pipeline entirely so
    # entry_ids, timestamps, field order etc. stay byte-for-byte identical
    # except for the stripped substrings.
    if remove_subs and not args.apply and not args.dry_run:
        cleaned, per_sub, changed = strip_unwanted_everywhere(entries, remove_subs)
        out_path = args.output or (Path(__file__).parent / "corrected_entries.json")
        save_local(out_path, cleaned)
        print("=" * 60)
        print("Remove-unwanted summary")
        print("=" * 60)
        _print_strip_report(per_sub, changed, len(cleaned))
        print(f"  Output: {out_path}  ({len(cleaned)} entries)")
        return 0

    # Canonicalize mode (default). Optional strip post-process.
    corrected, stats = correct_log(entries)
    verify_idempotent(corrected)
    summarize(stats, corrected)

    if remove_subs:
        corrected, per_sub, changed = strip_unwanted_everywhere(corrected, remove_subs)
        print()
        _print_strip_report(per_sub, changed, len(corrected))

    if args.output:
        save_local(args.output, corrected)
        print(f"\nWrote corrected log to {args.output}")

    if args.apply:
        if is_local:
            print("\n--apply ignored: source is a local file. Use --output to write locally, or pass a gs:// --source.", file=sys.stderr)
            return 2
        print("\nApplying to GCS...")
        backup_uri = gcs_backup(args.bucket, args.object)
        print(f"  Backed up original to {backup_uri}")
        gcs_upload(args.bucket, args.object, corrected)
        print(f"  Uploaded corrected log to gs://{args.bucket}/{args.object}")
    else:
        print("\n(dry-run: no uploads performed. Use --apply to write back to GCS.)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
