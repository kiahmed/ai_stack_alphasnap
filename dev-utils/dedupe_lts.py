import json
import os
import re
import math
import yaml
from collections import Counter

# ── Configuration ──────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.join(SCRIPT_DIR, "..")

INPUT_FILE = os.path.join(PROJECT_ROOT, "market_findings_log.json")
OUTPUT_FILE = os.path.join(PROJECT_ROOT, "market_findings_log_deduped.json")

# Load thresholds from values.yaml if available, else use defaults
try:
    with open(os.path.join(PROJECT_ROOT, "values.yaml"), "r") as f:
        _cfg = yaml.safe_load(f)
    DEDUP_CFG = _cfg.get("dedup", {})
except Exception:
    DEDUP_CFG = {}


TFIDF_THRESHOLD = DEDUP_CFG.get("tfidf_threshold", 0.45)
ENTITY_THRESHOLD = DEDUP_CFG.get("entity_threshold", 0.6)
NOVELTY_MIN = DEDUP_CFG.get("novelty_min_entities", 2)

# Colors for terminal output
RED = "\033[91m"
GREEN = "\033[92m"
CYAN = "\033[96m"
YELLOW = "\033[93m"
RESET = "\033[0m"

# ── Stopwords for ticker detection ─────────────────────────────
_STOP_UPPER = {
    'THE', 'AND', 'FOR', 'NOT', 'BUT', 'ALL', 'CAN', 'HAD', 'HER',
    'WAS', 'ONE', 'OUR', 'OUT', 'ARE', 'HAS', 'HIS', 'HOW', 'ITS', 'MAY', 'NEW',
    'NOW', 'OLD', 'SEE', 'WAY', 'WHO', 'DID', 'GET', 'HIM', 'LET', 'SAY',
    'SHE', 'TOO', 'USE', 'CEO', 'CFO', 'CTO', 'COO', 'IPO', 'ETF', 'GDP',
    'API', 'USA', 'USD', 'EUR', 'GBP', 'WITH', 'THIS', 'THAT', 'FROM',
    'THEY', 'BEEN', 'HAVE', 'WILL', 'EACH', 'MAKE', 'LIKE', 'LONG', 'VERY',
    'WHEN', 'WHAT', 'YOUR', 'SOME', 'THEM', 'THAN', 'MOST', 'ALSO', 'INTO',
    'OVER', 'SUCH', 'JUST', 'NEAR', 'TERM', 'PER', 'VIA', 'KEY', 'PRE',
    'PRO', 'BOTH', 'ONLY', 'SAME', 'MORE', 'LESS', 'FULL', 'HIGH', 'LOW',
    'NEXT', 'LAST', 'WEEK', 'YEAR', 'NEWS', 'PLUS', 'DEAL'
}

# ── TF-IDF helpers ─────────────────────────────────────────────
def _tokenize(text):
    """Lowercase, strip punctuation, split into words."""
    return re.findall(r"[a-z0-9]+(?:'[a-z]+)?", text.lower())

def _build_idf(documents):
    """Compute IDF from a list of token-lists."""
    n = len(documents)
    df = Counter()
    for doc in documents:
        df.update(set(doc))
    return {term: math.log((n + 1) / (count + 1)) + 1 for term, count in df.items()}

def _tfidf_vector(tokens, idf):
    """Build a TF-IDF vector (as a Counter-based sparse dict)."""
    tf = Counter(tokens)
    return {term: freq * idf.get(term, 1.0) for term, freq in tf.items()}

def _cosine_sim(vec_a, vec_b):
    """Cosine similarity between two sparse vectors."""
    common = set(vec_a.keys()) & set(vec_b.keys())
    dot = sum(vec_a[k] * vec_b[k] for k in common)
    mag_a = math.sqrt(sum(v * v for v in vec_a.values()))
    mag_b = math.sqrt(sum(v * v for v in vec_b.values()))
    if mag_a == 0 or mag_b == 0:
        return 0.0
    return dot / (mag_a * mag_b)

def _tfidf_similarity(a, b, idf):
    """TF-IDF cosine similarity between two strings."""
    vec_a = _tfidf_vector(_tokenize(a), idf)
    vec_b = _tfidf_vector(_tokenize(b), idf)
    return _cosine_sim(vec_a, vec_b)

# ── Entity extraction (tickers, dollar amounts, company names) ──
def _extract_entities(text):
    """Extract named entities and tickers from text.
    Returns (all_entities, tickers) — tickers tracked separately for novelty checks.
    """
    entities = set()
    tickers = set()
    # Dollar amounts: $40B, $30 billion, $500M
    for m in re.findall(r'\$[\d,.]+\s*[BMKTbmkt](?:illion|rillion)?', text):
        entities.add(m.strip().upper())
    # Percentages and multipliers
    entities.update(re.findall(r'[\d,.]+%', text))
    entities.update(re.findall(r'[\d,.]+[xX]\b', text))
    # Explicit $TICKER format
    tickers.update(re.findall(r'\$([A-Z]{1,5})\b', text))
    # Uppercase 2-5 char words (likely tickers), exclude stopwords
    for t in re.findall(r'\b([A-Z]{2,5})\b', text):
        if t not in _STOP_UPPER:
            tickers.add(t)
    # Multi-word capitalized names (company/product names)
    entities.update(re.findall(r'[A-Z][a-z]+(?:\s+[A-Z][a-z]+)+', text))
    # All entities includes tickers (for overlap scoring), but tickers tracked separately for novelty
    all_entities = entities | tickers
    return all_entities, tickers

def _merge_substring_entities(entities):
    """Merge entities where one is a substring of another (e.g. 'Red Cat' and 'Red Cat Holdings')."""
    merged = set()
    sorted_ents = sorted(entities, key=len, reverse=True)
    for ent in sorted_ents:
        ent_lower = ent.lower()
        if not any(ent_lower in existing.lower() for existing in merged):
            merged.add(ent)
    return merged

def _entity_overlap(entities_a, entities_b):
    """Jaccard-style entity overlap with substring matching."""
    if not entities_a or not entities_b:
        return 0.0
    # Merge substring variants before comparing
    merged_a = _merge_substring_entities(entities_a)
    merged_b = _merge_substring_entities(entities_b)
    intersection = merged_a & merged_b
    # Also count partial matches: "Red Cat" in A matches "Red Cat Holdings" in B
    for ea in merged_a:
        for eb in merged_b:
            if ea != eb and (ea.lower() in eb.lower() or eb.lower() in ea.lower()):
                intersection.add(ea)
    smaller = min(len(merged_a), len(merged_b))
    return len(intersection) / smaller if smaller > 0 else 0.0

# ── Main dedup logic ───────────────────────────────────────────
def deduplicate():
    print(f"{CYAN}--- Dedup: comparing {OUTPUT_FILE} against {INPUT_FILE} ---")
    print(f"    Thresholds: TF-IDF={TFIDF_THRESHOLD}, Entity={ENTITY_THRESHOLD}, Novelty≥{NOVELTY_MIN}{RESET}\n")

    # Load INPUT_FILE (the baseline to compare against)
    if not os.path.exists(INPUT_FILE):
        print(f"{RED}Error: {INPUT_FILE} not found.{RESET}")
        return
    try:
        with open(INPUT_FILE, 'r') as f:
            input_data = json.load(f)
    except Exception as e:
        print(f"{RED}Error reading {INPUT_FILE}: {e}{RESET}")
        return

    # Load OUTPUT_FILE (the new entries to check)
    if not os.path.exists(OUTPUT_FILE):
        print(f"{RED}Error: {OUTPUT_FILE} not found.{RESET}")
        return
    try:
        with open(OUTPUT_FILE, 'r') as f:
            output_data = json.load(f)
    except Exception as e:
        print(f"{RED}Error reading {OUTPUT_FILE}: {e}{RESET}")
        return

    # Group entries by category
    input_by_cat = {}
    for entry in input_data:
        cat = entry.get("category", "General")
        input_by_cat.setdefault(cat, []).append(entry)

    output_by_cat = {}
    for entry in output_data:
        cat = entry.get("category", "General")
        output_by_cat.setdefault(cat, []).append(entry)

    # Build IDF corpus from all findings across both files
    all_findings = [e.get("finding", "") for e in input_data + output_data]
    corpus_tokens = [_tokenize(f) for f in all_findings]
    idf = _build_idf(corpus_tokens)
    print(f"{CYAN}Built IDF from {len(all_findings)} findings ({len(idf)} unique terms){RESET}\n")

    all_cats = sorted(set(list(input_by_cat.keys()) + list(output_by_cat.keys())))
    merged_list = []
    summary = {}

    for cat in all_cats:
        input_entries = input_by_cat.get(cat, [])
        output_entries = output_by_cat.get(cat, [])

        print(f"{CYAN}Processing Category: {cat} (input: {len(input_entries)}, output: {len(output_entries)}){RESET}")

        # ── Phase 0: Self-dedup INPUT entries within this category ──
        deduped_input = []
        input_self_dups = 0
        for entry in input_entries:
            finding = entry.get("finding", "").strip()
            finding_entities, finding_tickers = _extract_entities(finding)
            is_dup = False

            for accepted in deduped_input:
                accepted_finding = accepted.get("finding", "").strip()
                accepted_entities, _ = _extract_entities(accepted_finding)
                tfidf_score = _tfidf_similarity(finding, accepted_finding, idf)
                ent_score = _entity_overlap(finding_entities, accepted_entities)

                if tfidf_score >= TFIDF_THRESHOLD or ent_score >= ENTITY_THRESHOLD:
                    novel_entities = (finding_entities - accepted_entities) - finding_tickers
                    if len(novel_entities) >= NOVELTY_MIN:
                        continue  # genuinely new info, keep

                    is_dup = True
                    input_self_dups += 1
                    print(f"{RED}  [INPUT SELF-DUP #{input_self_dups}] {cat} (tfidf={tfidf_score:.2f}, entity={ent_score:.2f}){RESET}")
                    print(f"    kept   : {accepted_finding[:140]}")
                    print(f"    removed: {finding[:140]}")
                    break

            if not is_dup:
                deduped_input.append(entry)

        if input_self_dups > 0:
            print(f"{YELLOW}  Input self-dedup: removed {input_self_dups} internal duplicates (was {len(input_entries)}, now {len(deduped_input)}){RESET}")
        input_entries = deduped_input

        # Pre-extract entities for all (deduped) input entries in this category
        input_texts = [e.get("finding", "").strip() for e in input_entries]
        input_entity_sets = [_extract_entities(t) for t in input_texts]

        unique_entries = []
        dup_count = 0

        for entry in output_entries:
            finding = entry.get("finding", "").strip()
            finding_entities, finding_tickers = _extract_entities(finding)
            is_duplicate = False

            # ── Compare against input (baseline) entries ──
            for i, existing_finding in enumerate(input_texts):
                base_entities, base_tickers = input_entity_sets[i]
                tfidf_score = _tfidf_similarity(finding, existing_finding, idf)
                ent_score = _entity_overlap(finding_entities, base_entities)

                if tfidf_score >= TFIDF_THRESHOLD or ent_score >= ENTITY_THRESHOLD:
                    # Check novelty: exclude tickers — they're alternate identifiers, not new info
                    novel_entities = (finding_entities - base_entities) - finding_tickers
                    if len(novel_entities) >= NOVELTY_MIN:
                        # New development on the same topic — keep it
                        print(f"{YELLOW}  [NOVELTY] Keeping despite match (tfidf={tfidf_score:.2f}, entity={ent_score:.2f}), {len(novel_entities)} novel entities: {novel_entities}{RESET}")
                        continue

                    is_duplicate = True
                    dup_count += 1
                    print(f"{RED}  [DUP #{dup_count}] {cat} (tfidf={tfidf_score:.2f}, entity={ent_score:.2f}){RESET}")
                    print(f"    input : {existing_finding[:140]}")
                    print(f"    output: {finding[:140]}")
                    break

            # ── Intra-batch dedup: compare against already-accepted entries ──
            if not is_duplicate:
                for accepted_entry in unique_entries:
                    accepted_finding = accepted_entry.get("finding", "").strip()
                    accepted_entities, accepted_tickers = _extract_entities(accepted_finding)
                    tfidf_score = _tfidf_similarity(finding, accepted_finding, idf)
                    ent_score = _entity_overlap(finding_entities, accepted_entities)
                    if tfidf_score >= TFIDF_THRESHOLD or ent_score >= ENTITY_THRESHOLD:
                        is_duplicate = True
                        dup_count += 1
                        print(f"{RED}  [INTRA-BATCH DUP #{dup_count}] {cat} (tfidf={tfidf_score:.2f}, entity={ent_score:.2f}){RESET}")
                        print(f"    accepted: {accepted_finding[:140]}")
                        print(f"    output  : {finding[:140]}")
                        break

            if not is_duplicate:
                unique_entries.append(entry)

        # Merge: deduped input entries + unique output entries, sorted by timestamp
        combined = input_entries + unique_entries
        combined.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
        merged_list.extend(combined)

        summary[cat] = {"input_self_dups": input_self_dups, "cross_dups": dup_count, "new": len(unique_entries), "total": len(combined)}
        print(f"{GREEN}  Finished {cat}: {input_self_dups} input self-dups, {dup_count} cross-dups removed, kept {len(unique_entries)} new entries. Final: {len(combined)}{RESET}\n")


    # Save merged result back to OUTPUT_FILE
    try:
        with open(OUTPUT_FILE, 'w') as f:
            json.dump(merged_list, f, indent=4)
        print(f"{GREEN}--------------------------------------------------------")
        print(f"✅ Deduplication complete. Saved to {OUTPUT_FILE}")
        print(f"--------------------------------------------------------{RESET}")
    except Exception as e:
        print(f"{RED}Error saving file: {e}{RESET}")
        return

    # Final summary per category
    print(f"\n{CYAN}--- Final Dedup Summary ---{RESET}")
    total_self_dups = 0
    total_cross_dups = 0
    total_new = 0
    total_merged = 0
    for cat, stats in summary.items():
        print(f"Category: {cat:20} | Self-Dups: {stats['input_self_dups']:3} | Cross-Dups: {stats['cross_dups']:3} | New Kept: {stats['new']:3} | Total: {stats['total']:3}")
        total_self_dups += stats["input_self_dups"]
        total_cross_dups += stats["cross_dups"]
        total_new += stats["new"]
        total_merged += stats["total"]
    print(f"{CYAN}--------------------------------------------------------")
    print(f"Input Entries:         {len(input_data)}")
    print(f"Output Entries:        {len(output_data)}")
    print(f"Input Self-Dups:       {total_self_dups}")
    print(f"Cross-File Dups:       {total_cross_dups}")
    print(f"New Entries Merged:     {total_new}")
    print(f"Final Total:           {total_merged}{RESET}")


if __name__ == "__main__":
    deduplicate()
