import json
import os
import re
import math
from collections import Counter

# Configuration
INPUT_FILE = "market_findings_log_2.json"
OUTPUT_FILE = "market_findings_log_deduped.json"
SIMILARITY_THRESHOLD = 0.45  # TF-IDF cosine threshold (tuned against real corpus)

# Colors for terminal output
RED = "\033[91m"
GREEN = "\033[92m"
CYAN = "\033[96m"
RESET = "\033[0m"

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

def get_similarity(a, b, idf):
    """TF-IDF cosine similarity between two strings."""
    tokens_a = _tokenize(a)
    tokens_b = _tokenize(b)
    vec_a = _tfidf_vector(tokens_a, idf)
    vec_b = _tfidf_vector(tokens_b, idf)
    return _cosine_sim(vec_a, vec_b)

def deduplicate():
    print(f"{CYAN}--- Dedup: comparing {OUTPUT_FILE} against {INPUT_FILE} ---{RESET}\n")

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

    # Group INPUT_FILE entries by category for efficient lookup
    input_by_cat = {}
    for entry in input_data:
        cat = entry.get("category", "General")
        input_by_cat.setdefault(cat, []).append(entry)

    # Group OUTPUT_FILE entries by category
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

        # Compare each output entry against all input entries in the same category
        unique_entries = []
        dup_count = 0

        for entry in output_entries:
            is_duplicate = False
            finding = entry.get("finding", "").strip()

            for existing in input_entries:
                existing_finding = existing.get("finding", "").strip()
                similarity = get_similarity(finding, existing_finding, idf)

                if similarity >= SIMILARITY_THRESHOLD:
                    is_duplicate = True
                    dup_count += 1
                    print(f"{RED}Duplicate found in {cat} (dup #{dup_count}) at similarity {similarity:.2f}:{RESET}")
                    print(f"  [Output]   : {finding[:120]}...")
                    print(f"  [Input]    : {existing_finding[:120]}...\n")
                    break

            if not is_duplicate:
                unique_entries.append(entry)

        # Merge: input entries + unique output entries, sorted by timestamp
        combined = input_entries + unique_entries
        combined.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
        merged_list.extend(combined)

        summary[cat] = {"duplicates": dup_count, "new": len(unique_entries), "total": len(combined)}
        print(f"{GREEN}Finished {cat}. Removed {dup_count} duplicates, kept {len(unique_entries)} new entries.{RESET}\n")

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
    total_dups = 0
    total_new = 0
    total_merged = 0
    for cat, stats in summary.items():
        print(f"Category: {cat:20} | Dups Removed: {stats['duplicates']:3} | New Kept: {stats['new']:3} | Total: {stats['total']:3}")
        total_dups += stats["duplicates"]
        total_new += stats["new"]
        total_merged += stats["total"]
    print(f"{CYAN}--------------------------------------------------------")
    print(f"Input Entries:      {len(input_data)}")
    print(f"Output Entries:     {len(output_data)}")
    print(f"Duplicates Removed: {total_dups}")
    print(f"New Entries Merged: {total_new}")
    print(f"Final Total:        {total_merged}{RESET}")

if __name__ == "__main__":
    deduplicate()
