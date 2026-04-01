#!/usr/bin/env python3
"""
Threshold sweep test for dedupe_lts.py TF-IDF cosine similarity.
Tests the Robotics category (172 entries) at multiple thresholds.
"""

import json
import os
import re
import math
import time
from collections import Counter
from itertools import combinations

# --- Exact copies of dedupe_lts.py functions ---

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

# --- Main sweep ---

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.join(SCRIPT_DIR, "..")
INPUT_FILE = os.path.join(PROJECT_ROOT, "market_findings_log.json")
THRESHOLDS = [0.25, 0.30, 0.35, 0.40, 0.45]

print("Loading data...")
with open(INPUT_FILE, 'r') as f:
    all_data = json.load(f)

print(f"Total entries: {len(all_data)}")

# Extract Robotics entries with their global indices
robotics = [(i, entry) for i, entry in enumerate(all_data) if entry.get("category") == "Robotics"]
print(f"Robotics entries: {len(robotics)}")

# Find the Red Cat pair by global index
red_cat_indices = {294, 324}
for gi, entry in robotics:
    if gi in red_cat_indices:
        print(f"  Global idx {gi}: {entry['finding'][:100]}...")

# Build IDF from the FULL corpus (matching dedupe_lts.py behavior)
print("\nBuilding IDF from full corpus...")
all_findings = [e.get("finding", "") for e in all_data]
corpus_tokens = [_tokenize(f) for f in all_findings]
idf = _build_idf(corpus_tokens)
print(f"IDF built: {len(idf)} unique terms")

# Pre-compute TF-IDF vectors for all Robotics entries
print("\nPre-computing TF-IDF vectors for Robotics...")
robotics_vectors = []
for gi, entry in robotics:
    finding = entry.get("finding", "").strip()
    tokens = _tokenize(finding)
    vec = _tfidf_vector(tokens, idf)
    robotics_vectors.append((gi, finding, vec))

# Compute ALL pairwise similarities
print(f"\nComputing pairwise similarities for {len(robotics)} entries ({len(robotics) * (len(robotics)-1) // 2} pairs)...")
t0 = time.time()

all_pairs = []
for idx_a in range(len(robotics_vectors)):
    gi_a, finding_a, vec_a = robotics_vectors[idx_a]
    for idx_b in range(idx_a + 1, len(robotics_vectors)):
        gi_b, finding_b, vec_b = robotics_vectors[idx_b]
        sim = _cosine_sim(vec_a, vec_b)
        all_pairs.append((sim, gi_a, gi_b, finding_a, finding_b))

elapsed = time.time() - t0
print(f"Computed {len(all_pairs)} pairs in {elapsed:.1f}s")

# Sort by similarity descending
all_pairs.sort(key=lambda x: x[0], reverse=True)

# --- Check the Red Cat pair specifically ---
red_cat_sim = None
for sim, gi_a, gi_b, fa, fb in all_pairs:
    if {gi_a, gi_b} == red_cat_indices:
        red_cat_sim = sim
        break

print(f"\n{'='*100}")
print(f"RED CAT / APIUM PAIR (indices 294 & 324)")
print(f"  Similarity: {red_cat_sim:.4f}")
print(f"  Entry 294: {all_data[294]['finding'][:120]}")
print(f"  Entry 324: {all_data[324]['finding'][:120]}")
print(f"{'='*100}")

# --- Top 30 highest-similarity pairs ---
print(f"\nTOP 30 HIGHEST-SIMILARITY PAIRS (Robotics category):")
print(f"{'-'*120}")
for rank, (sim, gi_a, gi_b, fa, fb) in enumerate(all_pairs[:30], 1):
    marker = " *** RED CAT ***" if {gi_a, gi_b} == red_cat_indices else ""
    print(f"  #{rank:2d}  sim={sim:.4f}  [{gi_a}] vs [{gi_b}]{marker}")
    print(f"        A: {fa[:90]}")
    print(f"        B: {fb[:90]}")
    print()

# --- Threshold sweep ---
print(f"\n{'='*100}")
print(f"THRESHOLD SWEEP RESULTS")
print(f"{'='*100}")

for thresh in THRESHOLDS:
    flagged = [(sim, gi_a, gi_b, fa, fb) for sim, gi_a, gi_b, fa, fb in all_pairs if sim >= thresh]
    catches_redcat = any({gi_a, gi_b} == red_cat_indices for sim, gi_a, gi_b, fa, fb in flagged)

    print(f"\n--- Threshold = {thresh:.2f} ---")
    print(f"  Pairs flagged as duplicates: {len(flagged)}")
    print(f"  Catches Red Cat/Apium pair: {'YES' if catches_redcat else 'NO'}")

    print(f"  Top 10 pairs at this threshold:")
    for rank, (sim, gi_a, gi_b, fa, fb) in enumerate(flagged[:10], 1):
        marker = " <-- RED CAT" if {gi_a, gi_b} == red_cat_indices else ""
        print(f"    {rank:2d}. sim={sim:.4f}  [{gi_a}] vs [{gi_b}]{marker}")
        print(f"        A: {fa[:80]}")
        print(f"        B: {fb[:80]}")

# --- False positive analysis in the 0.35-0.44 range ---
print(f"\n{'='*100}")
print(f"FALSE POSITIVE ANALYSIS: Pairs scoring 0.35 - 0.44")
print(f"{'='*100}")

border_pairs = [(sim, gi_a, gi_b, fa, fb) for sim, gi_a, gi_b, fa, fb in all_pairs if 0.35 <= sim < 0.45]
print(f"Total pairs in 0.35-0.44 range: {len(border_pairs)}")
print()

for rank, (sim, gi_a, gi_b, fa, fb) in enumerate(border_pairs, 1):
    marker = " <-- RED CAT" if {gi_a, gi_b} == red_cat_indices else ""
    print(f"  {rank:2d}. sim={sim:.4f}  [{gi_a}] vs [{gi_b}]{marker}")
    print(f"      A: {fa[:100]}")
    print(f"      B: {fb[:100]}")
    print()

# --- Histogram of similarity distribution ---
print(f"\n{'='*100}")
print(f"SIMILARITY DISTRIBUTION (Robotics, {len(all_pairs)} pairs)")
print(f"{'='*100}")

buckets = {}
for sim, gi_a, gi_b, fa, fb in all_pairs:
    bucket = round(sim * 20) / 20  # 0.05 increments
    buckets[bucket] = buckets.get(bucket, 0) + 1

for bucket in sorted(buckets.keys(), reverse=True):
    if bucket >= 0.15:
        bar = '#' * min(buckets[bucket], 80)
        print(f"  {bucket:.2f}: {buckets[bucket]:5d}  {bar}")

print(f"\nDone.")
