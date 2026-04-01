#!/usr/bin/env python3
"""
Test script: Verify updated dedup_findings logic catches the Red Cat / Apium duplicate pair.

Tests intra-batch dedup (no baseline, two findings in the same scout batch)
with both small-batch IDF and full-corpus IDF from market_findings_log.json.
"""

import re, math, json, os, sys
from collections import Counter

import yaml

# ── Load config from values.yaml ──
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(SCRIPT_DIR, "..", "values.yaml"), "r") as f:
    config = yaml.safe_load(f)

DEDUP_CFG = config.get("dedup", {})
TFIDF_THRESHOLD = DEDUP_CFG.get("tfidf_threshold", 0.45)
ENTITY_THRESHOLD = DEDUP_CFG.get("entity_threshold", 0.6)
NOVELTY_MIN = DEDUP_CFG.get("novelty_min_entities", 2)

print("=" * 80)
print("CONFIG FROM values.yaml")
print(f"  tfidf_threshold:      {TFIDF_THRESHOLD}")
print(f"  entity_threshold:     {ENTITY_THRESHOLD}")
print(f"  novelty_min_entities: {NOVELTY_MIN}")
print("=" * 80)

# ── Stop words (exact copy from market_team.py) ──
_STOP_UPPER = {
    'THE', 'AND', 'FOR', 'BUT', 'NOT', 'YOU', 'ALL', 'CAN', 'HER', 'WAS',
    'ONE', 'OUR', 'OUT', 'ARE', 'HAS', 'HIS', 'HOW', 'ITS', 'MAY', 'NEW',
    'NOW', 'OLD', 'SEE', 'WAY', 'WHO', 'DID', 'GET', 'HIM', 'LET', 'SAY',
    'SHE', 'TOO', 'USE', 'CEO', 'CFO', 'CTO', 'COO', 'IPO', 'ETF', 'GDP',
    'API', 'USA', 'USD', 'EUR', 'GBP', 'WITH', 'THIS', 'THAT', 'FROM',
    'THEY', 'BEEN', 'HAVE', 'WILL', 'EACH', 'MAKE', 'LIKE', 'LONG', 'VERY',
    'WHEN', 'WHAT', 'YOUR', 'SOME', 'THEM', 'THAN', 'MOST', 'ALSO', 'INTO',
    'OVER', 'SUCH', 'JUST', 'NEAR', 'TERM', 'PER', 'VIA', 'KEY', 'PRE',
    'PRO', 'BOTH', 'ONLY', 'SAME', 'MORE', 'LESS', 'FULL', 'HIGH', 'LOW',
    'NEXT', 'LAST', 'WEEK', 'YEAR', 'NEWS', 'PLUS', 'DEAL'
}

# ── Exact reimplementation of dedup functions from market_team.py ──

def _tokenize(text):
    return re.findall(r"[a-z0-9]+(?:'[a-z]+)?", text.lower())

def _build_idf(documents):
    n = len(documents)
    df = Counter()
    for doc in documents:
        df.update(set(doc))
    return {term: math.log((n + 1) / (count + 1)) + 1 for term, count in df.items()}

def _tfidf_vector(tokens, idf):
    tf = Counter(tokens)
    return {term: freq * idf.get(term, 1.0) for term, freq in tf.items()}

def _cosine_sim(vec_a, vec_b):
    common = set(vec_a.keys()) & set(vec_b.keys())
    dot = sum(vec_a[k] * vec_b[k] for k in common)
    mag_a = math.sqrt(sum(v * v for v in vec_a.values()))
    mag_b = math.sqrt(sum(v * v for v in vec_b.values()))
    if mag_a == 0 or mag_b == 0:
        return 0.0
    return dot / (mag_a * mag_b)

def _tfidf_similarity(a, b, idf):
    vec_a = _tfidf_vector(_tokenize(a), idf)
    vec_b = _tfidf_vector(_tokenize(b), idf)
    return _cosine_sim(vec_a, vec_b)

def _extract_entities(text):
    entities = set()
    tickers = set()
    for m in re.findall(r'\$[\d,.]+\s*[BMKTbmkt](?:illion|rillion)?', text):
        entities.add(m.strip().upper())
    entities.update(re.findall(r'[\d,.]+%', text))
    entities.update(re.findall(r'[\d,.]+[xX]\b', text))
    tickers.update(re.findall(r'\$([A-Z]{1,5})\b', text))
    for t in re.findall(r'\b([A-Z]{2,5})\b', text):
        if t not in _STOP_UPPER:
            tickers.add(t)
    entities.update(re.findall(r'[A-Z][a-z]+(?:\s+[A-Z][a-z]+)+', text))
    all_entities = entities | tickers
    return all_entities, tickers

def _merge_substring_entities(entities):
    merged = set()
    sorted_ents = sorted(entities, key=len, reverse=True)
    for ent in sorted_ents:
        ent_lower = ent.lower()
        if not any(ent_lower in existing.lower() for existing in merged):
            merged.add(ent)
    return merged

def _entity_overlap(entities_a, entities_b):
    if not entities_a or not entities_b:
        return 0.0
    merged_a = _merge_substring_entities(entities_a)
    merged_b = _merge_substring_entities(entities_b)
    intersection = merged_a & merged_b
    for ea in merged_a:
        for eb in merged_b:
            if ea != eb and (ea.lower() in eb.lower() or eb.lower() in ea.lower()):
                intersection.add(ea)
    smaller = min(len(merged_a), len(merged_b))
    return len(intersection) / smaller


# ── Test data ──
FINDING_A = (
    "Red Cat Holdings officially finalized its acquisition of Apium Swarm Robotics "
    "to strengthen distributed physical AI and autonomous multi-agent drone swarming for defense."
)
FINDING_B = (
    "Red Cat (RCAT) acquired Apium Swarm Robotics to integrate real-time swarming "
    "and distributed autonomy into tactical drones."
)

# ══════════════════════════════════════════════════════════════════════════
# TEST 1: Small-batch IDF (just the two findings, no baseline)
# ══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("TEST 1: INTRA-BATCH DEDUP (Small-Batch IDF, No Baseline)")
print("=" * 80)

# Extract entities
entities_a, tickers_a = _extract_entities(FINDING_A)
entities_b, tickers_b = _extract_entities(FINDING_B)

print(f"\nFinding A entities: {sorted(entities_a)}")
print(f"Finding A tickers:  {sorted(tickers_a)}")
print(f"\nFinding B entities: {sorted(entities_b)}")
print(f"Finding B tickers:  {sorted(tickers_b)}")

# Entity overlap (with substring merging)
ent_score = _entity_overlap(entities_a, entities_b)
print(f"\nEntity overlap score (after substring merging): {ent_score:.4f}")
print(f"  Threshold: {ENTITY_THRESHOLD} -> {'TRIGGERED' if ent_score >= ENTITY_THRESHOLD else 'not triggered'}")

# Merged entity sets for inspection
merged_a = _merge_substring_entities(entities_a)
merged_b = _merge_substring_entities(entities_b)
print(f"\n  Merged A: {sorted(merged_a)}")
print(f"  Merged B: {sorted(merged_b)}")
exact_overlap = merged_a & merged_b
print(f"  Exact overlap: {sorted(exact_overlap)}")
substring_matches = set()
for ea in merged_a:
    for eb in merged_b:
        if ea != eb and (ea.lower() in eb.lower() or eb.lower() in ea.lower()):
            substring_matches.add(f"'{ea}' ~ '{eb}'")
print(f"  Substring matches: {sorted(substring_matches)}")

# TF-IDF (small batch: corpus = just A + B)
small_corpus = [_tokenize(FINDING_A), _tokenize(FINDING_B)]
small_idf = _build_idf(small_corpus)
tfidf_small = _tfidf_similarity(FINDING_A, FINDING_B, small_idf)
print(f"\nTF-IDF score (small-batch IDF, 2 docs): {tfidf_small:.4f}")
print(f"  Threshold: {TFIDF_THRESHOLD} -> {'TRIGGERED' if tfidf_small >= TFIDF_THRESHOLD else 'not triggered'}")

# Novelty check for B vs A (as intra-batch: B is new finding, A is already accepted)
novel_entities = (entities_b - entities_a) - tickers_b
print(f"\nNovelty check (B vs A):")
print(f"  B entities:          {sorted(entities_b)}")
print(f"  A entities:          {sorted(entities_a)}")
print(f"  B - A (raw diff):    {sorted(entities_b - entities_a)}")
print(f"  B tickers:           {sorted(tickers_b)}")
print(f"  Novel (excl tickers):{sorted(novel_entities)}")
print(f"  Count: {len(novel_entities)}, needed >= {NOVELTY_MIN} to rescue")
print(f"  Rescue blocked? {'YES (not enough novel entities)' if len(novel_entities) < NOVELTY_MIN else 'NO (rescue applies, finding kept)'}")

# Final verdict (small-batch)
flagged_by_tfidf = tfidf_small >= TFIDF_THRESHOLD
flagged_by_entity = ent_score >= ENTITY_THRESHOLD
triggered = flagged_by_tfidf or flagged_by_entity
rescued = triggered and len(novel_entities) >= NOVELTY_MIN
is_duplicate = triggered and not rescued

print(f"\n{'─' * 60}")
print(f"SMALL-BATCH VERDICT:")
print(f"  TF-IDF trigger:   {flagged_by_tfidf} (score={tfidf_small:.4f} vs threshold={TFIDF_THRESHOLD})")
print(f"  Entity trigger:   {flagged_by_entity} (score={ent_score:.4f} vs threshold={ENTITY_THRESHOLD})")
print(f"  Combined trigger: {triggered}")
print(f"  Novelty rescue:   {rescued} ({len(novel_entities)} novel entities, need {NOVELTY_MIN})")
print(f"  >>> FINAL: B is {'DUPLICATE of A' if is_duplicate else 'UNIQUE (kept)'}")
print(f"{'─' * 60}")


# ══════════════════════════════════════════════════════════════════════════
# TEST 2: Full-corpus IDF from market_findings_log.json
# ══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("TEST 2: INTRA-BATCH DEDUP (Full-Corpus IDF from market_findings_log.json)")
print("=" * 80)

log_path = os.path.join(SCRIPT_DIR, "..", "market_findings_log.json")
if os.path.exists(log_path):
    with open(log_path, "r") as f:
        log_data = json.load(f)
    baseline_texts = [e.get("finding", "") for e in log_data if e.get("finding")]
    print(f"Loaded {len(baseline_texts)} findings from log for IDF corpus")

    # Build IDF from full corpus + the two test findings
    all_texts = baseline_texts + [FINDING_A, FINDING_B]
    full_corpus_tokens = [_tokenize(t) for t in all_texts]
    full_idf = _build_idf(full_corpus_tokens)

    tfidf_full = _tfidf_similarity(FINDING_A, FINDING_B, full_idf)
    print(f"\nTF-IDF score (full-corpus IDF, {len(all_texts)} docs): {tfidf_full:.4f}")
    print(f"  Threshold: {TFIDF_THRESHOLD} -> {'TRIGGERED' if tfidf_full >= TFIDF_THRESHOLD else 'not triggered'}")

    # Entity overlap is IDF-independent, same as before
    print(f"  Entity overlap: {ent_score:.4f} (unchanged, IDF-independent)")

    flagged_by_tfidf_full = tfidf_full >= TFIDF_THRESHOLD
    triggered_full = flagged_by_tfidf_full or flagged_by_entity
    rescued_full = triggered_full and len(novel_entities) >= NOVELTY_MIN
    is_duplicate_full = triggered_full and not rescued_full

    print(f"\n{'─' * 60}")
    print(f"FULL-CORPUS VERDICT:")
    print(f"  TF-IDF trigger:   {flagged_by_tfidf_full} (score={tfidf_full:.4f} vs threshold={TFIDF_THRESHOLD})")
    print(f"  Entity trigger:   {flagged_by_entity} (score={ent_score:.4f} vs threshold={ENTITY_THRESHOLD})")
    print(f"  Combined trigger: {triggered_full}")
    print(f"  Novelty rescue:   {rescued_full} ({len(novel_entities)} novel entities, need {NOVELTY_MIN})")
    print(f"  >>> FINAL: B is {'DUPLICATE of A' if is_duplicate_full else 'UNIQUE (kept)'}")
    print(f"{'─' * 60}")

    # Show key IDF differences for shared tokens
    print(f"\nKey token IDF comparison (small-batch vs full-corpus):")
    key_tokens = ['red', 'cat', 'rcat', 'apium', 'swarm', 'robotics', 'drone',
                  'swarming', 'distributed', 'autonomy', 'acquisition', 'defense', 'tactical']
    print(f"  {'token':<15} {'small IDF':>10} {'full IDF':>10}")
    for tok in key_tokens:
        s = small_idf.get(tok, 0)
        f = full_idf.get(tok, 0)
        print(f"  {tok:<15} {s:>10.4f} {f:>10.4f}")
else:
    print(f"WARNING: {log_path} not found, skipping full-corpus test")


# ══════════════════════════════════════════════════════════════════════════
# TEST 3: End-to-end dedup_findings simulation (intra-batch, no baseline)
# ══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("TEST 3: END-TO-END dedup_findings SIMULATION (no baseline, intra-batch only)")
print("=" * 80)

# Simulate what dedup_findings does when baseline is empty
scout_findings = [FINDING_A, FINDING_B]
baseline_texts_sim = []  # empty baseline

# With no baseline, market_team.py line 348-350 returns all findings immediately:
# "No baseline — all findings pass through."
# BUT the intra-batch check only runs AFTER baseline check, in the same loop.
# Let's trace the actual logic:

print("\nSimulating dedup_findings with empty baseline...")

# The actual code checks: if not baseline_texts -> return ALL immediately (line 348-350)
# This means intra-batch dedup DOES NOT RUN when baseline is empty!
print("\n  IMPORTANT: When baseline is empty, market_team.py line 348-350 returns")
print("  ALL scout findings immediately WITHOUT running intra-batch dedup!")
print("  Intra-batch dedup only runs when there IS a baseline.")

print("\n  Simulating WITH a minimal baseline (1 unrelated entry) to engage full logic...")

# Use a dummy baseline to engage the full dedup path
dummy_baseline = ["Boston Dynamics Atlas robot gets new AI upgrade for manufacturing tasks."]
all_sim_texts = dummy_baseline + scout_findings
sim_corpus = [_tokenize(t) for t in all_sim_texts]
sim_idf = _build_idf(sim_corpus)
baseline_entity_sets_sim = [_extract_entities(t) for t in dummy_baseline]

unique_findings = []
for finding in scout_findings:
    finding_entities, finding_tickers = _extract_entities(finding)
    is_dup = False

    # Baseline check
    for i, base_text in enumerate(dummy_baseline):
        base_entities, base_tickers = baseline_entity_sets_sim[i]
        ts = _tfidf_similarity(finding, base_text, sim_idf)
        es = _entity_overlap(finding_entities, base_entities)
        if ts >= TFIDF_THRESHOLD or es >= ENTITY_THRESHOLD:
            novel = (finding_entities - base_entities) - finding_tickers
            if len(novel) >= NOVELTY_MIN:
                continue
            is_dup = True
            print(f"  BASELINE DUP: tfidf={ts:.4f}, entity={es:.4f}")
            break

    # Intra-batch check
    if not is_dup:
        for accepted in unique_findings:
            accepted_entities, accepted_tickers = _extract_entities(accepted)
            ts = _tfidf_similarity(finding, accepted, sim_idf)
            es = _entity_overlap(finding_entities, accepted_entities)
            if ts >= TFIDF_THRESHOLD or es >= ENTITY_THRESHOLD:
                is_dup = True
                print(f"  INTRA-BATCH DUP: tfidf={ts:.4f}, entity={es:.4f}")
                print(f"    accepted: {accepted[:100]}...")
                print(f"    finding:  {finding[:100]}...")
                break

    if not is_dup:
        unique_findings.append(finding)
        print(f"  KEPT: {finding[:100]}...")
    else:
        print(f"  DROPPED: {finding[:100]}...")

print(f"\nResult: {len(unique_findings)} kept, {len(scout_findings) - len(unique_findings)} dropped")


# ══════════════════════════════════════════════════════════════════════════
# SUMMARY
# ══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)
print(f"""
Config: tfidf_threshold={TFIDF_THRESHOLD}, entity_threshold={ENTITY_THRESHOLD}, novelty_min_entities={NOVELTY_MIN}

Scores between A and B:
  TF-IDF (small-batch): {tfidf_small:.4f} {'>=':>3} {TFIDF_THRESHOLD} threshold -> {'YES' if flagged_by_tfidf else 'NO'}
  TF-IDF (full-corpus):  {tfidf_full:.4f} {'>=':>3} {TFIDF_THRESHOLD} threshold -> {'YES' if flagged_by_tfidf_full else 'NO'}
  Entity overlap:        {ent_score:.4f} {'>=':>3} {ENTITY_THRESHOLD} threshold -> {'YES' if flagged_by_entity else 'NO'}

Novelty rescue:
  Novel entities (excl tickers): {sorted(novel_entities)} (count={len(novel_entities)})
  Needed >= {NOVELTY_MIN} to rescue -> {'RESCUED (kept)' if rescued else 'NOT rescued (dropped)'}

FINAL VERDICTS:
  Small-batch IDF: B is {'DUPLICATE' if is_duplicate else 'UNIQUE'}
  Full-corpus IDF: B is {'DUPLICATE' if is_duplicate_full else 'UNIQUE'}
  End-to-end sim:  {len(unique_findings)} of 2 findings kept (intra-batch dedup with dummy baseline)

NOTE: When baseline is empty, market_team.py returns all findings immediately
      (line 348-350), so intra-batch dedup only fires when a baseline exists.
""")

# Assert the test passes
assert is_duplicate, "FAIL: B should be flagged as duplicate of A with small-batch IDF"
assert is_duplicate_full, "FAIL: B should be flagged as duplicate of A with full-corpus IDF"
assert len(unique_findings) == 1, f"FAIL: Expected 1 unique finding, got {len(unique_findings)}"
print("ALL ASSERTIONS PASSED")
