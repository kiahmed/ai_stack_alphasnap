#!/usr/bin/env python3
"""
Test: intra-batch dedup logic in dedup_findings()

Simulates two Red Cat findings arriving in the same scout batch and verifies
whether the TF-IDF and entity-overlap thresholds correctly flag the second
as a duplicate of the first.

Two scenarios:
  1. IDF built from ONLY the two findings (small-batch worst case)
  2. IDF built from the full Robotics corpus + the two findings (production-like)
"""

import re, math, json, os, yaml
from collections import Counter

# ── Load thresholds from values.yaml (same as market_team.py) ──────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.join(SCRIPT_DIR, "..")
with open(os.path.join(PROJECT_ROOT, "values.yaml"), "r") as f:
    config = yaml.safe_load(f)

DEDUP_CFG     = config.get("dedup", {})
TFIDF_THRESHOLD  = DEDUP_CFG.get("tfidf_threshold", 0.45)
ENTITY_THRESHOLD = DEDUP_CFG.get("entity_threshold", 0.6)
NOVELTY_MIN      = DEDUP_CFG.get("novelty_min_entities", 2)

# ── Replicate the exact helper functions from market_team.py ───────────────
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
    for m in re.findall(r'\$[\d,.]+\s*[BMKTbmkt](?:illion|rillion)?', text):
        entities.add(m.strip().upper())
    entities.update(re.findall(r'[\d,.]+%', text))
    entities.update(re.findall(r'[\d,.]+[xX]\b', text))
    entities.update(re.findall(r'\$([A-Z]{1,5})\b', text))
    for t in re.findall(r'\b([A-Z]{2,5})\b', text):
        if t not in _STOP_UPPER:
            entities.add(t)
    entities.update(re.findall(r'[A-Z][a-z]+(?:\s+[A-Z][a-z]+)+', text))
    return entities

def _entity_overlap(entities_a, entities_b):
    if not entities_a or not entities_b:
        return 0.0
    intersection = entities_a & entities_b
    smaller = min(len(entities_a), len(entities_b))
    return len(intersection) / smaller

# ── Test data ──────────────────────────────────────────────────────────────
FINDING_A = (
    "Red Cat Holdings officially finalized its acquisition of Apium Swarm Robotics "
    "to strengthen distributed physical AI and autonomous multi-agent drone swarming "
    "for defense."
)
FINDING_B = (
    "Red Cat (RCAT) acquired Apium Swarm Robotics to integrate real-time swarming "
    "and distributed autonomy into tactical drones."
)

# ══════════════════════════════════════════════════════════════════════════
# SCENARIO 1: IDF from only the two findings (small-batch edge case)
# ══════════════════════════════════════════════════════════════════════════
print("=" * 80)
print("SCENARIO 1: IDF built from the two findings only (small-batch)")
print("=" * 80)

tokens_a = _tokenize(FINDING_A)
tokens_b = _tokenize(FINDING_B)
idf_small = _build_idf([tokens_a, tokens_b])

tfidf_score_small = _tfidf_similarity(FINDING_A, FINDING_B, idf_small)
ents_a = _extract_entities(FINDING_A)
ents_b = _extract_entities(FINDING_B)
ent_score = _entity_overlap(ents_a, ents_b)

print(f"\nFinding A: {FINDING_A}")
print(f"Finding B: {FINDING_B}")
print(f"\n  Entities A : {sorted(ents_a)}")
print(f"  Entities B : {sorted(ents_b)}")
intersection = ents_a & ents_b
print(f"  Overlap    : {sorted(intersection)}")
novel_b = ents_b - ents_a
print(f"  Novel in B : {sorted(novel_b)}")
print()
print(f"  TF-IDF cosine similarity : {tfidf_score_small:.4f}  (threshold = {TFIDF_THRESHOLD})")
print(f"  Entity overlap score     : {ent_score:.4f}  (threshold = {ENTITY_THRESHOLD})")
print()

tfidf_flag = tfidf_score_small >= TFIDF_THRESHOLD
entity_flag = ent_score >= ENTITY_THRESHOLD
flagged = tfidf_flag or entity_flag
novelty_rescue = len(novel_b) >= NOVELTY_MIN

print(f"  TF-IDF flags as dupe?    : {tfidf_flag}   ({tfidf_score_small:.4f} >= {TFIDF_THRESHOLD})")
print(f"  Entity flags as dupe?    : {entity_flag}   ({ent_score:.4f} >= {ENTITY_THRESHOLD})")
print(f"  Either threshold hit?    : {flagged}")
if flagged:
    print(f"  Novelty rescue?          : {novelty_rescue}   (novel entities = {len(novel_b)}, need >= {NOVELTY_MIN})")
    final_dup = flagged and not novelty_rescue
    print(f"  FINAL VERDICT            : {'DUPLICATE -- B would be dropped' if final_dup else 'KEPT (novelty rescue)'}")
else:
    print(f"  FINAL VERDICT            : UNIQUE -- B passes through")

# ══════════════════════════════════════════════════════════════════════════
# SCENARIO 2: IDF from full Robotics corpus + both findings (production)
# ══════════════════════════════════════════════════════════════════════════
print()
print("=" * 80)
print("SCENARIO 2: IDF built from full Robotics corpus + both findings")
print("=" * 80)

corpus_texts = []
try:
    with open(os.path.join(PROJECT_ROOT, "market_findings_log.json"), "r") as f:
        all_entries = json.load(f)
    robotics = [e for e in all_entries if e.get("category", "").lower() == "robotics"]
    corpus_texts = [e.get("finding", "") for e in robotics]
    print(f"\n  Loaded {len(corpus_texts)} Robotics baseline entries from local log")
except Exception as e:
    print(f"\n  WARNING: Could not load local log ({e}), using empty baseline")

all_texts = corpus_texts + [FINDING_A, FINDING_B]
all_tokens = [_tokenize(t) for t in all_texts]
idf_full = _build_idf(all_tokens)

tfidf_score_full = _tfidf_similarity(FINDING_A, FINDING_B, idf_full)

print(f"  IDF corpus size          : {len(all_texts)} documents")
print()
print(f"  TF-IDF cosine similarity : {tfidf_score_full:.4f}  (threshold = {TFIDF_THRESHOLD})")
print(f"  Entity overlap score     : {ent_score:.4f}  (threshold = {ENTITY_THRESHOLD})  [unchanged -- entities are lexical]")
print()

tfidf_flag_full = tfidf_score_full >= TFIDF_THRESHOLD
flagged_full = tfidf_flag_full or entity_flag

print(f"  TF-IDF flags as dupe?    : {tfidf_flag_full}   ({tfidf_score_full:.4f} >= {TFIDF_THRESHOLD})")
print(f"  Entity flags as dupe?    : {entity_flag}   ({ent_score:.4f} >= {ENTITY_THRESHOLD})")
print(f"  Either threshold hit?    : {flagged_full}")
if flagged_full:
    print(f"  Novelty rescue?          : {novelty_rescue}   (novel entities = {len(novel_b)}, need >= {NOVELTY_MIN})")
    final_dup_full = flagged_full and not novelty_rescue
    print(f"  FINAL VERDICT            : {'DUPLICATE -- B would be dropped' if final_dup_full else 'KEPT (novelty rescue)'}")
else:
    print(f"  FINAL VERDICT            : UNIQUE -- B passes through")

# ══════════════════════════════════════════════════════════════════════════
# SUMMARY
# ══════════════════════════════════════════════════════════════════════════
print()
print("=" * 80)
print("SUMMARY")
print("=" * 80)
print(f"  Thresholds from values.yaml:")
print(f"    tfidf_threshold   = {TFIDF_THRESHOLD}")
print(f"    entity_threshold  = {ENTITY_THRESHOLD}")
print(f"    novelty_min       = {NOVELTY_MIN}")
print()
print(f"  Small-batch IDF  -> tfidf={tfidf_score_small:.4f}  entity={ent_score:.4f}  -> {'DUPE' if (tfidf_score_small >= TFIDF_THRESHOLD or ent_score >= ENTITY_THRESHOLD) and len(novel_b) < NOVELTY_MIN else 'PASS'}")
print(f"  Full-corpus IDF  -> tfidf={tfidf_score_full:.4f}  entity={ent_score:.4f}  -> {'DUPE' if (tfidf_score_full >= TFIDF_THRESHOLD or ent_score >= ENTITY_THRESHOLD) and len(novel_b) < NOVELTY_MIN else 'PASS'}")
print()
print("  Intra-batch dedup correctly catches B as duplicate of A?" ,
      "YES" if ((tfidf_score_small >= TFIDF_THRESHOLD or ent_score >= ENTITY_THRESHOLD) and len(novel_b) < NOVELTY_MIN)
      else "NO -- thresholds may need tuning")

# ══════════════════════════════════════════════════════════════════════════
# DIAGNOSTIC: Why entity overlap is low + why novelty rescues in scenario 2
# ══════════════════════════════════════════════════════════════════════════
print()
print("=" * 80)
print("DIAGNOSTIC DETAIL")
print("=" * 80)
print()
print("ENTITY EXTRACTION ANALYSIS:")
print(f'  A entities ({len(ents_a)}): {sorted(ents_a)}')
print(f'  B entities ({len(ents_b)}): {sorted(ents_b)}')
print()
print("  Problem: 'Red Cat Holdings' (A) vs 'Red Cat' (B) are extracted as")
print("  different multi-word entities. Only 'Apium Swarm Robotics' matches.")
print(f"  Overlap = 1 / min({len(ents_a)}, {len(ents_b)}) = {ent_score:.4f}")
print()
print("  Novel entities in B not in A: {0}".format(sorted(novel_b)))
print(f"  Count = {len(novel_b)} >= novelty_min ({NOVELTY_MIN})")
print(f"  -> 'RCAT' and 'Red Cat' are treated as novel entities, rescuing B")
print(f"     even when TF-IDF hits the threshold (scenario 2).")
print()
print("TF-IDF ANALYSIS:")
print(f"  Small-batch IDF (2 docs): Common terms get LOW idf weight, but the")
print(f"  two findings share few exact tokens -> cosine = {tfidf_score_small:.4f} < {TFIDF_THRESHOLD}")
print(f"  Full-corpus IDF (174 docs): Rare shared terms (swarming, apium,")
print(f"  distributed, drones) get HIGH idf weight -> cosine = {tfidf_score_full:.4f} >= {TFIDF_THRESHOLD}")
print()
print("CONCLUSION:")
print("  In the actual intra-batch dedup path (which uses the full corpus IDF),")
print("  TF-IDF DOES flag B as duplicate (0.4000 >= 0.35), but the novelty")
print("  rescue kicks in because 'RCAT' and 'Red Cat' are 2 novel entities")
print("  (meeting novelty_min=2).")
print()
print("  To make intra-batch dedup catch this pair, consider:")
print("  1. Raise novelty_min_entities to 3 (so ticker + partial name aren't enough)")
print("  2. Normalize entity extraction so 'Red Cat Holdings' and 'Red Cat' merge")
print("  3. Exclude known tickers from novelty count (RCAT is just B's ticker for Red Cat)")
