"""Test: Does the updated dedupe_lts.py (threshold=0.35) catch the Red Cat/Apium pair?"""

import json
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from dedupe_lts import _tokenize, _build_idf, _tfidf_vector, _cosine_sim, get_similarity, SIMILARITY_THRESHOLD

FINDING_A = (
    "Red Cat Holdings officially finalized its acquisition of Apium Swarm Robotics "
    "to strengthen distributed physical AI and autonomous multi-agent drone swarming for defense."
)
FINDING_B = (
    "Red Cat (RCAT) acquired Apium Swarm Robotics to integrate real-time swarming "
    "and distributed autonomy into tactical drones."
)

OLD_THRESHOLD = 0.45
NEW_THRESHOLD = SIMILARITY_THRESHOLD  # should be 0.35

# --- Load corpus and build IDF ---
with open(os.path.join(os.path.dirname(__file__), "..", "market_findings_log.json"), "r") as f:
    corpus_entries = json.load(f)

all_findings = [e.get("finding", "") for e in corpus_entries]
corpus_tokens = [_tokenize(f) for f in all_findings]
idf = _build_idf(corpus_tokens)

# --- Test the specific pair ---
score = get_similarity(FINDING_A, FINDING_B, idf)

print("=" * 64)
print("RED CAT / APIUM DUPLICATE TEST")
print("=" * 64)
print(f"Finding A: {FINDING_A[:90]}...")
print(f"Finding B: {FINDING_B[:90]}...")
print(f"TF-IDF cosine similarity : {score:.4f}")
print(f"New threshold (0.35)     : {NEW_THRESHOLD}")
print(f"Old threshold (0.45)     : {OLD_THRESHOLD}")
print(f"Flagged as dup at 0.35?  : {'YES' if score >= NEW_THRESHOLD else 'NO'}")
print(f"Would have been dup at 0.45? : {'YES' if score >= OLD_THRESHOLD else 'NO'}")

# --- Count Robotics duplicates at both thresholds ---
robotics = [e for e in corpus_entries if e.get("category", "") == "Robotics"]
robotics_findings = [e.get("finding", "").strip() for e in robotics]

pairs_035 = 0
pairs_045 = 0
n = len(robotics_findings)

for i in range(n):
    for j in range(i + 1, n):
        sim = get_similarity(robotics_findings[i], robotics_findings[j], idf)
        if sim >= NEW_THRESHOLD:
            pairs_035 += 1
        if sim >= OLD_THRESHOLD:
            pairs_045 += 1

print()
print("=" * 64)
print("ROBOTICS CATEGORY DUPLICATE PAIR COUNTS")
print("=" * 64)
print(f"Total Robotics entries       : {n}")
print(f"Duplicate pairs at 0.45 (old): {pairs_045}")
print(f"Duplicate pairs at 0.35 (new): {pairs_035}")
print(f"Additional pairs caught      : {pairs_035 - pairs_045}")
