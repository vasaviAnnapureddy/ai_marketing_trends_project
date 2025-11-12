# bandit.py (epsilon-greedy)
import random
import json
from sheets_utils import get_all_rows, write_rows

VARIANTS_SHEET = "VARIANTS"
SCORE_SHEET = "BANDIT_SCORES"  # store selections and outcomes (optional)

EPSILON = 0.1

def choose_variant():
    variants = get_all_rows(VARIANTS_SHEET)
    # read scores if exists
    scores = get_all_rows(SCORE_SHEET) if True else []
    # build score map
    score_map = {}
    for s in scores:
        vid = s.get("variant_id")
        score_map[vid] = float(s.get("score", 0))

    if random.random() < EPSILON or not score_map:
        # explore
        v = random.choice(variants)
        return v
    # exploit: choose highest score among seen
    best_id = max(score_map, key=lambda k: score_map[k])
    for v in variants:
        if v.get("variant_id") == best_id:
            return v
    # fallback
    return random.choice(variants)

if __name__ == "__main__":
    v = choose_variant()
    print("Chosen:", v.get("variant_id"), v.get("variant_text"))
