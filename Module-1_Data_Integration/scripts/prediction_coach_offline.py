# scripts/prediction_coach_offline.py
"""
Prediction coach:
 - If GROQ_API_KEY available, ask LLM to rank current (unscored) variants.
 - Else run a local heuristic ranking.
Writes predicted winner row to reports/ab_tests.csv if --auto-write.
"""
import os, csv, json, uuid, time
from pathlib import Path
from argparse import ArgumentParser

LOG = Path("reports/ab_tests.csv")

def read_log():
    if not LOG.exists(): return []
    with LOG.open("r", encoding="utf8") as fh:
        return list(csv.DictReader(fh))

def write_prediction(cand):
    # append predicted winner row for traceability
    with LOG.open("a", newline="", encoding="utf8") as fh:
        writer = csv.writer(fh)
        writer.writerow([f"pred_{uuid.uuid4().hex[:8]}", cand.get("topic",""), cand.get("variant_id",""), cand.get("variant_text",""), "", "", "", ""])
    print("Wrote predicted winner:", cand.get("variant_id"))

# Try to import groq from your repo
def try_llm_rank(candidates):
    try:
        from api_llm_writer import groq_generate
    except Exception as e:
        print("No groq wrapper available:", e)
        return None, None

    # build prompt
    hist = read_log()[-8:]
    hist_text = "\n".join([f"{h.get('variant_id')} replies={h.get('replies')} likes={h.get('likes')} ctr={h.get('ctr')}" for h in hist])
    cand_text = "\n".join([f"{c['variant_id']}: {c['variant_text'][:200].replace('\\n',' ')}" for c in candidates])
    prompt = (
        "You are a marketing analyst. Rank the following variants by expected CTR (highest first). "
        "Return a JSON array of objects: {variant_id, score, reason}.\n\n"
        "History:\n" + hist_text + "\n\nCandidates:\n" + cand_text + "\n\nReturn JSON only."
    )
    try:
        raw = groq_generate(prompt)
        # extract JSON from response
        import re
        m = re.search(r'(\[.*\])', raw, flags=re.S)
        json_text = m.group(1) if m else raw
        parsed = json.loads(json_text)
        return parsed, raw
    except Exception as e:
        print("LLM call failed:", e)
        return None, None

def heuristic_rank(candidates):
    scored=[]
    for c in candidates:
        txt = (c.get("variant_text") or "").lower()
        s = 0
        if "buy" in txt or "subscribe" in txt or "click" in txt or "signup" in txt: s+=0.03
        s += min(0.05, len(txt)/2000)
        if "ai" in txt or "openai" in txt or "gpt" in txt: s+=0.02
        scored.append({"variant_id":c["variant_id"], "score":round(s,4), "reason":"heuristic"})
    scored.sort(key=lambda x:-x["score"])
    return scored

def gather_candidates(limit=10):
    try:
        from sheets_utils import get_all_rows
        rows = get_all_rows("LLM_DRAFTS") or []
    except Exception:
        rows = []
    out=[]
    for r in rows[:limit]:
        vid = r.get("variant_id") or f"llm_{uuid.uuid4().hex[:6]}"
        text = (r.get("caption") or r.get("raw_text") or r.get("variant_text") or "")[:600]
        out.append({"variant_id":vid,"variant_text":text,"topic": r.get("topic_name") or r.get("topic","")})
    return out

def main(limit=5, auto_write=False):
    cands = gather_candidates(limit=limit)
    if not cands:
        print("No candidates.")
        return
    parsed, raw = try_llm_rank(cands)
    if not parsed:
        print("LLM not available or failed; using heuristic rank.")
        parsed = heuristic_rank(cands)
    print("Top predictions:")
    for p in parsed[:5]:
        print(p.get("variant_id"), "score:", p.get("score"), "reason:", p.get("reason","")[:120])
    top = parsed[0] if parsed else None
    if top and auto_write:
        # find candidate object
        cand = next((c for c in cands if c["variant_id"]==top["variant_id"]), None)
        if cand:
            write_prediction(cand)

if __name__=="__main__":
    p = ArgumentParser()
    p.add_argument("--limit", type=int, default=5)
    p.add_argument("--auto-write", action="store_true")
    args = p.parse_args()
    main(limit=args.limit, auto_write=args.auto_write)
