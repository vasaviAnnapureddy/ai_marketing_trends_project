# scripts/post_variants_offline.py
"""
Post variants locally by logging them into reports/ab_tests.csv.
This is the offline alternative to posting to Slack.
"""
import os, csv, uuid, time
from pathlib import Path
from sheets_utils import get_all_rows

LOG = Path("reports/ab_tests.csv")
HEADER = ["post_id","topic","variant_id","variant_text","ts","replies","likes","ctr"]

def ensure_log():
    if not LOG.exists():
        LOG.parent.mkdir(parents=True, exist_ok=True)
        with LOG.open("w", newline="", encoding="utf8") as fh:
            writer = csv.writer(fh)
            writer.writerow(HEADER)

def append_row(row):
    ensure_log()
    with LOG.open("a", newline="", encoding="utf8") as fh:
        writer = csv.writer(fh)
        writer.writerow(row)

def pick_candidates(limit=5):
    # try LLM_DRAFTS then VARIANTS
    for sheet in ("LLM_DRAFTS", "VARIANTS"):
        try:
            rows = get_all_rows(sheet) or []
        except Exception:
            rows = []
        if rows:
            out = []
            for r in rows[:limit]:
                vid = r.get("variant_id") or f"{sheet}_{uuid.uuid4().hex[:6]}"
                txt = (r.get("caption") or r.get("variant_text") or r.get("raw_text") or "")[:800]
                topic = r.get("topic_name") or r.get("topic") or ""
                out.append({"variant_id":str(vid),"topic":topic,"variant_text":txt})
            return out
    return []

def main(limit=5):
    ensure_log()
    cands = pick_candidates(limit=limit)
    if not cands:
        print("No candidates in sheets (LLM_DRAFTS/VARIANTS).")
        return
    for c in cands:
        post_id = f"local_{uuid.uuid4().hex[:8]}"
        ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        row = [post_id, c["topic"], c["variant_id"], c["variant_text"], ts, "", "", ""]
        append_row(row)
        print("Logged variant:", c["variant_id"], "post_id:", post_id)
    print("Done. See", LOG)

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=5)
    args = p.parse_args()
    main(limit=args.limit)
