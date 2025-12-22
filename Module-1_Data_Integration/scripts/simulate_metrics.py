# scripts/simulate_metrics.py
"""
Simulate or auto-generate metrics for logged posts in reports/ab_tests.csv.
Two modes:
 - auto: generate random reasonable replies/likes/ctr
 - heuristic: score based on text length / presence of CTA keywords
"""
import csv, random, math
from pathlib import Path
from argparse import ArgumentParser

LOG = Path("reports/ab_tests.csv")

def read_rows():
    if not LOG.exists():
        print("No log found:", LOG)
        return []
    with LOG.open("r", encoding="utf8") as fh:
        rows = list(csv.DictReader(fh))
    return rows

def write_rows(rows):
    if not rows:
        return
    header = list(rows[0].keys())
    with LOG.open("w", newline="", encoding="utf8") as fh:
        writer = csv.writer(fh)
        writer.writerow(header)
        for r in rows:
            writer.writerow([r.get(h,"") for h in header])

def auto_generate(rows, seed=None):
    random.seed(seed)
    out = []
    for r in rows:
        # skip already scored rows (ctr exists)
        if r.get("ctr"):
            out.append(r); continue
        # base CTR random between 0.5% and 8%
        ctr = round(random.random()*0.07 + 0.005, 4)
        likes = int(max(0, round(random.gauss(10 + ctr*1000, 8))))
        replies = int(max(0, round(random.gauss(2 + ctr*50, 3))))
        r["replies"] = str(replies)
        r["likes"] = str(likes)
        r["ctr"] = str(ctr)
        out.append(r)
    return out

def heuristic_generate(rows):
    out = []
    for r in rows:
        if r.get("ctr"):
            out.append(r); continue
        text = (r.get("variant_text") or "").lower()
        score = 0.01
        if "buy" in text or "cta" in text or "click" in text or "signup" in text:
            score += 0.02
        if len(text) < 80:
            score += 0.01
        if "openai" in text or "ai" in text or "gpt" in text:
            score += 0.015
        # jitter
        ctr = round(min(0.2, max(0.001, score + (random.random()-0.5)*0.01)),4)
        likes = int( max(0, round(ctr*1000 + random.gauss(5,5))) )
        replies = int(max(0, round(ctr*150 + random.gauss(1,2))))
        r["replies"]=str(replies); r["likes"]=str(likes); r["ctr"]=str(ctr)
        out.append(r)
    return out

def main(method="auto", seed=None):
    rows = read_rows()
    if not rows:
        return
    if method=="auto":
        out = auto_generate(rows, seed)
    else:
        out = heuristic_generate(rows)
    write_rows(out)
    print("Updated metrics for", len(out), "rows.")

if __name__=="__main__":
    p = ArgumentParser()
    p.add_argument("--method", choices=("auto","heuristic"), default="auto")
    p.add_argument("--seed", type=int, default=None)
    args = p.parse_args()
    main(method=args.method, seed=args.seed)
