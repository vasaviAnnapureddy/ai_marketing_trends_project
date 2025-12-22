# scripts/ab_testing.py
import csv
from pathlib import Path
from datetime import datetime

LOG = Path("reports/ab_tests.csv")

def ensure_log():
    if not LOG.exists():
        LOG.parent.mkdir(parents=True, exist_ok=True)
        with LOG.open("w", newline="", encoding="utf8") as fh:
            writer = csv.writer(fh)
            writer.writerow(["post_id","topic","variant_id","variant_text","ts","replies","likes","ctr"])

def log_variant(post_id: str, topic: str, variant_id: str, variant_text: str):
    ensure_log()
    ts = datetime.utcnow().isoformat() + "Z"
    with LOG.open("a", newline="", encoding="utf8") as fh:
        writer = csv.writer(fh)
        writer.writerow([post_id, topic, variant_id, variant_text, ts, "", "", ""])
    print("[ab_testing] logged variant:", variant_id)

def record_metrics(post_id: str, replies: int = 0, likes: int = 0, ctr: float | None = None):
    ensure_log()
    rows = []
    with LOG.open("r", newline="", encoding="utf8") as fh:
        reader = list(csv.reader(fh))
    header = reader[0]
    for r in reader[1:]:
        if r[0] == post_id:
            r[5] = str(replies)
            r[6] = str(likes)
            r[7] = str(ctr) if ctr is not None else r[7]
        rows.append(r)
    with LOG.open("w", newline="", encoding="utf8") as fh:
        writer = csv.writer(fh)
        writer.writerow(header)
        writer.writerows(rows)
    print("[ab_testing] recorded metrics for", post_id)

def pick_winner_by_ctr():
    ensure_log()
    best = None
    with LOG.open("r", newline="", encoding="utf8") as fh:
        reader = list(csv.DictReader(fh))
    for r in reader:
        try:
            val = float(r.get("ctr") or 0)
        except Exception:
            val = 0
        if best is None or val > best[0]:
            best = (val, r)
    if best:
        print("[ab_testing] winner:", best[1].get("variant_id"), "ctr=", best[0])
        return best[1]
    print("[ab_testing] no winner found")
    return None

if __name__ == "__main__":
    ensure_log()
    print("ab_tests log located at", LOG)
