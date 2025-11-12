# fix_topics_headers_small.py
from sheets_utils import get_all_rows, write_rows
from pprint import pprint

TOPIC_SHEETS = ["TOPICS_YOUTUBE", "TOPICS_REDDIT"]

# header we expect for topics files
TOPIC_HEADER = ["topic","topic_prob","topic_name","document","original_index","post_title","comment"]

def safe_replace(sheet_name):
    rows = get_all_rows(sheet_name)
    if not rows:
        print(f"{sheet_name} empty, nothing to do.")
        return
    # make list of lists and insert header
    vals = [TOPIC_HEADER]
    # if rows are dicts, try to map keys; otherwise keep as-is
    for r in rows:
        if isinstance(r, dict):
            row = [r.get(h, "") for h in TOPIC_HEADER]
        else:
            row = list(r)
        vals.append(row)
    write_rows(sheet_name, TOPIC_HEADER, vals[1:])
    print(f"[{sheet_name}] header inserted/replaced. rows_written={len(vals)-1}")

if __name__ == "__main__":
    for s in TOPIC_SHEETS:
        safe_replace(s)
