# merge_to_all_comments.py
"""
Safe merge RAW_YOUTUBE + RAW_REDDIT into ALL_COMMENTS.
Handles differing columns, missing column names, dedup, and writes in one bulk write.
"""
import pandas as pd
from sheets_utils import get_all_rows, write_rows
from pprint import pprint

RAW_SHEETS = [("RAW_YOUTUBE", "youtube"), ("RAW_REDDIT", "reddit")]
OUT_SHEET = "ALL_COMMENTS"

def normalize_df(rows, source_label):
    # rows is list of dicts returned by get_all_rows
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    # unify comment text column names: find likely text column
    text_cands = [c for c in df.columns if c.lower() in ("text", "comment", "comment_text")]
    if text_cands:
        df = df.rename(columns={text_cands[0]: "comment"})
    # ensure source and source columns exist
    df["source"] = source_label
    # Add canonical columns commonly used
    common = ["platform","video_id","post_title","source_url","comment_id","author","comment","likes_or_score","reply_count","created_utc"]
    for c in common:
        if c not in df.columns:
            df[c] = ""
    # keep canonical subset (plus any extra columns)
    return df

def main():
    dfs = []
    total_rows = 0
    for sheet_name, label in RAW_SHEETS:
        rows = get_all_rows(sheet_name)
        print(f"Reading {sheet_name} rows={len(rows)}")
        df = normalize_df(rows, label)
        total_rows += len(df)
        dfs.append(df)
    if not dfs:
        print("No input sheets found.")
        return
    # concat with ignore_index -> avoids reindexing errors
    combined = pd.concat(dfs, ignore_index=True, sort=False)
    before = len(combined)
    # drop exact duplicate comment text + source + comment_id if present
    if "comment" in combined.columns:
        combined = combined.drop_duplicates(subset=["comment","source","comment_id"], keep="first")
    combined = combined.fillna("")
    after = len(combined)
    print(f"Rows: before={before}, after_dedupe={after}")

    # Build header (order important)
    header = list(combined.columns)
    rows_out = combined[header].astype(str).values.tolist()
    # write in one go
    write_rows(OUT_SHEET, header, rows_out)
    print(f"write_rows finished: wrote {len(rows_out)} rows to sheet '{OUT_SHEET}'.")

if __name__ == "__main__":
    main()
