# scripts/merge_to_all_comments.py
"""
Safe merge RAW_YOUTUBE + RAW_REDDIT into ALL_COMMENTS.
Handles differing columns, missing column names, dedupe, keeps rolling window,
and falls back to a local CSV file if Sheets write fails.
"""
import os
import json
import pandas as pd
from pathlib import Path
from datetime import datetime
from sheets_utils import get_all_rows, write_rows
from pprint import pprint

RAW_SHEETS = [("RAW_YOUTUBE", "youtube"), ("RAW_REDDIT", "reddit")]
OUT_SHEET = "ALL_COMMENTS"

def normalize_df(rows, source_label):
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    # normalize likely text column
    text_cands = [c for c in df.columns if c.lower() in ("text", "comment", "comment_text")]
    if text_cands:
        df = df.rename(columns={text_cands[0]: "comment"})
    df["source"] = source_label
    common = ["platform","video_id","post_title","source_url","comment_id","author","comment","likes_or_score","reply_count","created_utc"]
    for c in common:
        if c not in df.columns:
            df[c] = ""
    return df

def keep_last_n_days(df: pd.DataFrame, days: int = 60) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    if "created_utc" not in df.columns:
        return df
    df = df.copy()
    df['created_utc'] = pd.to_datetime(df['created_utc'], errors='coerce', utc=True)
    cutoff = pd.Timestamp.utcnow().tz_convert('UTC') - pd.Timedelta(days=int(days))
    return df[df['created_utc'] >= cutoff]

def main():
    dfs = []
    for sheet_name, label in RAW_SHEETS:
        # prefer local CSV snapshots if Google Sheets RAW_* not present
        csv_path = Path(f"data/raw_{label}.csv")
        if csv_path.exists():
            print(f"[MERGE] Using local CSV {csv_path}")
            df = pd.read_csv(csv_path)
        else:
            print(f"[MERGE] Using Google Sheet RAW_{label.upper()}")
            rows = get_all_rows(sheet_name) or []
            df = normalize_df(rows, label)
        dfs.append(df)
        continue

        print(f"Reading {sheet_name}: rows={len(rows)}")
        df = normalize_df(rows, label)
        dfs.append(df)

    if not dfs:
        print("No input data found in RAW sheets.")
        return

    combined = pd.concat(dfs, ignore_index=True, sort=False)
    before = len(combined)

    # dedupe by comment + source + comment_id if available
    if set(("comment","source","comment_id")).issubset(combined.columns):
        combined = combined.drop_duplicates(subset=["comment","source","comment_id"], keep="first")
    else:
        combined = combined.drop_duplicates(keep="first")

    combined = combined.fillna("")

    # rolling window
    days = int(os.getenv("TIME_WINDOW_DAYS", "60"))
    combined = keep_last_n_days(combined, days=days)

    after = len(combined)
    print(f"Rows: before={before}, after_dedupe_and_window={after}")

    header = list(combined.columns)
    rows_out = combined[header].astype(str).values.tolist()

    # attempt write to Google Sheets; fallback to local CSV
    try:
        write_rows(OUT_SHEET, header, rows_out)
        print(f"Wrote {len(rows_out)} rows to sheet '{OUT_SHEET}'.")
    except Exception as e:
        print(f"[merge_to_all_comments] write_rows failed: {e}")
        print("[merge_to_all_comments] falling back to local CSV at data/debug_all_comments.csv")
        Path("data").mkdir(parents=True, exist_ok=True)
        combined.to_csv("data/debug_all_comments.csv", index=False)
        print("Wrote data/debug_all_comments.csv")

if __name__ == "__main__":
    main()


'''# merge_to_all_comments.py
"""
Safe merge RAW_YOUTUBE + RAW_REDDIT into ALL_COMMENTS.
Handles differing columns, missing column names, dedup, and writes in one bulk write.
"""
import pandas as pd
import os
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
    
    # keep only last TIME_WINDOW_DAYS (60) to implement rolling window
    combined = keep_last_n_days(combined, days=int(os.getenv("TIME_WINDOW_DAYS", 60)))

    # Build header (order important)
    header = list(combined.columns)
    rows_out = combined[header].astype(str).values.tolist()
    
    def keep_last_n_days(df, days=60):
        if "created_utc" not in df.columns:
        return df
    # normalize created_utc to datetime
    df['created_utc'] = pd.to_datetime(df['created_utc'], errors='coerce', utc=True)
    cutoff = pd.Timestamp.utcnow().tz_convert('UTC') - pd.Timedelta(days=days)
    return df[df['created_utc'] >= cutoff]
    # write in one go
    write_rows(OUT_SHEET, header, rows_out)
    print(f"write_rows finished: wrote {len(rows_out)} rows to sheet '{OUT_SHEET}'.")

if __name__ == "__main__":
    main()
'''