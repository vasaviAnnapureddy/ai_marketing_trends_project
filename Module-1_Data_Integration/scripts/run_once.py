import os, json, datetime as dt, pandas as pd
from scripts.pull_youtube import collect_youtube_raw
from scripts.pull_reddit import collect_reddit_raw
from scripts.clean_and_signals import apply_filters, compute_kpis
from scripts.sheets_append import append_rows


DATA = "data"
os.makedirs(DATA, exist_ok=True)

def main():
    # 1) Pull raw
    yt = collect_youtube_raw()
    rd = collect_reddit_raw()
    yt_path = os.path.join(DATA, "raw_youtube.csv")
    rd_path = os.path.join(DATA, "raw_reddit.csv")
    (yt if not yt.empty else yt.head(0)).to_csv(yt_path, index=False)
    (rd if not rd.empty else rd.head(0)).to_csv(rd_path, index=False)

    # 2) Clean + signals + DQ log
    dq_log=[]
    yt_clean = apply_filters(yt, "YouTube", dq_log)
    rd_clean = apply_filters(rd, "Reddit", dq_log)

    cy_path = os.path.join(DATA, "clean_youtube.csv")
    cr_path = os.path.join(DATA, "clean_reddit.csv")
    ca_path = os.path.join(DATA, "clean_all.csv")
    (yt_clean if not yt_clean.empty else yt_clean.head(0)).to_csv(cy_path, index=False)
    (rd_clean if not rd_clean.empty else rd_clean.head(0)).to_csv(cr_path, index=False)
    all_clean = pd.concat([yt_clean, rd_clean], ignore_index=True) if not yt_clean.empty or not rd_clean.empty else pd.DataFrame()
    (all_clean if not all_clean.empty else pd.DataFrame().head(0)).to_csv(ca_path, index=False)

    # 3) KPIs
    today = dt.datetime.utcnow().date().isoformat()
    yt_k = compute_kpis(yt_clean); rd_k = compute_kpis(rd_clean)
    kpi_rows = [
        [today,"YouTube", yt_k["total"], yt_k["pos"], yt_k["top"], yt_k["top_score"], yt_k["avg_ed"], "week1 one-time"],
        [today,"Reddit",  rd_k["total"], rd_k["pos"], rd_k["top"], rd_k["top_score"], rd_k["avg_ed"], "week1 one-time"],
    ]
    kpi_path = os.path.join(DATA, "kpi.csv")
    pd.DataFrame(kpi_rows, columns=["Date","Platform","TotalComments","PositivityRate","TopTrend","TopTrendScore","AvgEngagementDensity","Notes"]).to_csv(kpi_path, index=False)

    # 4) Append to Google Sheets
    try:
        append_rows(kpi_rows)
        sheets_msg = "Sheets append OK"
    except Exception as e:
        sheets_msg = f"Sheets append failed: {e}"

    # 5) DQ log + manifest
    dq_path = os.path.join(DATA, "data_quality_log.csv")
    pd.DataFrame(dq_log, columns=["platform","reason","count"]).to_csv(dq_path, index=False)
    manifest = {
        "timestamp_utc": dt.datetime.utcnow().isoformat()+"Z",
        "selection": {
            "youtube": "search, top decile by commentCount, ≤2 videos",
            "reddit":  "top(month) across subs, top quartile by score, ≤2 posts"
        },
        "caps": {"max_comments_per_post": 300, "max_posts_per_source": 2},
        "filters": {"window_days": 60, "lang_en_ascii_ratio": ">0.95", "toxicity_max": 0.30, "ed_min": 0.20, "must_have": "≥1 marketing signal OR hashtag"}
    }
    man_path = os.path.join(DATA, "source_manifest.json")
    with open(man_path,"w",encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)

    # concise summary
    print("SUMMARY")
    print(f" - Raw:   {yt_path} rows={len(yt)} | {rd_path} rows={len(rd)}")
    print(f" - Clean: {cy_path} rows={len(yt_clean)} | {cr_path} rows={len(rd_clean)} | {ca_path} rows={len(all_clean)}")
    print(f" - KPI:   {kpi_path} | rows=2 | {sheets_msg}")
    print(f" - Logs:  {dq_path}, {man_path}")

if __name__ == "__main__":
    main()
