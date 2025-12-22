# scripts/weekly_report.py
from pathlib import Path
import sys
SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))
import os
from datetime import datetime
from pathlib import Path
import pandas as pd

from reports_utils import date_window, plot_barh, plot_pie
from sheets_utils import get_all_rows, append_row, write_rows
from slack_sender import send_text, send_file

REPORTS_DIR = Path(os.getenv("REPORTS_DIR", "reports"))

# Sheets names used for rollups
TOPICS_SUMMARY_SHEET = os.getenv("TOPICS_SUMMARY_SHEET", "TOPICS_SUMMARY")
SENTIMENT_SHEET = os.getenv("OUT_SHEET", "SENTIMENT")
METRICS_WEEKLY = os.getenv("METRICS_WEEKLY_SHEET", "METRICS_WEEKLY")

def read_topics_between(start, end):
    rows = get_all_rows(TOPICS_SUMMARY_SHEET) or []
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    # assume there is 'created_utc' or similar; otherwise keep all
    if 'created_utc' in df.columns:
        df['created_utc'] = pd.to_datetime(df['created_utc'], errors='coerce', utc=True)
        return df[(df['created_utc'] >= start) & (df['created_utc'] < end)]
    return df

def read_sentiment_between(start, end):
    rows = get_all_rows(SENTIMENT_SHEET) or []
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    if 'created_utc' in df.columns:
        df['created_utc'] = pd.to_datetime(df['created_utc'], errors='coerce', utc=True)
        return df[(df['created_utc'] >= start) & (df['created_utc'] < end)]
    return df

def build_weekly(start, end):
    out_dir = REPORTS_DIR / f"{end.strftime('%Y-Week-%W')}"
    out_dir.mkdir(parents=True, exist_ok=True)

    topics = read_topics_between(start, end)
    sent = read_sentiment_between(start, end)

    # top topics (count)
    top_series = pd.Series(dtype=int)
    if not topics.empty and 'Name' in topics.columns:
        # if your topics summary uses Name / Topic columns adjust here
        name_col = 'Name' if 'Name' in topics.columns else ('topic_name' if 'topic_name' in topics.columns else 'topic')
        top_series = topics[name_col].value_counts().head(20)
    else:
        top_series = pd.Series({}, dtype=int)

    topic_png = out_dir / "topic_top20_bar.png"
    if not top_series.empty:
        plot_barh(top_series, str(topic_png), title="Top 20 Topics (this week)")

    # sentiment pie
    sent_counts = {}
    if not sent.empty and 'sentiment_label' in sent.columns:
        sent_counts = sent['sentiment_label'].value_counts().to_dict()
    else:
        # fallback if sentiment uses 'sentiment' or 'label'
        for c in ('sentiment','sentiment_label','sentiment_label'):
            if c in sent.columns:
                sent_counts = sent[c].value_counts().to_dict()
                break

    sent_png = out_dir / "sentiment_pie.png"
    if sent_counts:
        labels = list(sent_counts.keys())
        sizes = list(sent_counts.values())
        plot_pie(labels, sizes, str(sent_png), title="Sentiment split")

    # Build metrics row
    total_comments = 0
    pos = neg = neu = 0
    if not sent.empty:
        total_comments = len(sent)
        pos = int((sent['sentiment_label'] == 'positive').sum()) if 'sentiment_label' in sent.columns else 0
        neg = int((sent['sentiment_label'] == 'negative').sum()) if 'sentiment_label' in sent.columns else 0
        neu = total_comments - pos - neg

    # top_5 topics string
    top5 = []
    if not top_series.empty:
        top5 = [f"{i} ({c})" for i, c in zip(top_series.index[:5], top_series.values[:5])]

    # metrics row format (customize as you like)
    week_start = start.strftime("%Y-%m-%d")
    row = [week_start, total_comments, pos, neu, neg, "; ".join(top5)]
    # append to sheet as a new weekly row
    try:
        append_row(METRICS_WEEKLY, row)
    except Exception as e:
        print("[weekly_report] append_row failed:", e)

    # Slack message + files
    summary = (
        f"*Weekly snapshot* ({start.date()} → {end.date()})\n"
        f"Total comments: {total_comments} | Pos {pos} | Neu {neu} | Neg {neg}\n"
        f"Top topics: {', '.join([t.split(' (')[0] for t in top5]) if top5 else 'N/A'}"
    )
    send_text(summary)

    # Send generated files to Slack (if present)
    if topic_png.exists():
        send_file(os.getenv("SLACK_CHANNEL", "#marketing-bot"), str(topic_png), initial_comment="Top topics (week)")
    if sent_png.exists():
        send_file(os.getenv("SLACK_CHANNEL", "#marketing-bot"), str(sent_png), initial_comment="Sentiment split (week)")

    print("[weekly_report] done. pngs at:", out_dir)

if __name__ == "__main__":
    # default: last 7 days ending today
    end = pd.Timestamp.utcnow()
    start, end = date_window(days=int(os.getenv("LOOKBACK_DAYS", "7")), end=end)
    build_weekly(start, end)
