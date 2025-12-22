# scripts/metrics_analyzer.py
"""
Compute simple metrics from ALL_COMMENTS / SENTIMENT and post a short Slack summary.
This version is defensive about the slack_sender API: it tries several common names
and falls back to a safe no-op if none exist.
"""
import pandas as pd
from datetime import datetime
from sheets_utils import append_row, get_all_rows

# Defensive import: accept multiple function names from slack_sender
def _load_slack_send():
    import importlib
    try:
        mod = importlib.import_module("slack_sender")
    except Exception as e:
        print("[metrics_analyzer] slack_sender import failed:", e)
        return None

    # candidate function names (common in this repo)
    for name in ("send_to_slack", "send_text", "send_slack_message", "send_latest_drafts", "send_slack"):
        fn = getattr(mod, name, None)
        if callable(fn):
            print(f"[metrics_analyzer] using slack_sender.{name}()")
            return fn

    print("[metrics_analyzer] no usable function found in slack_sender; Slack sends will be disabled.")
    return None

_send_slack = _load_slack_send()

def _safe_send(msg):
    if not _send_slack:
        print("[metrics_analyzer] (no Slack) ", msg)
        return
    try:
        # many send functions accept just text; others accept (channel, text)
        try:
            _send_slack(msg)
        except TypeError:
            # try channel + text (use env default channel)
            import os
            channel = os.getenv("SLACK_CHANNEL", "#marketing-bot")
            try:
                _send_slack(channel, msg)
            except TypeError:
                # try kwargs style
                try:
                    _send_slack(text=msg)
                except Exception:
                    print("[metrics_analyzer] Slack call failed with unexpected signature.")
    except Exception as e:
        print("[metrics_analyzer] Slack send error:", e)

def compute_metrics():
    # read merged comments (ALL_COMMENTS) or fallback to a named sheet
    rows = get_all_rows("ALL_COMMENTS") or get_all_rows("Sheet1") or []
    df = pd.DataFrame(rows)
    total = len(df)
    if total == 0:
        print("No data")
        return

    # average comment length
    if "comment" in df.columns:
        avg_len = df['comment'].astype(str).apply(len).mean()
    else:
        avg_len = 0.0

    # detect sentiment column name robustly
    senti_col = None
    for cand in ("sentiment_label", "sentiment", "label"):
        if cand in df.columns:
            senti_col = cand
            break

    if senti_col is not None:
        pos = int((df[senti_col].astype(str) == "positive").sum())
        neg = int((df[senti_col].astype(str) == "negative").sum())
    else:
        pos = neg = 0

    neu = total - pos - neg

    # prefer METRICS_WEEKLY but fallback to KPI sheet name
    target_sheet = "METRICS_WEEKLY"
    row = [datetime.utcnow().isoformat(), total, round(avg_len, 1), pos, neg, neu]
    try:
        append_row(target_sheet, row)
        print(f"Appended metrics to {target_sheet}")
    except Exception as e:
        print(f"Could not append to {target_sheet}: {e}. Appending to KPI instead.")
        append_row("KPI", row)

    # Slack summary (no-op if Slack unavailable)
    msg = f"📈 Metrics: total={total} | pos={pos} | neg={neg} | avg_len={round(avg_len,1)}"
    _safe_send(msg)
    print("Metrics written (and Slack attempted if available).")

if __name__ == "__main__":
    compute_metrics()


'''# metrics_analyzer.py
import pandas as pd
from sheets_utils import append_row, get_all_rows
from slack_sender import send_slack_message
from datetime import datetime

def compute_metrics():
    rows = get_all_rows("Sheet1")
    df = pd.DataFrame(rows)
    total = len(df)
    if total == 0:
        print("No data")
        return
    avg_len = df['comment'].astype(str).apply(len).mean()
    pos = (df.get('sentiment','') == 'positive').sum()
    neg = (df.get('sentiment','') == 'negative').sum()
    neu = total - pos - neg
    metrics = [datetime.utcnow().isoformat(), total, round(avg_len,1), int(pos), int(neg), int(neu)]
    append_row("KPI", metrics)
    msg = f"📈 Metrics: total={total}, pos={pos}, neg={neg}, avg_len={round(avg_len,1)}"
    send_slack_message(msg)
    print("Metrics written and posted.")

if __name__ == "__main__":
    compute_metrics()
'''