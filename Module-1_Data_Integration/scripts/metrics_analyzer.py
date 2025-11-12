# metrics_analyzer.py
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
