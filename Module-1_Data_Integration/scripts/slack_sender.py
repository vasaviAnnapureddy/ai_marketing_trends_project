import os, json
from sheets_utils import get_all_rows
import requests

WEBHOOK = os.getenv("SLACK_WEBHOOK", "")
LLM_OUTPUT_SHEET = os.getenv("LLM_OUTPUT_SHEET", "LLM_DRAFTS")

def send_to_slack(text: str):
    if not WEBHOOK:
        print("[slack_sender] no webhook in .env, skipping.")
        return
    r = requests.post(WEBHOOK, json={"text": text}, timeout=20)
    if r.status_code >= 300:
        print("[slack_sender] error:", r.status_code, r.text)

def send_latest_drafts(limit=10):
    rows = get_all_rows(LLM_OUTPUT_SHEET) or []
    if not rows:
        print("[slack_sender] no rows to send.")
        return
    # take the last N by created_utc (sheet is usually append-only)
    try:
        rows = sorted(rows, key=lambda r: r.get("created_utc",""))
    except Exception:
        pass
    rows = rows[-limit:]

    for r in rows:
        topic = r.get("topic_name") or r.get("topic") or "Topic"
        cap = r.get("caption","")
        cta = r.get("cta","")
        src = r.get("platform","generic")
        msg = f"*{topic}* — _{src}_\n• *Caption:* {cap}\n• *CTA:* {cta}"
        send_to_slack(msg)

if __name__ == "__main__":
    send_latest_drafts()


'''# slack_sender.py
import os, requests
from dotenv import load_dotenv
load_dotenv()

SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK", None)

def send_slack_message(text):
    if not SLACK_WEBHOOK:
        print("No SLACK_WEBHOOK in .env — skipping Slack send")
        return
    payload = {"text": text}
    try:
        r = requests.post(SLACK_WEBHOOK, json=payload, timeout=10)
        if r.status_code != 200:
            print("Slack webhook failed:", r.status_code, r.text)
        else:
            print("Slack message sent.")
    except Exception as e:
        print("Slack send error:", e)
'''