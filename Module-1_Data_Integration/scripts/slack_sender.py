# scripts/slack_sender.py
import os
import requests
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from pathlib import Path
from typing import Optional
from sheets_utils import get_all_rows


WEBHOOK = os.getenv("SLACK_WEBHOOK", "").strip()
BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN", "").strip()
CHANNEL = os.getenv("SLACK_CHANNEL", "#marketing-bot")

def send_text(msg: str, channel: str = CHANNEL):
    """
    Send plain text to Slack. Prefer webhook if present, otherwise bot token.
    """
    if WEBHOOK:
        try:
            r = requests.post(WEBHOOK, json={"text": msg}, timeout=20)
            if r.status_code >= 300:
                print("[slack_sender] webhook failed", r.status_code, r.text)
        except Exception as e:
            print("[slack_sender] webhook exception:", e)
    elif BOT_TOKEN:
        client = WebClient(token=BOT_TOKEN)
        try:
            client.chat_postMessage(channel=channel, text=msg)
        except SlackApiError as e:
            print("[slack_sender] bot message error:", e.response.get("error"))
    else:
        print("[slack_sender] no webhook or bot token; message follows:\n", msg)

def send_file(channel: str, filepath: str, initial_comment: str = ""):
    """
    Upload a local file to Slack channel. Requires BOT_TOKEN (files:write).
    If BOT_TOKEN missing, fallback to uploading via webhook is not supported: instead post a text note.
    """
    filepath = str(Path(filepath))
    if not Path(filepath).exists():
        print("[slack_sender] file not found:", filepath)
        send_text(f"(Attempted to send file but not found) {filepath}")
        return

    if not BOT_TOKEN:
        # fallback: webhook can't upload file; just post text
        send_text(f"{initial_comment}\n(File saved locally: {filepath})")
        return

    client = WebClient(token=BOT_TOKEN)
    try:
        resp = client.files_upload(channels=channel, file=filepath, initial_comment=initial_comment)
        if not resp["ok"]:
            print("[slack_sender] files_upload returned not ok:", resp)
    except SlackApiError as e:
        print("[slack_sender] files_upload error:", e.response.get("error"))
    except Exception as e:
        print("[slack_sender] upload exception:", e)
        
def send_latest_drafts(limit: int = 10, sheet_name: str = "LLM_DRAFTS"):
    """
    Read last `limit` rows from LLM_DRAFTS and send a compact Slack message for each.
    This mirrors the behaviour expected by run_pipeline.py.
    """
    rows = get_all_rows(sheet_name) or []
    if not rows:
        print("[slack_sender] no LLM drafts found, skipping send_latest_drafts.")
        return

    # try to sort by created_utc if present else keep append order
    try:
        rows = sorted(rows, key=lambda r: r.get("created_utc",""))
    except Exception:
        pass
    to_send = rows[-limit:]

    for r in to_send:
        topic = r.get("topic_name") or r.get("topic") or "Topic"
        cap = r.get("caption", "") or r.get("raw_text", "")[:240]
        cta = r.get("cta", "")
        src = r.get("platform", "generic")
        msg = f"*{topic}* — _{src}_\n• Caption: {cap}\n• CTA: {cta}"
        send_text(msg)
    print(f"[slack_sender] send_latest_drafts: sent {len(to_send)} drafts.")


'''import os, json
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


'''
'''
# slack_sender.py
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