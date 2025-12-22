
# scripts/api_llm_writer.py  (replace existing)
import os, time, uuid, json
from datetime import datetime
import requests
from sheets_utils import get_all_rows, write_rows

# config via .env
API_KEY = os.getenv("GROQ_API_KEY") or os.getenv("OPENAI_API_KEY") or ""
API_URL = os.getenv("GROQ_API_URL", "https://api.groq.com/openai/v1/chat/completions")
MODEL   = os.getenv("GROQ_MODEL", os.getenv("OPENAI_MODEL", "llama-3.1-8b-instant"))
LLM_OUTPUT_SHEET = os.getenv("LLM_OUTPUT_SHEET", "LLM_DRAFTS")
TOPICS_SUMMARY_SHEET = os.getenv("TOPICS_SUMMARY_SHEET", "TOPICS_SUMMARY")
TEMP  = float(os.getenv("LLM_TEMPERATURE", "0.7"))
TOP_P = float(os.getenv("LLM_TOP_P", "0.95"))
MAX_TOK = int(os.getenv("LLM_MAX_NEW_TOKENS", "180"))

HDRS = {"Authorization": f"Bearer {API_KEY}", "Content-Type":"application/json"} if API_KEY else {}

def now_iso():
    return datetime.utcnow().isoformat() + "Z"

def build_prompt(topic_name: str, rep_docs: list, sentiment: str, platform: str):
    sample = " | ".join(rep_docs[:3]) if rep_docs else ""
    return (
        "You are a marketing copywriter.\n"
        "Goal: Create one short social caption (<=50 words) + a 1-line CTA for platform={platform}.\n"
        f"Tone should match sentiment: {sentiment}.\n"
        f"Topic name: {topic_name}\n"
        f"Representative audience comments (signal): {sample}\n\n"
        "Return strictly in the format: caption || CTA"
    )

def call_llm(prompt: str, timeout=30):
    if not API_KEY:
        raise RuntimeError("Missing API key (GROQ/OPENAI). Set GROQ_API_KEY or OPENAI_API_KEY in .env")
    payload = {
        "model": MODEL,
        "messages": [{"role":"user","content": prompt}],
        "temperature": TEMP,
        "top_p": TOP_P,
        "max_tokens": MAX_TOK
    }
    r = requests.post(API_URL, headers=HDRS, json=payload, timeout=timeout)
    if r.status_code != 200:
        raise RuntimeError(f"LLM API error {r.status_code}: {r.text}")
    try:
        data = r.json()
        text = data["choices"][0]["message"]["content"].strip()
        return text
    except Exception as e:
        raise RuntimeError(f"Failed to parse LLM response: {e}")

def split_caption_cta(text: str):
    if not text:
        return "", ""
    if "||" in text:
        a, b = text.split("||", 1)
        return a.strip().strip('"'), b.strip().strip('"')
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    if len(lines) >= 2:
        return lines[0], lines[1]
    return text, ""

def run():
    print("[api_llm_writer] reading topics from", TOPICS_SUMMARY_SHEET)
    rows = get_all_rows(TOPICS_SUMMARY_SHEET) or []
    if not rows:
        print("[api_llm_writer] no topics found, abort.")
        return

    out = []
    for row in rows:
        topic_id = str(row.get("Topic",""))
        topic_name = row.get("Name","") or ""
        rep = row.get("Representative_Docs","[]")
        try:
            rep_docs = json.loads(rep) if isinstance(rep, str) and rep.strip().startswith("[") else []
        except Exception:
            rep_docs = []
        sentiment = row.get("sentiment_hint") or row.get("sentiment","neutral")
        platform  = row.get("platform") or "generic"

        prompt = build_prompt(topic_name, rep_docs, sentiment, platform)
        txt = ""
        for attempt in range(3):
            try:
                txt = call_llm(prompt)
                break
            except Exception as e:
                print(f"[api_llm_writer] LLM error topic {topic_id} attempt {attempt+1}: {e}")
                time.sleep(1.5 * (attempt+1))
        if not txt:
            print(f"[api_llm_writer] skipping topic {topic_id} due to LLM failure.")
            continue
        caption, cta = split_caption_cta(txt)
        vid = f"{topic_id}_{uuid.uuid4().hex[:6]}"
        out.append([topic_id, topic_name, topic_id, vid, 0, rep_docs[0] if rep_docs else "", sentiment, platform, caption, cta, txt, len(txt), now_iso()])
        time.sleep(0.5)  # small throttle

    if out:
        header = ["topic","topic_name","topic_group","variant_id","variant_index","representative_doc","sentiment_hint","platform","caption","cta","raw_text","raw_length","created_utc"]
        write_rows(LLM_OUTPUT_SHEET, header, out)
        print(f"[api_llm_writer] wrote {len(out)} rows to {LLM_OUTPUT_SHEET}")
    else:
        print("[api_llm_writer] nothing generated")

if __name__ == "__main__":
    run()


'''

# scripts/api_llm_writer.py
import os
import time
import uuid
import json
from datetime import datetime
from typing import List

import requests
from sheets_utils import get_all_rows, write_rows

# Config
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "").strip()
GROQ_MODEL   = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")
TOPICS_SUMMARY_SHEET = os.getenv("TOPICS_SUMMARY_SHEET", "TOPICS_SUMMARY")
LLM_OUTPUT_SHEET     = os.getenv("LLM_OUTPUT_SHEET", "LLM_DRAFTS")
TEMP  = float(os.getenv("LLM_TEMPERATURE", "0.7"))
TOP_P = float(os.getenv("LLM_TOP_P", "0.95"))
MAX_TOK = int(os.getenv("LLM_MAX_NEW_TOKENS", "180"))
USE_HEURISTIC_IF_FAIL = os.getenv("LLM_USE_HEURISTIC_FALLBACK", "1") in ("1", "true", "True")

DEFAULT_PLATFORM = os.getenv("PLATFORM", "generic")
DEFAULT_SENTIMENT = os.getenv("SENTIMENT_HINT", "neutral")

API_URL = "https://api.groq.com/openai/v1/chat/completions"
HDRS = {"Content-Type": "application/json"}
if GROQ_API_KEY:
    HDRS["Authorization"] = f"Bearer {GROQ_API_KEY}"

def now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"

def build_prompt(topic_name: str, rep_docs: List[str], sentiment: str, platform: str) -> str:
    sample = " | ".join(rep_docs[:3]) if rep_docs else ""
    return (
        "You are a marketing copywriter.\n"
        f"Goal: Create one short social caption (<=50 words) + a 1-line CTA for platform={platform}.\n"
        f"Tone should match sentiment: {sentiment}.\n"
        f"Topic name: {topic_name}\n"
        f"Representative audience comments (signal): {sample}\n\n"
        "Return strictly in the format: caption || CTA"
    )

def groq_generate(prompt: str, timeout: int = 45, max_attempts: int = 4) -> str:
    if not GROQ_API_KEY:
        raise RuntimeError("Missing GROQ_API_KEY in .env")
    payload = {
        "model": GROQ_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": TEMP,
        "top_p": TOP_P,
        "max_tokens": MAX_TOK
    }
    for attempt in range(1, max_attempts + 1):
        try:
            r = requests.post(API_URL, headers=HDRS, json=payload, timeout=timeout)
        except requests.exceptions.RequestException as e:
            wait = 2 ** attempt
            print(f"[groq_generate] request error (attempt {attempt}): {e}. sleeping {wait}s")
            time.sleep(wait)
            continue
        if r.status_code == 200:
            try:
                data = r.json()
            except ValueError:
                raise RuntimeError("Groq API returned invalid JSON")
            # defensive access
            try:
                choices = data.get("choices") or []
                if not choices:
                    raise RuntimeError("Groq response missing choices")
                text = choices[0].get("message", {}).get("content", "")
                return text.strip()
            except Exception as e:
                raise RuntimeError(f"Groq parse error: {e}")
        elif r.status_code in (429, 503):
            wait = (2 ** attempt) + 0.5 * attempt
            print(f"[groq_generate] rate limit/service {r.status_code}, sleeping {wait}s")
            time.sleep(wait)
            continue
        else:
            # non-retriable error - raise to be handled by caller
            raise RuntimeError(f"Groq API error {r.status_code}: {r.text}")
    raise RuntimeError("Groq API failed after retries")

def split_caption_cta(text: str):
    if not text:
        return "", ""
    if "||" in text:
        a, b = text.split("||", 1)
        return a.strip().strip('"'), b.strip().strip('"')
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    if len(lines) >= 2:
        return lines[0], lines[1]
    return text.strip(), ""

def heuristic_caption_cta(topic_name: str, rep_docs: List[str], sentiment: str, platform: str):
    # simple safe fallback when LLM fails
    snippet = rep_docs[0][:100] if rep_docs else topic_name
    cap = f"{topic_name}: {snippet}"
    cta = "Learn more" if "generic" in platform else "Read more"
    return cap[:240], cta

def run_groq_llm_writer():
    print("[api_llm_writer] reading topics from:", TOPICS_SUMMARY_SHEET)
    rows = get_all_rows(TOPICS_SUMMARY_SHEET) or []
    if not rows:
        print("[api_llm_writer] no topics found, abort.")
        return

    out = []
    for row in rows:
        topic_id = str(row.get("Topic", "") or "")
        topic_name = row.get("Name") or ""
        rep = row.get("Representative_Docs") or "[]"
        try:
            rep_docs = json.loads(rep) if isinstance(rep, str) and rep.strip().startswith("[") else []
        except Exception:
            rep_docs = []
        sentiment = row.get("sentiment_hint") or DEFAULT_SENTIMENT
        platform = row.get("platform") or DEFAULT_PLATFORM

        prompt = build_prompt(topic_name, rep_docs, sentiment, platform)

        txt = ""
        # If GROQ key available, try LLM with retries
        if GROQ_API_KEY:
            try:
                txt = groq_generate(prompt)
            except Exception as e:
                print(f"[api_llm_writer] LLM generation failed for topic {topic_id}: {e}")
                txt = ""
        # fallback to heuristic if allowed
        if not txt and USE_HEURISTIC_IF_FAIL:
            print(f"[api_llm_writer] using heuristic fallback for topic {topic_id}")
            cap, cta = heuristic_caption_cta(topic_name, rep_docs, sentiment, platform)
            raw = f"{cap} || {cta}"
            txt = raw

        if not txt:
            print(f"[api_llm_writer] skipped topic {topic_id} (no output)")
            continue

        caption, cta = split_caption_cta(txt)
        vid = f"{topic_id}_{uuid.uuid4().hex[:6]}"

        out.append([
            topic_id,
            topic_name,
            topic_id,
            vid,
            0,
            rep_docs[0] if rep_docs else "",
            sentiment,
            platform,
            caption,
            cta,
            txt,
            len(txt),
            now_iso()
        ])
        time.sleep(0.5)  # gentle pacing

    if not out:
        print("[api_llm_writer] nothing generated.")
        return

    header = [
        "topic","topic_name","topic_group","variant_id","variant_index",
        "representative_doc","sentiment_hint","platform",
        "caption","cta","raw_text","raw_length","created_utc"
    ]
    print(f"[api_llm_writer] writing {len(out)} rows to {LLM_OUTPUT_SHEET}")
    write_rows(LLM_OUTPUT_SHEET, header, out)
    print("[api_llm_writer] done.")

if __name__ == "__main__":
    run_groq_llm_writer()
'''