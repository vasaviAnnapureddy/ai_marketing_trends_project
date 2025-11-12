import os, time, uuid, json
from datetime import datetime
import requests
from sheets_utils import get_all_rows, write_rows

GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL   = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")
TOPICS_SUMMARY_SHEET = os.getenv("TOPICS_SUMMARY_SHEET", "TOPICS_SUMMARY")
LLM_OUTPUT_SHEET     = os.getenv("LLM_OUTPUT_SHEET", "LLM_DRAFTS")
TEMP  = float(os.getenv("LLM_TEMPERATURE", "0.7"))
TOP_P = float(os.getenv("LLM_TOP_P", "0.95"))
MAX_TOK = int(os.getenv("LLM_MAX_NEW_TOKENS", "180"))

DEFAULT_PLATFORM = os.getenv("PLATFORM", "generic")
DEFAULT_SENTIMENT = os.getenv("SENTIMENT_HINT", "neutral")

API_URL = "https://api.groq.com/openai/v1/chat/completions"
HDRS    = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}

def now_iso():
    return datetime.utcnow().isoformat() + "Z"

def build_prompt(topic_name: str, rep_docs: list[str], sentiment: str, platform: str):
    sample = " | ".join(rep_docs[:3]) if rep_docs else ""
    return (
        "You are a marketing copywriter.\n"
        f"Goal: Create one short social caption (<=50 words) + a 1-line CTA for platform={platform}.\n"
        f"Tone should match sentiment: {sentiment}.\n"
        f"Topic name: {topic_name}\n"
        f"Representative audience comments (signal): {sample}\n\n"
        "Return strictly in the format: caption || CTA"
    )

def groq_generate(prompt: str, timeout=45):
    if not GROQ_API_KEY:
        raise RuntimeError("Missing GROQ_API_KEY in .env")
    payload = {
        "model": GROQ_MODEL,
        "messages": [{"role":"user","content": prompt}],
        "temperature": TEMP,
        "top_p": TOP_P,
        "max_tokens": MAX_TOK
    }
    r = requests.post(API_URL, headers=HDRS, json=payload, timeout=timeout)
    if r.status_code != 200:
        raise RuntimeError(f"Groq API error {r.status_code}: {r.text}")
    data = r.json()
    text = data["choices"][0]["message"]["content"].strip()
    return text

def split_caption_cta(text: str):
    if "||" in text:
        a, b = text.split("||", 1)
        return a.strip().strip('"'), b.strip().strip('"')
    # fallback: first line caption, second CTA
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    if len(lines) >= 2:
        return lines[0], lines[1]
    return text, ""

def run_groq_llm_writer():
    print("[api_llm_writer] reading topics from:", TOPICS_SUMMARY_SHEET)
    rows = get_all_rows(TOPICS_SUMMARY_SHEET)
    if not rows:
        print("[api_llm_writer] no topics found, abort.")
        return

    out = []
    for row in rows:
        topic_id = str(row.get("Topic", ""))
        topic_name = row.get("Name") or ""
        rep = row.get("Representative_Docs") or "[]"
        try:
            rep_docs = json.loads(rep) if isinstance(rep, str) and rep.strip().startswith("[") else []
        except Exception:
            rep_docs = []
        sentiment = row.get("sentiment_hint") or DEFAULT_SENTIMENT
        platform  = row.get("platform") or DEFAULT_PLATFORM

        prompt = build_prompt(topic_name, rep_docs, sentiment, platform)

        # retry up to 3 times
        txt = ""
        for attempt in range(3):
            try:
                txt = groq_generate(prompt)
                break
            except Exception as e:
                print(f"[api_llm_writer] gen error topic {topic_id} attempt {attempt+1}: {e}")
                time.sleep(1.2 * (attempt+1))
        if not txt:
            continue

        caption, cta = split_caption_cta(txt)
        vid = f"{topic_id}_{uuid.uuid4().hex[:6]}"

        out.append([
            topic_id,               # topic
            topic_name,             # topic_name
            topic_id,               # topic_group
            vid,                    # variant_id
            0,                      # variant_index
            rep_docs[0] if rep_docs else "",   # representative_doc
            sentiment,              # sentiment_hint
            platform,               # platform
            caption,                # caption
            cta,                    # cta
            txt,                    # raw_text
            len(txt),               # raw_length
            now_iso()               # created_utc
        ])
        time.sleep(0.1)

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
