# scripts/run_pipeline.py
from better_topics import run_all as build_topics
from api_llm_writer import run_groq_llm_writer
from slack_sender import send_latest_drafts

def run_pipeline():
    # 1) build topics from RAW_* sheets
    build_topics()
    # 2) draft copy with Groq and write to LLM_DRAFTS
    run_groq_llm_writer()
    # 3) alert team on Slack (last 10 drafts)
    send_latest_drafts(limit=10)

if __name__ == "__main__":
    run_pipeline()


'''# scripts/run_pipeline.py
"""
Orchestrates the cleaned, detailed-topic pipeline.
Calls only EXISTING module names to avoid import errors.
- better_topics.run_topics_detailed()  -> writes TOPICS + TOPICS_SUMMARY
- api_llm_writer.run()                 -> writes LLM_DRAFTS
- sentiment_intent (optional)          -> if present, runs sentiment
"""

import importlib

def _safe_import(name):
    try:
        return importlib.import_module(name)
    except Exception as e:
        print(f"[run_pipeline] Optional step '{name}' not available: {e}")
        return None

def main():
    # 1) Detailed topics (YouTube + Reddit)
    print(">> Building detailed topics...")
    bt = _safe_import("better_topics")
    if bt and hasattr(bt, "run_topics_detailed"):
        bt.run_topics_detailed(raw_sheets=["RAW_YOUTUBE","RAW_REDDIT"],
                               topics_sheet="TOPICS",
                               summary_sheet="TOPICS_SUMMARY")
    else:
        print("[run_pipeline] Skipping topics step (module/function missing).")

    # 2) LLM drafts via Groq API (one caption+CTA per topic)
    print(">> Generating LLM drafts...")
    llm = _safe_import("api_llm_writer")
    if llm and hasattr(llm, "run"):
        llm.run()
    else:
        print("[run_pipeline] Skipping LLM step (module/function missing).")

    # 3) Sentiment (optional; keeps your old file name)
    print(">> Running sentiment (optional)...")
    senti = _safe_import("sentiment_intent")
    if senti:
        try:
            # support either main() or run() in your existing file
            if hasattr(senti, "main"):
                senti.main()
            elif hasattr(senti, "run"):
                senti.run()
            else:
                print("[run_pipeline] sentiment_intent has no main()/run(); skipping.")
        except Exception as e:
            print("[run_pipeline] sentiment step failed:", e)

    print(">> Pipeline finished.")

if __name__ == "__main__":
    main()
'''