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


