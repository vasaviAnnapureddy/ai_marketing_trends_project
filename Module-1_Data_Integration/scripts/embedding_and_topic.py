# embedding_and_topic.py
import json
import numpy as np
from sentence_transformers import SentenceTransformer
from bertopic import BERTopic
from sheets_utils import get_all_rows, write_rows
import os

TOPICS_OUT = "TOPICS"
TOPICS_SUMMARY = "TOPICS_SUMMARY"
EMBED_MODEL = os.getenv("EMBED_MODEL", "all-MiniLM-L6-v2")  # lightweight

def load_docs(sheet_name):
    rows = get_all_rows(sheet_name)
    docs = []
    for r in rows:
        # prefer normalized 'comment' else fallback to 'text' or join first fields
        text = r.get("comment") or r.get("text") or " ".join([str(v) for k, v in list(r.items())[:5]])
        docs.append(text)
    return docs

def safe_topic_name(topic_model, tid):
    try:
        t = topic_model.get_topic(int(tid))
        if not t:
            return "outlier"
        # t is list of (word, score) pairs; produce comma string of top words
        words = [w for w, _ in t[:7]]
        return ", ".join(words)
    except Exception:
        return str(tid)

def run_topics_for(sheet_name, min_topic_size=50):
    print("Running topics for", sheet_name)
    docs = load_docs(sheet_name)
    if not docs:
        print("No docs found for", sheet_name)
        return

    # embeddings
    emb_model = SentenceTransformer(EMBED_MODEL)
    embeddings = emb_model.encode(docs, show_progress_bar=True, batch_size=64)

    # BERTopic: safer defaults
    topic_model = BERTopic(min_topic_size=min_topic_size, nr_topics='auto', verbose=False)
    topics, probs = topic_model.fit_transform(docs, embeddings)

    # prepare TOPICS rows (bulk)
    topic_rows = []
    for t, p, doc in zip(topics, probs, docs):
        # probs may be None or list; take max if list
        prob_val = 0.0
        if isinstance(p, (list, np.ndarray)) and len(p) > 0:
            prob_val = float(np.max(p))
        elif isinstance(p, (float, int)):
            prob_val = float(p)
        topic_name = safe_topic_name(topic_model, t) if t != -1 else "outlier"
        topic_rows.append({
            "topic": int(t),
            "topic_prob": prob_val,
            "topic_name": topic_name,
            "document": doc
        })

    # write TOPICS bulk (header + rows-out)
    header = ["topic", "topic_prob", "topic_name", "document"]
    rows_out = [[r[h] for h in header] for r in topic_rows]
    write_rows(TOPICS_OUT, header, rows_out)
    print(f"Wrote {len(rows_out)} rows to sheet '{TOPICS_OUT}'.")

    # summary (get_topic_info returns pandas DF)
    summary = topic_model.get_topic_info().fillna("")
    header2 = list(summary.columns)
    # convert any list-like cells to JSON strings to avoid Sheets invalid list error
    rows2 = []
    for row in summary.values.tolist():
        clean_row = []
        for v in row:
            if isinstance(v, (list, tuple, np.ndarray)):
                clean_row.append(json.dumps(v))
            else:
                clean_row.append(v if v is not None else "")
        rows2.append(clean_row)

    write_rows(TOPICS_SUMMARY, header2, rows2)
    print("Wrote TOPICS_SUMMARY (rows:", len(rows2), ")")

if __name__ == "__main__":
    # call per-source separately
    run_topics_for("RAW_YOUTUBE")
    run_topics_for("RAW_REDDIT")



'''# embedding_and_topic.py
import json
import numpy as np
from pathlib import Path

from sentence_transformers import SentenceTransformer
from bertopic import BERTopic

from sheets_utils import get_all_rows, write_rows  # your sheets util

# outputs (you can change names if you like)
TOPICS_OUT = "TOPICS"
TOPICS_SUMMARY = "TOPICS_SUMMARY"

EMBED_MODEL = "all-MiniLM-L6-v2"  # light/good default
EMBED_BATCH = 64
MIN_TOPIC_SIZE = 50  # safe default; tune later

def load_docs(sheet_name):
    rows = get_all_rows(sheet_name)
    docs = []
    for r in rows:
        # pick normalized 'comment' first, else 'text', else join first cols
        text = r.get("comment") or r.get("text") or " ".join([str(v) for v in list(r.values())[:5]])
        # ensure string and strip
        text = (text or "").strip()
        docs.append(text)
    return docs

def to_primitive(value):
    """Make values safe for Google Sheets: no lists, only primitives (string/number)."""
    if value is None:
        return ""
    if isinstance(value, (list, tuple, dict, np.ndarray)):
        try:
            return json.dumps(value, ensure_ascii=False)
        except Exception:
            return str(value)
    # numpy scalar case
    try:
        if isinstance(value, (np.integer, np.floating, np.bool_)):
            return value.item()
    except Exception:
        pass
    return value

def run_topics_for(sheet_name):
    print(f"Running topics for {sheet_name}...")
    docs = load_docs(sheet_name)
    if not docs:
        print(f"No docs found in sheet {sheet_name}. Skipping.")
        return

    # Embedding model
    emb_model = SentenceTransformer(EMBED_MODEL)
    print(f"Embedding {len(docs)} documents...")
    embeddings = emb_model.encode(docs, show_progress_bar=True, batch_size=EMBED_BATCH)

    # BERTopic: safer defaults
    topic_model = BERTopic(min_topic_size=MIN_TOPIC_SIZE, nr_topics='auto', verbose=False)
    print("Fitting BERTopic (this may take some time)...")
    # topics: list of topic ids per doc; probs: list/array of probabilities (or None)
    topics, probs = topic_model.fit_transform(docs, embeddings)

    # Prepare TOPICS rows (one row per doc)
    topic_rows = []
    for t, p, doc in zip(topics, probs, docs):
        # topic probability: if probs is array-like take max, else handle None
        topic_prob = 0.0
        if p is None:
            topic_prob = 0.0
        else:
            try:
                # p might be list/ndarray with per-topic probabilities
                topic_prob = float(np.max(p)) if hasattr(p, "__iter__") else float(p)
            except Exception:
                try:
                    topic_prob = float(p)
                except Exception:
                    topic_prob = 0.0

        # safe topic name: get_topic returns list of (word, score) tuples
        name_tokens = []
        if t == -1:
            topic_name = "outlier"
        else:
            try:
                topic_words = topic_model.get_topic(t) or []
                # take top N words; words are tuples (word, score)
                name_tokens = [w for w, s in topic_words][:8]
                topic_name = ", ".join(name_tokens) if name_tokens else str(t)
            except Exception:
                topic_name = str(t)

        row = {
            "topic": str(t),
            "topic_prob": topic_prob,
            "topic_name": topic_name,
            "document": doc
        }
        # ensure all primitive
        safe_row = {k: to_primitive(v) for k, v in row.items()}
        topic_rows.append(safe_row)

    # write topics: convert dict rows -> header + row lists
    if not topic_rows:
        print("No topic rows to write.")
    else:
        header = list(topic_rows[0].keys())
        rows_out = [[r[h] for h in header] for r in topic_rows]
        write_rows(TOPICS_OUT, header, rows_out)
        print(f"Wrote {len(rows_out)} rows to sheet '{TOPICS_OUT}'.")

    # Summary: topic_model.get_topic_info() returns pandas DF
    try:
        summary = topic_model.get_topic_info()
        # convert df to rows but stringify any list-like cells
        header2 = list(summary.columns)
        rows2 = []
        for _, row in summary.iterrows():
            out = []
            for cell in row:
                out.append(to_primitive(cell))
            rows2.append(out)
        write_rows(TOPICS_SUMMARY, header2, rows2)
        print(f"Wrote TOPICS_SUMMARY (rows={len(rows2)})")
    except Exception as e:
        print("Failed to write topics summary:", e)

    print("Finished topics for", sheet_name)

if __name__ == "__main__":
    # run for both raw sheets separately
    run_topics_for("RAW_YOUTUBE")
    run_topics_for("RAW_REDDIT")

'''
