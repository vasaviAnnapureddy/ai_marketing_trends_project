# scripts/better_topics.py
import os
import re
import json
import time
import numpy as np
import pandas as pd
from typing import List, Tuple

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import TruncatedSVD
from sklearn.cluster import MiniBatchKMeans
from sklearn.metrics.pairwise import cosine_distances

from cleantext import clean
from sheets_utils import get_all_rows, write_rows

# ----------------- Config from .env -----------------
TARGET_TOPICS  = int(os.getenv("TARGET_TOPICS", "30"))
MIN_SIZE       = int(os.getenv("TOPIC_MIN_SIZE", "5"))
TOP_N_WORDS    = int(os.getenv("TOPIC_LABEL_WORDS", "6"))
SOURCES        = os.getenv("TOPIC_SOURCES", "RAW_YOUTUBE,RAW_REDDIT").split(",")
TOPICS_SHEET   = os.getenv("TOPICS_SHEET", "TOPICS")
SUMMARY_SHEET  = os.getenv("TOPICS_SUMMARY_SHEET", "TOPICS_SUMMARY")
TFIDF_MIN_DF   = int(os.getenv("TFIDF_MIN_DF", "1"))

# ----------------- Helpers -----------------
URL_RE   = re.compile(r"https?://\S+|www\.\S+", re.I)
WS_RE    = re.compile(r"\s+")
PUNCT_RE = re.compile(r"[^\w\s]")

EXTRA_STOP = set("""
just like people think time make made really know got going much many also one get even good well
thing things want see look little lot ive im youre hes shes theyre cant dont wont didnt isnt wasnt
video channel watch show episode guy guys hey oh hmm uh huh lol lmao omg stuff kind sorta sorta
""".split())

BRAND_HINTS = {
    "google":"Google", "apple":"Apple", "meta":"Meta", "facebook":"Facebook", "instagram":"Instagram",
    "tiktok":"TikTok", "youtube":"YouTube", "linkedin":"LinkedIn", "openai":"OpenAI", "microsoft":"Microsoft",
    "amazon":"Amazon", "x":"Twitter", "twitter":"Twitter", "snap":"Snapchat", "spotify":"Spotify",
    "netflix":"Netflix", "adobe":"Adobe"
}
VERTICAL_HINTS = {
    "music":"Music", "song":"Music", "artist":"Music",
    "pricing":"Pricing", "price":"Pricing",
    "ads":"Advertising", "ad":"Advertising", "cpc":"Advertising", "campaign":"Advertising",
    "ux":"UX", "ui":"UX", "design":"Design",
    "seo":"SEO", "search":"SEO",
    "product":"Product", "feature":"Product",
    "brand":"Branding", "branding":"Branding",
    "marketing":"Marketing", "market":"Marketing",
    "ai":"AI", "llm":"AI", "model":"AI",
    "video":"Video", "shorts":"Video", "reel":"Video",
    "creator":"Creator", "influencer":"Creator",
    "startup":"Startup", "founder":"Startup"
}

def normalize(text: str) -> str:
    if not isinstance(text, str):
        return ""
    t = text.lower()
    t = URL_RE.sub(" ", t)
    t = clean(t, lower=True, no_line_breaks=True, no_urls=True, no_emails=True,
              no_phone_numbers=True, no_numbers=False, no_punct=False, replace_with_punct=" ")
    t = PUNCT_RE.sub(" ", t)
    t = WS_RE.sub(" ", t).strip()
    return t

def custom_tokenizer(s: str) -> List[str]:
    toks = [w for w in WS_RE.split(s) if w]
    return [w for w in toks if w not in EXTRA_STOP and len(w) > 2 and not w.isnumeric()]

def _brand_vertical_label(top_terms: List[str]) -> str:
    for w in top_terms:
        lw = w.lower()
        if lw in BRAND_HINTS: return BRAND_HINTS[lw]
        if lw in VERTICAL_HINTS: return VERTICAL_HINTS[lw]
    return None

def _label_from_terms(top_terms: List[str]) -> str:
    if not top_terms:
        return "Unknown"
    seed = _brand_vertical_label(top_terms)
    if seed:
        extras = [w for w in top_terms if w.lower() not in BRAND_HINTS and w.lower() not in VERTICAL_HINTS]
        return " / ".join([seed] + extras[:2]) if extras else seed
    return " ".join(top_terms[:3]).title()

def _top_terms_for_cluster(tfidf, feature_names, doc_ids, k=TOP_N_WORDS) -> List[str]:
    if len(doc_ids) == 0:
        return []
    sub = tfidf[doc_ids]
    mean_vec = np.asarray(sub.mean(axis=0)).ravel()
    idx = np.argsort(-mean_vec)[:k*3]
    words = [feature_names[i] for i in idx]
    words = [w for w in words if w not in EXTRA_STOP and len(w) > 2]
    out, seen = [], set()
    for w in words:
        if w not in seen:
            out.append(w); seen.add(w)
        if len(out) >= k:
            break
    return out

def _closest_docs(emb, center, doc_ids, topn=3) -> List[int]:
    X = emb[doc_ids]
    d = cosine_distances(X, center.reshape(1, -1)).ravel()
    rank = np.argsort(d)[:topn]
    return [doc_ids[i] for i in rank]

# ----------------- Main -----------------
def run_for_source(sheet_name: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    rows = get_all_rows(sheet_name) or []
    if not rows:
        return pd.DataFrame(), pd.DataFrame()

    # pull text field (robust to different column names and non-strings)
    docs_raw = []
    for r in rows:
        v = r.get("document") or r.get("text") or r.get("comment") or ""
        if v is None:
            v = ""
        elif not isinstance(v, str):
            try:
                v = str(v)
            except Exception:
                v = ""
        if v and v.strip() and v.strip().lower() != "[removed]":
            docs_raw.append(v)

    if not docs_raw:
        return pd.DataFrame(), pd.DataFrame()

    docs = [normalize(x) for x in docs_raw]

    # Vectorize - pass token_pattern=None when using custom tokenizer to avoid warning
    vec = TfidfVectorizer(
        stop_words="english",
        tokenizer=custom_tokenizer,
        token_pattern=None,
        min_df=TFIDF_MIN_DF,
        max_df=0.98,
        ngram_range=(1, 2),
        max_features=100000
    )
    X = vec.fit_transform(docs)
    feature_names = np.array(vec.get_feature_names_out())

    # LSA embedding (TruncatedSVD)
    n_comp = min(100, max(2, X.shape[1] - 1))
    svd = TruncatedSVD(n_components=n_comp)
    Z = svd.fit_transform(X)

    # Choose number of clusters
    k = min(TARGET_TOPICS, max(2, max(2, X.shape[0] // max(1, MIN_SIZE))))
    km = MiniBatchKMeans(n_clusters=k, random_state=42, batch_size=2048, n_init=10)
    labels = km.fit_predict(Z)

    # collect clusters
    clusters = {}
    for i, lab in enumerate(labels):
        clusters.setdefault(lab, []).append(i)

    topics_rows = []
    kept = []
    for lab, doc_ids in clusters.items():
        if len(doc_ids) < MIN_SIZE:
            continue
        top_terms = _top_terms_for_cluster(X, feature_names, doc_ids, k=TOP_N_WORDS)
        label = _label_from_terms([t.lower() for t in top_terms]) if top_terms else f"topic_{lab}"
        center = Z[doc_ids].mean(axis=0)
        rep_idx = _closest_docs(Z, center, doc_ids, topn=3)
        rep_docs = [docs_raw[j] for j in rep_idx]
        for j in doc_ids:
            topics_rows.append([
                str(lab), 1.0, label, docs_raw[j], sheet_name
            ])
        kept.append((lab, label, len(doc_ids), rep_docs))

    # fallback: if nothing kept, pick biggest clusters
    if not kept and clusters:
        by_size = sorted(((lab, len(ids)) for lab, ids in clusters.items()), key=lambda x: -x[1])[:min(k, 10)]
        topics_rows = []
        kept = []
        for lab, _ in by_size:
            doc_ids = clusters[lab]
            top_terms = _top_terms_for_cluster(X, feature_names, doc_ids, k=TOP_N_WORDS)
            label = _label_from_terms([t.lower() for t in top_terms]) if top_terms else f"topic_{lab}"
            center = Z[doc_ids].mean(axis=0)
            rep_idx = _closest_docs(Z, center, doc_ids, topn=3)
            rep_docs = [docs_raw[j] for j in rep_idx]
            for j in doc_ids:
                topics_rows.append([str(lab), 1.0, label, docs_raw[j], sheet_name])
            kept.append((lab, label, len(doc_ids), rep_docs))

    topics_df = pd.DataFrame(topics_rows, columns=["topic", "topic_prob", "topic_name", "document", "source"])

    sum_rows = []
    for lab, label, count, rep_docs in kept:
        sum_rows.append([lab, count, label, json.dumps(rep_docs, ensure_ascii=False)])
    summary_df = pd.DataFrame(sum_rows, columns=["Topic", "Count", "Name", "Representative_Docs"])

    return topics_df, summary_df

def run_all():
    all_topics = []
    all_summ = []
    for src in SOURCES:
        tdf, sdf = run_for_source(src)
        if not tdf.empty:
            all_topics.append(tdf)
        if not sdf.empty:
            all_summ.append(sdf)

    if not all_topics and not all_summ:
        print("[better_topics] nothing to write.")
        return

    if all_topics:
        tdf = pd.concat(all_topics, ignore_index=True)
        write_rows(TOPICS_SHEET, list(tdf.columns), tdf.values.tolist())
        print(f"[better_topics] wrote {len(tdf)} rows to {TOPICS_SHEET}")

    if all_summ:
        sdf = pd.concat(all_summ, ignore_index=True)
        sdf = sdf.sort_values("Count", ascending=False)
        write_rows(SUMMARY_SHEET, list(sdf.columns), sdf.values.tolist())
        print(f"[better_topics] wrote {len(sdf)} rows to {SUMMARY_SHEET}")

if __name__ == "__main__":
    run_all()


'''# scripts/better_topics.py
import os, re, math, json, random
import numpy as np
import pandas as pd

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import TruncatedSVD
from sklearn.cluster import MiniBatchKMeans
from sklearn.metrics.pairwise import cosine_distances

from cleantext import clean

from sheets_utils import get_all_rows, write_rows

# ----------------- Config from .env -----------------
TARGET_TOPICS  = int(os.getenv("TARGET_TOPICS", "30"))
MIN_SIZE       = int(os.getenv("TOPIC_MIN_SIZE", "5"))
TOP_N_WORDS    = int(os.getenv("TOPIC_LABEL_WORDS", "6"))
SOURCES        = ["RAW_YOUTUBE", "RAW_REDDIT"]
TOPICS_SHEET   = os.getenv("TOPICS_SHEET", "TOPICS")
SUMMARY_SHEET  = os.getenv("TOPICS_SUMMARY_SHEET", "TOPICS_SUMMARY")

# ----------------- Helpers -----------------
URL_RE   = re.compile(r"https?://\S+|www\.\S+", re.I)
WS_RE    = re.compile(r"\s+")
PUNCT_RE = re.compile(r"[^\w\s]")

# Common filler you saw (“just, like, people …”) kept out of labels
EXTRA_STOP = set("""
just like people think time make made really know got going much many also one get even good well
thing things want see look little lot ive im youre hes shes theyre cant dont wont didnt isnt wasnt
video channel watch show episode guy guys hey oh hmm uh huh lol lmao omg stuff kind sorta sorta
""".split())

BRAND_HINTS = {
    "google":"Google", "apple":"Apple", "meta":"Meta", "facebook":"Facebook", "instagram":"Instagram",
    "tiktok":"TikTok", "youtube":"YouTube", "linkedin":"LinkedIn", "openai":"OpenAI", "microsoft":"Microsoft",
    "amazon":"Amazon", "x":"Twitter", "twitter":"Twitter", "snap":"Snapchat", "spotify":"Spotify",
    "netflix":"Netflix", "adobe":"Adobe"
}
VERTICAL_HINTS = {
    "music":"Music", "song":"Music", "artist":"Music",
    "pricing":"Pricing", "price":"Pricing",
    "ads":"Advertising", "ad":"Advertising", "cpc":"Advertising", "campaign":"Advertising",
    "ux":"UX", "ui":"UX", "design":"Design",
    "seo":"SEO", "search":"SEO",
    "product":"Product", "feature":"Product",
    "brand":"Branding", "branding":"Branding",
    "marketing":"Marketing", "market":"Marketing",
    "ai":"AI", "llm":"AI", "model":"AI",
    "video":"Video", "shorts":"Video", "reel":"Video",
    "creator":"Creator", "influencer":"Creator",
    "startup":"Startup", "founder":"Startup"
}

def normalize(text: str) -> str:
    if not isinstance(text, str):
        return ""
    t = text.lower()
    t = URL_RE.sub(" ", t)
    t = clean(t, lower=True, no_line_breaks=True, no_urls=True, no_emails=True,
              no_phone_numbers=True, no_numbers=False, no_punct=False, replace_with_punct=" ")
    t = PUNCT_RE.sub(" ", t)
    t = WS_RE.sub(" ", t).strip()
    return t

def custom_tokenizer(s: str):
    # tokenizer that removes extra stop tokens from labels
    toks = [w for w in WS_RE.split(s) if w]
    return [w for w in toks if w not in EXTRA_STOP and len(w) > 2 and not w.isnumeric()]

def _brand_vertical_label(top_terms):
    # If any brand/vertical appears in top terms, use that as label seed
    for w in top_terms:
        if w in BRAND_HINTS: return BRAND_HINTS[w]
        if w in VERTICAL_HINTS: return VERTICAL_HINTS[w]
    return None

def _label_from_terms(top_terms):
    seed = _brand_vertical_label(top_terms)
    if seed:
        # Append 2 more informative words after seed
        extras = [w for w in top_terms if w.lower() not in BRAND_HINTS and w.lower() not in VERTICAL_HINTS]
        return " / ".join([seed] + extras[:2]) if extras else seed
    return " ".join(top_terms[:3]).title()

def _top_terms_for_cluster(tfidf, feature_names, doc_ids, k=TOP_N_WORDS):
    # mean tf-idf vector for documents in cluster
    if len(doc_ids) == 0: return []
    sub = tfidf[doc_ids]
    mean_vec = np.asarray(sub.mean(axis=0)).ravel()
    idx = np.argsort(-mean_vec)[:k*3]  # take more, we’ll filter
    words = [feature_names[i] for i in idx]
    # remove extras again for safety
    words = [w for w in words if w not in EXTRA_STOP and len(w) > 2]
    # de-dup while preserving order
    out, seen = [], set()
    for w in words:
        if w not in seen:
            out.append(w); seen.add(w)
        if len(out) >= k: break
    return out

def _closest_docs(emb, center, doc_ids, topn=3):
    X = emb[doc_ids]
    d = cosine_distances(X, center.reshape(1,-1)).ravel()
    rank = np.argsort(d)[:topn]
    return [doc_ids[i] for i in rank]

# ----------------- Main -----------------
def run_for_source(sheet_name: str):
    rows = get_all_rows(sheet_name)
    if not rows:
        return pd.DataFrame(), pd.DataFrame()

     # pull text field (your sheets usually have 'document' or 'text' column)
    docs_raw = []
    for r in rows:
        # get candidate value from common fields
        v = r.get("document") or r.get("text") or r.get("comment") or ""
        # coerce to string safely (handles floats / numbers / None)
        if v is None:
            v = ""
        else:
            # if it's not a string, convert to string; this avoids AttributeError on .strip()
            if not isinstance(v, str):
                try:
                    v = str(v)
                except Exception:
                    v = ""
        # now trim and filter out removed/empty
        if v and v.strip() and v.strip().lower() != "[removed]":
            docs_raw.append(v)
        if not docs_raw:
            return pd.DataFrame(), pd.DataFrame()

    docs = [normalize(x) for x in docs_raw]

    # Vectorize
    vec = TfidfVectorizer(
        stop_words="english",
        tokenizer=custom_tokenizer,
        min_df=1,          # safe lower bound
        max_df=0.98,       # must be > min_df; avoids the previous conflict
        ngram_range=(1,2),
        max_features=100000
    )
    X = vec.fit_transform(docs)
    feature_names = np.array(vec.get_feature_names_out())

    # Embed with LSA (fast, CPU)
    svd = TruncatedSVD(n_components=min(100, X.shape[1]-1))
    Z = svd.fit_transform(X)  # dense 2D array

    # Cluster
    k = min(TARGET_TOPICS, max(2, X.shape[0] // MIN_SIZE))  # don’t over-ask
    km = MiniBatchKMeans(n_clusters=k, random_state=42, batch_size=2048, n_init=10)
    labels = km.fit_predict(Z)

    # Build topic rows
    topics_rows = []
    clusters = {}
    for i, lab in enumerate(labels):
        clusters.setdefault(lab, []).append(i)

    # Filter by min size AND generate labels
    kept = []
    for lab, doc_ids in clusters.items():
        if len(doc_ids) < MIN_SIZE:
            continue
        top_terms = _top_terms_for_cluster(X, feature_names, doc_ids, k=TOP_N_WORDS)
        label = _label_from_terms([t.lower() for t in top_terms]) if top_terms else f"topic_{lab}"
        # representative docs via centroid in Z-space
        center = Z[doc_ids].mean(axis=0)
        rep_idx = _closest_docs(Z, center, doc_ids, topn=3)
        rep_docs = [docs_raw[j] for j in rep_idx]

        # Write each member row for TOPICS sheet
        for j in doc_ids:
            topics_rows.append([
                str(lab), 1.0, label, docs_raw[j], sheet_name  # topic, topic_prob, topic_name, document, source
            ])
        kept.append((lab, label, len(doc_ids), rep_docs))

    # If everything was filtered out, relax threshold once (fallback)
    if not kept:
        # Pick top clusters by size
        by_size = sorted(((lab, len(ids)) for lab, ids in clusters.items()), key=lambda x: -x[1])[:min(k,10)]
        topics_rows = []
        kept = []
        for lab, _ in by_size:
            doc_ids = clusters[lab]
            top_terms = _top_terms_for_cluster(X, feature_names, doc_ids, k=TOP_N_WORDS)
            label = _label_from_terms([t.lower() for t in top_terms]) if top_terms else f"topic_{lab}"
            center = Z[doc_ids].mean(axis=0)
            rep_idx = _closest_docs(Z, center, doc_ids, topn=3)
            rep_docs = [docs_raw[j] for j in rep_idx]
            for j in doc_ids:
                topics_rows.append([str(lab), 1.0, label, docs_raw[j], sheet_name])
            kept.append((lab, label, len(doc_ids), rep_docs))

    topics_df = pd.DataFrame(topics_rows, columns=["topic","topic_prob","topic_name","document","source"])

    # Summary
    sum_rows = []
    for lab, label, count, rep_docs in kept:
        sum_rows.append([lab, count, label, json.dumps(rep_docs, ensure_ascii=False)])
    summary_df = pd.DataFrame(sum_rows, columns=["Topic","Count","Name","Representative_Docs"])

    return topics_df, summary_df

def run_all():
    all_topics = []
    all_summ = []
    for src in SOURCES:
        tdf, sdf = run_for_source(src)
        if not tdf.empty:
            all_topics.append(tdf)
        if not sdf.empty:
            all_summ.append(sdf)

    if not all_topics and not all_summ:
        print("[better_topics] nothing to write.")
        return

    if all_topics:
        tdf = pd.concat(all_topics, ignore_index=True)
        write_rows(TOPICS_SHEET, list(tdf.columns), tdf.values.tolist())
        print(f"[better_topics] wrote {len(tdf)} rows to {TOPICS_SHEET}")

    if all_summ:
        sdf = pd.concat(all_summ, ignore_index=True)
        # sort by Count desc
        sdf = sdf.sort_values("Count", ascending=False)
        write_rows(SUMMARY_SHEET, list(sdf.columns), sdf.values.tolist())
        print(f"[better_topics] wrote {len(sdf)} rows to {SUMMARY_SHEET}")

if __name__ == "__main__":
    run_all()
'''