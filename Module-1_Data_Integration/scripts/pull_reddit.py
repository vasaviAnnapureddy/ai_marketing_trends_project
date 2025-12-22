# scripts/pull_reddit.py

import os
import warnings
import json
import datetime as dt
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
import praw
from prawcore.exceptions import Forbidden, NotFound, TooManyRequests, RequestException, ResponseException

# Quiet deprecation noise
warnings.filterwarnings("ignore", category=DeprecationWarning)

# Load .env from project root
REPO_ENV = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(dotenv_path=REPO_ENV)

# ---- Env & knobs
CLIENT_ID  = os.getenv("REDDIT_CLIENT_ID")
SECRET     = os.getenv("REDDIT_SECRET")
USERAGENT  = os.getenv("REDDIT_USERAGENT", "marketing-trends/0.1")

TIME_WINDOW_DAYS      = int(os.getenv("TIME_WINDOW_DAYS", "60"))
MAX_POSTS_PER_SOURCE  = int(os.getenv("MAX_POSTS_PER_SOURCE", "2"))
MAX_COMMENTS_PER_POST = int(os.getenv("MAX_COMMENTS_PER_POST", "150"))

# speed knobs (with safe defaults)
TOP_LIMIT       = int(os.getenv("REDDIT_TOP_LIMIT", "40"))
HOT_LIMIT       = int(os.getenv("REDDIT_HOT_LIMIT", "25"))
SEARCH_KW_MAX   = int(os.getenv("REDDIT_SEARCH_KW_MAX", "6"))
SEARCH_LIMIT    = int(os.getenv("REDDIT_SEARCH_LIMIT", "20"))

if not (CLIENT_ID and SECRET):
    raise SystemExit("Missing Reddit CLIENT_ID or SECRET in .env")

# Read-only client (works with praw 7.8+ incl. 8.x)
reddit = praw.Reddit(
    client_id=CLIENT_ID,
    client_secret=SECRET,
    user_agent=USERAGENT
)
reddit.read_only = True

# Subreddits & keywords
SUBS = [
    "marketing", "socialmedia", "advertising",
    "technology", "gadgets", "Entrepreneur",
    "startups", "iphone", "Android", "ecommerce", "DigitalMarketing"
]

KW = [
    "launch","announcement","preorder","drop",
    "discount","giveaway","brand","campaign","ad","promo",
    "iphone","samsung","tesla","laptop","earbuds","smartwatch", "android"
]

def utc_now():
    return dt.datetime.now(dt.timezone.utc)

def is_recent(utc_ts: float) -> bool:
    created = dt.datetime.fromtimestamp(utc_ts, tz=dt.timezone.utc)
    return (utc_now() - created).days <= TIME_WINDOW_DAYS

def pick_posts():
    """
    Gather candidates from top(month), hot, and search, filter by recency,
    select best by adaptive score threshold, and cap per subreddit.
    """
    chosen = []
    for s in SUBS:
        print(f"[reddit] scanning r/{s} …")
        try:
            sub = reddit.subreddit(s)
            cands = []

            # Top-of-month & hot
            cands.extend(list(sub.top(time_filter="month", limit=TOP_LIMIT)))
            cands.extend(list(sub.hot(limit=HOT_LIMIT)))

            # Keyword search
            for q in KW[:SEARCH_KW_MAX]:
                try:
                    cands.extend(list(sub.search(q, sort="relevance", time_filter="month", limit=SEARCH_LIMIT)))
                except Exception:
                    pass

            # window filter
            cands = [p for p in cands if hasattr(p, "created_utc") and is_recent(p.created_utc)]
            if not cands:
                continue

            # adaptive threshold on score; relax if needed
            scores = sorted(int(getattr(p, "score", 0)) for p in cands)
            def thr_at(pct):
                if not scores: return 0
                k = max(0, min(len(scores)-1, int(pct * len(scores)) - 1))
                return scores[k]

            kept_block = []
            for pct in (0.75, 0.6, 0.5, 0.4, 0.3):
                thr = thr_at(pct)
                kept_block = [p for p in cands if int(getattr(p, "score", 0)) >= thr][:MAX_POSTS_PER_SOURCE]
                if kept_block:
                    break

            chosen.extend(kept_block)
        except Exception:
            # subreddit unavailable or rate-limited; just continue
            continue
    return chosen

def fetch_comments(post):
    """
    Fetch up to MAX_COMMENTS_PER_POST recent comments per post.
    Robust to deleted/suspended users and transient API errors.
    """
    out = []
    try:
        post.comments.replace_more(limit=0)
    except Exception:
        return out

    kept = 0
    for c in post.comments.list():
        try:
            created = dt.datetime.fromtimestamp(c.created_utc, tz=dt.timezone.utc)
            if (utc_now() - created).days > TIME_WINDOW_DAYS:
                continue

            # avoid author karma lookups; many authors are deleted/suspended
            author_name = None
            try:
                a = getattr(c, "author", None)
                author_name = getattr(a, "name", None) if a else None
            except Exception:
                author_name = None

            out.append({
                "platform": "reddit",
                "post_id": post.id,
                "comment_id": c.id,
                "created_utc": created.isoformat().replace("+00:00", "Z"),
                "text": c.body or "",
                "likes_or_score": int(getattr(c, "score", 0) or 0),
                "reply_count": len(getattr(c, "replies", [])) if getattr(c, "replies", None) else 0,
                "creator_heart_or_awards": len(getattr(c, "all_awardings", [])) if getattr(c, "all_awardings", None) else 0,
                "author_cred_proxy": 0,   # kept constant to avoid costly author API calls
                "author": author_name,
                "source_url": f"https://reddit.com{getattr(post, 'permalink', '')}",
            })
            kept += 1
            if kept >= MAX_COMMENTS_PER_POST:
                break

        except (Forbidden, NotFound):
            continue
        except (TooManyRequests, ResponseException, RequestException):
            continue
        except Exception:
            continue
    return out

def collect_reddit_raw() -> pd.DataFrame:
    rows = []
    per_sub = {}
    posts = pick_posts()
    for p in posts:
        subname = getattr(getattr(p, "subreddit", None), "display_name", "unknown")
        per_sub[subname] = per_sub.get(subname, 0) + 1
        if per_sub[subname] > MAX_POSTS_PER_SOURCE:
            continue
        rows += fetch_comments(p)
    return pd.DataFrame(rows)

if __name__ == "__main__":
    if __name__ == "__main__":
        os.makedirs("data/raw", exist_ok=True)
    df = collect_reddit_raw()
    # save CSV and jsonl daily snapshot
    out_csv = f"data/raw/raw_reddit_{dt.datetime.utcnow().strftime('%Y-%m-%dT%H%MZ')}.csv"
    out_jsonl = f"data/raw/raw_reddit_{dt.datetime.utcnow().strftime('%Y-%m-%d')}.jsonl"
    df.to_csv(out_csv, index=False)
    # jsonl
    with open(out_jsonl, "w", encoding="utf8") as fh:
        for rec in df.to_dict(orient="records"):
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
    # also write a stable file used by pipeline
    df.to_csv("data/raw_reddit.csv", index=False)
    print(f"Wrote {out_csv} | rows = {len(df)}")

