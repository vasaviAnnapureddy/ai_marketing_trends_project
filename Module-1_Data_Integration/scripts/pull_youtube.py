# scripts/pull_youtube.py
import os, requests, datetime as dt, pandas as pd
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("YOUTUBE_API_KEY")

# ----- ENV (kept simple; matches your .env) -----
REGIONS             = [r.strip().upper() for r in os.getenv("YT_REGIONS", "US,GB,CA,AU,IE").split(",") if r.strip()]
TARGET_VIDEOS       = int(os.getenv("YT_MAX_VIDEOS", "80"))
COMMENTS_PER_VIDEO  = int(os.getenv("YT_COMMENTS_PER_VIDEO", "20"))
POPULAR_MAX_PER_CAT = int(os.getenv("YT_POPULAR_MAX_PER_CAT", "80"))
ONLY_ENGLISH        = os.getenv("YT_ONLY_ENGLISH", "1") in ("1","true","True","yes","YES")

# Marketing search (comma-separated list; no inner quotes)
SEARCH_KWS          = [k.strip() for k in os.getenv("YT_SEARCH_KEYWORDS", "").split(",") if k.strip()]
SEARCH_MAX_VIDS_KW  = int(os.getenv("YT_SEARCH_MAX_VIDEOS_PER_KW", "8"))
COMMENTS_PER_SEARCH = int(os.getenv("YT_COMMENTS_PER_SEARCH_VIDEO", "25"))

BASE = "https://www.googleapis.com/youtube/v3"

# Stable categories for mostPopular (avoid the ones that often 404)
YTCATS = {
    "music": 10,
    "entertainment": 24,
    "gaming": 20,
    "news": 25,
    "science_tech": 28,
}

# ---------------- helpers ----------------
def is_english_ascii(text: str, thresh: float = 0.95) -> bool:
    if not text:
        return False
    ascii_ratio = sum(1 for ch in text if ord(ch) < 128) / max(1, len(text))
    return ascii_ratio > thresh

def yt_get(path, params):
    if not API_KEY:
        raise SystemExit("Missing YOUTUBE_API_KEY in .env")
    try:
        r = requests.get(f"{BASE}/{path}", params={**params, "key": API_KEY}, timeout=30)
        r.raise_for_status()
        return r.json()
    except requests.HTTPError:
        try:
            detail = r.json()
        except Exception:
            detail = r.text
        raise SystemExit(f"YT API error {r.status_code} on {path}: {detail}")

# --------------- Stream A: Popular ---------------
def popular_candidates():
    """
    Use low-quota videos.list?chart=mostPopular with part=snippet,statistics.
    Store title/channel/engagement now; never call videos?id=... later.
    """
    seen = {}
    for region in REGIONS:
        for cat_name, cat_id in YTCATS.items():
            token, fetched = None, 0
            while True:
                params = {
                    "part": "snippet,statistics",
                    "chart": "mostPopular",
                    "regionCode": region,
                    "videoCategoryId": cat_id,
                    "maxResults": 50,
                }
                if token:
                    params["pageToken"] = token
                try:
                    data = yt_get("videos", params)
                except SystemExit:
                    break  # quietly skip rare gaps
                for it in data.get("items", []):
                    vid = it.get("id")
                    if not vid or vid in seen:
                        continue
                    sn = it.get("snippet") or {}
                    st = it.get("statistics") or {}
                    seen[vid] = {
                        "stream": "popular",
                        "region": region,
                        "category": cat_name,
                        "title": sn.get("title", ""),
                        "channel_id": sn.get("channelId", ""),
                        "comment_count": int(st.get("commentCount", 0) or 0),
                        "view_count":    int(st.get("viewCount", 0) or 0),
                        "like_count":    int(st.get("likeCount", 0) or 0),
                    }
                    fetched += 1
                    if fetched >= POPULAR_MAX_PER_CAT:
                        break
                if fetched >= POPULAR_MAX_PER_CAT:
                    break
                token = data.get("nextPageToken")
                if not token:
                    break
    return seen  # dict[video_id] -> meta

# --------------- Stream B: Marketing Search ---------------
def search_marketing_videos():
    """
    Use search.list to find brand/product/ads videos.
    We only take snippet and mark as 'marketing' stream.
    """
    if not SEARCH_KWS:
        return {}
    seen = {}
    for kw in SEARCH_KWS:
        token, fetched = None, 0
        while True:
            params = {"part": "snippet", "q": kw, "type": "video", "maxResults": 50}
            if token:
                params["pageToken"] = token
            try:
                data = yt_get("search", params)
            except SystemExit:
                break
            for it in data.get("items", []):
                vid = (it.get("id") or {}).get("videoId")
                if not vid or vid in seen:
                    continue
                sn = it.get("snippet") or {}
                seen[vid] = {
                    "stream": "marketing",
                    "region": "marketing",
                    "category": "marketing",
                    "title": sn.get("title", ""),
                    "channel_id": sn.get("channelId", ""),
                    # optimistic priors so they rank reasonably vs popular:
                    "comment_count": 100,
                    "view_count": 0,
                    "like_count": 0,
                }
                fetched += 1
                if fetched >= SEARCH_MAX_VIDS_KW:
                    break
            if fetched >= SEARCH_MAX_VIDS_KW:
                break
            token = data.get("nextPageToken")
            if not token:
                break
    return seen

# ---------------- selection ----------------
def select_videos(candidates):
    """
    Sort by comment_count desc, cap ≤2 per channel, take up to TARGET_VIDEOS.
    """
    ordered = sorted(
        candidates.items(),
        key=lambda kv: (kv[1].get("comment_count", 0), kv[1].get("view_count", 0)),
        reverse=True,
    )
    picked, per_channel = [], {}
    for vid, meta in ordered:
        ch = meta.get("channel_id", "")
        if ch:
            per_channel[ch] = per_channel.get(ch, 0) + 1
            if per_channel[ch] > 2:
                continue
        picked.append(vid)
        if len(picked) >= TARGET_VIDEOS:
            break
    return picked

# ---------------- comments ----------------
def fetch_comments(video_id, keep_english=True, max_keep=20):
    rows, token, kept = [], None, 0
    while True:
        params = {
            "part": "snippet",
            "videoId": video_id,
            "maxResults": 50,
            "textFormat": "plainText",
            "order": "relevance",
        }
        if token:
            params["pageToken"] = token
        try:
            data = yt_get("commentThreads", params)
        except SystemExit:
            break
        for it in data.get("items", []):
            s  = (it.get("snippet", {}).get("topLevelComment", {}).get("snippet", {}) or {})
            th = (it.get("snippet") or {})
            txt = s.get("textDisplay", "")
            if keep_english and not is_english_ascii(txt):
                continue
            rows.append({
                "platform": "YouTube",
                "video_id": video_id,
                "post_title": "",  # filled from meta below
                "source_url": f"https://www.youtube.com/watch?v={video_id}",
                "comment_id": it.get("id", ""),
                "author": s.get("authorDisplayName", ""),
                "text": txt,
                "likes_or_score": int(s.get("likeCount", 0) or 0),
                "reply_count": int(th.get("totalReplyCount", 0) or 0),
                "creator_heart_or_awards": 0,
                "created_utc": s.get("publishedAt", ""),
                "collected_utc": dt.datetime.utcnow().isoformat("T") + "Z",
            })
            kept += 1
            if kept >= max_keep:
                break
        if kept >= max_keep:
            break
        token = data.get("nextPageToken")
        if not token:
            break
    return rows

# ---------------- orchestrator ----------------
def collect_youtube_raw():
    # Build both streams
    pop = popular_candidates()
    mkt = search_marketing_videos()
    cands = {}
    cands.update(pop)
    cands.update(mkt)
    print(f"[YT] candidates — popular: {len(pop)} | marketing: {len(mkt)} | combined: {len(cands)}")

    selected = select_videos(cands)

    rows = []
    for vid in selected:
        meta = cands.get(vid, {})
        max_keep = COMMENTS_PER_SEARCH if meta.get("stream") == "marketing" else COMMENTS_PER_VIDEO
        coms = fetch_comments(vid, keep_english=ONLY_ENGLISH, max_keep=max_keep)
        for r in coms:
            r["category"] = meta.get("category", "")
            r["region"]   = meta.get("region", "")
            r["post_title"] = meta.get("title", "")
            r["view_count"] = meta.get("view_count", 0)
            r["like_count"] = meta.get("like_count", 0)
            r["channel_subscribers"] = 0  # optional backfill later
            rows.append(r)

    return pd.DataFrame(rows)

# ---------------- CLI ----------------
if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)
    df = collect_youtube_raw()
    out = "data/raw_youtube.csv"
    df.to_csv(out, index=False)
    print("Wrote", out, "| rows=", len(df))
