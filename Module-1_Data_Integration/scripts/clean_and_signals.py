import json
from scripts.utils import (
    is_english, toxicity, sentiment, hashtags, marketing_signals, hours_since, json_arr
)


def engagement_density(row):
    return (row["likes_or_score"] + row["reply_count"] + 5*row["creator_heart_or_awards"]) / max(1.0, hours_since(row["created_utc"]))

def apply_filters(df, platform_name, dq_log):
    if df.empty: return df
    df = df[df["text"].astype(str).str.len() > 6].copy()

    df["lang"] = df["text"].apply(lambda t: "en" if is_english(t) else "other")
    dq_log.append((platform_name, "lang_other", int((df["lang"]!="en").sum())))
    df = df[df["lang"]=="en"]

    df["toxicity_score"] = df["text"].apply(toxicity)
    dq_log.append((platform_name, "toxicity_gt_0.30", int((df["toxicity_score"]>0.30).sum())))
    df = df[df["toxicity_score"]<=0.30]

    df["sentiment_score"] = df["text"].apply(sentiment)
    df["hashtags"] = df["text"].apply(hashtags)
    df["marketing_signals"] = df["text"].apply(marketing_signals)
    df["engagement_density"] = df.apply(engagement_density, axis=1)

    dq_log.append((platform_name, "ed_lt_0.20", int((df["engagement_density"]<0.20).sum())))
    df = df[df["engagement_density"]>=0.20]

    keep = (df["marketing_signals"].str.len()>0) | (df["hashtags"].str.len()>0)
    dq_log.append((platform_name, "no_signal_or_hashtag", int((~keep).sum())))
    df = df[keep]

    df["hashtags"] = df["hashtags"].apply(json_arr)
    df["marketing_signals"] = df["marketing_signals"].apply(json_arr)
    return df

def compute_kpis(df):
    if df.empty:
        return dict(total=0, pos=0.0, top="n/a", top_score=0.0, avg_ed=0.0)
    pos = float((df["sentiment_score"]>0.2).mean())
    tmp = df.copy()
    tmp["marketing_signals"] = tmp["marketing_signals"].apply(lambda s: json.loads(s))
    sigs = tmp.explode("marketing_signals").dropna(subset=["marketing_signals"])
    top = sigs["marketing_signals"].value_counts().index[0] if not sigs.empty else "n/a"
    if top != "n/a":
        top_mask = tmp["marketing_signals"].apply(lambda L: top in L)
        top_score = float(tmp.loc[top_mask, "engagement_density"].mean())
    else:
        top_score = 0.0
    avg_ed = float(df["engagement_density"].mean())
    return dict(total=len(df), pos=round(pos,3), top=top, top_score=round(top_score,3), avg_ed=round(avg_ed,3))
