import re, json, datetime as dt
BAD = {"idiot","stupid","trash","worst","hate","scam","cringe"}
SIGNALS = {
 "launch","announcement","preorder","drop","collab","ugc","giveaway","challenge",
 "comment below","subscribe","link in bio","waitlist","cpm","cpc","roas","conversion","retarget",
 "short-form","reel","short","story","carousel","duet","stitch","brand deal","referral","limited time","discount","early access"
}

def is_english(text:str)->bool:
    if not text: return False
    ascii_ratio = sum(1 for ch in text if ord(ch)<128)/max(1,len(text))
    return ascii_ratio > 0.95

def toxicity(text:str)->float:
    t = (text or "").lower()
    return 0.6 if any(w in t for w in BAD) else 0.05

def sentiment(text:str)->float:
    t = (text or "").lower()
    if any(w in t for w in ["great","love","awesome","useful","works"]): return 0.7
    if any(w in t for w in ["hate","worst","useless","broken"]): return -0.6
    return 0.1

def hashtags(text:str):
    return list({m.lower() for m in re.findall(r"#\w+", text or "")})

def marketing_signals(text:str):
    t = (text or "").lower()
    return [s for s in SIGNALS if s in t]

def hours_since(ts_iso:str)->float:
    now = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)
    ts  = dt.datetime.fromisoformat(ts_iso.replace("Z","+00:00"))
    return max(1.0, (now - ts).total_seconds()/3600)

def json_arr(arr): return json.dumps(list(arr), ensure_ascii=False)
def iso(dtobj):   return dtobj.isoformat().replace("+00:00","Z")
