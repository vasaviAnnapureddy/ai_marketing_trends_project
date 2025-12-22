# scripts/sentiment_intent.py (VADER, hardened)
import os
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from sheets_utils import get_all_rows, write_rows  # your helper
import pandas as pd

INPUT_SHEET = os.getenv("INPUT_SHEET", "ALL_COMMENTS")
OUT_SHEET = os.getenv("OUT_SHEET", "SENTIMENT")

analyzer = SentimentIntensityAnalyzer()

def label_from_compound(c):
    # thresholds common: >0.05 positive, <-0.05 negative, else neutral
    if c >= 0.05:
        return "positive"
    if c <= -0.05:
        return "negative"
    return "neutral"

def main():
    rows = get_all_rows(INPUT_SHEET) or []
    out = []
    for r in rows:
        text = r.get("comment") or r.get("text") or ""
        scores = analyzer.polarity_scores(text)
        compound = scores["compound"]
        lab = label_from_compound(compound)
        out_row = {
            "comment": text,
            "neg": scores["neg"],
            "neu": scores["neu"],
            "pos": scores["pos"],
            "compound": compound,
            "sentiment_label": lab,
            "source": r.get("source", "")
        }
        out.append(out_row)

    if not out:
        print("No comments to score.")
        return

    header = list(out[0].keys())
    rows_out = [[o[h] for h in header] for o in out]

    # Try writing to Sheets; fallback to local CSV for quick debugging
    try:
        write_rows(OUT_SHEET, header, rows_out)
        print(f"Wrote {len(rows_out)} sentiment rows to sheet '{OUT_SHEET}'")
    except Exception as e:
        print(f"Warning: write_rows failed ({e}). Saving local CSV for debug.")
        df = pd.DataFrame(out)
        os.makedirs("data", exist_ok=True)
        csv_path = "data/debug_sentiment.csv"
        df.to_csv(csv_path, index=False)
        print(f"Wrote local CSV {csv_path}")

if __name__ == "__main__":
    main()


'''# sentiment_intent.py (VADER)
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from sheets_utils import get_all_rows, write_rows

INPUT_SHEET = "ALL_COMMENTS"
OUT_SHEET = "SENTIMENT"

analyzer = SentimentIntensityAnalyzer()

def label_from_compound(c):
    # thresholds common: >0.05 positive, <-0.05 negative, else neutral
    if c >= 0.05:
        return "positive"
    if c <= -0.05:
        return "negative"
    return "neutral"

def main():
    rows = get_all_rows(INPUT_SHEET)
    out = []
    for r in rows:
        text = r.get("comment") or r.get("text") or ""
        scores = analyzer.polarity_scores(text)
        compound = scores["compound"]
        lab = label_from_compound(compound)
        out_row = {
            "comment": text,
            "neg": scores["neg"],
            "neu": scores["neu"],
            "pos": scores["pos"],
            "compound": compound,
            "sentiment_label": lab,
            "source": r.get("source", "")
        }
        out.append(out_row)
    header = list(out[0].keys()) if out else ["comment","neg","neu","pos","compound","sentiment_label","source"]
    rows_out = [[o[h] for h in header] for o in out]
    write_rows(OUT_SHEET, header, rows_out)
    print(f"Wrote {len(rows_out)} sentiment rows")

if __name__ == "__main__":
    main()
'''