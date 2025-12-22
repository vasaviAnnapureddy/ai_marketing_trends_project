from sheets_utils import get_all_rows, write_rows

IN_SHEET = "ALL_COMMENTS"
OUT_SHEET = "VARIANTS"

TEMPLATES = [
    "Key take: {snippet}",
    "People say: \"{snippet}\" — this made us think about [topic]",
    "Hot take: {snippet} — what do you think?",
    "Short: {snippet}"
]

def snippet(text, limit=120):
    # ensure text is a string (avoid TypeError when text is float/None)
    try:
        text = "" if text is None else str(text)
    except Exception:
        text = ""
    return (text[:limit].strip() + "...") if len(text) > limit else text

def main():
    rows = get_all_rows(IN_SHEET)
    out = []
    for r in rows:
        txt = r.get("comment") or r.get("text") or ""
        sn = snippet(txt, limit=100)
        for i, t in enumerate(TEMPLATES):
            out.append({
                "variant_id": f"{r.get('comment_id','')}_{i}",
                "original_comment": txt,
                "variant_text": t.format(snippet=sn),
                "source": r.get("source","")
            })
    header = list(out[0].keys()) if out else ["variant_id","original_comment","variant_text","source"]
    rows_out = [[o[h] for h in header] for o in out]
    write_rows(OUT_SHEET, header, rows_out)
    print(f"Wrote {len(rows_out)} variants")

if __name__ == "__main__":
    main()
