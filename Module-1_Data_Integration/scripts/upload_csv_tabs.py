# scripts/upload_csv_tabs.py
import os, csv, argparse
from typing import List
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

def get_sheets():
    sheet_id = os.getenv("GSHEET_ID")
    sa_path  = os.getenv("GOOGLE_SA_JSON_PATH")
    if not sheet_id or not sa_path:
        raise SystemExit("Missing GSHEET_ID or GOOGLE_SA_JSON_PATH in .env")
    creds = Credentials.from_service_account_file(
        sa_path,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return sheet_id, build("sheets", "v4", credentials=creds)

def ensure_tab(svc, sheet_id: str, title: str):
    meta = svc.spreadsheets().get(spreadsheetId=sheet_id).execute()
    sheets = {s["properties"]["title"]: s["properties"]["sheetId"] for s in meta.get("sheets", [])}
    if title in sheets:
        return sheets[title]
    # create the tab
    req = {"requests": [{"addSheet": {"properties": {"title": title}}}]}
    resp = svc.spreadsheets().batchUpdate(spreadsheetId=sheet_id, body=req).execute()
    return resp["replies"][0]["addSheet"]["properties"]["sheetId"]

def clear_tab(svc, sheet_id: str, title: str):
    svc.spreadsheets().values().clear(
        spreadsheetId=sheet_id,
        range=f"{title}!A:Z"
    ).execute()

def read_csv(path: str) -> List[List[str]]:
    with open(path, newline="", encoding="utf-8") as f:
        return [row for row in csv.reader(f)]

def upload_csv(path: str, title: str):
    sheet_id, svc = get_sheets()
    ensure_tab(svc, sheet_id, title)
    clear_tab(svc, sheet_id, title)
    rows = read_csv(path)
    if not rows:
        print(f"{title}: CSV empty, skipped.")
        return
    # batch upload in chunks so we do not hit payload limits
    BATCH = 10000
    start_row = 1
    for i in range(0, len(rows), BATCH):
        chunk = rows[i:i+BATCH]
        end_row = start_row + len(chunk) - 1
        svc.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range=f"{title}!A{start_row}",
            valueInputOption="RAW",
            body={"values": chunk}
        ).execute()
        start_row = end_row + 1
    print(f"Uploaded {len(rows)} rows to tab {title}.")

if __name__ == "__main__":
    import dotenv
    dotenv.load_dotenv()

    parser = argparse.ArgumentParser(description="Upload CSVs to Google Sheets tabs")
    parser.add_argument("--raw-youtube", default="data/raw_youtube.csv")
    parser.add_argument("--raw-reddit",  default="data/raw_reddit.csv")
    parser.add_argument("--clean-all",   default="data/clean_all.csv")
    parser.add_argument("--skip-missing", action="store_true")
    args = parser.parse_args()

    files = [
        (args.raw_youtube, "RAW_YOUTUBE"),
        (args.raw_reddit,  "RAW_REDDIT"),
        (args.clean_all,   "CLEAN_ALL"),
    ]

    for path, tab in files:
        if os.path.exists(path):
            upload_csv(path, tab)
        else:
            if args.skip-missing:
                print(f"Missing {path}, skipped.")
            else:
                print(f"Missing {path}. Create it or pass --skip-missing.")
