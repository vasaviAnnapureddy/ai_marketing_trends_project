import os
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

load_dotenv()
sheet_id = os.getenv("GSHEET_ID")
sa_path  = os.getenv("GOOGLE_SA_JSON_PATH")

if not (sheet_id and sa_path and os.path.exists(sa_path)):
    raise SystemExit("Check GSHEET_ID and GOOGLE_SA_JSON_PATH in .env")

creds = Credentials.from_service_account_file(sa_path, scopes=["https://www.googleapis.com/auth/spreadsheets"])
svc = build("sheets","v4",credentials=creds)

# read the spreadsheet title and first sheet to confirm access
meta = svc.spreadsheets().get(spreadsheetId=sheet_id, includeGridData=False).execute()
print("OK: Connected to Sheet:", meta.get("properties",{}).get("title"))

# append a harmless test row to KPIs
body = {"values":[["TEST_ONLY_DO_NOT_USE","youtube",0,0,"n/a",0,0,"connectivity check"]]}
svc.spreadsheets().values().append(
    spreadsheetId=sheet_id,
    range="KPIs!A:Z",
    valueInputOption="USER_ENTERED",
    body=body
).execute()
print("OK: Wrote a test row to KPIs. You can delete it inside the sheet.")
