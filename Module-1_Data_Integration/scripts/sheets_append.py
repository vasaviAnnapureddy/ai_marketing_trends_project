import os
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
load_dotenv()

def append_rows(rows):
    sheet_id = os.getenv("GSHEET_ID")
    sa_path  = os.getenv("GOOGLE_SA_JSON_PATH")
    creds = Credentials.from_service_account_file(sa_path, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    svc = build("sheets", "v4", credentials=creds)
    # Use A1 to avoid "A:Z" parse issues
    svc.spreadsheets().values().append(
        spreadsheetId=sheet_id,
        range="KPIs!A1",
        valueInputOption="USER_ENTERED",
        body={"values": rows}
    ).execute()
